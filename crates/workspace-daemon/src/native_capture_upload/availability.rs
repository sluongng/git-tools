//! Cached observations of exact remote manifest/DATA, never full backup health.
use super::*;
use serde_json::json;

type Key = ([u8; 16], [u8; 16], u64);
const INTERVAL: Duration = Duration::from_secs(30);
const MAX_AGE: Duration = Duration::from_secs(120);
fn key(ticket: &PendingCapture) -> Key {
    (
        ticket.registration.workspace.0,
        ticket.registration.op_store.0,
        ticket.registration.generation,
    )
}
#[derive(Default)]
pub(super) struct Cache(Mutex<Cached>);
#[derive(Default)]
struct Cached {
    revision: u64,
    entries: BTreeMap<Key, Observation>,
    last_attempt: &'static str,
}
struct Observation {
    checked: tokio::time::Instant,
    state: &'static str,
    reason: &'static str,
}
impl Cache {
    pub(super) fn invalidate(&self) {
        self.fail("selection_changed");
    }
    fn fail(&self, reason: &'static str) {
        if let Ok(mut cache) = self.0.lock() {
            cache.last_attempt = reason;
            cache.revision = cache.revision.saturating_add(1);
            cache.entries.clear();
        }
    }
    fn revision(&self) -> Option<u64> {
        self.0
            .lock()
            .ok()
            .and_then(|c| (c.revision != u64::MAX).then_some(c.revision))
    }
    fn complete(
        &self,
        revision: u64,
        key: Key,
        limit: usize,
        state: &'static str,
        reason: &'static str,
    ) {
        if let Ok(mut cache) = self.0.lock() {
            if revision == u64::MAX || cache.revision != revision {
                return;
            }
            if !cache.entries.contains_key(&key) && cache.entries.len() >= limit {
                return;
            }
            cache.last_attempt = reason;
            cache.entries.insert(
                key,
                Observation {
                    checked: tokio::time::Instant::now(),
                    state,
                    reason,
                },
            );
        }
    }
    pub(super) fn status(&self) -> serde_json::Value {
        let last = self
            .0
            .lock()
            .map(|c| {
                if c.last_attempt.is_empty() {
                    "never"
                } else {
                    c.last_attempt
                }
            })
            .unwrap_or("unavailable");
        let entries: Vec<_> = self
            .0
            .lock()
            .map(|cache| {
                cache
                    .entries
                    .values()
                    .map(|entry| {
                        let age = entry.checked.elapsed();
                        json!({"state":if age >= MAX_AGE {"stale"} else {entry.state},
                "last_attempt":entry.reason,"age_ms":age.as_millis().min(u64::MAX as u128) as u64,
                "selection_current":false})
                    })
                    .collect()
            })
            .unwrap_or_default();
        json!({"scope":"selected_manifest_and_capture_data","max_age_ms":MAX_AGE.as_millis() as u64,
            "state":if entries.is_empty() {"unknown"} else {"observed"}, "last_attempt":last,
            "native_file_closure_verified":false,"selected_capsules":entries})
    }
}
pub(super) struct Schedule {
    due: tokio::time::Instant,
    after: Option<Key>,
}
impl Default for Schedule {
    fn default() -> Self {
        Self {
            due: tokio::time::Instant::now(),
            after: None,
        }
    }
}
fn reason(error: &anyhow::Error) -> &'static str {
    for cause in error.chain() {
        if let Some(status) = cause.downcast_ref::<tonic::Status>() {
            return match status.code() {
                tonic::Code::NotFound => "not_found",
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated => "permission_denied",
                tonic::Code::DeadlineExceeded => "timeout",
                tonic::Code::ResourceExhausted => "budget_exceeded",
                tonic::Code::DataLoss => "corrupt",
                _ => "transport",
            };
        }
    }
    "invalid_selection"
}
impl Worker {
    #[cfg(test)]
    pub(super) fn availability_due_now(&self) {
        self.availability_schedule.lock().unwrap().due = tokio::time::Instant::now();
    }
    #[cfg(test)]
    pub(super) fn expire_availability(&self) {
        for entry in self
            .stats
            .availability
            .0
            .lock()
            .unwrap()
            .entries
            .values_mut()
        {
            entry.checked -= MAX_AGE;
        }
    }
    pub(super) async fn observe_availability_due(self: &Arc<Self>) {
        {
            let Ok(mut schedule) = self.availability_schedule.lock() else {
                return;
            };
            let now = tokio::time::Instant::now();
            if now < schedule.due {
                return;
            }
            schedule.due = now + INTERVAL;
        }
        // Never queue another physical job behind publication/cleanup or a timed-out job.
        let Ok(permit) = self.admission.clone().try_acquire_owned() else {
            return;
        };
        let admission = Arc::new(Admission { _permit: permit });
        let Some(revision) = self.stats.availability.revision() else {
            return;
        };
        #[cfg(test)]
        let timeout_ms = self
            ._hooks
            .availability_timeout_ms
            .unwrap_or(self.policy.timeout_ms);
        #[cfg(not(test))]
        let timeout_ms = self.policy.timeout_ms;
        let deadline = tokio::time::Instant::now() + Duration::from_millis(timeout_ms);
        let worker = self.clone();
        // The physical task owns admission even when the outer waiter expires.
        // Only the waiter commits observations; detached completion can never turn green.
        let task = tokio::spawn(async move {
            let _physical = admission.clone();
            let tickets = worker.host.capture_registry().pending_captures().await?;
            ensure!(
                tokio::time::Instant::now() < deadline,
                "availability deadline exceeded"
            );
            ensure!(
                tickets.len() <= worker.policy.max_jobs,
                "availability inventory limit"
            );
            let mut latest = BTreeMap::<Key, PendingCapture>::new();
            for ticket in tickets {
                let key = key(&ticket);
                if latest
                    .get(&key)
                    .is_none_or(|old| old.registry_epoch < ticket.registry_epoch)
                {
                    latest.insert(key, ticket);
                }
            }
            let selected = {
                let mut schedule = worker
                    .availability_schedule
                    .lock()
                    .map_err(|_| anyhow::anyhow!("availability cursor unavailable"))?;
                let wanted = latest
                    .keys()
                    .find(|k| schedule.after.is_none_or(|old| **k > old))
                    .copied()
                    .or_else(|| latest.keys().next().copied());
                schedule.after = wanted;
                wanted.and_then(|key| latest.remove(&key).map(|ticket| (key, ticket)))
            };
            let Some((key, ticket)) = selected else {
                return Ok(None);
            };
            let result = worker
                .observe_selected_capsule(ticket, admission, deadline)
                .await;
            Ok::<_, anyhow::Error>(Some((key, result)))
        });
        match tokio::time::timeout_at(deadline, task).await {
            Ok(Ok(Ok(Some((key, result))))) if tokio::time::Instant::now() < deadline => {
                let (state, reason) = match result {
                    Ok(true) => ("available", "verified"),
                    Ok(false) => ("unknown", "unpublished"),
                    Err(error) => {
                        let reason = reason(&error);
                        (
                            if reason == "not_found" {
                                "unavailable"
                            } else {
                                "unknown"
                            },
                            reason,
                        )
                    }
                };
                self.stats.availability.complete(
                    revision,
                    key,
                    self.policy.max_jobs,
                    state,
                    reason,
                );
            }
            Ok(Ok(Ok(None))) => self.stats.availability.fail("no_selection"),
            Ok(Ok(Err(error))) => self.stats.availability.fail(reason(&error)),
            Ok(Err(_)) => self.stats.availability.fail("task_failed"),
            _ => self.stats.availability.fail("timeout"),
        }
    }
    async fn observe_selected_capsule(
        self: &Arc<Self>,
        ticket: PendingCapture,
        admission: Arc<Admission>,
        deadline: tokio::time::Instant,
    ) -> Result<bool> {
        let (mount, alias, identity) = self.host.capture_mapping(&ticket)?;
        ensure!(!self.host.active(&mount), "availability workspace busy");
        let source = self
            .sources
            .get(&alias)
            .context("availability provider absent")?
            .clone();
        ensure!(source.identity == identity, "availability provider changed");
        let workspace = crate::native_workspaces::capture_upload_workspace(
            &self.config.state_dir,
            &mount,
            &alias,
            &identity,
        )?;
        let registry = self.host.capture_registry();
        #[cfg(test)]
        self._hooks
            .availability_reads
            .fetch_add(1, Ordering::Relaxed);
        let read = Arc::new(registry.read_capture(ticket.id).await?);
        ensure!(
            tokio::time::Instant::now() < deadline,
            "availability deadline exceeded"
        );
        ensure!(read.pending() == &ticket, "availability ticket changed");
        let owner: Owner = Arc::new(JobOwner {
            _read: read,
            _admission: admission,
        });
        let worker = self.clone();
        let saved = blocking_owned(owner.clone(), move || {
            #[cfg(test)]
            if let Some(pause) = worker._hooks.availability_pause.lock().unwrap().take() {
                pause.block();
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "availability deadline exceeded"
            );
            let name = format!("{}.published", hex(&ticket.id.0));
            if path_absent(&worker.directory, &name)? {
                return Ok(None);
            }
            let mut intent =
                Intent::decode(&worker.read_durable(&format!("{}.job", hex(&ticket.id.0)))?)?;
            worker.apply_published(&mut intent, &worker.read_bounded_durable(&name, 4096)?)?;
            ensure!(
                intent.header.valid()
                    && intent.header.ticket == ticket.id.0
                    && intent.header.ticket_binding == ticket.binding()
                    && intent.header.provider == identity
                    && intent.header.workspace == workspace,
                "availability binding differs"
            );
            // Saved bytes, SDK retained payloads and semantic verification share the job budget.
            let reserve = (intent.manifest.len() as u64 + intent.capture.len() as u64)
                .checked_mul(4)
                .and_then(|v| v.checked_add(SUCCESSOR_MEMORY))
                .context("availability memory overflow")?;
            ensure!(
                reserve <= worker.policy.max_memory_bytes,
                "availability memory limit"
            );
            ensure!(
                tokio::time::Instant::now() < deadline,
                "availability deadline exceeded"
            );
            #[cfg(test)]
            worker
                ._hooks
                .availability_proofs
                .fetch_add(1, Ordering::Relaxed);
            worker.verify_saved_reserved(&ticket, &intent, owner.clone(), reserve)?;
            Ok(Some(intent))
        })
        .await?;
        let Some(intent) = &saved.value else {
            return Ok(false);
        };
        let client = source
            .control
            .client_for_owned_work(saved._owner.clone(), deadline)
            .await?;
        let selector = RetainedVersionSelector {
            workspace_id: workspace,
            generation: intent
                .header
                .expected_generation
                .checked_add(1)
                .context("availability generation overflow")?,
            manifest_id: CloudManifestId(intent.header.manifest_id),
        };
        let version = client
            .cloud_retained_version_selected(selector, CloudRawDigest::of(&intent.manifest))
            .await?;
        ensure!(
            version.manifest() == intent.manifest,
            "availability manifest differs"
        );
        let captured = client.cloud_captured_version(version)?;
        ensure!(
            captured.descriptor().selected == CloudRawDigest::of(&intent.capture),
            "availability DATA selection differs"
        );
        let data = client.cloud_read_capture(&captured).await?;
        ensure!(
            data.payload() == intent.capture,
            "availability DATA differs"
        );
        Ok(true)
    }
}
