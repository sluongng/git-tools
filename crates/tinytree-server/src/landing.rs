//! Bounded landing transport with optional operator-owned check orchestration.
//! Accepted authorization does not depend on a live token; clients never submit verdicts.
use crate::{RepositoryService, auth::Identity, config::ConfigError};
use prost::Message;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tinytree_proto::landing_v1 as p;
use tinytree_store as s;
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};
use tonic::{Request, Response, Status, codec::CompressionEncoding};

// Pinned tonic/zstd streaming decode permits a 128 MiB window (window_log=27)
// before the decoded-message limit is reached. Reserve that window plus ample
// context/block scratch even for tiny compressed status requests. Window-limit
// regression tests must be revisited when the compression stack is upgraded.
const ZSTD_DECODE_WORK: u64 = 256 * 1024 * 1024;
const COORDINATOR_WORK: u64 = 2 * 1024 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LandingProfile {
    pub name: String,
    pub version: String,
    pub repositories: Vec<String>,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LandingConfig {
    pub enabled: bool,
    pub profiles: Vec<LandingProfile>,
    /// Explicit operator-selected profile for provisioned Forge accounts.
    pub forge_profile: Option<String>,
    pub checker: Option<crate::LandingCheckerConfig>,
    pub local_owner_profiles: Vec<String>,
    pub committer_name: String,
    pub committer_email: String,
    pub job_ttl_seconds: u64,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_inflight: usize,
    /// RPC admission only. Background preparation uses max_heavy_jobs/memory.
    pub max_inflight_per_repository: usize,
    pub max_heavy_jobs: usize,
    pub max_buffered_bytes: u64,
    pub poll_interval_ms: u64,
    pub operation_timeout_ms: u64,
    pub scan_records: usize,
    pub scan_bytes: usize,
}
impl Default for LandingConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            profiles: Vec::new(),
            forge_profile: None,
            checker: None,
            local_owner_profiles: Vec::new(),
            committer_name: "Tinytree landing".into(),
            committer_email: "landing@example.invalid".into(),
            job_ttl_seconds: 86400,
            max_request_bytes: 16 * 1024,
            max_response_bytes: 128 * 1024,
            max_inflight: 2,
            max_inflight_per_repository: 2,
            max_heavy_jobs: 1,
            max_buffered_bytes: 2 * 1024 * 1024 * 1024,
            poll_interval_ms: 1000,
            operation_timeout_ms: 120000,
            scan_records: 16,
            scan_bytes: 1024 * 1024,
        }
    }
}
impl LandingConfig {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if let Some(checker) = &self.checker {
            checker.validate()?;
        }
        if self.profiles.len() > 32
            || (self.enabled && self.profiles.is_empty())
            || self.local_owner_profiles.len() > 32
            || self.committer_name.is_empty()
            || self.committer_name.len() > 1024
            || self.committer_email.is_empty()
            || self.committer_email.len() > 1024
            || !(1..=7 * 86400).contains(&self.job_ttl_seconds)
            || !(1024..=64 * 1024).contains(&self.max_request_bytes)
            || !(4096..=1024 * 1024).contains(&self.max_response_bytes)
            || !(1..=32).contains(&self.max_inflight)
            || !(1..=self.max_inflight).contains(&self.max_inflight_per_repository)
            || !(1..=4).contains(&self.max_heavy_jobs)
            || !(1024 * 1024..=512 * 1024 * 1024 * 1024).contains(&self.max_buffered_bytes)
            || !(10..=60000).contains(&self.poll_interval_ms)
            || !(1000..=600000).contains(&self.operation_timeout_ms)
            || !(1..=128).contains(&self.scan_records)
            || !(4096..=16 * 1024 * 1024).contains(&self.scan_bytes)
        {
            return Err(ConfigError("invalid landing service bounds"));
        }
        let mut names = HashSet::new();
        for profile in &self.profiles {
            if !profile_name(&profile.name)
                || !names.insert(&profile.name)
                || profile.version.is_empty()
                || profile.version.len() > 128
                || profile.version.contains('\0')
                || (profile.repositories.is_empty()
                    && self.forge_profile.as_ref() != Some(&profile.name))
                || profile.repositories.len() > 128
                || profile
                    .repositories
                    .iter()
                    .any(|r| s::validate_repository_id(r).is_err())
                || profile.repositories.iter().collect::<HashSet<_>>().len()
                    != profile.repositories.len()
            {
                return Err(ConfigError("invalid landing profile"));
            }
        }
        if self
            .forge_profile
            .as_ref()
            .is_some_and(|name| !self.profiles.iter().any(|p| &p.name == name))
        {
            return Err(ConfigError("unknown Forge landing profile"));
        }
        self.validate_grants(&self.local_owner_profiles)?;
        Ok(())
    }
    pub(crate) fn validate_grants(&self, grants: &[String]) -> Result<(), ConfigError> {
        if grants.len() > 32
            || grants.iter().collect::<HashSet<_>>().len() != grants.len()
            || grants
                .iter()
                .any(|g| !self.profiles.iter().any(|p| &p.name == g))
        {
            return Err(ConfigError("unknown or duplicate landing profile grant"));
        }
        Ok(())
    }
    pub(crate) fn validate_repositories(&self, repos: &[String]) -> Result<(), ConfigError> {
        if self
            .profiles
            .iter()
            .any(|p| p.repositories.iter().any(|r| !repos.contains(r)))
        {
            return Err(ConfigError("unknown landing profile repository"));
        }
        Ok(())
    }
    fn configured(&self, repo: &str, name: &str, version: &str) -> bool {
        self.enabled
            && self.profiles.iter().any(|p| {
                p.name == name && p.version == version && p.repositories.iter().any(|r| r == repo)
            })
    }
    pub(crate) fn validate_store_with_checker_history(
        &self,
        policy: &s::Config,
    ) -> Result<(), ConfigError> {
        let charges = self.validate_store(policy)?;
        // Pure preflight cannot inspect historical checker state. Budget its
        // catalogs even if execution is disabled; startup reserves them only
        // when the checker is configured or durable obligations exist.
        let minimum = charges.minimum(self, crate::landing_check::CheckerHost::catalog_work())?;
        if minimum > self.max_buffered_bytes {
            return Err(ConfigError("landing checker catalogs exceed memory pool"));
        }
        Ok(())
    }
    pub(crate) fn validate_store(&self, policy: &s::Config) -> Result<Charges, ConfigError> {
        let sum = |values: &[u64]| {
            values.iter().try_fold(0u64, |a, b| {
                a.checked_add(*b)
                    .ok_or(ConfigError("landing resource bound overflow"))
            })
        };
        let mul = |a: u64, b: u64| {
            a.checked_mul(b)
                .ok_or(ConfigError("landing resource bound overflow"))
        };
        let status = sum(&[
            ZSTD_DECODE_WORK,
            mul(policy.landing.max_record_bytes as u64, 128)?,
            mul(self.max_response_bytes as u64, 8)?,
            mul(self.max_request_bytes as u64, 128)?,
        ])?;
        if !self.enabled {
            if status > self.max_buffered_bytes {
                return Err(ConfigError("landing status exceeds configured memory pool"));
            }
            return Ok(Charges {
                heavy: status,
                maintenance: status,
                status,
                coordinator: 0,
            });
        }
        if self.job_ttl_seconds > policy.landing.max_job_ttl_seconds as u64 {
            return Err(ConfigError("landing TTL exceeds Store policy"));
        }
        let response_bound = (policy.landing.candidate.max_stack as u64)
            .checked_mul(256)
            .and_then(|n| n.checked_add(4096))
            .ok_or(ConfigError("landing response bound overflow"))?
            .max(policy.landing.candidate.max_path_bytes as u64 + 4096);
        if response_bound > self.max_response_bytes as u64
            || self.scan_bytes < policy.landing.max_record_bytes + 256
        {
            return Err(ConfigError(
                "landing response or discovery budget cannot represent Store policy",
            ));
        }
        // Root-union admission can traverse retained cloud catalogs even when the
        // cloud RPC service is disabled. Include its conservative Store profile.
        let cloud = policy
            .cloud_resource_profile()
            .map_err(|_| ConfigError("landing Store resource profile overflow"))?;
        let heavy = sum(&[
            ZSTD_DECODE_WORK,
            cloud.version_v3_bytes,
            mul(policy.landing.candidate.max_metadata_bytes as u64, 8)?,
            mul(policy.landing.candidate.max_output_bytes as u64, 4)?,
            mul(policy.model_limits.max_object_bytes as u64, 8)?,
            mul(self.max_request_bytes as u64, 128)?,
        ])?;
        let maintenance = sum(&[
            mul(policy.model_limits.max_object_bytes as u64, 16)?,
            mul(policy.landing.max_record_bytes as u64, 128)?,
            mul(self.scan_bytes as u64, 8)?,
        ])?;
        let required = sum(&[
            COORDINATOR_WORK,
            mul(heavy, self.max_heavy_jobs as u64)?,
            maintenance,
            mul(status, self.max_inflight as u64)?,
        ])?;
        if required > self.max_buffered_bytes {
            return Err(ConfigError(
                "landing Store work exceeds memory pool; configure explicit bounded Store limits",
            ));
        }
        for value in [heavy, maintenance, status, self.max_buffered_bytes] {
            kib(value)?;
        }
        Ok(Charges {
            heavy,
            maintenance,
            status,
            coordinator: COORDINATOR_WORK,
        })
    }
}
fn profile_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn kib(bytes: u64) -> Result<u32, ConfigError> {
    u32::try_from(bytes.div_ceil(1024)).map_err(|_| ConfigError("landing resource bound overflow"))
}
#[derive(Clone, Copy)]
pub(crate) struct Charges {
    heavy: u64,
    maintenance: u64,
    status: u64,
    coordinator: u64,
}
impl Charges {
    fn minimum(&self, config: &LandingConfig, checker_memory: u64) -> Result<u64, ConfigError> {
        checker_memory
            .checked_add(if config.enabled {
                self.heavy
                    .checked_mul(config.max_heavy_jobs as u64)
                    .and_then(|n| n.checked_add(self.maintenance))
                    .and_then(|n| {
                        n.checked_add(self.status.checked_mul(config.max_inflight as u64)?)
                    })
                    .and_then(|n| n.checked_add(self.coordinator))
                    .ok_or(ConfigError("landing checker resource overflow"))?
            } else {
                self.status
            })
            .ok_or(ConfigError("landing checker memory overflow"))
    }
}
pub(crate) struct LandingHost {
    pub config: LandingConfig,
    store: s::Store,
    checker: Option<Arc<crate::landing_check::CheckerHost>>,
    charges: Charges,
    calls: Arc<Semaphore>,
    heavy: Arc<Semaphore>,
    maintenance: Arc<Semaphore>,
    bytes: Arc<Semaphore>,
    repositories: HashMap<String, Arc<Semaphore>>,
    active: AtomicUsize,
    changed: Notify,
    closing: AtomicBool,
    wake: Arc<Notify>,
    capacity_changed: Arc<Notify>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    preparing: Mutex<HashSet<[u8; 32]>>,
    #[cfg(test)]
    pause: Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
}
pub(crate) struct Work {
    _memory: OwnedSemaphorePermit,
    _call: Option<OwnedSemaphorePermit>,
    _heavy: Option<OwnedSemaphorePermit>,
    _maintenance: Option<OwnedSemaphorePermit>,
    repository: Mutex<Option<OwnedSemaphorePermit>>,
    pub deadline: Instant,
    _ticket: Ticket,
}
struct Ticket(Arc<LandingHost>);
impl Drop for Ticket {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
impl LandingHost {
    pub(crate) fn forge_profile(&self) -> Result<Option<s::LandingCheckProfile>, Status> {
        if !self.config.enabled
            || !self
                .config
                .checker
                .as_ref()
                .is_some_and(|checker| checker.enabled)
        {
            return Ok(None);
        }
        let Some(name) = &self.config.forge_profile else {
            return Ok(None);
        };
        let profile = self
            .config
            .profiles
            .iter()
            .find(|p| &p.name == name)
            .ok_or_else(|| Status::failed_precondition("Forge check profile unavailable"))?;
        Ok(Some(s::LandingCheckProfile {
            name: profile.name.clone(),
            version: profile.version.clone(),
        }))
    }
    fn background_configured(&self, repo: &str, name: &str, version: &str) -> bool {
        self.config.configured(repo, name, version)
            || (self
                .forge_profile()
                .ok()
                .flatten()
                .is_some_and(|p| p.name == name && p.version == version)
                && self
                    .store
                    .forge_account_repositories(1024)
                    .is_ok_and(|repos| repos.iter().any(|r| r == repo)))
    }
    fn background_repositories(&self) -> Result<Vec<String>, Status> {
        let mut repos = self.repositories.keys().cloned().collect::<Vec<_>>();
        if self.forge_profile()?.is_some() {
            repos.extend(
                self.store
                    .forge_account_repositories(1024)
                    .map_err(crate::service::store_status)?,
            );
        }
        repos.sort();
        repos.dedup();
        Ok(repos)
    }
    pub fn new(
        store: s::Store,
        config: LandingConfig,
        repos: &[String],
    ) -> Result<Arc<Self>, ConfigError> {
        let charges = config.validate_store(&store.resource_config())?;
        let checker_expected = store
            .external_checks_directory()
            .map_err(|_| ConfigError("external checker state unavailable"))?
            .is_some()
            || (config.enabled
                && config
                    .checker
                    .as_ref()
                    .is_some_and(|checker| checker.enabled));
        let checker_memory = if checker_expected {
            crate::landing_check::CheckerHost::catalog_work()
        } else {
            0
        };
        let minimum = charges.minimum(&config, checker_memory)?;
        if minimum > config.max_buffered_bytes {
            return Err(ConfigError("landing checker catalogs exceed memory pool"));
        }
        let checker = crate::landing_check::CheckerHost::open(
            store.clone(),
            config.checker.clone(),
            config.profiles.clone(),
            repos,
            config.enabled,
            config.forge_profile.clone(),
        )?;
        let host = Arc::new(Self {
            calls: Arc::new(Semaphore::new(config.max_inflight)),
            heavy: Arc::new(Semaphore::new(config.max_heavy_jobs)),
            maintenance: Arc::new(Semaphore::new(1)),
            bytes: Arc::new(Semaphore::new((config.max_buffered_bytes / 1024) as usize)),
            repositories: repos
                .iter()
                .map(|r| {
                    (
                        r.clone(),
                        Arc::new(Semaphore::new(config.max_inflight_per_repository)),
                    )
                })
                .collect(),
            charges,
            checker,
            store,
            config,
            active: AtomicUsize::new(0),
            changed: Notify::new(),
            closing: AtomicBool::new(false),
            wake: Arc::new(Notify::new()),
            capacity_changed: Arc::new(Notify::new()),
            worker: Mutex::new(None),
            preparing: Mutex::new(HashSet::new()),
            #[cfg(test)]
            pause: Mutex::new(None),
        });
        if let Some(checker) = &host.checker {
            let memory = host
                .bytes
                .clone()
                .try_acquire_many_owned(kib(checker_memory)?)
                .map_err(|_| ConfigError("checker catalog memory exhausted"))?;
            checker.attach(Arc::downgrade(&host), memory);
        }
        Ok(host)
    }
    pub(crate) fn admit_check_transition(self: &Arc<Self>) -> Result<Arc<Work>, Status> {
        self.reserve(
            "Prepare",
            Instant::now() + Duration::from_millis(self.config.operation_timeout_ms),
            false,
        )
    }
    fn reserve(
        self: &Arc<Self>,
        method: &str,
        deadline: Instant,
        rpc: bool,
    ) -> Result<Arc<Work>, Status> {
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("landing service is draining"));
        }
        let slot = |pool: &Arc<Semaphore>| {
            pool.clone()
                .try_acquire_owned()
                .map_err(|_| Status::resource_exhausted("landing work budget exhausted"))
        };
        let call = if rpc { Some(slot(&self.calls)?) } else { None };
        let heavy = if matches!(
            method,
            "StartLanding" | "Prepare" | "AbandonLandingPublication"
        ) {
            Some(slot(&self.heavy)?)
        } else {
            None
        };
        let maintenance = if method == "Maintenance" {
            Some(slot(&self.maintenance)?)
        } else {
            None
        };
        let charge = if heavy.is_some() {
            self.charges.heavy
        } else if maintenance.is_some() {
            self.charges.maintenance
        } else {
            self.charges.status
        };
        let memory = self
            .bytes
            .clone()
            .try_acquire_many_owned(
                kib(charge).map_err(|_| Status::resource_exhausted("landing memory bound"))?,
            )
            .map_err(|_| Status::resource_exhausted("landing memory budget exhausted"))?;
        self.active.fetch_add(1, Ordering::AcqRel);
        if self.closing.load(Ordering::Acquire) {
            self.active.fetch_sub(1, Ordering::AcqRel);
            self.changed.notify_waiters();
            return Err(Status::unavailable("landing service is draining"));
        }
        Ok(Arc::new(Work {
            _memory: memory,
            _call: call,
            _heavy: heavy,
            _maintenance: maintenance,
            repository: Mutex::new(None),
            deadline,
            _ticket: Ticket(self.clone()),
        }))
    }
    pub fn admit(self: &Arc<Self>, method: &str, deadline: Instant) -> Result<Arc<Work>, Status> {
        self.reserve(method, deadline, true)
    }
    fn repository(
        &self,
        work: &Work,
        identity: &Identity,
        repo: &str,
        profile: Option<&str>,
    ) -> Result<s::LandingOwner, Status> {
        if let Some(profile) = profile {
            identity.authorize_landing(repo, profile, &self.config.local_owner_profiles)?;
        } else {
            identity.authorize(repo, false)?;
        }
        let gate = self
            .repositories
            .get(repo)
            .ok_or_else(|| Status::permission_denied("landing repository access denied"))?;
        let permit = gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("landing repository budget exhausted"))?;
        *work
            .repository
            .lock()
            .map_err(|_| Status::unavailable("landing admission unavailable"))? = Some(permit);
        Ok(s::LandingOwner(identity.landing_owner_hash()))
    }
    pub fn start(self: &Arc<Self>) -> Result<(), Status> {
        if let Some(checker) = &self.checker {
            checker.start()?;
        }
        if !self.config.enabled {
            return Ok(());
        }
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| Status::unavailable("landing worker unavailable"))?;
        if worker.is_some() {
            return Ok(());
        }
        if self.closing.load(Ordering::Acquire) {
            return Err(Status::unavailable("landing service is draining"));
        }
        let control_memory = self
            .bytes
            .clone()
            .try_acquire_many_owned(
                kib(self.charges.coordinator)
                    .map_err(|_| Status::resource_exhausted("landing coordinator memory bound"))?,
            )
            .map_err(|_| Status::resource_exhausted("landing coordinator memory exhausted"))?;
        let weak = Arc::downgrade(self);
        let wake = self.wake.clone();
        let capacity = self.capacity_changed.clone();
        let interval = Duration::from_millis(self.config.poll_interval_ms);

        *worker = Some(tokio::spawn(async move {
            let _control_memory = control_memory;
            let initial = || {
                weak.upgrade()
                    .and_then(|host| host.background_repositories().ok())
                    .unwrap_or_default()
                    .into_iter()
                    .map(|r| (r, None))
                    .collect::<VecDeque<(String, Option<s::LandingScanCursor>)>>()
            };
            let mut preparation_pages = initial();
            let mut expiry_pages = initial();
            // Retain one bounded page until every eligible job gets an attempt.
            // In particular, a poison first Intent cannot steal every retry slot.
            let mut ready: VecDeque<(String, s::PendingLanding)> = VecDeque::new();
            let mut next_expiry = Instant::now() + interval;
            loop {
                let Some(host) = weak.upgrade() else {
                    break;
                };
                if host.closing.load(Ordering::Acquire) {
                    break;
                }
                let mut blocked = false;
                let mut failed_scan = false;
                if let Some((repo, pending)) = ready.front() {
                    let due = crate::service::now()
                        .map(|now| pending.expires_at <= now)
                        .unwrap_or(true);
                    if due
                        || !host.background_configured(
                            repo,
                            &pending.profile.name,
                            &pending.profile.version,
                        )
                        || host.schedule(pending, repo)
                    {
                        ready.pop_front();
                    } else {
                        blocked = true;
                    }
                }
                if ready.is_empty() {
                    if let Some((repo, after)) = preparation_pages.pop_front() {
                        match host.scan(repo.clone(), after.clone(), false).await {
                            Ok(page) => {
                                if let Some(cursor) = page.next {
                                    preparation_pages.push_back((repo.clone(), Some(cursor)));
                                }
                                for pending in page.jobs {
                                    if pending.state == s::LandingState::Intent
                                        && host.background_configured(
                                            &repo,
                                            &pending.profile.name,
                                            &pending.profile.version,
                                        )
                                    {
                                        ready.push_back((repo.clone(), pending));
                                    }
                                }
                            }
                            Err(_) => {
                                preparation_pages.push_back((repo, after));
                                blocked = true;
                                failed_scan = true;
                            }
                        }
                    }
                }
                if expiry_pages.is_empty() && Instant::now() >= next_expiry {
                    expiry_pages = initial();
                    next_expiry = Instant::now() + interval;
                }
                if let Some((repo, after)) = expiry_pages.pop_front() {
                    match host.scan(repo.clone(), after.clone(), true).await {
                        Ok(page) => {
                            if let Some(cursor) = page.next {
                                expiry_pages.push_back((repo, Some(cursor)));
                            }
                        }
                        Err(_) => {
                            expiry_pages.push_back((repo, after));
                            blocked = true;
                            failed_scan = true;
                        }
                    }
                }
                let wait = failed_scan
                    || ((blocked || (preparation_pages.is_empty() && ready.is_empty()))
                        && expiry_pages.is_empty());
                let waiting_for_capacity = blocked && !ready.is_empty() && !failed_scan;
                drop(host);
                if wait {
                    if waiting_for_capacity {
                        tokio::select! {_=wake.notified()=>{},_=capacity.notified()=>{},_=tokio::time::sleep(interval)=>{}}
                    } else {
                        tokio::select! {_=wake.notified()=>{},_=tokio::time::sleep(interval)=>{}}
                    }
                    if preparation_pages.is_empty() && ready.is_empty() {
                        preparation_pages = initial();
                    }
                } else {
                    tokio::task::yield_now().await;
                }
            }
        }));
        Ok(())
    }
    async fn scan(
        self: &Arc<Self>,
        repo: String,
        after: Option<s::LandingScanCursor>,
        expire: bool,
    ) -> Result<s::LandingScanPage, Status> {
        let deadline = Instant::now() + Duration::from_millis(self.config.operation_timeout_ms);
        let work = self.reserve("Maintenance", deadline, false)?;
        let store = self.store.clone();
        let checker = self.checker.clone();
        let limits = s::LandingScanLimits {
            max_records: self.config.scan_records,
            max_scan_bytes: self.config.scan_bytes,
            max_jobs: self.config.scan_records,
            max_output_bytes: 64 * 1024,
        };
        execute(work, move || {
            let mut page = store
                .scan_pending_landings(&repo, after.as_ref(), limits)
                .map_err(crate::service::store_status)?;
            if expire {
                for job in &page.jobs {
                    if Instant::now() >= deadline {
                        return Err(Status::deadline_exceeded(
                            "landing discovery deadline exceeded",
                        ));
                    }
                    let now = crate::service::now()?;
                    if job.expires_at <= now {
                        if let Some(checker) = &checker {
                            let _ = checker.expire_if_safe(job, &store, &repo, now);
                        } else {
                            let _ = job.handle.expire(&store, now);
                        }
                    }
                }
                page.jobs.clear();
            }
            Ok(page)
        })
        .await
    }
    fn schedule(self: &Arc<Self>, pending: &s::PendingLanding, repo: &str) -> bool {
        let mut preparing = match self.preparing.lock() {
            Ok(v) => v,
            Err(_) => return false,
        };
        if preparing.contains(&pending.id) {
            return true;
        }
        let deadline = Instant::now() + Duration::from_millis(self.config.operation_timeout_ms);
        let Ok(work) = self.reserve("Prepare", deadline, false) else {
            return false;
        };
        if !self
            .background_repositories()
            .is_ok_and(|repos| repos.iter().any(|r| r == repo))
        {
            return false;
        }
        preparing.insert(pending.id);
        drop(preparing);
        struct Job {
            host: Arc<LandingHost>,
            id: [u8; 32],
        }
        impl Drop for Job {
            fn drop(&mut self) {
                if let Ok(mut jobs) = self.host.preparing.lock() {
                    jobs.remove(&self.id);
                }
                self.host.capacity_changed.notify_one();
            }
        }
        let job = Job {
            host: self.clone(),
            id: pending.id,
        };
        let store = self.store.clone();
        let handle = pending.handle.clone();
        drop(tokio::task::spawn_blocking(move || {
            let _job = job;
            let _work = work;
            if Instant::now() >= deadline {
                return;
            }
            if let Ok(now) = crate::service::now() {
                let _ = handle.prepare(&store, now);
            }
        }));
        true
    }
    #[cfg(test)]
    pub(crate) fn pause_job(
        &self,
        entered: tokio::sync::oneshot::Sender<()>,
        resume: std::sync::mpsc::Receiver<()>,
    ) {
        *self.pause.lock().unwrap() = Some((entered, resume));
    }
    #[cfg(test)]
    pub(crate) fn active_jobs(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }
    pub async fn drain(&self) {
        if let Some(checker) = &self.checker {
            checker.close();
        }
        self.closing.store(true, Ordering::Release);
        self.wake.notify_one();
        let worker = self.worker.lock().ok().and_then(|mut w| w.take());
        if let Some(worker) = worker {
            let _ = worker.await;
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                if let Some(checker) = &self.checker {
                    checker.drain().await;
                }
                return;
            }
            changed.await;
        }
    }
}
struct Owned<T> {
    value: T,
    _work: Arc<Work>,
}
async fn execute<T: Send + 'static>(
    work: Arc<Work>,
    f: impl FnOnce() -> Result<T, Status> + Send + 'static,
) -> Result<T, Status> {
    let deadline = work.deadline;
    let job = tokio::task::spawn_blocking(move || {
        #[cfg(test)]
        {
            let pause = work._ticket.0.pause.lock().unwrap().take();
            if let Some((entered, resume)) = pause {
                let _ = entered.send(());
                let _ = resume.recv();
            }
        }
        Owned {
            value: if Instant::now() >= deadline {
                Err(Status::deadline_exceeded("landing deadline exceeded"))
            } else {
                f()
            },
            _work: work,
        }
    });
    let result = tokio::time::timeout_at(deadline, job)
        .await
        .map_err(|_| Status::deadline_exceeded("landing deadline exceeded"))?
        .map_err(|_| Status::internal("landing worker failed"))?;
    result.value
}
#[derive(Clone)]
pub(crate) struct LandingService {
    repository: RepositoryService,
}
impl LandingService {
    pub fn new(repository: RepositoryService) -> Self {
        Self { repository }
    }
    pub fn grpc(self) -> p::landing_server::LandingServer<Self> {
        let config = self.repository.service_config().landing;
        p::landing_server::LandingServer::new(self)
            .accept_compressed(CompressionEncoding::Zstd)
            .send_compressed(CompressionEncoding::Zstd)
            .max_decoding_message_size(config.max_request_bytes)
            .max_encoding_message_size(config.max_response_bytes)
    }
}
fn work<T>(request: &Request<T>) -> Result<Arc<Work>, Status> {
    request
        .extensions()
        .get::<Arc<Work>>()
        .cloned()
        .ok_or_else(|| Status::unavailable("landing ingress admission required"))
}
fn token(value: &str) -> Result<(), Status> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Status::invalid_argument("invalid landing replay key"));
    }
    Ok(())
}
fn commit(bytes: &[u8]) -> Result<tinytree_model::CommitId, Status> {
    tinytree_model::CommitId::from_slice(bytes)
        .map_err(|_| Status::invalid_argument("landing commit ID must be 32 bytes"))
}
fn response(
    host: &LandingHost,
    job: s::LandingJob,
    work: Arc<Work>,
    repo: &str,
    key: &str,
) -> Result<Response<p::LandingResponse>, Status> {
    let availability = if !host.config.enabled {
        p::PreparationAvailability::PreparationDisabled
    } else if host
        .config
        .configured(repo, &job.profile.name, &job.profile.version)
    {
        p::PreparationAvailability::ProfileConfigured
    } else {
        p::PreparationAvailability::ProfileUnavailable
    };
    let phase = match job.state {
        s::LandingState::Intent => p::Phase::Intent,
        s::LandingState::Prepared => p::Phase::PreparedPendingChecks,
        s::LandingState::Checked => p::Phase::Checked,
        s::LandingState::Committed => p::Phase::Committed,
        s::LandingState::Conflict => p::Phase::Conflict,
        s::LandingState::Unsupported => p::Phase::Unsupported,
        s::LandingState::CheckFailed => p::Phase::CheckFailed,
        s::LandingState::StaleTarget => p::Phase::StaleTarget,
        s::LandingState::Expired => p::Phase::Expired,
        s::LandingState::CheckUnavailable => p::Phase::CheckUnavailable,
        s::LandingState::PublicationAbandoned => p::Phase::PublicationAbandoned,
    };
    let candidate = job.candidate.map(|c| p::Candidate {
        head: c.head.0.to_vec(),
        root_tree: c.root_tree.0.to_vec(),
        rewrites: c
            .rewrites
            .into_iter()
            .filter(|r| r.old != r.new)
            .map(|r| p::Rewrite {
                old_commit: r.old.0.to_vec(),
                new_commit: r.new.0.to_vec(),
                change_id: r.change_id.0.to_vec(),
            })
            .collect(),
    });
    let receipt = job.receipt.map(|r| p::Receipt {
        reference: Some(tinytree_proto::v1::Reference {
            name: r.reference.name,
            generation: r.reference.generation,
            commit: r.reference.commit.map(crate::service::descriptor_wire),
        }),
        rewrites: r
            .rewrites
            .into_iter()
            .map(|r| p::Rewrite {
                old_commit: r.old.0.to_vec(),
                new_commit: r.new.0.to_vec(),
                change_id: r.change_id.0.to_vec(),
            })
            .collect(),
        evidence: r.evidence.to_vec(),
    });
    let value = p::LandingResponse {
        repository_id: repo.to_owned(),
        idempotency_key: key.to_owned(),
        job: Some(p::LandingJob {
            id: job.id.to_vec(),
            phase: phase as i32,
            roots_retained: job.roots_retained,
            expires_at_seconds: job.expires_at,
            profile_name: job.profile.name,
            profile_version: job.profile.version,
            candidate,
            receipt,
            conflict_path: job.conflict_path,
            preparation: availability as i32,
            checker_progress: host
                .checker
                .as_ref()
                .and_then(|checker| checker.progress(repo, job.id)),
            recovery_inspection: None,
        }),
    };
    if value.encoded_len() > host.config.max_response_bytes {
        return Err(Status::resource_exhausted(
            "landing response exceeds budget",
        ));
    }
    let mut response = Response::new(value);
    response
        .extensions_mut()
        .insert(crate::body_deadline::ResponseBudget::new(work));
    Ok(response)
}
#[tonic::async_trait]
impl p::landing_server::Landing for LandingService {
    async fn start_landing(
        &self,
        request: Request<p::StartLandingRequest>,
    ) -> Result<Response<p::LandingResponse>, Status> {
        let work = work(&request)?;
        let identity = self.repository.query_identity(request.metadata())?;
        let host = self.repository.landing_host();
        if !host.config.enabled {
            return Err(Status::unavailable("landing preparation is disabled"));
        }
        let input = request.into_inner();
        token(&input.idempotency_key)?;
        if !profile_name(&input.profile_name)
            || input.expected_profile_version.is_empty()
            || input.expected_profile_version.len() > 128
            || input.destination.is_empty()
            || input.destination.len() > 512
            || input.destination.contains('\0')
        {
            return Err(Status::invalid_argument("invalid landing request"));
        }
        let owner = host.repository(
            &work,
            &identity,
            &input.repository_id,
            Some(&input.profile_name),
        )?;
        let base = commit(&input.source_base)?;
        let head = commit(&input.source_head)?;
        let target = commit(&input.expected_head)?;
        let store = host.store.clone();
        let selected = host.clone();
        let repo = input.repository_id.clone();
        let key = input.idempotency_key.clone();
        let job = execute(work.clone(), move || {
            let accepted = store
                .landing_job(&input.repository_id, owner, &input.idempotency_key)
                .map_err(crate::service::store_status)?;
            if accepted.is_none()
                && !selected.config.configured(
                    &input.repository_id,
                    &input.profile_name,
                    &input.expected_profile_version,
                )
            {
                return Err(Status::failed_precondition(
                    "landing profile version or repository unavailable",
                ));
            }
            let now = crate::service::now()?;
            let millis = now
                .checked_mul(1000)
                .ok_or_else(|| Status::out_of_range("landing timestamp overflow"))?;
            store
                .begin_landing(
                    &input.repository_id,
                    owner,
                    &s::LandingRequest {
                        idempotency_key: input.idempotency_key,
                        source_base: base,
                        source_head: head,
                        reference_name: input.destination,
                        expected_generation: input.expected_generation,
                        expected_head: target,
                        committer: tinytree_model::Signature {
                            name: selected.config.committer_name.clone(),
                            email: selected.config.committer_email.clone(),
                            timestamp: tinytree_model::Timestamp {
                                millis_since_epoch: millis,
                                tz_offset_minutes: 0,
                            },
                        },
                        check_profile: s::LandingCheckProfile {
                            name: input.profile_name,
                            version: input.expected_profile_version,
                        },
                        expires_at: now
                            .checked_add(selected.config.job_ttl_seconds as i64)
                            .ok_or_else(|| Status::out_of_range("landing expiry overflow"))?,
                    },
                    now,
                )
                .map_err(crate::service::store_status)
        })
        .await?;
        host.wake.notify_one();
        response(&host, job, work, &repo, &key)
    }
    async fn get_landing(
        &self,
        request: Request<p::GetLandingRequest>,
    ) -> Result<Response<p::LandingResponse>, Status> {
        let work = work(&request)?;
        let identity = self.repository.query_identity(request.metadata())?;
        let host = self.repository.landing_host();
        let input = request.into_inner();
        token(&input.idempotency_key)?;
        let owner = host.repository(&work, &identity, &input.repository_id, None)?;
        let store = host.store.clone();
        let repo = input.repository_id.clone();
        let key = input.idempotency_key.clone();
        let checker = host.checker.clone();
        let inspect = input.inspect_recovery;
        let (job, inspection) = execute(work.clone(), move || {
            let job = store
                .landing_job(&input.repository_id, owner, &input.idempotency_key)
                .map_err(crate::service::store_status)?
                .ok_or_else(|| Status::not_found("landing job unavailable"))?;
            let inspection = if inspect {
                checker
                    .as_ref()
                    .and_then(|c| c.inspect_recovery(&input.repository_id, job.id))
            } else {
                None
            };
            Ok((job, inspection))
        })
        .await?;
        let mut response = response(&host, job, work, &repo, &key)?;
        response.get_mut().job.as_mut().unwrap().recovery_inspection = inspection;
        if response.get_ref().encoded_len() > host.config.max_response_bytes {
            return Err(Status::resource_exhausted(
                "landing response exceeds budget",
            ));
        }
        Ok(response)
    }
    async fn recover_landing(
        &self,
        request: Request<p::RecoverLandingRequest>,
    ) -> Result<Response<p::LandingResponse>, Status> {
        let work = work(&request)?;
        let identity = self.repository.query_identity(request.metadata())?;
        let host = self.repository.landing_host();
        let input = request.into_inner();
        token(&input.idempotency_key)?;
        let expected: [u8; 32] = input
            .expected_job_id
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("invalid landing job ID"))?;
        let owner = host.repository(&work, &identity, &input.repository_id, None)?;
        let store = host.store.clone();
        let repo = input.repository_id.clone();
        let key = input.idempotency_key.clone();
        let attempts = input.expected_attempts;
        let recoveries = input.expected_recoveries;
        let job = execute(work.clone(), move || {
            store
                .landing_job(&input.repository_id, owner, &input.idempotency_key)
                .map_err(crate::service::store_status)?
                .ok_or_else(|| Status::not_found("landing job unavailable"))
        })
        .await?;
        if job.id != expected {
            return Err(Status::failed_precondition("landing job differs"));
        }
        identity.authorize_landing(&repo, &job.profile.name, &host.config.local_owner_profiles)?;
        if !host.config.enabled
            || !host
                .config
                .configured(&repo, &job.profile.name, &job.profile.version)
        {
            return Err(Status::failed_precondition(
                "current landing profile unavailable",
            ));
        }
        let checker = host
            .checker
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("checker unavailable"))?;
        checker.recover_once(&repo, expected, attempts, recoveries)?;
        response(&host, job, work, &repo, &key)
    }
    async fn abandon_landing_publication(
        &self,
        request: Request<p::AbandonLandingPublicationRequest>,
    ) -> Result<Response<p::LandingResponse>, Status> {
        let work = work(&request)?;
        let identity = self.repository.query_identity(request.metadata())?;
        let host = self.repository.landing_host();
        let input = request.into_inner();
        token(&input.idempotency_key)?;
        if !profile_name(&input.profile_name)
            || input.expected_profile_version.is_empty()
            || input.expected_profile_version.len() > 128
        {
            return Err(Status::invalid_argument("invalid landing profile binding"));
        }
        if input.expected_attempts != 1
            || input.expected_recoveries == 0
            || input.expected_recoveries > 256
        {
            return Err(Status::invalid_argument(
                "invalid publication attempt binding",
            ));
        }
        let binding = s::LandingPublicationBinding {
            job_id: input
                .expected_job_id
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("invalid landing job ID"))?,
            candidate: commit(&input.candidate_head)?,
            root_tree: tinytree_model::TreeId(
                input
                    .candidate_root_tree
                    .as_slice()
                    .try_into()
                    .map_err(|_| Status::invalid_argument("invalid candidate tree ID"))?,
            ),
            profile: s::LandingCheckProfile {
                name: input.profile_name.clone(),
                version: input.expected_profile_version.clone(),
            },
        };
        let owner = host.repository(&work, &identity, &input.repository_id, None)?;
        let store = host.store.clone();
        let repo = input.repository_id.clone();
        let key = input.idempotency_key.clone();
        let lookup_repo = repo.clone();
        let lookup_key = key.clone();
        let job = execute(work.clone(), move || {
            store
                .landing_job(&lookup_repo, owner, &lookup_key)
                .map_err(crate::service::store_status)?
                .ok_or_else(|| Status::not_found("landing job unavailable"))
        })
        .await?;
        let candidate = job
            .candidate
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("landing candidate unavailable"))?;
        if job.id != binding.job_id
            || job.profile != binding.profile
            || candidate.head != binding.candidate
            || candidate.root_tree != binding.root_tree
        {
            return Err(Status::failed_precondition(
                "landing publication binding differs",
            ));
        }
        identity.authorize_landing(&repo, &job.profile.name, &host.config.local_owner_profiles)?;
        if !host.config.enabled
            || !host
                .config
                .configured(&repo, &job.profile.name, &job.profile.version)
        {
            return Err(Status::failed_precondition(
                "current landing profile unavailable",
            ));
        }
        if job.state == s::LandingState::PublicationAbandoned {
            return response(&host, job, work, &repo, &key);
        }
        let checker = host
            .checker
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("checker unavailable"))?
            .clone();
        let mutation_repo = repo.clone();
        let job = execute(work.clone(), move || {
            checker.abandon_publication(
                &mutation_repo,
                &binding,
                input.expected_attempts,
                input.expected_recoveries,
            )
        })
        .await?;
        response(&host, job, work, &repo, &key)
    }
}
