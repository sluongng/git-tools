//! A bounded one-reference local replication worker. Observations, selected
//! export outbox work, and verified delivery are deliberately separate facts.
use crate::{
    ExportOptions,
    git::Git,
    state::{Phase as ExportPhase, Resources, State, WatchLease, hex},
    worker::{self, TrustSnapshot},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tinytree_client::{Config, NativeClient, protocol as p};
use tokio::{sync::watch as channel, task::JoinHandle, time::Instant};
#[path = "watch_state.rs"]
mod disk;
const WATCH_RESERVE: u64 = 2 * 1024 * 1024 + 4 * 65536;

#[derive(Clone)]
pub struct WatchOptions {
    pub export: ExportOptions,
    pub watch_dir: PathBuf,
    pub poll_interval: Duration,
    pub probe_timeout: Duration,
    pub max_backoff: Duration,
    #[cfg(test)]
    pub(crate) test_pause: Option<Arc<TestPause>>,
}
impl WatchOptions {
    pub fn new(export: ExportOptions, watch_dir: PathBuf) -> Self {
        Self {
            export,
            watch_dir,
            poll_interval: Duration::from_secs(5),
            probe_timeout: Duration::from_secs(5),
            max_backoff: Duration::from_secs(300),
            #[cfg(test)]
            test_pause: None,
        }
    }
    pub fn validate(&self) -> Result<()> {
        self.export.limits.validate()?;
        for path in [
            &self.watch_dir,
            &self.export.state_dir,
            &self.export.destination_git,
        ] {
            ensure!(
                path.is_absolute()
                    && path.components().all(|c| !matches!(
                        c,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )),
                "watch/export/destination paths must be absolute and normalized"
            );
        }
        for (left, right) in [
            (&self.watch_dir, &self.export.state_dir),
            (&self.watch_dir, &self.export.destination_git),
            (&self.export.state_dir, &self.export.destination_git),
        ] {
            ensure!(
                !left.starts_with(right) && !right.starts_with(left),
                "watch/export/destination directories must be pairwise independent"
            );
        }

        ensure!(
            (Duration::from_millis(100)..=Duration::from_secs(3600)).contains(&self.poll_interval)
                && (Duration::from_millis(100)..=Duration::from_secs(60))
                    .contains(&self.probe_timeout)
                && self.max_backoff >= self.poll_interval
                && self.max_backoff <= Duration::from_secs(3600),
            "invalid local watcher timing bounds"
        );
        ensure!(
            self.watch_dir.as_os_str().as_encoded_bytes().len() <= 4096,
            "watch directory exceeds path limit"
        );
        ensure!(
            self.export.source_reference != "_tinytree",
            "watch selection must be public"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WatchPhase {
    Starting,
    Probing,
    Exporting,
    Backoff,
    Draining,
    Idle,
    Stopped,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WatchError {
    SourceUnavailable,
    SourceAbsent,
    SourceRegressed,
    DestinationDiverged,
    ReceiverUnavailable,
    ExportFailed,
    IdentityChanged,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservedPosition {
    pub generation: u64,
    pub native_head: Option<String>,
    pub observed_at_unix_ms: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveredPosition {
    pub generation: u64,
    pub native_head: String,
    pub git_head: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WatchStatus {
    pub version: u32,
    /// Last persisted phase, not proof that an actor is currently alive.
    pub phase: WatchPhase,
    pub source_reference: String,
    pub destination_reference: String,
    pub observed_desired: Option<ObservedPosition>,
    pub last_probe_succeeded: bool,
    pub delivered: Option<DeliveredPosition>,
    pub pending_export_phase: Option<String>,
    pub last_full_verification_unix_ms: Option<u64>,
    pub last_receiver_ref_check_unix_ms: Option<u64>,
    /// Gap against the timestamped observation, not a new server/graph query.
    pub lag_at_last_observation: Option<u64>,
    pub last_error: Option<WatchError>,
    pub consecutive_failures: u32,
    pub next_retry_unix_ms: Option<u64>,
    pub export_attempts: u64,
    pub probes: u64,
    pub poll_interval_ms: u64,
    pub probe_timeout_ms: u64,
    pub max_backoff_ms: u64,
    pub updated_at_unix_ms: u64,
    /// A surviving physical child may hold this fence after actor death.
    pub writer_fence_held: Option<bool>,
    pub clock_skewed: bool,
}
impl WatchStatus {
    fn initial(options: &WatchOptions, now: u64) -> Self {
        Self {
            version: 1,
            phase: WatchPhase::Starting,
            source_reference: options.export.source_reference.clone(),
            destination_reference: options.export.destination_reference.clone(),
            observed_desired: None,
            last_probe_succeeded: false,
            delivered: None,
            pending_export_phase: None,
            last_full_verification_unix_ms: None,
            last_receiver_ref_check_unix_ms: None,
            lag_at_last_observation: None,
            last_error: None,
            consecutive_failures: 0,
            next_retry_unix_ms: None,
            export_attempts: 0,
            probes: 0,
            poll_interval_ms: millis(options.poll_interval),
            probe_timeout_ms: millis(options.probe_timeout),
            max_backoff_ms: millis(options.max_backoff),
            updated_at_unix_ms: now,
            writer_fence_held: None,
            clock_skewed: false,
        }
    }
    fn unknown(&mut self) {
        self.last_probe_succeeded = false;
        self.lag_at_last_observation = None;
    }
    fn failure(&mut self, error: WatchError, options: &WatchOptions, now: u64) {
        self.unknown();
        self.phase = WatchPhase::Backoff;
        self.last_error = Some(error);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1).min(32);
        let factor = 1u64 << self.consecutive_failures.saturating_sub(1).min(31);
        let delay = millis(options.poll_interval)
            .saturating_mul(factor)
            .min(millis(options.max_backoff));
        self.next_retry_unix_ms = Some(now.saturating_add(delay));
    }
    fn success(&mut self) {
        self.last_error = None;
        self.consecutive_failures = 0;
        self.next_retry_unix_ms = None;
    }
}

pub struct WatchHandle {
    stop: channel::Sender<bool>,
    task: Option<JoinHandle<Result<WatchStatus>>>,
}
impl WatchHandle {
    pub fn request_shutdown(&self) {
        let _ = self.stop.send(true);
    }
    /// Cancellation leaves the borrowed handle and its Drop shutdown signal
    /// reachable; the sole JoinHandle is never moved out across an await.
    pub async fn wait(&mut self) -> Result<WatchStatus> {
        let task = self
            .task
            .as_mut()
            .context("local watcher was already joined")?;
        let joined = task.await;
        self.task = None;
        joined.map_err(|_| anyhow::anyhow!("local watcher failed; inspect durable state"))?
    }
    pub async fn shutdown(&mut self) -> Result<WatchStatus> {
        self.request_shutdown();
        self.wait().await
    }
}
impl Drop for WatchHandle {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}
/// No await separates spawning the actor from returning its lifecycle owner.
/// Filesystem/connection startup happens inside the owned, cancellable actor.
pub fn start_watch(config: Config, options: WatchOptions) -> Result<WatchHandle> {
    options.validate()?;
    worker::validate_input(&config, &options.export)?;
    worker::admit_memory(&config, &options.export.limits, WATCH_RESERVE)?;
    let runtime = tokio::runtime::Handle::try_current()
        .context("local watcher requires an active Tokio runtime")?;
    let (stop, receiver) = channel::channel(false);
    let terminate = stop.clone();
    let task = runtime.spawn(async move { run(config, options, receiver, terminate).await });
    Ok(WatchHandle {
        stop,
        task: Some(task),
    })
}
pub fn watch_status(directory: &Path) -> Result<WatchStatus> {
    disk::read_status(directory)
}
fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
fn now_ms() -> Result<u64> {
    Ok(u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("wall clock precedes epoch")?
            .as_millis(),
    )?)
}
async fn stopped(stop: &mut channel::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}
async fn wait_until(stop: &mut channel::Receiver<bool>, when: Instant) -> bool {
    tokio::select! {biased;_=stopped(stop)=>false,_=tokio::time::sleep_until(when)=>true}
}
async fn drain(owner: &Arc<Resources>) {
    // The actor is the only producer of new jobs/clients. Once its logical
    // future has ended, every other strong reference is accepted physical work.
    while Arc::strong_count(owner) > 1 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn update_outbox(status: &mut WatchStatus, state: &State) {
    status.delivered = state.delivered.as_ref().map(|p| DeliveredPosition {
        generation: p.generation,
        native_head: p.native.clone(),
        git_head: p.git.clone(),
    });
    status.pending_export_phase = state.pending.as_ref().map(|p| {
        match p.phase {
            ExportPhase::Acquiring => "acquiring",
            ExportPhase::Held => "held",
            ExportPhase::Ready => "ready",
            ExportPhase::Release => "release",
        }
        .into()
    });
}
fn identity(export_identity: [u8; 32], owner: &Resources, directory: &Path) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-local-git-watch-v1\0");
    h.update(&export_identity);
    for path in [owner.state.path(), owner.destination.path(), directory] {
        let bytes = path.as_os_str().as_encoded_bytes();
        h.update(&(bytes.len() as u64).to_le_bytes());
        h.update(bytes);
    }
    *h.finalize().as_bytes()
}
async fn connect_probe(
    config: &Config,
    trust: &TrustSnapshot,
    timeout: Duration,
) -> Result<NativeClient> {
    let mut bounded = config.clone();
    let ms = millis(timeout);
    bounded.rpc.operation_timeout_ms = bounded.rpc.operation_timeout_ms.min(ms);
    bounded.rpc.connect_timeout_ms = bounded.rpc.connect_timeout_ms.min(ms);
    let client = if let Some(ca) = &trust.ca {
        NativeClient::connect_with_ca_certificate(bounded, ca.clone()).await
    } else {
        NativeClient::connect(bounded).await
    };
    client.map_err(|_| anyhow::anyhow!("native probe connection unavailable"))
}
async fn probe(
    client: &NativeClient,
    config: &Config,
    reference: &str,
    owner: &Arc<Resources>,
    deadline: Instant,
) -> Result<ObservedPosition> {
    let view = client
        .clone()
        .with_rpc_lifetime_guard(owner.clone())
        .with_operation_deadline(deadline)
        .open(vec![reference.into()])
        .await
        .map_err(|_| anyhow::anyhow!("native probe unavailable"))?;
    ensure!(
        view.repository_id == config.repository_id && view.references.len() == 1,
        "probe repository/reference identity differs"
    );
    let position = &view.references[0];
    let head = if let Some(d) = &position.commit {
        let k = d.key.as_ref().context("probe descriptor absent")?;
        ensure!(
            k.kind == p::ObjectKind::Commit as i32
                && k.native_id.len() == 32
                && k.native_id.iter().any(|b| *b != 0),
            "probe commit identity differs"
        );
        Some(hex(&k.native_id))
    } else {
        None
    };
    Ok(ObservedPosition {
        generation: position.generation,
        native_head: head,
        observed_at_unix_ms: now_ms()?,
    })
}

async fn run(
    config: Config,
    options: WatchOptions,
    mut stop: channel::Receiver<bool>,
    terminate: channel::Sender<bool>,
) -> Result<WatchStatus> {
    let disk = disk::Disk::open(&options.watch_dir)?;
    let mut bounded_export = options.export.clone();
    bounded_export.limits.max_memory_bytes = bounded_export
        .limits
        .max_memory_bytes
        .checked_sub(WATCH_RESERVE)
        .context("watch memory reservation exceeds allowance")?;
    let owner = Resources::open_watched(
        bounded_export,
        Some(WatchLease {
            fence: disk.lock.clone(),
            cancel: stop.clone(),
            _owner: disk.clone(),
            #[cfg(test)]
            test_pause: options.test_pause.clone(),
        }),
    )?;
    ensure!(
        !owner.state.path().starts_with(disk.directory.path())
            && !disk.directory.path().starts_with(owner.state.path())
            && !owner.destination.path().starts_with(disk.directory.path())
            && !disk.directory.path().starts_with(owner.destination.path()),
        "watch/export/destination directories must be pairwise independent"
    );
    let outcome = run_inner(config, options, &mut stop, &disk, &owner).await;
    if outcome.is_err() {
        let _ = terminate.send(true);
        drain(&owner).await;
    }
    outcome
}
async fn run_inner(
    config: Config,
    options: WatchOptions,
    stop: &mut channel::Receiver<bool>,
    disk: &Arc<disk::Disk>,
    owner: &Arc<Resources>,
) -> Result<WatchStatus> {
    let (export_identity, ca) = worker::scope(&config, owner, None)?;
    let trust = TrustSnapshot { ca };
    let id = identity(export_identity, &owner, disk.directory.path());
    let mut status = disk
        .load(id)?
        .unwrap_or_else(|| WatchStatus::initial(&options, now_ms().unwrap_or(0)));
    ensure!(
        status.source_reference == options.export.source_reference
            && status.destination_reference == options.export.destination_reference,
        "watch reference binding differs"
    );
    let state = owner.load(export_identity)?;
    update_outbox(&mut status, &state);
    drop(state);
    status.unknown();
    status.phase = WatchPhase::Starting;
    status.poll_interval_ms = millis(options.poll_interval);
    status.probe_timeout_ms = millis(options.probe_timeout);
    status.max_backoff_ms = millis(options.max_backoff);
    let now = now_ms()?;
    let remaining = status
        .next_retry_unix_ms
        .map(|n| n.saturating_sub(now).min(millis(options.max_backoff)))
        .unwrap_or(0);
    if remaining > 0 {
        status.phase = WatchPhase::Backoff;
        status.next_retry_unix_ms = Some(now.saturating_add(remaining));
    }
    disk.save(id, &mut status, owner.clone()).await?;
    let mut next = Instant::now() + Duration::from_millis(remaining);
    let mut verified_this_run = false;
    let mut client = None;
    loop {
        if !wait_until(stop, next).await {
            break;
        }
        if worker::scope(&config, owner, Some(&trust))
            .map(|value| value.0)
            .ok()
            != Some(export_identity)
        {
            status.failure(WatchError::IdentityChanged, &options, now_ms()?);
            disk.save(id, &mut status, owner.clone()).await?;
            next = retry_instant(&status, &options)?;
            continue;
        }
        // The continuously held export lease excludes other mapping writers.
        // Reload at startup/after drained work, not O(history) on idle polls.
        let mut export_needed = status.pending_export_phase.is_some();
        if !export_needed {
            status.phase = WatchPhase::Probing;
            status.unknown();
            status.probes = status.probes.saturating_add(1);
            disk.save(id, &mut status, owner.clone()).await?;
            let deadline = Instant::now() + options.probe_timeout;
            if client.is_none() {
                let connected = tokio::select! {biased;_=stopped(stop)=>break,result=connect_probe(&config,&trust,options.probe_timeout)=>result};
                match connected {
                    Ok(c) => client = Some(c),
                    Err(_) => {
                        status.failure(WatchError::SourceUnavailable, &options, now_ms()?);
                        disk.save(id, &mut status, owner.clone()).await?;
                        next = retry_instant(&status, &options)?;
                        continue;
                    }
                }
            }
            let observed = tokio::select! {biased;_=stopped(stop)=>break,result=probe(client.as_ref().unwrap(),&config,&options.export.source_reference,owner,deadline)=>result};
            // Per-RPC guards are independent of the bare cached client, so
            // baseline one proves all accepted codec/blocking work has ended.
            drain(owner).await;
            if *stop.borrow() {
                break;
            }
            match observed {
                Err(_) => {
                    client = None;
                    drain(&owner).await;
                    status.failure(WatchError::SourceUnavailable, &options, now_ms()?);
                    disk.save(id, &mut status, owner.clone()).await?;
                    next = retry_instant(&status, &options)?;
                    continue;
                }
                Ok(observed) => {
                    let regressed = status.observed_desired.as_ref().is_some_and(|old| {
                        observed.generation < old.generation
                            || (observed.generation == old.generation
                                && observed.native_head != old.native_head)
                    }) || status.delivered.as_ref().is_some_and(|old| {
                        observed.generation < old.generation
                            || (observed.generation == old.generation
                                && observed.native_head.as_deref()
                                    != Some(old.native_head.as_str()))
                    });
                    if regressed {
                        status.failure(WatchError::SourceRegressed, &options, now_ms()?);
                        disk.save(id, &mut status, owner.clone()).await?;
                        next = retry_instant(&status, &options)?;
                        continue;
                    }
                    status.observed_desired = Some(observed.clone());
                    status.last_probe_succeeded = true;
                    status.lag_at_last_observation = status
                        .delivered
                        .as_ref()
                        .and_then(|d| observed.generation.checked_sub(d.generation))
                        .filter(|gap| *gap > 0);
                    disk.save(id, &mut status, owner.clone()).await?;
                    if observed.native_head.is_none() {
                        status.failure(WatchError::SourceAbsent, &options, now_ms()?);
                        disk.save(id, &mut status, owner.clone()).await?;
                        next = retry_instant(&status, &options)?;
                        continue;
                    }
                    if let Some(delivered) = &status.delivered {
                        let git = Git::new(owner.clone(), deadline)?;
                        let current = tokio::select! {biased;_=stopped(stop)=>break,result=git.reference()=>result};
                        drop(git);
                        let failure = match current {
                            Err(_) => Some(WatchError::ReceiverUnavailable),
                            Ok(current) => {
                                status.last_receiver_ref_check_unix_ms = Some(now_ms()?);
                                if current.as_deref() != Some(delivered.git_head.as_str()) {
                                    Some(WatchError::DestinationDiverged)
                                } else {
                                    None
                                }
                            }
                        };
                        if let Some(error) = failure {
                            drop(client.take());
                            drain(owner).await;
                            status.failure(error, &options, now_ms()?);
                            disk.save(id, &mut status, owner.clone()).await?;
                            next = retry_instant(&status, &options)?;
                            continue;
                        }
                    }
                    export_needed = !verified_this_run
                        || status.delivered.as_ref().is_none_or(|d| {
                            d.generation != observed.generation
                                || Some(d.native_head.as_str()) != observed.native_head.as_deref()
                        });
                    if !export_needed {
                        status.phase = WatchPhase::Idle;
                        status.lag_at_last_observation = Some(0);
                        status.success();
                        disk.save(id, &mut status, owner.clone()).await?;
                        next = Instant::now() + options.poll_interval;
                        continue;
                    }
                }
            }
        }
        if export_needed {
            drop(client.take());
            drain(&owner).await;
            if *stop.borrow() {
                break;
            }
            status.phase = WatchPhase::Exporting;
            // This is a timestamped observed gap, not the worker's potentially
            // newer selected job. Reconcile it only from the exporter journal.
            status.export_attempts = status.export_attempts.saturating_add(1);
            disk.save(id, &mut status, owner.clone()).await?;
            if *stop.borrow() {
                break;
            }
            let bounded = config.clone();
            let physical = owner.clone();
            let captured = trust.clone();
            let deadline = Instant::now() + options.export.limits.timeout;
            let mut job = tokio::spawn(async move {
                worker::run_with_resources(bounded, physical, deadline, Some(&captured)).await
            });
            let result = tokio::select! {biased;_=stopped(stop)=>{job.abort();let _=job.await;None},result=&mut job=>Some(result)};
            status.phase = WatchPhase::Draining;
            status.unknown();
            disk.save(id, &mut status, owner.clone()).await?;
            drain(&owner).await;
            let reloaded = owner.load(export_identity)?;
            update_outbox(&mut status, &reloaded);
            status.unknown();
            if result.is_none() {
                break;
            }
            match result.unwrap() {
                Ok(Ok(_)) => {
                    verified_this_run = true;
                    status.last_full_verification_unix_ms = Some(now_ms()?);
                    status.success();
                    status.phase = WatchPhase::Idle;
                    next = Instant::now() + options.poll_interval;
                }
                _ => {
                    status.failure(WatchError::ExportFailed, &options, now_ms()?);
                    next = retry_instant(&status, &options)?;
                }
            }
            disk.save(id, &mut status, owner.clone()).await?;
        }
    }
    drop(client.take());
    status.phase = WatchPhase::Draining;
    status.unknown();
    disk.save(id, &mut status, owner.clone()).await?;
    drain(&owner).await;
    update_outbox(&mut status, &owner.load(export_identity)?);
    status.phase = WatchPhase::Stopped;
    status.unknown();
    // Preserve a failed attempt's durable retry deadline across graceful restart.
    disk.save(id, &mut status, owner.clone()).await?;
    drain(&owner).await;
    Ok(status)
}
fn retry_instant(status: &WatchStatus, options: &WatchOptions) -> Result<Instant> {
    let now = now_ms()?;
    let remaining = status
        .next_retry_unix_ms
        .unwrap_or(now)
        .saturating_sub(now)
        .min(millis(options.max_backoff));
    Ok(Instant::now() + Duration::from_millis(remaining))
}

#[cfg(test)]
pub(crate) struct TestPause {
    phase: Option<WatchPhase>,
    armed: std::sync::atomic::AtomicBool,
    pub(crate) entered: std::sync::atomic::AtomicBool,
    release: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
}
#[cfg(test)]
impl TestPause {
    pub(crate) fn new(phase: Option<WatchPhase>, armed: bool) -> Arc<Self> {
        Arc::new(Self {
            phase,
            armed: std::sync::atomic::AtomicBool::new(armed),
            entered: std::sync::atomic::AtomicBool::new(false),
            release: std::sync::Mutex::new(false),
            changed: std::sync::Condvar::new(),
        })
    }
    pub(crate) fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    pub(crate) fn unblock(&self) {
        *self.release.lock().unwrap() = true;
        self.changed.notify_all();
    }
    pub(crate) fn block(&self, phase: Option<&WatchPhase>) {
        if self.phase.as_ref() != phase
            || !self.armed.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        self.entered
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let mut released = self.release.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
}
