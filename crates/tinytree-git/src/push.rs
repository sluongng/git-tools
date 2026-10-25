//! Explicit, one-way remote backup of already delivered public Git history.
use crate::{
    ExportOptions,
    state::{self, hex, valid_hex},
    worker,
};
use anyhow::{Context, Result, ensure};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};
#[path = "push_state.rs"]
mod disk;
#[path = "push_git.rs"]
mod git;
const TRANSPORT_POLICY: &str = "https-file-no-ambient-auth-session-pipe-v1";
#[derive(Clone)]
pub struct PushOptions {
    pub export: ExportOptions,
    pub push_dir: PathBuf,
    pub remote: String,
    pub remote_reference: String,
    pub poll_interval: Duration,
    pub operation_timeout: Duration,
    pub max_backoff: Duration,
    #[cfg(test)]
    pub(crate) after_push: bool,
    #[cfg(test)]
    pub(crate) after_ack: bool,
}
impl PushOptions {
    pub fn new(export: ExportOptions, push_dir: PathBuf, remote: String) -> Self {
        Self {
            export,
            push_dir,
            remote,
            remote_reference: "refs/heads/main".into(),
            poll_interval: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(60),
            max_backoff: Duration::from_secs(300),
            #[cfg(test)]
            after_push: false,
            #[cfg(test)]
            after_ack: false,
        }
    }
    fn validate(&self) -> Result<()> {
        self.export.limits.validate()?;
        let reserve = (self.export.limits.max_state_bytes as u64) * 4
            + 32 * 1024 * 1024
            + 2 * (disk::CAP as u64);
        ensure!(
            reserve <= self.export.limits.max_memory_bytes,
            "push state and verification memory budgets do not fit"
        );
        validate_remote(&self.remote)?;
        ensure!(
            valid_reference(&self.remote_reference)
                && valid_reference(&self.export.destination_reference),
            "push requires exact public branch references"
        );
        ensure!(
            self.export.source_reference != "_tinytree",
            "push source must be public"
        );
        for p in [
            &self.push_dir,
            &self.export.state_dir,
            &self.export.destination_git,
        ] {
            ensure!(
                p.is_absolute()
                    && p.components().all(|c| !matches!(
                        c,
                        std::path::Component::ParentDir | std::path::Component::CurDir
                    ))
                    && p.as_os_str().as_encoded_bytes().len() <= 4096,
                "push paths must be normalized absolute paths"
            );
        }
        for (a, b) in [
            (&self.push_dir, &self.export.state_dir),
            (&self.push_dir, &self.export.destination_git),
            (&self.export.state_dir, &self.export.destination_git),
        ] {
            ensure!(
                !a.starts_with(b) && !b.starts_with(a),
                "push/export/local directories must be independent"
            );
        }
        ensure!(
            (Duration::from_millis(100)..=Duration::from_secs(3600)).contains(&self.poll_interval)
                && (Duration::from_millis(100)..=Duration::from_secs(600))
                    .contains(&self.operation_timeout)
                && self.max_backoff >= self.poll_interval
                && self.max_backoff <= Duration::from_secs(3600),
            "invalid push timing bounds"
        );
        Ok(())
    }
}
fn valid_reference(r: &str) -> bool {
    r.starts_with("refs/heads/")
        && r.len() <= 4096
        && r.len() > 11
        && !r.ends_with('/')
        && !r.ends_with('.')
        && !r.contains("..")
        && !r.contains("@{")
        && !r.contains("//")
        && r.split('/')
            .all(|s| !s.starts_with('.') && !s.ends_with(".lock"))
        && !r
            .bytes()
            .any(|b| b <= 32 || b == 127 || b"~^:?*[\\".contains(&b))
}
fn validate_remote(r: &str) -> Result<()> {
    ensure!(
        !r.is_empty()
            && r.len() <= 4096
            && !r.bytes().any(|b| b <= 32 || b == 127)
            && !r.contains(['?', '#']),
        "invalid push remote"
    );
    if let Some(rest) = r.strip_prefix("https://") {
        let authority = rest.split('/').next().unwrap_or("");
        ensure!(
            !authority.is_empty()
                && !authority.contains('@')
                && !authority.starts_with('-')
                && !rest.contains('\\'),
            "invalid HTTPS push remote"
        );
    } else {
        let local = r.strip_prefix("file://").unwrap_or(r);
        ensure!(
            local.starts_with('/')
                && !local.contains("//")
                && !local.split('/').any(|part| part == "." || part == "..")
                && !local.contains([':', '%', '\\'])
                && Path::new(local).components().all(|c| !matches!(
                    c,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )),
            "push remote must be HTTPS or an absolute local path"
        );
    }
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PushPosition {
    pub generation: u64,
    pub native_head: String,
    pub git_head: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingPush {
    pub position: PushPosition,
    pub acknowledged: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushPhase {
    Starting,
    Idle,
    Verifying,
    Pushing,
    Backoff,
    Draining,
    Stopped,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushError {
    CheckpointUnavailable,
    CheckpointRegressed,
    RemoteUnavailable,
    RemoteDiverged,
    PushUnconfirmed,
    IdentityChanged,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PushStatus {
    pub version: u32,
    pub phase: PushPhase,
    pub destination_reference: String,
    pub desired: Option<PushPosition>,
    pub pending: Option<PendingPush>,
    pub delivered: Option<PushPosition>,
    pub last_error: Option<PushError>,
    pub consecutive_failures: u32,
    pub next_retry_unix_ms: Option<u64>,
    pub last_remote_check_unix_ms: Option<u64>,
    pub lag_at_last_observation: Option<u64>,
    pub attempts: u64,
    pub poll_interval_ms: u64,
    pub operation_timeout_ms: u64,
    pub max_backoff_ms: u64,
    pub updated_at_unix_ms: u64,
    pub writer_fence_held: Option<bool>,
    pub clock_skewed: bool,
}
impl PushStatus {
    fn initial(o: &PushOptions) -> Self {
        Self {
            version: 1,
            phase: PushPhase::Starting,
            destination_reference: o.remote_reference.clone(),
            desired: None,
            pending: None,
            delivered: None,
            last_error: None,
            consecutive_failures: 0,
            next_retry_unix_ms: None,
            last_remote_check_unix_ms: None,
            lag_at_last_observation: None,
            attempts: 0,
            poll_interval_ms: ms(o.poll_interval),
            operation_timeout_ms: ms(o.operation_timeout),
            max_backoff_ms: ms(o.max_backoff),
            updated_at_unix_ms: 0,
            writer_fence_held: None,
            clock_skewed: false,
        }
    }
    fn fail(&mut self, e: PushError) -> Result<()> {
        self.phase = PushPhase::Backoff;
        self.last_error = Some(e);
        self.lag_at_last_observation = None;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1).min(32);
        let delay = self
            .poll_interval_ms
            .saturating_mul(1u64 << self.consecutive_failures.saturating_sub(1).min(31))
            .min(self.max_backoff_ms);
        self.next_retry_unix_ms = Some(now_ms()?.saturating_add(delay));
        Ok(())
    }
    fn success(&mut self) {
        self.phase = PushPhase::Idle;
        self.last_error = None;
        self.consecutive_failures = 0;
        self.next_retry_unix_ms = None;
        self.lag_at_last_observation = self.desired.as_ref().and_then(|d| {
            self.delivered
                .as_ref()
                .and_then(|p| d.generation.checked_sub(p.generation))
        });
    }
}
pub(super) fn validate_status(s: &PushStatus) -> Result<()> {
    ensure!(
        s.version == 1
            && valid_reference(&s.destination_reference)
            && s.consecutive_failures <= 32
            && (100..=3_600_000).contains(&s.poll_interval_ms)
            && (100..=600_000).contains(&s.operation_timeout_ms)
            && (s.poll_interval_ms..=3_600_000).contains(&s.max_backoff_ms),
        "invalid push checkpoint policy"
    );
    for p in [
        s.desired.as_ref(),
        s.pending.as_ref().map(|p| &p.position),
        s.delivered.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        ensure!(
            p.generation > 0 && valid_hex(&p.native_head, 64) && valid_hex(&p.git_head, 40),
            "invalid push checkpoint position"
        );
    }
    let not_before =
        |new: &PushPosition, old: &PushPosition| new.generation > old.generation || new == old;
    if let Some(delivered) = &s.delivered {
        ensure!(
            s.desired.as_ref().is_some_and(|d| not_before(d, delivered)),
            "push desired position precedes delivery"
        );
    }
    if let Some(p) = &s.pending {
        ensure!(
            s.desired
                .as_ref()
                .is_some_and(|d| not_before(d, &p.position))
                && s.delivered
                    .as_ref()
                    .is_none_or(|d| not_before(&p.position, d)),
            "push pending position ordering differs"
        );
        if p.acknowledged {
            ensure!(
                s.delivered.as_ref() == Some(&p.position),
                "push ACK checkpoint differs"
            );
        }
    }
    Ok(())
}
pub struct PushHandle {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<PushStatus>>>,
}
impl PushHandle {
    pub fn request_shutdown(&self) {
        let _ = self.stop.send(true);
    }
    pub async fn wait(&mut self) -> Result<PushStatus> {
        let task = self.task.as_mut().context("push actor already joined")?;
        let result = task.await;
        self.task = None;
        result.map_err(|_| anyhow::anyhow!("push actor failed; inspect durable state"))?
    }
    pub async fn shutdown(&mut self) -> Result<PushStatus> {
        self.request_shutdown();
        self.wait().await
    }
}
impl Drop for PushHandle {
    fn drop(&mut self) {
        self.request_shutdown();
    }
}
pub fn start_push_watch(
    config: tinytree_client::Config,
    options: PushOptions,
) -> Result<PushHandle> {
    options.validate()?;
    worker::validate_input(&config, &options.export)?;
    let runtime = tokio::runtime::Handle::try_current()?;
    let (stop, receiver) = watch::channel(false);
    let task = runtime.spawn(run(config, options, receiver));
    Ok(PushHandle {
        stop,
        task: Some(task),
    })
}
pub fn push_status(path: &Path) -> Result<PushStatus> {
    disk::read(path)
}
fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}
pub(super) fn now_ms() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}
pub(super) async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}
async fn drain(owner: &Arc<git::Owner>) {
    while Arc::strong_count(owner) > 1 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn program(path: &Path) -> Result<PathBuf> {
    let p = if path.is_absolute() {
        path.to_path_buf()
    } else if path.components().count() == 1 {
        std::env::split_paths(&std::env::var_os("PATH").unwrap_or_default())
            .map(|p| p.join(path))
            .find(|p| p.is_file())
            .context("push Git executable unavailable")?
    } else {
        std::env::current_dir()?.join(path)
    };
    let p = p.canonicalize()?;
    ensure!(p.is_file(), "push Git executable must be regular");
    Ok(p)
}
fn identity(scope: [u8; 32], o: &PushOptions, program: &Path) -> Result<[u8; 32]> {
    let mut h = blake3::Hasher::new();
    h.update(TRANSPORT_POLICY.as_bytes());
    h.update(&scope);
    for p in [
        &o.export.state_dir,
        &o.export.destination_git,
        &o.push_dir,
        program,
    ] {
        let b = p.as_os_str().as_encoded_bytes();
        h.update(&(b.len() as u64).to_le_bytes());
        h.update(b);
    }
    for s in [&o.remote, &o.remote_reference] {
        h.update(&(s.len() as u64).to_le_bytes());
        h.update(s.as_bytes());
    }
    Ok(*h.finalize().as_bytes())
}
async fn local_policy(owner: &Arc<git::Owner>, deadline: Instant) -> Result<()> {
    let version = owner.run(vec!["version".into()], 256, deadline).await?;
    let version = std::str::from_utf8(&version)?
        .trim()
        .strip_prefix("git version ")
        .context("unsupported push Git version")?;
    let mut parts = version.split('.');
    let major: usize = parts.next().context("Git version absent")?.parse()?;
    let minor: usize = parts.next().context("Git version absent")?.parse()?;
    ensure!(
        major > 2 || major == 2 && minor >= 38,
        "push requires Git 2.38 or newer"
    );
    ensure!(
        owner
            .run(
                vec!["rev-parse".into(), "--is-bare-repository".into()],
                128,
                deadline
            )
            .await?
            == b"true\n",
        "push source must be bare Git"
    );
    ensure!(
        owner
            .run(
                vec!["rev-parse".into(), "--show-object-format".into()],
                128,
                deadline
            )
            .await?
            == b"sha1\n",
        "push source must use SHA-1"
    );
    for name in [
        "shallow",
        "info/grafts",
        "objects/info/alternates",
        "objects/info/http-alternates",
        "config.worktree",
        "commondir",
        "worktrees",
        "refs/replace",
    ] {
        ensure!(
            std::fs::symlink_metadata(owner.local.path().join(name))
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
            "push source has unsupported indirection or partial history"
        );
    }
    let config = owner
        .run(
            vec![
                "config".into(),
                "--local".into(),
                "--null".into(),
                "--list".into(),
            ],
            65536,
            deadline,
        )
        .await?;
    for item in config.split(|b| *b == 0).filter(|r| !r.is_empty()) {
        let key = item.split(|b| *b == b'\n').next().unwrap();
        ensure!(
            [
                b"core.repositoryformatversion".as_slice(),
                b"core.filemode",
                b"core.bare",
                b"core.logallrefupdates",
                b"extensions.objectformat"
            ]
            .contains(&key),
            "push source has unsupported local configuration"
        );
    }
    Ok(())
}
async fn reference(
    owner: &Arc<git::Owner>,
    pin: &str,
    deadline: Instant,
) -> Result<Option<String>> {
    let (symbolic, _) = owner
        .raw(
            vec!["symbolic-ref".into(), "--quiet".into(), pin.into()],
            4096,
            deadline,
        )
        .await?;
    ensure!(
        symbolic.code() == Some(1),
        "symbolic or unreadable private push pin preserved"
    );
    let (status, out) = owner
        .raw(
            vec![
                "rev-parse".into(),
                "--verify".into(),
                "--quiet".into(),
                "--end-of-options".into(),
                pin.into(),
            ],
            128,
            deadline,
        )
        .await?;
    if !status.success() {
        ensure!(out.is_empty(), "invalid private push pin");
        return Ok(None);
    }
    let oid = std::str::from_utf8(&out)?.trim_end_matches('\n');
    ensure!(valid_hex(oid, 40), "invalid private push pin");
    Ok(Some(oid.into()))
}
async fn remote(
    owner: &Arc<git::Owner>,
    o: &PushOptions,
    deadline: Instant,
) -> Result<Option<String>> {
    let (status, out) = owner
        .raw(
            vec![
                "ls-remote".into(),
                "--refs".into(),
                "--exit-code".into(),
                "--".into(),
                o.remote.clone(),
                o.remote_reference.clone(),
            ],
            8192,
            deadline,
        )
        .await?;
    if status.code() == Some(2) && out.is_empty() {
        return Ok(None);
    }
    ensure!(status.success(), "remote reference unavailable");
    let text = std::str::from_utf8(&out)?;
    let mut lines = text.lines();
    let line = lines.next().context("remote branch absent")?;
    ensure!(
        lines.next().is_none(),
        "remote returned unexpected references"
    );
    let (oid, r) = line.split_once('\t').context("invalid remote reference")?;
    ensure!(
        valid_hex(oid, 40) && r == o.remote_reference,
        "remote reference identity differs"
    );
    Ok(Some(oid.into()))
}
async fn pin_selected(
    owner: &Arc<git::Owner>,
    pin: &str,
    p: &PushPosition,
    deadline: Instant,
) -> Result<()> {
    match reference(owner, pin, deadline).await? {
        Some(oid) => ensure!(oid == p.git_head, "unknown private push pin preserved"),
        None => {
            owner
                .run(
                    vec![
                        "update-ref".into(),
                        "--no-deref".into(),
                        pin.into(),
                        p.git_head.clone(),
                        "0".repeat(40),
                    ],
                    1024,
                    deadline,
                )
                .await?;
        }
    }
    Ok(())
}
async fn clear_pin(
    owner: &Arc<git::Owner>,
    pin: &str,
    p: &PushPosition,
    deadline: Instant,
) -> Result<()> {
    if let Some(oid) = reference(owner, pin, deadline).await? {
        ensure!(oid == p.git_head, "unknown private push pin preserved");
        owner
            .run(
                vec![
                    "update-ref".into(),
                    "--no-deref".into(),
                    "-d".into(),
                    pin.into(),
                    p.git_head.clone(),
                ],
                1024,
                deadline,
            )
            .await?;
    }
    Ok(())
}
async fn verify(
    owner: &Arc<git::Owner>,
    o: &PushOptions,
    scope: [u8; 32],
    p: &PushPosition,
    deadline: Instant,
) -> Result<()> {
    let mapping = state::mapped_checkpoint(
        &o.export.state_dir,
        scope,
        &o.export.limits,
        &p.native_head,
        &p.git_head,
    )?;
    let bytes = owner
        .run(
            vec!["cat-file".into(), "commit".into(), p.git_head.clone()],
            usize::try_from(mapping.payload_size)?,
            deadline,
        )
        .await?;
    ensure!(
        bytes.len() as u64 == mapping.payload_size
            && *blake3::hash(&bytes).as_bytes() == mapping.payload_blake3,
        "delivered commit integrity differs"
    );
    owner
        .run(
            vec![
                "fsck".into(),
                "--strict".into(),
                "--no-reflogs".into(),
                "--no-dangling".into(),
                p.git_head.clone(),
            ],
            65536,
            deadline,
        )
        .await?;
    Ok(())
}
async fn run(
    config: tinytree_client::Config,
    o: PushOptions,
    mut stop: watch::Receiver<bool>,
) -> Result<PushStatus> {
    let disk = disk::Disk::open(&o.push_dir)?;
    let local = PrivateDir::open_existing(&o.export.destination_git)?;
    let program = program(&o.export.git_program)?;
    let (scope, _) = worker::scope_for_destination(&config, &o.export, local.path(), None)?;
    let id = identity(scope, &o, &program)?;
    let owner = Arc::new(git::Owner {
        disk: disk.clone(),
        local,
        program,
        cancel: stop.clone(),
    });
    let result = run_inner(&config, &o, scope, id, &disk, &owner, &mut stop).await;
    drain(&owner).await;
    while Arc::strong_count(&disk) > 2 {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    result
}
async fn run_inner(
    config: &tinytree_client::Config,
    o: &PushOptions,
    scope: [u8; 32],
    id: [u8; 32],
    disk: &Arc<disk::Disk>,
    owner: &Arc<git::Owner>,
    stop: &mut watch::Receiver<bool>,
) -> Result<PushStatus> {
    let pin = format!("refs/tinytree-backup/{}/pending", hex(&id));
    let mut s = disk.load(id)?.unwrap_or_else(|| PushStatus::initial(o));
    s.poll_interval_ms = ms(o.poll_interval);
    s.operation_timeout_ms = ms(o.operation_timeout);
    s.max_backoff_ms = ms(o.max_backoff);
    s.phase = PushPhase::Starting;
    s.lag_at_last_observation = None;
    disk.save(id, &mut s).await?;
    let mut verified = None;
    let mut next = Instant::now()
        + Duration::from_millis(
            s.next_retry_unix_ms
                .unwrap_or(0)
                .saturating_sub(now_ms()?)
                .min(ms(o.max_backoff)),
        );
    loop {
        tokio::select! {biased;_=stopped(stop)=>break,_=tokio::time::sleep_until(next)=>{}}
        let result: Result<(), PushError> = async {
            let current =
                worker::scope_for_destination(config, &o.export, owner.local.path(), None)
                    .map_err(|_| PushError::IdentityChanged)?
                    .0;
            if current != scope {
                return Err(PushError::IdentityChanged);
            }
            local_policy(owner, Instant::now() + o.operation_timeout)
                .await
                .map_err(|_| PushError::CheckpointUnavailable)?;
            let checkpoint =
                state::delivered_checkpoint(&o.export.state_dir, scope, &o.export.limits)
                    .map_err(|_| PushError::CheckpointUnavailable)?;
            if let Some((p, _)) = checkpoint {
                let desired = PushPosition {
                    generation: p.generation,
                    native_head: p.native,
                    git_head: p.git,
                };
                if s.desired.as_ref().is_some_and(|old| {
                    desired.generation < old.generation
                        || (desired.generation == old.generation && &desired != old)
                }) {
                    return Err(PushError::CheckpointRegressed);
                }
                s.desired = Some(desired);
                s.lag_at_last_observation = None;
            } else {
                return Err(PushError::CheckpointUnavailable);
            }
            if let Some(p) = s.pending.clone() {
                if p.acknowledged {
                    #[cfg(test)]
                    if o.after_ack {
                        return Err(PushError::PushUnconfirmed);
                    }
                    let deadline = Instant::now() + o.operation_timeout;
                    verify(owner, o, scope, &p.position, deadline)
                        .await
                        .map_err(|_| PushError::CheckpointUnavailable)?;
                    let observed = remote(owner, o, deadline)
                        .await
                        .map_err(|_| PushError::RemoteUnavailable)?;
                    s.last_remote_check_unix_ms =
                        Some(now_ms().map_err(|_| PushError::RemoteUnavailable)?);
                    if observed.as_deref() != Some(p.position.git_head.as_str()) {
                        return Err(PushError::RemoteDiverged);
                    }
                    clear_pin(
                        owner,
                        &pin,
                        &p.position,
                        Instant::now() + o.operation_timeout,
                    )
                    .await
                    .map_err(|_| PushError::PushUnconfirmed)?;
                    s.pending = None;
                    disk.save(id, &mut s)
                        .await
                        .map_err(|_| PushError::PushUnconfirmed)?;
                    verified = None;
                }
            }
            if s.pending.is_none() {
                let Some(desired) = s.desired.clone() else {
                    return Err(PushError::CheckpointUnavailable);
                };
                if s.delivered.as_ref() == Some(&desired) {
                    let observed = remote(owner, o, Instant::now() + o.operation_timeout)
                        .await
                        .map_err(|_| PushError::RemoteUnavailable)?;
                    s.last_remote_check_unix_ms =
                        Some(now_ms().map_err(|_| PushError::RemoteUnavailable)?);
                    if observed.as_deref() != Some(desired.git_head.as_str()) {
                        return Err(PushError::RemoteDiverged);
                    }
                    return Ok(());
                }
                s.pending = Some(PendingPush {
                    position: desired,
                    acknowledged: false,
                });
                disk.save(id, &mut s)
                    .await
                    .map_err(|_| PushError::PushUnconfirmed)?;
            }
            let p = s.pending.as_ref().unwrap().position.clone();
            let deadline = Instant::now() + o.operation_timeout;
            local_policy(owner, deadline)
                .await
                .map_err(|_| PushError::CheckpointUnavailable)?;
            if verified.as_ref() != Some(&p.git_head) {
                s.phase = PushPhase::Verifying;
                disk.save(id, &mut s)
                    .await
                    .map_err(|_| PushError::PushUnconfirmed)?;
                pin_selected(owner, &pin, &p, deadline)
                    .await
                    .map_err(|_| PushError::CheckpointUnavailable)?;
                verify(owner, o, scope, &p, deadline)
                    .await
                    .map_err(|_| PushError::CheckpointUnavailable)?;
                verified = Some(p.git_head.clone());
            }
            let observed = remote(owner, o, deadline)
                .await
                .map_err(|_| PushError::RemoteUnavailable)?;
            s.last_remote_check_unix_ms = Some(now_ms().map_err(|_| PushError::RemoteUnavailable)?);
            if observed.as_deref() != Some(p.git_head.as_str()) {
                if observed.as_deref() != s.delivered.as_ref().map(|p| p.git_head.as_str()) {
                    return Err(PushError::RemoteDiverged);
                }
                s.phase = PushPhase::Pushing;
                s.attempts = s.attempts.saturating_add(1);
                disk.save(id, &mut s)
                    .await
                    .map_err(|_| PushError::PushUnconfirmed)?;
                let _ = owner
                    .run(
                        vec![
                            "push".into(),
                            "--porcelain".into(),
                            "--no-follow-tags".into(),
                            "--no-verify".into(),
                            "--recurse-submodules=no".into(),
                            "--".into(),
                            o.remote.clone(),
                            format!("{}:{}", p.git_head, o.remote_reference),
                        ],
                        65536,
                        deadline,
                    )
                    .await;
                s.phase = PushPhase::Draining;
                disk.save(id, &mut s)
                    .await
                    .map_err(|_| PushError::PushUnconfirmed)?;
                drain(owner).await;
                #[cfg(test)]
                if o.after_push {
                    return Err(PushError::PushUnconfirmed);
                }
                // Fresh bounded readback reconciles success even after an error/timeout.
                if remote(owner, o, Instant::now() + o.operation_timeout)
                    .await
                    .map_err(|_| PushError::RemoteUnavailable)?
                    .as_deref()
                    != Some(p.git_head.as_str())
                {
                    return Err(PushError::PushUnconfirmed);
                }
                s.last_remote_check_unix_ms =
                    Some(now_ms().map_err(|_| PushError::RemoteUnavailable)?);
            }
            s.delivered = Some(p.clone());
            s.pending.as_mut().unwrap().acknowledged = true;
            disk.save(id, &mut s)
                .await
                .map_err(|_| PushError::PushUnconfirmed)?;
            #[cfg(test)]
            if o.after_ack {
                return Err(PushError::PushUnconfirmed);
            }
            drain(owner).await;
            clear_pin(owner, &pin, &p, Instant::now() + o.operation_timeout)
                .await
                .map_err(|_| PushError::PushUnconfirmed)?;
            s.pending = None;
            verified = None;
            disk.save(id, &mut s)
                .await
                .map_err(|_| PushError::PushUnconfirmed)?;
            Ok(())
        }
        .await;
        drain(owner).await;
        if *stop.borrow() {
            break;
        }
        match result {
            Ok(()) => s.success(),
            Err(error) => s.fail(error)?,
        }
        disk.save(id, &mut s).await?;
        next = Instant::now()
            + Duration::from_millis(
                s.next_retry_unix_ms
                    .map(|n| n.saturating_sub(now_ms().unwrap_or(0)))
                    .unwrap_or(ms(o.poll_interval))
                    .min(ms(o.max_backoff)),
            );
    }
    s.phase = PushPhase::Draining;
    s.lag_at_last_observation = None;
    disk.save(id, &mut s).await?;
    drain(owner).await;
    s.phase = PushPhase::Stopped;
    disk.save(id, &mut s).await?;
    Ok(s)
}
