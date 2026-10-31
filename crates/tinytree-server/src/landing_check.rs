//! Operator-owned checker orchestration. Durable obligations are independent
//! of current execution configuration and veto expiry until cleanup is proven.
use crate::{config::ConfigError, landing::LandingProfile};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tinytree_check_runner as runner;
use tinytree_proto::landing_v1::{CheckerErrorCode, CheckerProgress, CheckerStage};
use tinytree_store as s;
use tokio::sync::{Notify, Semaphore, watch};

const MAX_RECORD: usize = 64 * 1024;
const MAX_JOBS: usize = 1024;
const MAX_CATALOG: usize = 64 * 1024 * 1024;

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LandingCheckerConfig {
    pub enabled: bool,
    pub executable: PathBuf,
    pub sha256: String,
    pub config: PathBuf,
    pub max_checks: usize,
    pub max_checks_per_repository: usize,
    pub max_jobs: usize,
    pub max_attempts: usize,
    pub max_recovery_attempts: usize,
    pub retry_delay_seconds: u64,
    pub execution_timeout_seconds: u64,
    pub cleanup_grace_seconds: u64,
}
impl Default for LandingCheckerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            executable: PathBuf::new(),
            sha256: String::new(),
            config: PathBuf::new(),
            max_checks: 2,
            max_checks_per_repository: 1,
            max_jobs: 128,
            max_attempts: 3,
            max_recovery_attempts: 64,
            retry_delay_seconds: 10,
            execution_timeout_seconds: 3600,
            cleanup_grace_seconds: 600,
        }
    }
}
impl LandingCheckerConfig {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if !(1..=8).contains(&self.max_checks)
            || !(1..=self.max_checks).contains(&self.max_checks_per_repository)
            || !(1..=MAX_JOBS).contains(&self.max_jobs)
            || !(1..=16).contains(&self.max_attempts)
            || !(1..=256).contains(&self.max_recovery_attempts)
            || !(1..=600).contains(&self.retry_delay_seconds)
            || !(10..=86400).contains(&self.execution_timeout_seconds)
            || !(10..=3600).contains(&self.cleanup_grace_seconds)
        {
            return Err(ConfigError("invalid landing checker limits"));
        }
        if self.enabled
            && (!absolute(&self.executable) || !absolute(&self.config) || !hex(&self.sha256))
        {
            return Err(ConfigError("invalid landing checker authority"));
        }
        Ok(())
    }
}
fn absolute(path: &Path) -> bool {
    path.is_absolute()
        && path.as_os_str().len() <= 4096
        && !path.components().any(|c| {
            matches!(
                c,
                std::path::Component::ParentDir | std::path::Component::CurDir
            )
        })
}
fn hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn id(id: [u8; 32]) -> String {
    blake3::Hash::from(id).to_hex().to_string()
}
fn now() -> Result<i64> {
    crate::service::now().map_err(Into::into)
}
fn nonce() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}
type Key = (String, [u8; 32]);

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Authority {
    binary: String,
    configuration: String,
    targets: Vec<String>,
    tests: bool,
    environment: Vec<String>,
    timeout: u64,
    grace: u64,
    retry: u64,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Phase {
    Reserved,
    DispatchIntent,
    Uncertain,
    Verified,
    RetryReady,
    Terminal,
    UnavailableReady,
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum UnavailableReason {
    PermanentUnavailability,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    repository: String,
    job: [u8; 32],
    request: runner::Request,
    authority: Authority,
    phase: Phase,
    receipt: Option<runner::Receipt>,
    verdict_eligible: bool,
    attempts: usize,
    recoveries: usize,
    retry_at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    unavailable_reason: Option<UnavailableReason>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u32,
    record: Record,
    checksum: String,
}
fn decode_record(bytes: &[u8]) -> Result<Record> {
    let envelope: Envelope = serde_json::from_slice(bytes)?;
    ensure!(
        matches!(envelope.version, 1 | 2)
            && envelope.version == envelope.record.version
            && envelope.checksum == digest(&serde_json::to_vec(&envelope.record)?),
        "checker journal checksum differs"
    );
    envelope.record.validate()?;
    Ok(envelope.record)
}

impl Record {
    fn safe_unavailable(&self) -> bool {
        self.verdict_eligible
            && self.receipt.as_ref().is_some_and(|receipt| {
                receipt.cleanup_confirmed && receipt.outcome == runner::Outcome::Unavailable
            })
    }
    fn uncertain(&self) -> bool {
        matches!(self.phase, Phase::DispatchIntent | Phase::Uncertain)
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            ((self.version == 1
                && self.unavailable_reason.is_none()
                && self.phase != Phase::UnavailableReady)
                || (self.version == 2
                    && matches!(self.phase, Phase::UnavailableReady | Phase::Terminal)
                    && self.unavailable_reason
                        == Some(UnavailableReason::PermanentUnavailability)
                    && self.safe_unavailable()))
                && s::validate_repository_id(&self.repository).is_ok()
                && self.request.repository_id == self.repository
                && self.request.job_id == id(self.job),
            "invalid checker journal scope"
        );
        self.request.validate()?;
        ensure!(
            hex(&self.authority.binary)
                && hex(&self.authority.configuration)
                && self.authority.targets.len() <= 128
                && self
                    .authority
                    .targets
                    .iter()
                    .map(String::len)
                    .sum::<usize>()
                    <= 8192
                && self.attempts > 0
                && self.attempts <= 16
                && self.recoveries <= 256
                && (10..=86400).contains(&self.authority.timeout)
                && (10..=3600).contains(&self.authority.grace)
                && (1..=600).contains(&self.authority.retry),
            "invalid checker journal bounds"
        );
        if let Some(receipt) = &self.receipt {
            validate_receipt(receipt, &self.request, &self.authority)?;
        }
        ensure!(
            self.phase != Phase::Verified
                || (self.verdict_eligible
                    && self.receipt.as_ref().is_some_and(|r| r.cleanup_confirmed
                        && matches!(r.outcome, runner::Outcome::Passed | runner::Outcome::Failed))),
            "unverified checker journal"
        );
        ensure!(
            self.phase != Phase::RetryReady
                || self.receipt.as_ref().is_some_and(|r| r.cleanup_confirmed
                    && (r.outcome == runner::Outcome::Unavailable || !self.verdict_eligible)),
            "retry lacks confirmed cleanup"
        );
        Ok(())
    }
}
struct Slot {
    directory: PrivateDir,
    record: Record,
    root: Option<s::RootReadGuard>,
    active: bool,
    handle: Option<s::LandingHandle>,
    diagnostic: CheckerProgress,
}
struct State {
    slots: BTreeMap<Key, Slot>,
    // One exact-keyed ephemeral admission failure, never a cross-job history.
    reservation_error: Option<(Key, CheckerProgress)>,
}

struct Current {
    config: LandingCheckerConfig,
    runner: runner::Config,
    bytes: Vec<u8>,
}
pub(crate) struct CheckerHost {
    store: s::Store,
    directory: PrivateDir,
    _owner: fs::File,
    state: Mutex<State>,
    current: Option<Current>,
    policy: LandingCheckerConfig,
    profiles: Vec<LandingProfile>,
    forge_profile: Option<String>,
    landing_enabled: bool,
    repositories: Vec<String>,
    global: Arc<Semaphore>,
    per_repo: Mutex<HashMap<String, Arc<Semaphore>>>,
    active: AtomicUsize,
    closing: AtomicBool,
    changed: Notify,
    wake: Arc<Notify>,
    cancel: watch::Sender<bool>,
    worker: Mutex<Option<tokio::task::JoinHandle<()>>>,
    admission: Mutex<Option<std::sync::Weak<crate::landing::LandingHost>>>,
    memory: Mutex<Option<tokio::sync::OwnedSemaphorePermit>>,
}

fn read_private(path: &Path, limit: usize) -> Result<Vec<u8>> {
    ensure!(absolute(path), "invalid checker configuration path");
    let directory =
        PrivateDir::open_existing(path.parent().context("checker config parent missing")?)?;
    Ok(directory.read(
        path.file_name()
            .context("checker config filename missing")?,
        limit,
    )?)
}
fn digest(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}
fn candidate_key(request: &runner::Request) -> Result<tinytree_model::ObjectKey> {
    let bytes = blake3::Hash::from_hex(&request.candidate_commit)?
        .as_bytes()
        .to_owned();
    Ok(tinytree_model::ObjectKey {
        kind: tinytree_model::Kind::Commit,
        id: tinytree_model::ObjectId(bytes),
    })
}
fn validate_receipt(
    receipt: &runner::Receipt,
    request: &runner::Request,
    authority: &Authority,
) -> Result<()> {
    receipt.validate_for(request)?;
    if matches!(
        receipt.outcome,
        runner::Outcome::Passed | runner::Outcome::Failed
    ) {
        ensure!(
            receipt.cleanup_confirmed,
            "verdict cleanup is not confirmed"
        );
        let evidence = receipt
            .evidence
            .as_ref()
            .context("verdict lacks check evidence")?;
        ensure!(
            evidence.requested_targets == authority.targets,
            "checker targets differ from frozen policy"
        );
        if receipt.outcome == runner::Outcome::Passed {
            ensure!(
                evidence.bazel_exit_code == 0 && (!authority.tests || evidence.tests_verified),
                "passing checker policy differs"
            )
        } else {
            ensure!(
                matches!(evidence.bazel_exit_code, 1 | 3),
                "negative checker lacks completed build/test failure"
            )
        }
    }
    Ok(())
}

impl CheckerHost {
    pub(crate) fn open(
        store: s::Store,
        configuration: Option<LandingCheckerConfig>,
        profiles: Vec<LandingProfile>,
        repositories: &[String],
        landing_enabled: bool,
        forge_profile: Option<String>,
    ) -> Result<Option<Arc<Self>>, ConfigError> {
        let settings = configuration.clone().unwrap_or_default();
        settings.validate()?;
        if settings.enabled && !cfg!(target_os = "linux") {
            return Err(ConfigError("local landing checks require Linux"));
        }
        let current = if settings.enabled && landing_enabled {
            let bytes = read_private(&settings.config, 65536)
                .map_err(|_| ConfigError("cannot read landing checker policy"))?;
            let runner: runner::Config = toml::from_str(
                std::str::from_utf8(&bytes)
                    .map_err(|_| ConfigError("invalid checker policy encoding"))?,
            )
            .map_err(|_| ConfigError("invalid checker TOML policy"))?;
            runner
                .validate()
                .map_err(|_| ConfigError("invalid checker command policy"))?;
            if runner.runner_sha256 != settings.sha256 {
                return Err(ConfigError("checker executable pin differs from policy"));
            }
            for profile in &profiles {
                let selected = runner
                    .profile(&profile.name)
                    .map_err(|_| ConfigError("landing checker profile unavailable"))?;
                if runner
                    .profile_version(&profile.name)
                    .map_err(|_| ConfigError("cannot fingerprint checker profile"))?
                    != profile.version
                    || (forge_profile.as_ref() == Some(&profile.name) && !selected.forge_accounts)
                    || profile
                        .repositories
                        .iter()
                        .any(|repo| !selected.repositories.contains(repo))
                {
                    return Err(ConfigError("landing profile differs from checker policy"));
                }
            }
            runner::Binary {
                path: settings.executable.clone(),
                sha256: settings.sha256.clone(),
            }
            .verify()
            .map_err(|_| ConfigError("landing checker executable pin differs"))?;
            Some(Current {
                config: settings.clone(),
                runner,
                bytes,
            })
        } else {
            None
        };
        let path = if current.is_some() {
            Some(
                store
                    .enable_external_checks()
                    .map_err(|_| ConfigError("cannot activate external landing checks"))?,
            )
        } else {
            store
                .external_checks_directory()
                .map_err(|_| ConfigError("external checker state is unavailable"))?
        };
        let Some(path) = path else { return Ok(None) };
        let directory = PrivateDir::open_existing(&path)
            .map_err(|_| ConfigError("invalid external checker directory"))?;
        let owner = directory
            .open_lock("journal.lock")
            .map_err(|_| ConfigError("invalid checker ownership lock"))?;
        owner
            .try_lock_exclusive()
            .map_err(|_| ConfigError("another landing check coordinator owns this store"))?;
        let jobs = PrivateDir::open_existing(path.join("jobs"))
            .map_err(|_| ConfigError("invalid checker jobs directory"))?;
        recover_staging(&jobs)
            .map_err(|_| ConfigError("invalid unpublished checker reservation"))?;
        let mut slots = BTreeMap::new();
        let mut total = 0;
        for entry in
            fs::read_dir(jobs.path()).map_err(|_| ConfigError("cannot read checker catalog"))?
        {
            let entry = entry.map_err(|_| ConfigError("cannot read checker catalog entry"))?;
            if slots.len() >= MAX_JOBS
                || !entry
                    .file_type()
                    .map_err(|_| ConfigError("invalid checker job type"))?
                    .is_dir()
            {
                return Err(ConfigError("invalid or excessive checker catalog"));
            }
            let child = PrivateDir::open_existing(entry.path())
                .map_err(|_| ConfigError("invalid private checker job"))?;
            let bytes = child
                .read("state.json", MAX_RECORD)
                .map_err(|_| ConfigError("checker job state missing or oversized"))?;
            total += bytes.len();
            if total > MAX_CATALOG {
                return Err(ConfigError("checker catalog exceeds memory bound"));
            }
            let record =
                decode_record(&bytes).map_err(|_| ConfigError("invalid checker job state"))?;
            record
                .validate()
                .map_err(|_| ConfigError("invalid checker job binding"))?;
            if !(repositories.contains(&record.repository)
                || (store
                    .forge_account_repositories(1024)
                    .map_err(|_| ConfigError("Forge account discovery failed"))?
                    .contains(&record.repository)))
                || entry.file_name().to_str()
                    != Some(job_name(&record.repository, record.job).as_str())
            {
                return Err(ConfigError("checker job scope differs"));
            }
            let root = if record.uncertain() {
                Some(
                    store
                        .pin_root(
                            &record.repository,
                            candidate_key(&record.request)
                                .map_err(|_| ConfigError("invalid checker candidate"))?,
                        )
                        .map_err(|_| ConfigError("uncertain checker candidate is unavailable"))?,
                )
            } else {
                None
            };
            slots.insert(
                (record.repository.clone(), record.job),
                Slot {
                    directory: child,
                    record,
                    root,
                    active: false,
                    handle: None,
                    diagnostic: CheckerProgress::default(),
                },
            );
        }
        let (cancel, _) = watch::channel(false);
        Ok(Some(Arc::new(Self {
            store,
            directory,
            _owner: owner,
            state: Mutex::new(State {
                slots,
                reservation_error: None,
            }),
            current,
            policy: settings.clone(),
            profiles,
            forge_profile,
            landing_enabled,
            repositories: repositories.to_vec(),
            global: Arc::new(Semaphore::new(settings.max_checks)),
            per_repo: Mutex::new(
                repositories
                    .iter()
                    .map(|repo| {
                        (
                            repo.clone(),
                            Arc::new(Semaphore::new(settings.max_checks_per_repository)),
                        )
                    })
                    .collect(),
            ),
            active: AtomicUsize::new(0),
            closing: AtomicBool::new(false),
            changed: Notify::new(),
            wake: Arc::new(Notify::new()),
            cancel,
            worker: Mutex::new(None),
            admission: Mutex::new(None),
            memory: Mutex::new(None),
        })))
    }
    pub(crate) fn catalog_work() -> u64 {
        MAX_CATALOG as u64 * 8 + 16 * 1024 * 1024
    }
    pub(crate) fn attach(
        &self,
        host: std::sync::Weak<crate::landing::LandingHost>,
        memory: tokio::sync::OwnedSemaphorePermit,
    ) {
        *self.admission.lock().expect("new checker admission") = Some(host);
        *self.memory.lock().expect("new checker memory") = Some(memory);
    }
    fn configured(&self, repo: &str, profile: &s::LandingCheckProfile) -> bool {
        self.landing_enabled
            && self.current.is_some()
            && self.profiles.iter().any(|p| {
                p.name == profile.name
                    && p.version == profile.version
                    && (p.repositories.iter().any(|r| r == repo)
                        || (self.forge_profile.as_ref() == Some(&p.name)
                            && self
                                .store
                                .forge_account_repositories(1024)
                                .is_ok_and(|repos| repos.iter().any(|r| r == repo))))
            })
    }
    /// The decision and Store transition are covered by the same gate used to
    /// persist dispatch intents. A precheck followed by an unlocked expiry is
    /// insufficient across a concurrently starting physical check.
    pub(crate) fn expire_if_safe(
        &self,
        pending: &s::PendingLanding,
        store: &s::Store,
        repo: &str,
        now: i64,
    ) -> s::Result<()> {
        let state = self.state.lock().map_err(|_| s::Error::Poisoned)?;
        if state
            .slots
            .get(&(repo.to_owned(), pending.id))
            .is_some_and(|slot| slot.active || slot.record.uncertain())
        {
            return Ok(());
        }
        pending.handle.expire(store, now)?;
        Ok(())
    }
    fn save(slot: &Slot) -> Result<()> {
        slot.record.validate()?;
        let bytes = serde_json::to_vec(&Envelope {
            version: slot.record.version,
            record: slot.record.clone(),
            checksum: digest(&serde_json::to_vec(&slot.record)?),
        })?;
        ensure!(bytes.len() <= MAX_RECORD, "checker journal exceeds bound");
        slot.directory.replace("state.json", &bytes)?;
        Ok(())
    }
    fn refresh(&self, repo: &str, profile: &s::LandingCheckProfile) -> Result<&Current> {
        ensure!(
            self.configured(repo, profile),
            "checker profile is no longer configured"
        );
        let current = self.current.as_ref().context("checker disabled")?;
        ensure!(
            read_private(&current.config.config, 65536)? == current.bytes,
            "checker policy changed"
        );
        ensure!(
            current.runner.profile_version(&profile.name)? == profile.version,
            "checker profile fingerprint changed"
        );
        runner::Binary {
            path: current.config.executable.clone(),
            sha256: current.config.sha256.clone(),
        }
        .verify()?;
        Ok(current)
    }
    fn pin_executable(&self, current: &Current) -> Result<PathBuf> {
        let binaries = PrivateDir::open_existing(self.directory.path().join("binaries"))?;
        let path = binaries.path().join(&current.config.sha256);
        if path.exists() {
            runner::Binary {
                path: path.clone(),
                sha256: current.config.sha256.clone(),
            }
            .verify()?;
            return Ok(path);
        }
        ensure!(
            fs::read_dir(binaries.path())?.count() < 16,
            "checker executable snapshot quota reached"
        );
        let temporary = binaries.path().join(format!(".pending-{}", nonce()));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o500).custom_flags(libc::O_NOFOLLOW);
        }
        let mut output = options.open(&temporary)?;
        let input = fs::File::open(&current.config.executable)?;
        std::io::copy(&mut input.take(1024 * 1024 * 1024 + 1), &mut output)?;
        output.flush()?;
        output.sync_all()?;
        runner::Binary {
            path: temporary.clone(),
            sha256: current.config.sha256.clone(),
        }
        .verify()?;
        fs::rename(temporary, &path)?;
        fs::File::open(binaries.path())?.sync_all()?;
        Ok(path)
    }
    fn reserve_new(
        &self,
        repo: &str,
        pending: &s::PendingLanding,
        stage: &mut CheckerStage,
    ) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("checker gate poisoned"))?;
        if state.slots.contains_key(&(repo.to_owned(), pending.id)) {
            return Ok(());
        }
        *stage = CheckerStage::ReservationDirectory;
        let jobs = PrivateDir::open_existing(self.directory.path().join("jobs"))?;
        recover_staging(&jobs)?;
        let published = jobs.path().join(job_name(repo, pending.id));
        if published.exists() {
            let directory = PrivateDir::open_existing(published)?;
            let record = decode_record(&directory.read("state.json", MAX_RECORD)?)?;
            ensure!(
                record.repository == repo
                    && record.job == pending.id
                    && record.phase == Phase::Reserved,
                "unexpected published checker reservation"
            );
            state.slots.insert(
                (repo.to_owned(), pending.id),
                Slot {
                    directory,
                    record,
                    root: None,
                    active: false,
                    handle: Some(pending.handle.clone()),
                    diagnostic: CheckerProgress::default(),
                },
            );
            return Ok(());
        }
        *stage = CheckerStage::AuthorityRefresh;
        let current = self.refresh(repo, &pending.profile)?;
        ensure!(
            state.slots.len() < current.config.max_jobs,
            "checker catalog quota reached"
        );
        let mut physical = 0;
        for item in fs::read_dir(jobs.path())? {
            item?;
            physical += 1;
            ensure!(
                physical < current.config.max_jobs && physical < MAX_JOBS,
                "checker namespace quota reached"
            );
        }
        let job = pending.handle.status(&self.store)?;
        ensure!(
            job.state == s::LandingState::Prepared,
            "candidate is not prepared"
        );
        *stage = CheckerStage::CandidatePin;
        let lease = pending.handle.pin_candidate(&self.store, now()?)?;
        let identity = lease.identity();
        let request = runner::Request {
            schema: 1,
            job_id: id(job.id),
            attempt_id: nonce(),
            repository_id: repo.to_owned(),
            candidate_commit: id(identity.candidate.0),
            root_tree: id(identity.root_tree.0),
            profile_name: identity.profile.name.clone(),
            profile_version: identity.profile.version.clone(),
        };
        let profile = current.runner.profile(&identity.profile.name)?;
        let record = Record {
            version: 1,
            repository: repo.into(),
            job: job.id,
            request: request.clone(),
            authority: Authority {
                binary: current.config.sha256.clone(),
                configuration: digest(&current.bytes),
                targets: profile.targets.clone(),
                tests: profile.mode == runner::Mode::Test,
                environment: current.runner.workspace_environment.clone(),
                timeout: current.config.execution_timeout_seconds,
                grace: current.config.cleanup_grace_seconds,
                retry: current.config.retry_delay_seconds,
            },
            phase: Phase::Reserved,
            receipt: None,
            verdict_eligible: false,
            attempts: 1,
            recoveries: 0,
            retry_at: 0,
            unavailable_reason: None,
        };
        let staging_name = format!(
            ".pending-{}-{}",
            job_name(repo, pending.id),
            request.attempt_id
        );
        *stage = CheckerStage::ReservationPersist;
        let directory = PrivateDir::create(jobs.path().join(&staging_name))?;
        let root = Some(self.store.pin_root(repo, candidate_key(&request)?)?);
        let mut slot = Slot {
            directory,
            record,
            root,
            active: false,
            handle: Some(pending.handle.clone()),
            diagnostic: CheckerProgress::default(),
        };
        Self::save(&slot)?;
        let target = jobs.path().join(job_name(repo, pending.id));
        publish_reservation(slot.directory.path(), &target)?;
        fs::File::open(jobs.path())?.sync_all()?;
        slot.directory = PrivateDir::open_existing(target)?;
        state.slots.insert((repo.to_owned(), pending.id), slot);
        Ok(())
    }
}
fn job_name(repo: &str, job: [u8; 32]) -> String {
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-check-job-v1\0");
    hash.update(repo.as_bytes());
    hash.update(&job);
    hash.finalize().to_hex().to_string()
}

struct CheckWork {
    host: Arc<CheckerHost>,
    key: Key,
    _global: tokio::sync::OwnedSemaphorePermit,
    _repository: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for CheckWork {
    fn drop(&mut self) {
        if let Ok(mut state) = self.host.state.lock() {
            if let Some(slot) = state.slots.get_mut(&self.key) {
                slot.active = false;
                if !slot.record.uncertain() {
                    slot.root = None;
                }
            }
        }
        self.host.active.fetch_sub(1, Ordering::AcqRel);
        self.host.changed.notify_waiters();
        self.host.wake.notify_one();
    }
}
impl CheckerHost {
    fn schedule(
        self: &Arc<Self>,
        repo: &str,
        pending: Option<&s::PendingLanding>,
        existing: Option<Key>,
    ) -> bool {
        if self.closing.load(Ordering::Acquire) {
            return true;
        }
        let key = existing.unwrap_or_else(|| {
            (
                repo.to_owned(),
                pending.expect("pending or existing job").id,
            )
        });
        if let Some(pending) = pending {
            let known = self
                .state
                .lock()
                .map(|state| state.slots.contains_key(&key))
                .unwrap_or(true);
            if !known {
                if pending.state != s::LandingState::Prepared
                    || !self.configured(repo, &pending.profile)
                {
                    return true;
                }
                let mut stage = CheckerStage::Reservation;
                if self.reserve_new(repo, pending, &mut stage).is_err() {
                    if let Ok(mut state) = self.state.lock() {
                        state.reservation_error = Some((
                            key.clone(),
                            CheckerProgress {
                                stage: stage as i32,
                                error_stage: stage as i32,
                                error_code: CheckerErrorCode::ReservationFailed as i32,
                                ..Default::default()
                            },
                        ));
                    }
                    return true;
                }
            }
        }
        let Ok(global) = self.global.clone().try_acquire_owned() else {
            return false;
        };
        let repository = {
            let Ok(mut repos) = self.per_repo.lock() else {
                return true;
            };
            if !repos.contains_key(repo)
                && repos.len() >= self.repositories.len().saturating_add(1024)
            {
                return true;
            }
            repos
                .entry(repo.to_owned())
                .or_insert_with(|| Arc::new(Semaphore::new(self.policy.max_checks_per_repository)))
                .clone()
        };
        let Ok(repository) = repository.try_acquire_owned() else {
            return false;
        };
        let Ok(mut state) = self.state.lock() else {
            return true;
        };
        let Some(slot) = state.slots.get_mut(&key) else {
            return true;
        };
        if let Some(pending) = pending {
            slot.handle = Some(pending.handle.clone());
        }
        if slot.active
            || matches!(slot.record.phase, Phase::Terminal | Phase::UnavailableReady)
            || slot.record.retry_at > now().unwrap_or(i64::MAX)
        {
            return true;
        }
        if slot.record.uncertain() && slot.record.recoveries >= self.policy.max_recovery_attempts {
            return true;
        }
        if !slot.record.uncertain() && slot.handle.is_none() {
            return true;
        }
        slot.active = true;
        slot.diagnostic = CheckerProgress {
            stage: CheckerStage::Queued as i32,
            ..Default::default()
        };
        self.active.fetch_add(1, Ordering::AcqRel);
        drop(state);
        let work = CheckWork {
            host: self.clone(),
            key: key.clone(),
            _global: global,
            _repository: repository,
        };
        let host = self.clone();
        drop(tokio::spawn(async move {
            let _work = work;
            if host.process(key.clone()).await.is_err() {
                host.process_error(&key, CheckerErrorCode::ProcessFailed);
            }
        }));
        true
    }
    pub(crate) fn start(self: &Arc<Self>) -> Result<(), tonic::Status> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| tonic::Status::unavailable("checker worker unavailable"))?;
        if worker.is_some() {
            return Ok(());
        }
        let weak = Arc::downgrade(self);
        let wake = self.wake.clone();

        *worker = Some(tokio::spawn(async move {
            let initial = || {
                weak.upgrade()
                    .map(|host| {
                        let mut repos = host.repositories.clone();
                        if host.forge_profile.is_some() {
                            if let Ok(accounts) = host.store.forge_account_repositories(1024) {
                                repos.extend(accounts);
                            }
                        }
                        repos.sort();
                        repos.dedup();
                        repos
                    })
                    .unwrap_or_default()
                    .into_iter()
                    .map(|repo| (repo, None))
                    .collect::<VecDeque<(String, Option<s::LandingScanCursor>)>>()
            };
            let mut pages = initial();
            let mut ready: VecDeque<(String, s::PendingLanding)> = VecDeque::new();
            let mut recovery_after = None;
            let mut unavailable_after = None;
            loop {
                let Some(host) = weak.upgrade() else { break };
                if host.closing.load(Ordering::Acquire) {
                    break;
                }
                // Terminal is an absorbing dispatch decision, not a frozen
                // attempt-limit claim. Replay only safe, durably saved cleanup
                // receipts; this awaited worker participates in host drain.
                let unavailable = host.state.lock().ok().and_then(|state| {
                    state
                        .slots
                        .iter()
                        .find(|(key, slot)| {
                            !slot.active
                                && !slot.record.uncertain()
                                && (slot.record.phase == Phase::UnavailableReady
                                    || (slot.record.phase == Phase::Terminal
                                        && slot.record.version == 1
                                        && slot.record.safe_unavailable()))
                                && unavailable_after.as_ref().is_none_or(|after| *key > after)
                        })
                        .map(|(key, _)| key.clone())
                });
                if let Some(key) = unavailable {
                    let selected = key.clone();
                    let owned = host.clone();
                    let _ = tokio::task::spawn_blocking(move || owned.apply_unavailable(&selected))
                        .await;
                    unavailable_after = Some(key);
                } else {
                    unavailable_after = None;
                }
                let recovery = host.state.lock().ok().and_then(|state| {
                    let select = |key: &Key, slot: &Slot| {
                        slot.record.uncertain()
                            && !slot.active
                            && slot.record.retry_at <= now().unwrap_or(i64::MIN)
                            && recovery_after.as_ref().is_none_or(|after| key > after)
                    };
                    state
                        .slots
                        .iter()
                        .find(|(key, slot)| select(key, slot))
                        .map(|(key, _)| key.clone())
                });
                let mut blocked = false;
                if let Some(key) = recovery {
                    if host.schedule(&key.0, None, Some(key.clone())) {
                        recovery_after = Some(key);
                    } else {
                        blocked = true
                    }
                } else {
                    recovery_after = None;
                }
                if let Some((repo, pending)) = ready.front() {
                    if host.schedule(repo, Some(pending), None) {
                        ready.pop_front();
                    } else {
                        blocked = true
                    }
                }
                if ready.is_empty() && !blocked {
                    if let Some((repo, after)) = pages.pop_front() {
                        let store = host.store.clone();
                        let queried = repo.clone();
                        let page = tokio::task::spawn_blocking(move || {
                            store.scan_pending_landings(
                                &queried,
                                after.as_ref(),
                                s::LandingScanLimits {
                                    max_records: 16,
                                    max_scan_bytes: 1024 * 1024,
                                    max_jobs: 16,
                                    max_output_bytes: 64 * 1024,
                                },
                            )
                        })
                        .await;
                        match page {
                            Ok(Ok(page)) => {
                                if let Some(next) = page.next {
                                    pages.push_back((repo.clone(), Some(next)));
                                }
                                for job in page.jobs {
                                    if matches!(
                                        job.state,
                                        s::LandingState::Prepared | s::LandingState::Checked
                                    ) {
                                        ready.push_back((repo.clone(), job));
                                    }
                                }
                            }
                            _ => {
                                pages.push_back((repo, None));
                                blocked = true;
                            }
                        }
                    }
                }
                let wait = blocked || (pages.is_empty() && ready.is_empty());
                drop(host);
                if wait {
                    tokio::select! {_=wake.notified()=>{},_=tokio::time::sleep(Duration::from_secs(1))=>{}}
                    if pages.is_empty() && ready.is_empty() {
                        pages = initial();
                    }
                } else {
                    tokio::task::yield_now().await;
                }
            }
        }));
        Ok(())
    }
    pub(crate) fn close(&self) {
        self.closing.store(true, Ordering::Release);
        let _ = self.cancel.send(true);
        self.wake.notify_waiters();
    }
    pub(crate) async fn drain(&self) {
        self.close();
        let worker = self.worker.lock().ok().and_then(|mut worker| worker.take());
        if let Some(worker) = worker {
            let _ = worker.await;
        }
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.active.load(Ordering::Acquire) == 0 {
                return;
            }
            changed.await;
        }
    }
    pub(crate) fn progress(&self, repo: &str, id: [u8; 32]) -> Option<CheckerProgress> {
        let state = self.state.lock().ok()?;
        let key = (repo.to_owned(), id);
        if let Some(slot) = state.slots.get(&key) {
            let mut value = slot.diagnostic.clone();
            value.active_task = slot.active;
            value.attempts = slot.record.attempts.min(u32::MAX as usize) as u32;
            value.recoveries = slot.record.recoveries.min(u32::MAX as usize) as u32;
            value.retry_at_seconds = slot.record.retry_at;
            return Some(value);
        }
        state
            .reservation_error
            .as_ref()
            .filter(|(selected, _)| selected == &key)
            .map(|(_, value)| value.clone())
    }
    fn diagnostic_stage(&self, key: &Key, stage: CheckerStage) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(slot) = state.slots.get_mut(key) {
                slot.diagnostic.stage = stage as i32;
            }
        }
    }
    fn process_error(&self, key: &Key, code: CheckerErrorCode) {
        if let Ok(mut state) = self.state.lock() {
            if let Some(slot) = state.slots.get_mut(key) {
                slot.diagnostic.error_code = code as i32;
                slot.diagnostic.error_stage = slot.diagnostic.stage;
            }
        }
    }
    async fn process(self: &Arc<Self>, key: Key) -> Result<()> {
        self.diagnostic_stage(&key, CheckerStage::Preparation);
        let host = self.clone();
        let selected = key.clone();
        let prepared = tokio::task::spawn_blocking(move || host.prepare_step(&selected)).await??;
        let Some((record, attempt, mode, _candidate)) = prepared else {
            return Ok(());
        };
        self.diagnostic_stage(&key, CheckerStage::CandidatePin);
        let _root = self
            .store
            .pin_root(&record.repository, candidate_key(&record.request)?)?;
        let executable = self
            .directory
            .path()
            .join("binaries")
            .join(&record.authority.binary);
        self.diagnostic_stage(&key, CheckerStage::BinaryVerify);
        runner::Binary {
            path: executable.clone(),
            sha256: record.authority.binary.clone(),
        }
        .verify()?;
        self.diagnostic_stage(&key, CheckerStage::ConfigurationVerify);
        let config = attempt.path().join("checker.toml");
        let bytes = attempt.read("checker.toml", 65536)?;
        ensure!(
            digest(&bytes) == record.authority.configuration,
            "frozen checker configuration differs"
        );
        let mut command = tokio::process::Command::new(executable);
        command
            .arg("--config")
            .arg(config)
            .arg(mode)
            .current_dir(attempt.path())
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", attempt.path());
        self.diagnostic_stage(&key, CheckerStage::EnvironmentBinding);
        for name in &record.authority.environment {
            command.env(
                name,
                std::env::var(name).context("checker management environment unavailable")?,
            );
        }
        let stdout = attempt
            .path()
            .join(format!("{mode}-{}.stdout", record.recoveries));
        let stderr = attempt
            .path()
            .join(format!("{mode}-{}.stderr", record.recoveries));
        let input = serde_json::to_vec(&record.request)?;
        self.diagnostic_stage(&key, CheckerStage::Execution);
        let output = crate::landing_check_process::execute(
            command,
            &input,
            &stdout,
            &stderr,
            Duration::from_secs(record.authority.timeout),
            Duration::from_secs(record.authority.grace),
            self.cancel.subscribe(),
        )
        .await;
        if output.is_err() {
            self.process_error(&key, CheckerErrorCode::ExecutionUncertain);
        }
        self.diagnostic_stage(&key, CheckerStage::ReceiptVerify);
        let original_eligible =
            mode == "run" || crate::landing_check_process::allows_verdict(&attempt);
        let result = output.and_then(|output| {
            let receipt: runner::Receipt = serde_json::from_slice(&output.bytes)?;
            validate_receipt(&receipt, &record.request, &record.authority)?;
            Ok((receipt, output.eligible && original_eligible))
        });
        if result.is_err()
            && self
                .progress(&key.0, key.1)
                .is_some_and(|p| p.error_code == 0)
        {
            self.process_error(&key, CheckerErrorCode::ReceiptInvalid);
        }
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| anyhow::anyhow!("checker gate poisoned"))?;
            let slot = state
                .slots
                .get_mut(&key)
                .context("checker job disappeared")?;
            let delay = retry_delay(slot.record.authority.retry, slot.record.recoveries) as i64;
            match result {
                Ok((receipt, eligible)) if receipt.cleanup_confirmed => {
                    slot.record.verdict_eligible = eligible;
                    slot.record.phase =
                        if receipt.outcome == runner::Outcome::Unavailable || !eligible {
                            Phase::RetryReady
                        } else {
                            Phase::Verified
                        };
                    slot.record.receipt = Some(receipt);
                    slot.record.retry_at = now()?.saturating_add(delay);
                    Self::save(slot)?;
                }
                _ => {
                    slot.record.phase = Phase::Uncertain;
                    slot.record.receipt = None;
                    slot.record.retry_at = now()?.saturating_add(delay);
                    Self::save(slot)?;
                    return Ok(());
                }
            }
        }
        self.diagnostic_stage(&key, CheckerStage::VerdictApply);
        let host = self.clone();
        tokio::task::spawn_blocking(move || host.apply_verified(&key)).await?
    }
    fn prepare_step(
        &self,
        key: &Key,
    ) -> Result<
        Option<(
            Record,
            PrivateDir,
            &'static str,
            Option<s::LandingCandidateLease>,
        )>,
    > {
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("checker gate poisoned"))?;
        let slot = state
            .slots
            .get_mut(key)
            .context("checker job unavailable")?;
        if slot.record.phase == Phase::Verified {
            drop(state);
            self.apply_verified(key)?;
            return Ok(None);
        }
        if matches!(slot.record.phase, Phase::Terminal | Phase::UnavailableReady) {
            return Ok(None);
        }
        if slot.record.phase == Phase::RetryReady {
            let maximum = self
                .current
                .as_ref()
                .map_or(0, |current| current.config.max_attempts);
            if slot.record.attempts >= maximum
                || !self.configured(
                    &key.0,
                    &s::LandingCheckProfile {
                        name: slot.record.request.profile_name.clone(),
                        version: slot.record.request.profile_version.clone(),
                    },
                )
            {
                slot.record.phase = Phase::Terminal;
                slot.root = None;
                Self::save(slot)?;
                return Ok(None);
            }
            slot.record.request.attempt_id = nonce();
            slot.record.attempts += 1;
            slot.record.recoveries = 0;
            slot.record.receipt = None;
            slot.record.verdict_eligible = false;
            slot.record.phase = Phase::Reserved;
            Self::save(slot)?;
        }
        let recovering = slot.record.uncertain();
        let mut candidate = None;
        let attempt = if !recovering {
            let handle = slot
                .handle
                .as_ref()
                .context("candidate handle not discovered")?;
            let job = handle.status(&self.store)?;
            if job.expires_at <= now()?
                || job.state != s::LandingState::Prepared
                || self.closing.load(Ordering::Acquire)
            {
                slot.record.phase = Phase::Terminal;
                slot.root = None;
                Self::save(slot)?;
                return Ok(None);
            }
            let profile = s::LandingCheckProfile {
                name: slot.record.request.profile_name.clone(),
                version: slot.record.request.profile_version.clone(),
            };
            let current = match self.refresh(&key.0, &profile) {
                Ok(current) => current,
                Err(_) => {
                    // The state mutex is already held; report this swallowed
                    // prelaunch failure without re-entering diagnostic helpers.
                    slot.diagnostic.stage = CheckerStage::AuthorityRefresh as i32;
                    slot.diagnostic.error_stage = CheckerStage::AuthorityRefresh as i32;
                    slot.diagnostic.error_code = CheckerErrorCode::ProcessFailed as i32;
                    slot.record.phase = Phase::Terminal;
                    slot.root = None;
                    Self::save(slot)?;
                    return Ok(None);
                }
            };
            self.pin_executable(current)?;
            ensure!(
                slot.record.authority.configuration == digest(&current.bytes)
                    && slot.record.authority.binary == current.config.sha256,
                "reserved checker authority differs"
            );
            let directory =
                PrivateDir::create(slot.directory.path().join(&slot.record.request.attempt_id))?;
            if fs::read_dir(directory.path())?
                .any(|item| item.map_or(true, |entry| entry.file_name() != "checker.toml"))
            {
                slot.record.phase = Phase::Uncertain;
                Self::save(slot)?;
                anyhow::bail!("reserved attempt contains execution state")
            }
            directory.replace("checker.toml", &current.bytes)?;
            let lease = handle.pin_candidate(&self.store, now()?)?;
            ensure!(
                id(lease.identity().candidate.0) == slot.record.request.candidate_commit
                    && id(lease.identity().root_tree.0) == slot.record.request.root_tree
                    && lease.identity().profile == profile,
                "prepared candidate changed"
            );
            candidate = Some(lease);
            slot.record.phase = Phase::DispatchIntent;
            directory
        } else {
            ensure!(
                slot.record.recoveries < 256,
                "checker recovery budget exhausted"
            );
            slot.record.recoveries += 1;
            PrivateDir::open_existing(slot.directory.path().join(&slot.record.request.attempt_id))?
        };
        slot.record.retry_at = now()?
            .saturating_add(
                retry_delay(slot.record.authority.retry, slot.record.recoveries) as i64,
            );
        if slot.root.is_none() {
            slot.root = Some(
                self.store
                    .pin_root(&key.0, candidate_key(&slot.record.request)?)?,
            );
        }
        Self::save(slot)?;
        Ok(Some((
            slot.record.clone(),
            attempt,
            if recovering { "recover" } else { "run" },
            candidate,
        )))
    }
    fn apply_unavailable(&self, key: &Key) -> Result<()> {
        let host = self
            .admission
            .lock()
            .map_err(|_| anyhow::anyhow!("checker admission poisoned"))?
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .context("landing admission unavailable")?;
        let _work = host.admit_check_transition()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("checker gate poisoned"))?;
        let slot = state
            .slots
            .get_mut(key)
            .context("checker job unavailable")?;
        if slot.active || slot.record.uncertain() || self.closing.load(Ordering::Acquire) {
            return Ok(());
        }
        // An in-memory Terminal can precede a failed save. Only this checked
        // latest envelope proves that dispatch is durably absorbed. No current
        // policy refresh or reconstructed legacy attempt budget is authority.
        let saved = decode_record(&slot.directory.read("state.json", MAX_RECORD)?)?;
        ensure!(
            saved.repository == key.0
                && saved.job == key.1
                && serde_json::to_vec(&saved.request)? == serde_json::to_vec(&slot.record.request)?
                && serde_json::to_vec(&saved.authority)?
                    == serde_json::to_vec(&slot.record.authority)?,
            "saved unavailable checker binding differs"
        );
        if !saved.safe_unavailable()
            || !matches!(saved.phase, Phase::Terminal | Phase::UnavailableReady)
            || (saved.phase == Phase::Terminal && saved.version == 2)
        {
            return Ok(());
        }
        let handle = self
            .store
            .recover_landing_handle(&key.0, key.1)?
            .context("saved unavailable landing job disappeared")?;
        let job = handle.status(&self.store)?;
        let candidate = job
            .candidate
            .as_ref()
            .context("unavailable candidate missing")?;
        ensure!(
            job.id == saved.job
                && id(candidate.head.0) == saved.request.candidate_commit
                && id(candidate.root_tree.0) == saved.request.root_tree
                && job.profile.name == saved.request.profile_name
                && job.profile.version == saved.request.profile_version,
            "unavailable checker job binding differs"
        );
        if !matches!(
            job.state,
            s::LandingState::Prepared
                | s::LandingState::CheckUnavailable
                | s::LandingState::Expired
        ) {
            return Ok(()); // Never overwrite a different check/verdict/ref state.
        }
        slot.record = saved;
        if slot.record.phase != Phase::UnavailableReady {
            slot.record.version = 2;
            slot.record.phase = Phase::UnavailableReady;
            slot.record.unavailable_reason = Some(UnavailableReason::PermanentUnavailability);
            Self::save(slot)?; // Durable finish-only intent BEFORE the Store write.
        }
        if job.state == s::LandingState::Prepared {
            let identity = s::LandingCheckIdentity {
                candidate: candidate.head,
                root_tree: candidate.root_tree,
                profile: job.profile.clone(),
            };
            let terminal = handle.record_check_unavailable(&self.store, &identity, now()?)?;
            ensure!(
                matches!(
                    terminal.state,
                    s::LandingState::CheckUnavailable | s::LandingState::Expired
                ),
                "unavailable transition returned nonterminal state"
            );
        }
        slot.record.phase = Phase::Terminal;
        slot.root = None;
        Self::save(slot)?;
        Ok(())
    }
    fn apply_verified(&self, key: &Key) -> Result<()> {
        if !self.landing_enabled {
            return Ok(());
        }
        let owner = self
            .admission
            .lock()
            .map_err(|_| anyhow::anyhow!("checker admission poisoned"))?
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
            .context("landing admission unavailable")?;
        let _work = owner.admit_check_transition()?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("checker gate poisoned"))?;
        let slot = state
            .slots
            .get_mut(key)
            .context("verified checker job unavailable")?;
        if slot.record.phase != Phase::Verified || self.closing.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(handle) = slot.handle.as_ref() else {
            return Ok(());
        };
        ensure!(
            slot.record.verdict_eligible,
            "checker verdict is not transport eligible"
        );
        let receipt = slot
            .record
            .receipt
            .as_ref()
            .context("verified receipt missing")?;
        validate_receipt(receipt, &slot.record.request, &slot.record.authority)?;
        let job = handle.status(&self.store)?;
        let candidate = job
            .candidate
            .as_ref()
            .context("verified candidate missing")?;
        ensure!(
            job.id == slot.record.job
                && id(candidate.head.0) == slot.record.request.candidate_commit
                && id(candidate.root_tree.0) == slot.record.request.root_tree
                && job.profile.name == slot.record.request.profile_name
                && job.profile.version == slot.record.request.profile_version,
            "verified job binding differs"
        );
        if job.expires_at <= now()? {
            handle.expire(&self.store, now()?)?;
            slot.record.phase = Phase::Terminal;
            slot.root = None;
            Self::save(slot)?;
            return Ok(());
        }
        self.refresh(&key.0, &job.profile)?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-trusted-check-receipt-v1\0");
        hash.update(&receipt.encode()?);
        let identity = s::LandingCheckIdentity {
            candidate: candidate.head,
            root_tree: candidate.root_tree,
            profile: job.profile.clone(),
        };
        let checked = handle.record_check(
            &self.store,
            &identity,
            s::LandingVerdict {
                passed: receipt.outcome == runner::Outcome::Passed,
                evidence: *hash.finalize().as_bytes(),
            },
            now()?,
        )?;
        if checked.state == s::LandingState::Checked
            && !self.store.forge_review_manages_landing(&key.0, key.1)?
        {
            // This also gates restart finalization; a bare Checked state without
            // our exact saved receipt never authorizes publication.
            self.refresh(&key.0, &job.profile)?;
            if self.closing.load(Ordering::Acquire) {
                return Ok(());
            }
            handle.finalize(&self.store, now()?)?;
        }
        slot.record.phase = Phase::Terminal;
        slot.root = None;
        Self::save(slot)?;
        Ok(())
    }
}
fn retry_delay(base: u64, recoveries: usize) -> u64 {
    base.saturating_mul(1u64 << recoveries.min(6)).min(600)
}

fn recover_staging(jobs: &PrivateDir) -> Result<()> {
    let mut entries = 0;
    for item in fs::read_dir(jobs.path())? {
        let item = item?;
        entries += 1;
        ensure!(
            entries <= MAX_JOBS * 2,
            "checker reservation inventory exceeds bound"
        );
        let name = item.file_name();
        let Some(name) = name.to_str() else {
            anyhow::bail!("invalid reservation name")
        };
        if !name.starts_with(".pending-") {
            continue;
        }
        let pieces: Vec<_> = name.split('-').collect();
        ensure!(
            pieces.len() == 3 && pieces[0] == ".pending" && hex(pieces[1]) && hex(pieces[2]),
            "invalid unpublished reservation name"
        );
        let directory = PrivateDir::open_existing(item.path())?;
        let mut children = 0;
        for child in fs::read_dir(directory.path())? {
            let child = child?;
            children += 1;
            ensure!(
                children <= 8
                    && child.file_type()?.is_file()
                    && child.metadata()?.len() <= MAX_RECORD as u64,
                "invalid unpublished reservation contents"
            );
            let file = child.file_name();
            let file = file.to_str().context("invalid reservation child name")?;
            if file == "state.json" {
                let record = decode_record(&directory.read(file, MAX_RECORD)?)?;
                ensure!(
                    record.phase == Phase::Reserved
                        && job_name(&record.repository, record.job) == pieces[1]
                        && record.request.attempt_id == pieces[2],
                    "published work in unpublished namespace"
                );
            } else {
                let suffix = file
                    .strip_prefix(".private-")
                    .and_then(|s| s.strip_suffix(".tmp"))
                    .context("foreign unpublished reservation data")?;
                let (pid, sequence) = suffix
                    .split_once('-')
                    .context("invalid reservation temporary name")?;
                ensure!(
                    !pid.is_empty()
                        && !sequence.is_empty()
                        && pid.bytes().all(|b| b.is_ascii_digit())
                        && sequence.bytes().all(|b| b.is_ascii_digit()),
                    "invalid reservation temporary identity"
                );
            }
        }
        // Dispatch is forbidden before atomic publication to the non-staging
        // name. This namespace contains no user files or checker subprocesses.
        drop(directory);
        jobs.remove_child_tree(name, 8, 1)?;
    }
    Ok(())
}
#[cfg(target_os = "linux")]
fn publish_reservation(source: &Path, target: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(source.as_os_str().as_bytes())?;
    let target = std::ffi::CString::new(target.as_os_str().as_bytes())?;
    let result = unsafe {
        libc::syscall(
            libc::SYS_renameat2,
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            target.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    ensure!(result == 0, "cannot atomically publish checker reservation");
    Ok(())
}
#[cfg(not(target_os = "linux"))]
fn publish_reservation(_: &Path, _: &Path) -> Result<()> {
    anyhow::bail!("local checker publication requires Linux")
}
