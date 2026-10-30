use crate::candidate_http::CandidateClient;
use crate::prepared_source::PreparedSource;
use crate::{Config, Evidence, Mode, Outcome, Receipt, Request, bep, process};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use tinytree_forge_core::checker::CheckerLease;
use tokio::{process::Command, sync::watch, time::Instant};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema: u32,
    request: Request,
    config: Config,
    workspace_id: String,
    mount_attempted: bool,
    cleanup_sequence: u32,
    receipt: Option<Receipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease: Option<CheckerLease>,
}
fn forge_repository(config: &Config, request: &Request) -> Result<bool> {
    let profile = config.profile(&request.profile_name)?;
    Ok(profile.forge_accounts
        && request
            .repository_id
            .strip_prefix("forge-")
            .is_some_and(|id| {
                !id.is_empty()
                    && id.len() <= 64
                    && id
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            }))
}
fn actual_store<'a>(config: &'a Config, request: &'a Request) -> Result<&'a str> {
    Ok(if forge_repository(config, request)? {
        &request.repository_id
    } else {
        &config.profile(&request.profile_name)?.store
    })
}
/// Deterministic trusted-template derivation. No endpoint, credential, alias or
/// command is accepted from repository contents or the check request.
pub(crate) fn derived_daemon(
    config: &Config,
    request: &Request,
    root: &Path,
    template: &[u8],
) -> Result<Vec<u8>> {
    if !forge_repository(config, request)? {
        return Ok(template.to_vec());
    }
    let mut daemon: toml::Value = toml::from_str(std::str::from_utf8(template)?)?;
    let profile = config.profile(&request.profile_name)?;
    let mut repository = daemon
        .get("native")
        .and_then(|v| v.get("repositories"))
        .and_then(|v| v.get(&profile.store))
        .cloned()
        .context("Forge checker template is unavailable")?;
    ensure!(
        repository
            .get("checker_forge_accounts")
            .and_then(toml::Value::as_bool)
            == Some(true)
            && repository.get("forge").is_none(),
        "Forge checker template is not explicitly raw-remote enabled"
    );
    repository
        .as_table_mut()
        .context("invalid raw checker template")?
        .get_mut("remote")
        .and_then(toml::Value::as_table_mut)
        .context("missing raw candidate provider")?
        .insert(
            "server_retained_candidate".into(),
            toml::Value::Table(
                [
                    ("job_id".into(), toml::Value::String(request.job_id.clone())),
                    (
                        "head".into(),
                        toml::Value::String(request.candidate_commit.clone()),
                    ),
                    (
                        "root_tree".into(),
                        toml::Value::String(request.root_tree.clone()),
                    ),
                ]
                .into_iter()
                .collect(),
            ),
        );
    let remote = repository
        .get_mut("remote")
        .and_then(|v| v.get_mut("remote"))
        .and_then(toml::Value::as_table_mut)
        .context("Forge checker template requires a raw remote provider")?;
    remote.insert(
        "repository_id".into(),
        toml::Value::String(request.repository_id.clone()),
    );
    let table = daemon
        .as_table_mut()
        .context("invalid checker daemon template")?;
    table.insert(
        "state_dir".into(),
        toml::Value::String(
            root.join("d")
                .to_str()
                .context("invalid daemon state path")?
                .into(),
        ),
    );
    // A check daemon owns only its exact account provider and private registry.
    let native = table
        .get_mut("native")
        .and_then(toml::Value::as_table_mut)
        .context("missing native checker template")?;
    native.insert(
        "repositories".into(),
        toml::Value::Table(
            [(request.repository_id.clone(), repository)]
                .into_iter()
                .collect(),
        ),
    );
    let workspaces = native
        .entry("workspaces")
        .or_insert_with(|| toml::Value::Table(Default::default()))
        .as_table_mut()
        .context("invalid native checker workspace policy")?;
    workspaces.insert(
        "registry_path".into(),
        toml::Value::String(
            root.join("daemon-registry")
                .to_str()
                .context("invalid daemon registry path")?
                .into(),
        ),
    );
    for name in ["capture", "upload", "gc"] {
        let settings = workspaces
            .entry(name)
            .or_insert_with(|| toml::Value::Table(Default::default()))
            .as_table_mut()
            .context("invalid checker workspace settings")?;
        settings.insert("policy".into(), toml::Value::String("disabled".into()));
    }
    for name in ["bos", "bes"] {
        let settings = table
            .entry(name)
            .or_insert_with(|| toml::Value::Table(Default::default()))
            .as_table_mut()
            .context("invalid checker optional service")?;
        settings.insert("enabled".into(), toml::Value::Boolean(false));
    }
    if let Some(git) = table
        .get_mut("git_proxy")
        .and_then(toml::Value::as_table_mut)
    {
        git.insert(
            "repositories".into(),
            toml::Value::Table(Default::default()),
        );
    }
    let derived = toml::to_string(&daemon)?.into_bytes();
    ensure!(
        derived.len() <= 65536,
        "derived checker daemon configuration limit"
    );
    Ok(derived)
}

struct Attempt {
    root: PrivateDir,
    _lock: fs::File,
    journal: Journal,
    environment: BTreeMap<String, String>,
}
impl Attempt {
    fn open(config: &Config, request: &Request, recovery: bool) -> Result<Self> {
        request.validate()?;
        config.validate()?;
        if !recovery {
            let profile = config.profile(&request.profile_name)?;
            ensure!(
                profile.repositories.contains(&request.repository_id)
                    || (profile.forge_accounts
                        && request
                            .repository_id
                            .strip_prefix("forge-")
                            .is_some_and(|id| !id.is_empty()
                                && id.len() <= 64
                                && id.bytes().all(|b| b.is_ascii_lowercase()
                                    || b.is_ascii_digit()
                                    || b == b'-'))),
                "repository is not authorized by check profile"
            );
            ensure!(
                config.profile_version(&request.profile_name)? == request.profile_version,
                "frozen check profile is unavailable"
            );
        }
        let directory = if recovery {
            PrivateDir::open_existing(&config.scratch)?
        } else {
            PrivateDir::create(&config.scratch)?
        };
        let admission = directory.open_lock("admission.lock")?;
        admission.lock_exclusive()?;
        let mut count = 0;
        for entry in fs::read_dir(directory.path())? {
            let entry = entry?;
            if entry.file_name() == "admission.lock" {
                continue;
            }
            ensure!(
                entry.file_type()?.is_dir()
                    && entry.file_name().to_str().is_some_and(crate::hex_id),
                "unknown check scratch entry"
            );
            count += 1;
            ensure!(
                count <= if recovery { 4096 } else { config.max_attempts },
                "check scratch attempt quota exceeded"
            );
        }
        ensure!(
            directory.path().join(request.workspace_id()).exists()
                || (!recovery && count < config.max_attempts),
            "check scratch attempt quota reached"
        );
        let root = if recovery {
            PrivateDir::open_existing(directory.path().join(request.workspace_id()))?
        } else {
            PrivateDir::create(directory.path().join(request.workspace_id()))?
        };
        drop(admission);
        let lock = root.open_lock("owner.lock")?;
        lock.try_lock_exclusive()
            .context("check attempt is already owned")?;
        let existing = match root.read("attempt.json", 256 * 1024) {
            Ok(bytes) => Some(serde_json::from_slice::<Journal>(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        ensure!(
            !recovery || existing.is_some(),
            "recovery journal is missing"
        );
        let journal = existing.unwrap_or_else(|| Journal {
            schema: 1,
            request: request.clone(),
            config: config.clone(),
            workspace_id: request.workspace_id(),
            mount_attempted: false,
            cleanup_sequence: 0,
            receipt: None,
            lease: None,
        });
        ensure!(
            journal.schema == 1
                && journal.request == *request
                && journal.workspace_id == request.workspace_id(),
            "attempt journal binding differs"
        );
        journal.config.validate()?;
        ensure!(
            journal.config.scratch == config.scratch,
            "frozen scratch authority differs"
        );
        if let Some(receipt) = &journal.receipt {
            receipt.validate_for(request)?;
        }
        let mut environment = BTreeMap::new();
        for name in &journal.config.workspace_environment {
            environment.insert(
                name.clone(),
                std::env::var(name).context("required management environment is unavailable")?,
            );
        }
        let attempt = Self {
            root,
            _lock: lock,
            journal,
            environment,
        };
        attempt.save()?;
        Ok(attempt)
    }
    fn save(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.journal)?;
        ensure!(
            bytes.len() <= 256 * 1024,
            "attempt journal exceeds replay budget"
        );
        self.root.replace("attempt.json", &bytes)?;
        Ok(())
    }
    fn path(&self, child: &str) -> PathBuf {
        self.root.path().join(child)
    }
    fn command(&self, program: &Path, management: bool) -> Command {
        let mut command = Command::new(program);
        command
            .env_clear()
            .current_dir(self.root.path())
            .env("PATH", "/usr/bin:/bin")
            .env("HOME", self.path("home"))
            .env("XDG_CONFIG_HOME", self.path("home"))
            .env("TMPDIR", self.path("temporary"));
        if management {
            command.envs(&self.environment);
        }
        command
    }
    fn workspace(&self) -> Command {
        let mut command = self.command(&self.path("tools/workspace"), true);
        command.arg("--config").arg(self.path("daemon.toml"));
        command
    }
    fn next_cleanup(&mut self) -> Result<String> {
        ensure!(
            self.journal.cleanup_sequence < 64,
            "check cleanup retry limit reached"
        );
        self.journal.cleanup_sequence += 1;
        self.save()?;
        Ok(format!("cleanup-{}", self.journal.cleanup_sequence))
    }
    async fn control(&self, arguments: &[&str], name: &str, timeout: Duration) -> Result<Value> {
        self.control_cancellable(arguments, name, timeout, None)
            .await
    }
    async fn control_cancellable(
        &self,
        arguments: &[&str],
        name: &str,
        timeout: Duration,
        cancellation: Option<&mut watch::Receiver<bool>>,
    ) -> Result<Value> {
        let mut command = self.workspace();
        command.args(arguments);
        let stdout = self.path(&format!("{name}.stdout"));
        let stderr = self.path(&format!("{name}.stderr"));
        let limits = [(stdout.clone(), 65536), (stderr.clone(), 65536)];
        let status =
            process::execute(command, &stdout, &stderr, timeout, &limits, cancellation).await?;
        ensure!(status == 0, "workspace control operation failed");
        let result: Value =
            serde_json::from_slice(&self.root.read(format!("{name}.stdout"), 65536)?)?;
        ensure!(result["ok"] == true, "workspace control reply rejected");
        Ok(result["report"].clone())
    }
    fn prepare(&self, config: &Config) -> Result<()> {
        for name in ["home", "temporary", "tools"] {
            PrivateDir::create(self.path(name))?;
        }
        if config.https_snapshot.is_none() {
            PrivateDir::create(self.path("source"))?;
        }
        let mut binaries = vec![(&config.bazel, "bazel")];
        if let Some(snapshot) = &config.https_snapshot {
            binaries.push((&snapshot.readonly_helper, "readonly-snapshot"));
        } else {
            binaries.push((config.native_workspace()?, "workspace"));
        }
        binaries.extend(
            config
                .tools
                .iter()
                .map(|(name, binary)| (binary, name.as_str())),
        );
        for (binary, name) in binaries {
            binary.verify()?;
            let target = self.path(&format!("tools/{name}"));
            let source = fs::File::open(&binary.path)?;
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o500).custom_flags(libc::O_NOFOLLOW);
            }
            let mut output = options.open(&target)?;
            std::io::copy(&mut source.take(1024 * 1024 * 1024 + 1), &mut output)?;
            output.flush()?;
            output.sync_all()?;
            crate::Binary {
                path: target,
                sha256: binary.sha256.clone(),
            }
            .verify()?;
        }
        if config.https_snapshot.is_some() {
            ensure!(
                config.profile_version(&self.journal.request.profile_name)?
                    == self.journal.request.profile_version,
                "profile changed during attempt setup"
            );
            return Ok(());
        }
        let bytes = crate::config::read_config(config.native_daemon()?)?;
        ensure!(
            config.profile_version_for_daemon(&self.journal.request.profile_name, &bytes)?
                == self.journal.request.profile_version,
            "management configuration changed during setup"
        );
        self.root.replace("daemon-template.toml", &bytes)?;
        self.root.replace(
            "daemon.toml",
            &derived_daemon(config, &self.journal.request, self.root.path(), &bytes)?,
        )?;
        // Revalidate after copying: a concurrent operator edit cannot silently
        // execute under the server's older frozen profile fingerprint.
        ensure!(
            config.profile_version(&self.journal.request.profile_name)?
                == self.journal.request.profile_version,
            "profile changed during attempt setup"
        );
        Ok(())
    }
}

/// Owns physical work independently of an awaiting caller. Dropping the
/// handle requests cancellation; its task retains journal/lock until cleanup.
/// Embedders must keep the Tokio runtime alive and drain handles at shutdown.
pub struct RunningCheck {
    task: tokio::task::JoinHandle<Result<Receipt>>,
    cancellation: watch::Sender<bool>,
}
impl RunningCheck {
    pub fn cancel(&self) {
        let _ = self.cancellation.send(true);
    }
    pub async fn wait(mut self) -> Result<Receipt> {
        (&mut self.task).await?
    }
}
impl Drop for RunningCheck {
    fn drop(&mut self) {
        self.cancel();
    }
}
pub fn start(
    config: Config,
    request: Request,
    cancellation: watch::Receiver<bool>,
) -> RunningCheck {
    start_owned(config, request, None, cancellation)
}
pub fn start_leased(
    config: Config,
    lease: CheckerLease,
    client: CandidateClient,
    cancellation: watch::Receiver<bool>,
) -> RunningCheck {
    start_owned(
        config,
        lease.request.clone(),
        Some((lease, client)),
        cancellation,
    )
}
pub async fn run_leased(
    config: &Config,
    lease: CheckerLease,
    client: CandidateClient,
    cancellation: watch::Receiver<bool>,
) -> Result<Receipt> {
    start_leased(config.clone(), lease, client, cancellation)
        .wait()
        .await
}
fn start_owned(
    config: Config,
    request: Request,
    leased: Option<(CheckerLease, CandidateClient)>,
    mut cancellation: watch::Receiver<bool>,
) -> RunningCheck {
    let (sender, receiver) = watch::channel(*cancellation.borrow());
    let bridge_sender = sender.clone();
    let task = tokio::spawn(async move {
        let bridge = tokio::spawn(async move {
            loop {
                if *cancellation.borrow() {
                    let _ = bridge_sender.send(true);
                    break;
                }
                if cancellation.changed().await.is_err() {
                    let _ = bridge_sender.send(true);
                    break;
                }
            }
        });
        let result = run_owned(&config, request, leased, receiver).await;
        bridge.abort();
        result
    });
    RunningCheck {
        task,
        cancellation: sender,
    }
}
pub async fn run(
    config: &Config,
    request: Request,
    cancellation: watch::Receiver<bool>,
) -> Result<Receipt> {
    start(config.clone(), request, cancellation).wait().await
}
pub async fn recover(config: &Config, request: Request) -> Result<Receipt> {
    let config = config.clone();
    tokio::spawn(async move { recover_owned(&config, request).await }).await?
}

/// One physical attempt is durably named. A repeated completed request returns
/// its saved receipt; incomplete state requires explicit recovery, not relaunch.
async fn run_owned(
    config: &Config,
    request: Request,
    leased: Option<(CheckerLease, CandidateClient)>,
    mut cancelled: watch::Receiver<bool>,
) -> Result<Receipt> {
    #[cfg(not(target_os = "linux"))]
    anyhow::bail!("native immutable check execution requires Linux acceptance");
    ensure!(
        config.https_snapshot.is_some() == leased.is_some(),
        "source backend requires its exact execution authority"
    );
    if let Some((_, client)) = &leased {
        let snapshot = config
            .https_snapshot
            .as_ref()
            .context("snapshot source is unavailable")?;
        ensure!(
            client.matches_config(&snapshot.origin, &snapshot.credential_environment),
            "checker transport differs from frozen source configuration"
        );
    }
    let mut attempt = Attempt::open(config, &request, false)?;
    if let Some((lease, _)) = &leased {
        lease.validate()?;
        ensure!(lease.request == request, "lease request differs");
        if let Some(original) = &attempt.journal.lease {
            ensure!(
                original.immutable_binding()? == lease.immutable_binding()?,
                "attempt lease binding differs"
            );
        } else {
            ensure!(
                !attempt.journal.mount_attempted && !attempt.path("tools").exists(),
                "incomplete source requires recovery"
            );
            attempt.journal.lease = Some(lease.clone());
            attempt.save()?;
        }
    }

    if let Some(receipt) = &attempt.journal.receipt {
        return Ok(receipt.clone());
    }
    ensure!(
        !attempt.journal.mount_attempted && !attempt.path("tools").exists(),
        "incomplete attempt requires recovery"
    );
    attempt.prepare(config)?;
    let profile = config.profile(&request.profile_name)?;
    let store = actual_store(config, &request)?;
    let deadline = Instant::now() + Duration::from_secs(profile.timeout_seconds);
    let mut evidence: Option<Evidence> = None;
    let mut outcome = Outcome::Unavailable;
    let mut reason = "check_unavailable";
    let work = async {
        ensure!(!*cancelled.borrow(), "check cancelled before mount");
        attempt.journal.mount_attempted = true;
        attempt.save()?;
        let mut arguments = startup(&attempt);
        if config.https_snapshot.is_some() {
            arguments.insert(0, "--batch".into());
        }
        arguments.push(profile.mode.command().into());
        arguments.extend(profile.targets.clone());
        arguments.extend(profile_flags(&attempt, &profile.flags)?);
        arguments.extend(build_flags(config, &attempt, profile));
        let prepared = if let Some((lease, client)) = &leased {
            PreparedSource::https(
                config,
                &request,
                lease,
                client,
                attempt.root.path(),
                &mut cancelled,
                deadline,
                arguments.clone(),
            )
            .await?
        } else {
            let root = attempt.path("source");
            let root_string = root.to_str().context("invalid source path")?;
            let mounted = attempt
                .control_cancellable(
                    &[
                        "native-candidate-mount",
                        root_string,
                        "--store",
                        store,
                        "--commit",
                        &request.candidate_commit,
                        "--workspace-id",
                        &attempt.journal.workspace_id,
                    ],
                    "mount",
                    remaining(deadline)?,
                    Some(&mut cancelled),
                )
                .await?;
            ensure!(
                mounted["binding"]["root"].as_str() == Some(root_string)
                    && mounted["binding"]["repository"].as_str() == Some(store),
                "mounted binding differs"
            );
            let proof = attempt
                .control_cancellable(
                    &[
                        "native-candidate-inspect",
                        "--workspace-id",
                        &attempt.journal.workspace_id,
                        "--commit",
                        &request.candidate_commit,
                    ],
                    "inspect",
                    remaining(deadline)?,
                    Some(&mut cancelled),
                )
                .await?;
            validate_proof(
                &proof,
                &request,
                &attempt.journal.workspace_id,
                &root,
                store,
            )?;
            require_readonly_mount(&root, &attempt.journal.workspace_id)?;
            PreparedSource::native(root)
        };
        let (program, args, working_directory) =
            prepared.execution(attempt.root.path(), arguments)?;
        let mut command = attempt.command(&program, false);
        command.current_dir(working_directory).args(args);
        let stdout = attempt.path("build.stdout");
        let stderr = attempt.path("build.stderr");
        let mut limits = vec![
            (stdout.clone(), config.max_log_bytes),
            (stderr.clone(), config.max_log_bytes),
            (attempt.path("bep.jsonl"), config.max_bep_bytes),
            (
                attempt.path("execution.compact"),
                config.max_execution_log_bytes,
            ),
        ];
        if config.https_snapshot.is_some() {
            limits.push((attempt.path("snapshot-proof.json"), 65536));
        }
        let status = process::execute(
            command,
            &stdout,
            &stderr,
            remaining(deadline)?,
            &limits,
            Some(&mut cancelled),
        )
        .await?;
        let completion = bep::verify(
            &attempt.path("bep.jsonl"),
            &attempt.path("execution.compact"),
            profile,
            status,
            config.max_bep_bytes,
            config.max_execution_log_bytes,
        )?;
        prepared.verify_completed(status)?;
        if config.https_snapshot.is_none() {
            let root = attempt.path("source");
            require_readonly_mount(&root, &attempt.journal.workspace_id)?;
            let after = attempt
                .control_cancellable(
                    &[
                        "native-candidate-inspect",
                        "--workspace-id",
                        &attempt.journal.workspace_id,
                        "--commit",
                        &request.candidate_commit,
                    ],
                    "inspect-after",
                    remaining(deadline)?,
                    Some(&mut cancelled),
                )
                .await?;
            validate_proof(
                &after,
                &request,
                &attempt.journal.workspace_id,
                &root,
                store,
            )?;
        }
        ensure!(!*cancelled.borrow(), "check cancelled before verdict");
        outcome = if completion.success {
            Outcome::Passed
        } else {
            Outcome::Failed
        };
        reason = if completion.success {
            "check_passed"
        } else {
            "check_failed"
        };
        evidence = Some(completion.evidence);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if work.is_err() {
        outcome = Outcome::Unavailable;
        reason = if *cancelled.borrow() {
            "check_cancelled"
        } else {
            "check_unavailable"
        };
    }
    let cleanup_name = attempt.next_cleanup()?;
    let cleanup_confirmed = cleanup(config, &attempt, &cleanup_name).await.is_ok();
    if !cleanup_confirmed {
        outcome = Outcome::Unavailable;
        reason = "cleanup_required";
    }
    if *cancelled.borrow() {
        outcome = Outcome::Unavailable;
        reason = "check_cancelled";
    }
    let receipt = Receipt {
        schema: 1,
        request,
        workspace_id: attempt.journal.workspace_id.clone(),
        outcome,
        reason: reason.into(),
        cleanup_confirmed,
        evidence,
    };
    receipt.validate_for(&receipt.request)?;
    attempt.journal.receipt = Some(receipt.clone());
    attempt.save()?;
    Ok(receipt)
}

/// Resolve surviving state using the original bound attempt. This never retries
/// its check, deletes scratch, or turns an unavailable attempt into success.
async fn recover_owned(config: &Config, request: Request) -> Result<Receipt> {
    #[cfg(not(target_os = "linux"))]
    anyhow::bail!("native check recovery requires Linux acceptance");
    let mut attempt = Attempt::open(config, &request, true)?;
    if let Some(receipt) = &attempt.journal.receipt {
        if receipt.cleanup_confirmed {
            return Ok(receipt.clone());
        }
    }
    if attempt.journal.config.https_snapshot.is_some() {
        let frozen = attempt.journal.config.clone();
        let lease = attempt
            .journal
            .lease
            .as_ref()
            .context("snapshot recovery lease is missing")?;
        lease.validate()?;
        ensure!(
            lease.request == request
                && frozen.profile_version(&request.profile_name)? == request.profile_version,
            "snapshot recovery binding differs"
        );
        for (name, binary) in [
            ("bazel", &frozen.bazel),
            (
                "readonly-snapshot",
                &frozen.https_snapshot.as_ref().unwrap().readonly_helper,
            ),
        ] {
            crate::Binary {
                path: attempt.path(&format!("tools/{name}")),
                sha256: binary.sha256.clone(),
            }
            .verify()?;
        }
        let cleanup_name = attempt.next_cleanup()?;
        let cleanup_confirmed = cleanup(&frozen, &attempt, &cleanup_name).await.is_ok();
        let receipt = Receipt {
            schema: 1,
            request,
            workspace_id: attempt.journal.workspace_id.clone(),
            outcome: Outcome::Unavailable,
            reason: if cleanup_confirmed {
                "recovered_without_verdict"
            } else {
                "cleanup_required"
            }
            .into(),
            cleanup_confirmed,
            evidence: attempt
                .journal
                .receipt
                .as_ref()
                .and_then(|r| r.evidence.clone()),
        };
        receipt.validate_for(&receipt.request)?;
        attempt.journal.receipt = Some(receipt.clone());
        attempt.save()?;
        return Ok(receipt);
    }
    ensure!(
        attempt.path("tools/workspace").is_file() && attempt.path("tools/bazel").is_file(),
        "incomplete setup requires operator inspection"
    );
    let config = attempt.journal.config.clone();
    let frozen_daemon = match attempt.root.read("daemon-template.toml", 65536) {
        Ok(bytes) => bytes,
        Err(e)
            if e.kind() == std::io::ErrorKind::NotFound
                && !forge_repository(&config, &request)? =>
        {
            attempt.root.read("daemon.toml", 65536)?
        }
        Err(e) => return Err(e.into()),
    };
    ensure!(
        attempt.root.read("daemon.toml", 65536)?
            == derived_daemon(&config, &request, attempt.root.path(), &frozen_daemon)?,
        "derived cleanup configuration binding differs"
    );
    ensure!(
        config.profile_version_for_daemon(&request.profile_name, &frozen_daemon)?
            == request.profile_version,
        "frozen cleanup configuration binding differs"
    );
    crate::Binary {
        path: attempt.path("tools/workspace"),
        sha256: config.native_workspace()?.sha256.clone(),
    }
    .verify()?;
    crate::Binary {
        path: attempt.path("tools/bazel"),
        sha256: config.bazel.sha256.clone(),
    }
    .verify()?;
    let cleanup_name = attempt.next_cleanup()?;
    let cleanup_confirmed = cleanup(&config, &attempt, &cleanup_name).await.is_ok();
    let receipt = Receipt {
        schema: 1,
        request,
        workspace_id: attempt.journal.workspace_id.clone(),
        outcome: Outcome::Unavailable,
        reason: if cleanup_confirmed {
            "recovered_without_verdict"
        } else {
            "cleanup_required"
        }
        .into(),
        cleanup_confirmed,
        evidence: attempt
            .journal
            .receipt
            .as_ref()
            .and_then(|r| r.evidence.clone()),
    };
    attempt.journal.receipt = Some(receipt.clone());
    attempt.save()?;
    Ok(receipt)
}

async fn cleanup(config: &Config, attempt: &Attempt, prefix: &str) -> Result<()> {
    process::require_quiescent(attempt.root.path())?;
    let timeout = Duration::from_secs(config.cleanup_timeout_seconds);
    let mut command = attempt.command(&attempt.path("tools/bazel"), false);
    if config.https_snapshot.is_some() {
        command.arg("--batch");
    }
    command.args(startup(attempt)).arg("shutdown");
    let stdout = attempt.path(&format!("{prefix}-shutdown.stdout"));
    let stderr = attempt.path(&format!("{prefix}-shutdown.stderr"));
    let limits = [
        (stdout.clone(), config.max_log_bytes),
        (stderr.clone(), config.max_log_bytes),
    ];
    let shutdown = process::execute(command, &stdout, &stderr, timeout, &limits, None).await;
    ensure!(
        shutdown? == 0,
        "Bazel shutdown is not confirmed; retain source"
    );
    process::require_quiescent(attempt.root.path())?;
    if config.https_snapshot.is_some() {
        ensure!(
            mount(&attempt.path("source"))?.is_none()
                && mount(&attempt.path("source-staging"))?.is_none(),
            "unexpected snapshot mount remains"
        );
        crate::prepared_source::cleanup_snapshot(attempt.root.path())?;
        return Ok(());
    }
    // Source retirement requires confirmed server and process quiescence.
    let unmount = if attempt.journal.mount_attempted && mount(&attempt.path("source"))?.is_some() {
        attempt
            .control(
                &[
                    "native-unmount",
                    "--workspace-id",
                    &attempt.journal.workspace_id,
                ],
                &format!("{prefix}-unmount"),
                timeout,
            )
            .await
            .map(|_| ())
    } else {
        Ok(())
    };
    ensure!(unmount.is_ok(), "source unmount is not confirmed");
    process::require_quiescent(attempt.root.path())?;
    let absent = mount(&attempt.path("source"))?.is_none();
    let mut retired = !attempt.journal.mount_attempted;
    if absent && attempt.journal.mount_attempted {
        let arguments = [
            "native-candidate-retire",
            "--workspace-id",
            &attempt.journal.workspace_id,
            "--expected-commit",
            &attempt.journal.request.candidate_commit,
        ];
        retired = attempt
            .control(&arguments, &format!("{prefix}-retire"), timeout)
            .await
            .is_ok_and(|reply| reply["retired"] == true);
        if !retired {
            // An interrupted stop may have no kernel mount but retain Ready
            // manager state. A completed retirement is already idempotent.
            attempt
                .control(
                    &[
                        "native-unmount",
                        "--workspace-id",
                        &attempt.journal.workspace_id,
                    ],
                    &format!("{prefix}-stop"),
                    timeout,
                )
                .await
                .ok();
            retired = attempt
                .control(&arguments, &format!("{prefix}-retire-retry"), timeout)
                .await
                .is_ok_and(|reply| reply["retired"] == true);
        }
    }
    ensure!(absent && retired, "cleanup is not confirmed");
    if forge_repository(config, &attempt.journal.request)? && attempt.path("d").exists() {
        // Shutdown never auto-starts a daemon; wait for its owner lock before
        // acknowledging that an isolated check's provider has stopped.
        let state = PrivateDir::open_existing(attempt.path("d"))?;
        let lock = state.open_lock("daemon.lock")?;
        if lock.try_lock_exclusive().is_err() {
            attempt
                .control(&["shutdown"], &format!("{prefix}-daemon-shutdown"), timeout)
                .await?;
            let deadline = Instant::now() + timeout;
            loop {
                if lock.try_lock_exclusive().is_ok() {
                    break;
                }
                ensure!(
                    Instant::now() < deadline,
                    "check daemon shutdown remains uncertain"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
        FileExt::unlock(&lock)?;
    }
    Ok(())
}
fn remaining(deadline: Instant) -> Result<Duration> {
    let duration = deadline.saturating_duration_since(Instant::now());
    ensure!(!duration.is_zero(), "check attempt deadline exceeded");
    Ok(duration)
}
fn startup(attempt: &Attempt) -> Vec<String> {
    vec![
        format!("--output_base={}", attempt.path("output").display()),
        "--nohome_rc".into(),
        "--nosystem_rc".into(),
        "--noworkspace_rc".into(),
        "--digest_function=BLAKE3".into(),
        "--unix_digest_hash_attribute_name=user.blake3".into(),
    ]
}
fn profile_flags(attempt: &Attempt, flags: &[String]) -> Result<Vec<String>> {
    flags
        .iter()
        .map(|flag| {
            if let Some((prefix, rest)) = flag.split_once("@runner_tool(") {
                let tool = rest
                    .strip_suffix(')')
                    .ok_or_else(|| anyhow::anyhow!("invalid profile tool"))?;
                ensure!(
                    attempt.journal.config.tools.contains_key(tool),
                    "profile tool unavailable"
                );
                Ok(format!(
                    "{prefix}{}",
                    attempt.path(&format!("tools/{tool}")).display()
                ))
            } else {
                Ok(flag.clone())
            }
        })
        .collect()
}
fn build_flags(config: &Config, attempt: &Attempt, profile: &crate::Profile) -> Vec<String> {
    let socket = format!("unix:{}", config.rbe_socket.display());
    let mut flags = vec![
        "--remote_executor=grpc://localhost".into(),
        "--remote_cache=grpc://localhost".into(),
        format!("--remote_proxy={socket}"),
        "--nogoogle_default_credentials".into(),
        format!("--remote_instance_name={}", config.remote_instance_name),
        "--bes_backend=".into(),
        "--remote_cache_compression".into(),
        format!("--remote_timeout={}", config.remote_timeout_seconds),
        "--remote_download_toplevel".into(),
        "--noremote_local_fallback".into(),
        "--spawn_strategy=remote".into(),
        "--disk_cache=".into(),
        "--lockfile_mode=off".into(),
        "--experimental_convenience_symlinks=ignore".into(),
        "--symlink_prefix=/".into(),
        "--nostamp".into(),
        "--color=no".into(),
        "--curses=no".into(),
        format!(
            "--repository_cache={}",
            attempt.path("repository-cache").display()
        ),
        format!(
            "--build_event_json_file={}",
            attempt.path("bep.jsonl").display()
        ),
        format!(
            "--execution_log_compact_file={}",
            attempt.path("execution.compact").display()
        ),
    ];
    flags.push(
        if profile.force_remote_execution {
            "--noremote_accept_cached"
        } else {
            "--remote_accept_cached"
        }
        .into(),
    );
    if profile.force_remote_execution && profile.mode == Mode::Test {
        flags.push("--nocache_test_results".into());
    }
    flags
}
fn validate_proof(
    value: &Value,
    request: &Request,
    id: &str,
    root: &Path,
    store: &str,
) -> Result<()> {
    ensure!(
        value["version"] == 1
            && value["workspace_id"] == id
            && value["commit"] == request.candidate_commit
            && value["root_tree"] == request.root_tree
            && value["access"] == "immutable_candidate"
            && value["root"].as_str() == root.to_str()
            && value["store"] == store
            && value["active"] == true,
        "immutable candidate proof differs"
    );
    Ok(())
}
struct Mount {
    source: String,
    readonly: bool,
    fuse: bool,
}
fn mount(root: &Path) -> Result<Option<Mount>> {
    #[cfg(target_os = "linux")]
    {
        let bytes = fs::read("/proc/self/mountinfo")?;
        ensure!(
            bytes.len() <= 8 * 1024 * 1024,
            "mount inventory limit exceeded"
        );
        let text = std::str::from_utf8(&bytes)?;
        let encoded = root
            .to_str()
            .context("mount root encoding")?
            .replace('\\', "\\134")
            .replace(' ', "\\040")
            .replace('\t', "\\011")
            .replace('\n', "\\012");
        let mut result = None;
        for line in text.lines() {
            let Some((before, after)) = line.split_once(" - ") else {
                continue;
            };
            let mut fields = before.split_whitespace();
            let mount_path = fields.nth(4);
            let options = fields.next();
            if mount_path != Some(encoded.as_str()) {
                continue;
            }
            ensure!(result.is_none(), "stacked candidate mounts are unsupported");
            let mut after = after.split_whitespace();
            let filesystem = after.next().unwrap_or("");
            let source = after.next().unwrap_or("");
            result = Some(Mount {
                source: source.into(),
                readonly: options.is_some_and(|s| s.split(',').any(|v| v == "ro")),
                fuse: filesystem.starts_with("fuse"),
            });
        }
        Ok(result)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = root;
        anyhow::bail!("native kernel mount inspection unsupported")
    }
}
fn require_readonly_mount(root: &Path, id: &str) -> Result<()> {
    let mounted = mount(root)?.context("candidate is not kernel-mounted")?;
    ensure!(
        mounted.fuse && mounted.readonly && mounted.source == format!("git-tools-native-{id}"),
        "kernel candidate policy differs"
    );
    Ok(())
}
