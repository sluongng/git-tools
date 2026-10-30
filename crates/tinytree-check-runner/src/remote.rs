//! One saved remote check intent. Cloudflare selects and owns the candidate;
//! this controller owns only disposable execution and exact result delivery.
use crate::{
    Config, Outcome, Receipt,
    candidate_http::{CandidateClient, CheckerHttpError},
    runner,
};
use anyhow::{Context, Result, bail, ensure};
use fs2::FileExt;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tinytree_forge_core::{
    accounts::AccountId,
    checker::{self, Call, CheckerLease, Command, Reply},
};
use tokio::{sync::watch, time::Instant};

const RECORD_LIMIT: usize = 128 * 1024;
const TOTAL_LIMIT: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteReport {
    pub schema: u32,
    pub account_id: String,
    pub claim_id: String,
    pub result: Reply,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Binding {
    account: String,
    profile: String,
    version: String,
    claim: String,
    origin: String,
    credential_reference: String,
}
#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Phase {
    Claiming,
    Leased,
    ExecutionIntent,
    Receipt,
    Terminal,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema: u32,
    binding: Binding,
    phase: Phase,
    lease: Option<CheckerLease>,
    receipt: Option<Receipt>,
    result_id: Option<String>,
    release_receipt: Option<Receipt>,
    terminal: Option<Reply>,
}
struct Owned {
    root: PrivateDir,
    _lock: fs::File,
    name: String,
    journal: Journal,
}
impl Owned {
    fn open(config: &Config, binding: Binding, recovery: bool) -> Result<Self> {
        // Runner scratch contains only native attempt directories. Keep the
        // bounded control outbox in a distinct deterministic private sibling.
        let mut name = config
            .scratch
            .file_name()
            .context("remote control root requires a named scratch directory")?
            .to_os_string();
        name.push("-remote");
        let control = config.scratch.with_file_name(name);
        let root = if recovery {
            PrivateDir::open_existing(&control)?
        } else {
            PrivateDir::create(&control)?
        };
        let identity = blake3::hash(binding.claim.as_bytes()).to_hex().to_string();
        let name = format!("remote-{identity}.json");
        let admission = root.open_lock("remote-admission.lock")?;
        admission.lock_exclusive()?;
        let _bytes = match root.read(&name, RECORD_LIMIT) {
            Ok(bytes) => bytes,
            Err(error) if !recovery && error.kind() == std::io::ErrorKind::NotFound => {
                let mut count = 0usize;
                let mut total = 0u64;
                for entry in fs::read_dir(root.path())? {
                    let entry = entry?;
                    if entry
                        .file_name()
                        .to_str()
                        .is_some_and(|s| s.starts_with("remote-") && s.ends_with(".json"))
                    {
                        let metadata = fs::symlink_metadata(entry.path())?;
                        ensure!(
                            metadata.is_file() && metadata.len() <= RECORD_LIMIT as u64,
                            "invalid remote journal storage"
                        );
                        count += 1;
                        total = total
                            .checked_add(metadata.len())
                            .context("remote journal quota overflow")?;
                    }
                }
                ensure!(
                    count < config.max_attempts.min(1024)
                        && total + RECORD_LIMIT as u64 <= TOTAL_LIMIT,
                    "remote journal quota exhausted"
                );
                let journal = Journal {
                    schema: 1,
                    binding: binding.clone(),
                    phase: Phase::Claiming,
                    lease: None,
                    receipt: None,
                    result_id: None,
                    release_receipt: None,
                    terminal: None,
                };
                let bytes = serde_json::to_vec(&journal)?;
                ensure!(bytes.len() <= RECORD_LIMIT, "remote journal exceeds limit");
                root.replace(&name, &bytes)?;
                bytes
            }
            Err(error) => return Err(error.into()),
        };
        let lock = root.open_lock(format!("remote-{identity}.lock"))?;
        lock.try_lock_exclusive()
            .context("remote check is already owned")?;
        // Reload after ownership: an earlier owner may have progressed between
        // admission and this lock, but its source/result identity cannot change.
        let bytes = root.read(&name, RECORD_LIMIT)?;
        let journal: Journal = serde_json::from_slice(&bytes)?;
        ensure!(
            journal.schema == 1 && journal.binding == binding,
            "remote check binding differs"
        );
        drop(admission);
        Ok(Self {
            root,
            _lock: lock,
            name,
            journal,
        })
    }
    fn save(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.journal)?;
        ensure!(bytes.len() <= RECORD_LIMIT, "remote journal exceeds limit");
        self.root.replace(&self.name, &bytes)?;
        Ok(())
    }
    fn report(&self, result: Reply) -> RemoteReport {
        RemoteReport {
            schema: 1,
            account_id: self.journal.binding.account.clone(),
            claim_id: self.journal.binding.claim.clone(),
            result,
        }
    }
    fn terminal(&mut self, result: Reply) -> Result<RemoteReport> {
        self.journal.terminal = Some(result.clone());
        self.journal.phase = Phase::Terminal;
        self.save()?;
        Ok(self.report(result))
    }
    fn call(&self, request_id: String, command: Command) -> Call {
        Call {
            schema: 1,
            account_id: self.journal.binding.account.clone(),
            request_id,
            command,
        }
    }
}

fn now() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}
fn binding(config: &Config, account: &str, profile: &str, claim: &str) -> Result<Binding> {
    config.validate()?;
    AccountId(account.to_owned()).validate()?;
    checker::request_id(claim)?;
    let source = config
        .https_snapshot
        .as_ref()
        .context("remote checks require explicit HTTPS snapshot configuration")?;
    let selected = config.profile(profile)?;
    ensure!(
        selected
            .repositories
            .iter()
            .any(|r| r == &format!("forge-{account}"))
            || selected.forge_accounts,
        "account is not authorized by profile"
    );
    Ok(Binding {
        account: account.to_owned(),
        profile: profile.to_owned(),
        version: config.profile_version(profile)?,
        claim: claim.to_owned(),
        origin: source.origin.clone(),
        credential_reference: source.credential_environment.clone(),
    })
}
fn unavailable(lease: &CheckerLease, reason: &str, cleanup: bool) -> Receipt {
    Receipt {
        schema: 1,
        request: lease.request.clone(),
        workspace_id: lease.request.workspace_id(),
        outcome: Outcome::Unavailable,
        reason: reason.to_owned(),
        cleanup_confirmed: cleanup,
        evidence: None,
    }
}
fn frozen(owned: &Owned, lease: &CheckerLease) -> Result<()> {
    lease.validate()?;
    ensure!(
        lease.scope.account_id == owned.journal.binding.account
            && lease.request.profile_name == owned.journal.binding.profile
            && lease.request.profile_version == owned.journal.binding.version,
        "selected checker lease differs from saved profile"
    );
    if let Some(original) = &owned.journal.lease {
        ensure!(
            original.immutable_binding()? == lease.immutable_binding()?,
            "selected checker lease was rebound"
        );
    }
    Ok(())
}

/// Execute at most the single candidate selected by this durable claim key.
pub async fn run_remote(
    config: &Config,
    account: &str,
    profile: &str,
    claim_id: &str,
    mut cancel: watch::Receiver<bool>,
) -> Result<RemoteReport> {
    let mut owned = Owned::open(config, binding(config, account, profile, claim_id)?, false)?;
    if let Some(terminal) = &owned.journal.terminal {
        return Ok(owned.report(terminal.clone()));
    }
    ensure!(
        matches!(owned.journal.phase, Phase::Claiming | Phase::Leased),
        "saved remote execution requires recover-remote"
    );
    let client = CandidateClient::from_config(
        &owned.journal.binding.origin,
        &owned.journal.binding.credential_reference,
    )?;
    let deadline = Instant::now() + Duration::from_secs(120);
    if owned.journal.phase == Phase::Claiming {
        for round in 0..512 {
            ensure!(
                !*cancel.borrow() && Instant::now() < deadline,
                "saved claim requires resume"
            );
            let command = if round == 0 {
                Command::GetClaim {
                    claim_id: claim_id.to_owned(),
                }
            } else {
                Command::Claim {
                    profile_name: profile.to_owned(),
                    profile_version: owned.journal.binding.version.clone(),
                }
            };
            let reply = client
                .call(
                    &owned.call(claim_id.to_owned(), command),
                    &mut cancel,
                    deadline,
                )
                .await?;
            match reply {
                Reply::Idle {} => return owned.terminal(Reply::Idle {}),
                Reply::Leased { lease } => {
                    frozen(&owned, &lease)?;
                    owned.journal.lease = Some(lease);
                    owned.journal.phase = Phase::Leased;
                    owned.save()?;
                    break;
                }
                Reply::NotFound {} | Reply::Preparing { .. } => {}
                _ => bail!("unexpected saved claim response"),
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    let lease = owned
        .journal
        .lease
        .clone()
        .context("saved claim preparation limit; resume original claim")?;
    frozen(&owned, &lease)?;
    ensure!(
        lease.expires_at > now()? && !*cancel.borrow(),
        "lease expired before execution; recover original claim"
    );
    owned.journal.phase = Phase::ExecutionIntent;
    owned.save()?;
    let execution_deadline = Instant::now()
        + Duration::from_secs(
            config.profile(profile)?.timeout_seconds + config.cleanup_timeout_seconds,
        );
    let mut heartbeat = Heartbeat::start(client.clone(), lease.clone(), cancel, execution_deadline);
    let result =
        runner::run_leased(config, lease.clone(), client.clone(), heartbeat.receiver()).await;
    let renewal = heartbeat.stop().await;
    let receipt = result?;
    receipt.validate_for(&lease.request)?;
    owned.journal.receipt = Some(receipt);
    owned.journal.result_id = Some(format!("complete-{}", lease.lease_id));
    if renewal.is_err() {
        owned.journal.release_receipt = Some(unavailable(
            &lease,
            "lease_uncertain",
            owned
                .journal
                .receipt
                .as_ref()
                .is_some_and(|r| r.cleanup_confirmed),
        ));
    }
    owned.journal.phase = Phase::Receipt;
    owned.save()?;
    deliver(&mut owned, &client).await
}

/// Recovery never executes Bazel or downloads source. An execution intent is
/// reconciled exclusively through the existing owned runner cleanup protocol.
pub async fn recover_remote(
    config: &Config,
    account: &str,
    profile: &str,
    claim_id: &str,
    mut cancel: watch::Receiver<bool>,
) -> Result<RemoteReport> {
    let mut owned = Owned::open(config, binding(config, account, profile, claim_id)?, true)?;
    if let Some(terminal) = &owned.journal.terminal {
        return Ok(owned.report(terminal.clone()));
    }
    let client = CandidateClient::from_config(
        &owned.journal.binding.origin,
        &owned.journal.binding.credential_reference,
    )?;
    if owned.journal.phase == Phase::Claiming {
        let reply = client
            .call(
                &owned.call(
                    claim_id.to_owned(),
                    Command::GetClaim {
                        claim_id: claim_id.to_owned(),
                    },
                ),
                &mut cancel,
                Instant::now() + Duration::from_secs(30),
            )
            .await?;
        match reply {
            Reply::Idle {} => return owned.terminal(Reply::Idle {}),
            Reply::NotFound {} | Reply::Preparing { .. } => return Ok(owned.report(reply)),
            Reply::Leased { lease } => {
                frozen(&owned, &lease)?;
                owned.journal.lease = Some(lease);
                owned.journal.phase = Phase::Leased;
                owned.save()?;
            }
            _ => bail!("unexpected saved claim response"),
        }
    }
    let lease = owned.journal.lease.clone().context("saved lease missing")?;
    frozen(&owned, &lease)?;
    let receipt = if matches!(owned.journal.phase, Phase::ExecutionIntent | Phase::Receipt) {
        runner::recover(config, lease.request.clone()).await?
    } else {
        unavailable(&lease, "execution_not_started", true)
    };
    receipt.validate_for(&lease.request)?;
    if let Some(original) = &owned.journal.receipt {
        if original.cleanup_confirmed {
            ensure!(original == &receipt, "recovered physical receipt differs");
        } else {
            owned.journal.release_receipt = Some(unavailable(
                &lease,
                "execution_recovered",
                receipt.cleanup_confirmed,
            ));
        }
    } else {
        owned.journal.receipt = Some(receipt.clone());
        // Recovery cannot assert continued live execution authority. It may
        // recover an already accepted result, otherwise closes cleanup only.
        owned.journal.release_receipt = Some(unavailable(
            &lease,
            "execution_recovered",
            receipt.cleanup_confirmed,
        ));
    }
    owned.journal.result_id = Some(format!("complete-{}", lease.lease_id));
    owned.journal.phase = Phase::Receipt;
    owned.save()?;
    deliver(&mut owned, &client).await
}

async fn deliver(owned: &mut Owned, client: &CandidateClient) -> Result<RemoteReport> {
    let lease = owned.journal.lease.clone().context("saved lease missing")?;
    let receipt = owned
        .journal
        .receipt
        .clone()
        .context("saved execution receipt missing")?;
    let result_id = owned
        .journal
        .result_id
        .clone()
        .context("saved result key missing")?;
    ensure!(
        receipt.cleanup_confirmed
            || owned
                .journal
                .release_receipt
                .as_ref()
                .is_some_and(|r| r.cleanup_confirmed),
        "remote custody requires physical cleanup recovery"
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    // Cleanup/reporting is deliberately drainable after external cancellation.
    let (_sender, mut cancel) = watch::channel(false);
    for _ in 0..3 {
        let found = client
            .call(
                &owned.call(
                    format!("get-{result_id}"),
                    Command::GetResult {
                        lease: lease.reference(),
                        result_request_id: result_id.clone(),
                    },
                ),
                &mut cancel,
                deadline,
            )
            .await?;
        match found {
            Reply::Completed { ref completion } => {
                ensure!(
                    completion.receipt_digest == checker::receipt_digest(&receipt)?
                        && completion.outcome == receipt.outcome,
                    "accepted checker result differs from saved receipt"
                );
                return owned.terminal(found);
            }
            Reply::NotFound {} => {}
            _ => bail!("unexpected saved result response"),
        }
        if owned.journal.release_receipt.is_some() {
            return release(owned, client, &mut cancel, deadline).await;
        }
        let completed = client
            .call(
                &owned.call(
                    result_id.clone(),
                    Command::Complete {
                        lease: lease.reference(),
                        receipt: receipt.clone(),
                    },
                ),
                &mut cancel,
                deadline,
            )
            .await;
        match completed {
            Ok(reply @ Reply::Completed { .. }) => return owned.terminal(reply),
            Ok(_) => bail!("unexpected checker completion"),
            Err(error)
                if error.downcast_ref::<CheckerHttpError>().is_some_and(|e| {
                    matches!(
                        (e.status, e.code.as_str()),
                        (400, "failed_precondition") | (409, "aborted")
                    )
                }) =>
            {
                owned.journal.release_receipt = Some(unavailable(
                    &lease,
                    "lease_no_longer_current",
                    receipt.cleanup_confirmed,
                ));
                owned.save()?;
                return release(owned, client, &mut cancel, deadline).await;
            }
            Err(_) => {} // A lost ACK must be read/replayed, never re-executed.
        }
    }
    bail!("saved result delivery requires recover-remote")
}
async fn release(
    owned: &mut Owned,
    client: &CandidateClient,
    cancel: &mut watch::Receiver<bool>,
    deadline: Instant,
) -> Result<RemoteReport> {
    let lease = owned
        .journal
        .lease
        .as_ref()
        .context("saved lease missing")?;
    let receipt = owned
        .journal
        .release_receipt
        .as_ref()
        .context("saved cleanup receipt missing")?;
    ensure!(
        receipt.cleanup_confirmed && receipt.outcome == Outcome::Unavailable,
        "remote custody cleanup is not confirmed"
    );
    let reply = client
        .call(
            &owned.call(
                format!("release-{}", lease.lease_id),
                Command::Release {
                    lease: lease.reference(),
                    receipt: receipt.clone(),
                },
            ),
            cancel,
            deadline,
        )
        .await?;
    ensure!(
        matches!(reply, Reply::Released { .. }),
        "unexpected checker cleanup response"
    );
    owned.terminal(reply)
}

struct Heartbeat {
    stop: watch::Sender<bool>,
    cancel: watch::Sender<bool>,
    task: Option<tokio::task::JoinHandle<Result<()>>>,
}
impl Heartbeat {
    fn start(
        client: CandidateClient,
        mut lease: CheckerLease,
        mut external: watch::Receiver<bool>,
        deadline: Instant,
    ) -> Self {
        let (stop, mut stopped) = watch::channel(false);
        let (cancel, mut receiver) = watch::channel(false);
        let failed = cancel.clone();
        let task = tokio::spawn(async move {
            let result = async {
                loop {
                    ensure!(!*external.borrow() && Instant::now() < deadline, "checker execution cancelled or expired");
                    let seconds = lease.expires_at.saturating_sub(now()?);
                    ensure!(seconds > 0, "checker lease expired");
                    let delay = Duration::from_secs((seconds / 3).clamp(1, 20));
                    tokio::select! {
                        biased;
                        _ = stopped.changed() => return Ok(()),
                        changed = external.changed() => { ensure!(changed.is_ok(), "checker cancellation owner disappeared"); continue; },
                        _ = tokio::time::sleep(delay) => {}
                    }
                    let call = Call { schema: 1, account_id: lease.scope.account_id.clone(), request_id: format!("renew-{}-{}", lease.lease_id, lease.generation),
                        command: Command::Renew { lease: lease.reference(), expected_generation: lease.generation } };
                    let reply = tokio::select! {
                        biased;
                        _ = stopped.changed() => return Ok(()),
                        changed = external.changed() => { ensure!(changed.is_ok(), "checker cancellation owner disappeared"); continue; },
                        reply = client.call(&call, &mut receiver, deadline) => reply?,
                    };
                    let Reply::Leased { lease: renewed } = reply else { bail!("unexpected checker renewal"); };
                    ensure!(renewed.immutable_binding()? == lease.immutable_binding()? && renewed.expires_at > now()?, "checker renewal rebound execution");
                    lease = renewed;
                }
            }.await;
            if result.is_err() {
                let _ = failed.send(true);
            }
            result
        });
        Self {
            stop,
            cancel,
            task: Some(task),
        }
    }
    fn receiver(&self) -> watch::Receiver<bool> {
        self.cancel.subscribe()
    }
    async fn stop(&mut self) -> Result<()> {
        let _ = self.stop.send(true);
        self.task
            .take()
            .context("checker heartbeat already drained")?
            .await?
    }
}
impl Drop for Heartbeat {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        let _ = self.cancel.send(true);
    }
}
