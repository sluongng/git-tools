use crate::{
    ExportOptions, Report,
    convert::{Converter, decode},
    git::Git,
    state::{Pending, Phase, Position, Resources, State, hex, usage},
};
use anyhow::{Context, Result, ensure};
use std::{io::Read, sync::Arc};
use tinytree_client::{Config, NativeClient, protocol as p};
use tokio::time::Instant;

pub(crate) async fn run(
    config: Config,
    options: ExportOptions,
    deadline: Instant,
) -> Result<Report> {
    validate_input(&config, &options)?;
    let owner = Resources::open(options)?;
    run_with_resources(config, owner, deadline, None).await
}
#[derive(Clone)]
pub(crate) struct TrustSnapshot {
    pub ca: Option<Arc<[u8]>>,
}
pub(crate) fn validate_input(config: &Config, options: &ExportOptions) -> Result<()> {
    config.validate()?;
    ensure!(
        !options.source_reference.is_empty()
            && options.source_reference.len() <= 4096
            && !options.source_reference.starts_with("_tinytree/")
            && !options.source_reference.contains('\0'),
        "export selection must be an exact public reference"
    );
    ensure!(
        options.destination_reference.starts_with("refs/heads/")
            && options.destination_reference.len() <= 4096,
        "destination must be one exact branch reference"
    );
    Ok(())
}
pub(crate) async fn run_with_resources(
    mut config: Config,
    owner: Arc<Resources>,
    deadline: Instant,
    trust: Option<&TrustSnapshot>,
) -> Result<Report> {
    validate_input(&config, &owner.options)?;
    let (identity, ca) = scope(&config, &owner, trust)?;
    let mut state = owner.load(identity)?;
    let git = Git::new(owner.clone(), deadline)?;
    git.initialize().await?;
    let maximum = owner
        .options
        .limits
        .max_objects
        .checked_mul(4)
        .and_then(|n| n.checked_add(4096))
        .context("disk inventory bound overflow")?;
    let physical = owner.clone();
    let disk = tokio::task::spawn_blocking(move || {
        let destination = usage(physical.destination.path(), maximum, 64)?;
        ensure!(
            destination <= physical.options.limits.max_destination_bytes,
            "Git destination disk budget exceeded"
        );
        let spool = usage(physical.spool.path(), 4096, 4)?;
        ensure!(
            spool <= physical.options.limits.max_file_bytes as u64 * 2,
            "adapter temporary files require bounded reconciliation"
        );
        Ok::<_, anyhow::Error>(destination)
    })
    .await??;
    let mut commits = 0;
    let mut file_bytes = 0;

    // Prepared bytes/mappings can finish their local CAS/readback even if the
    // native service is temporarily unavailable. Retention cleanup stays pending.
    if state
        .pending
        .as_ref()
        .is_some_and(|p| p.phase == Phase::Ready)
    {
        finish_ready(&owner, &git, &mut state).await?;
    }
    let had_release = state
        .pending
        .as_ref()
        .is_some_and(|p| p.phase == Phase::Release);
    admit_memory(&config, &owner.options.limits, 0)?;
    config.limits.max_file_bytes = config
        .limits
        .max_file_bytes
        .min(owner.options.limits.max_file_bytes as u64);
    config.limits.max_metadata_bytes = config
        .limits
        .max_metadata_bytes
        .min(owner.options.limits.max_metadata_bytes);
    config.limits.max_temporary_bytes = config
        .limits
        .max_temporary_bytes
        .min(owner.options.limits.max_file_bytes as u32);
    let remaining = deadline
        .saturating_duration_since(Instant::now())
        .as_millis();
    ensure!(remaining > 0, "native connection deadline exceeded");
    let remaining = u64::try_from(remaining)?;
    config.rpc.operation_timeout_ms = config.rpc.operation_timeout_ms.min(remaining);
    config.rpc.connect_timeout_ms = config.rpc.connect_timeout_ms.min(remaining);
    let client = if let Some(ca) = ca {
        NativeClient::connect_with_ca_certificate(config.clone(), ca).await
    } else {
        NativeClient::connect(config.clone()).await
    }
    .map_err(|_| anyhow::anyhow!("native client connection failed; pending state retained"))?
    .with_operation_deadline(deadline)
    .with_lifetime_guard(owner.clone());
    if had_release {
        release(&owner, &git, &client, &config.repository_id, &mut state).await?;
        return report(&state, commits, file_bytes, true);
    }
    if state.pending.is_none() {
        let observed = client
            .open(vec![owner.options.source_reference.clone()])
            .await
            .map_err(|_| anyhow::anyhow!("cannot observe selected public native reference"))?;
        ensure!(
            observed.repository_id == config.repository_id && observed.references.len() == 1,
            "native repository identity differs"
        );
        let selected = &observed.references[0];
        let descriptor = selected
            .commit
            .as_ref()
            .context("selected public native reference is absent; Git deletion is not automatic")?;
        let key = descriptor
            .key
            .as_ref()
            .context("native reference descriptor absent")?;
        ensure!(
            key.kind == p::ObjectKind::Commit as i32
                && key.native_id.len() == 32
                && key.native_id.iter().any(|b| *b != 0),
            "public reference does not name a stored commit"
        );
        let native = hex(&key.native_id);
        let current = git.reference().await?;
        match &state.delivered {
            Some(previous) => {
                ensure!(
                    selected.generation >= previous.generation,
                    "native reference generation regressed"
                );
                ensure!(
                    current.as_deref() == Some(previous.git.as_str()),
                    "destination diverged; no overwrite attempted"
                );
                if native == previous.native {
                    let mapping = state
                        .mappings
                        .get(&native)
                        .context("delivered mapping absent")?;
                    git.verify_object(
                        "commit",
                        &mapping.git,
                        mapping.payload_blake3,
                        mapping.payload_size,
                    )
                    .await?;
                    git.run(
                        &[
                            "fsck",
                            "--strict",
                            "--no-reflogs",
                            "--no-dangling",
                            &mapping.git,
                        ],
                        Vec::new(),
                        65536,
                    )
                    .await?;
                    state.delivered.as_mut().unwrap().generation = selected.generation;
                    save(&owner, &state, deadline).await?;
                    return report(&state, 0, 0, true);
                }
                ensure!(
                    selected.generation > previous.generation,
                    "native reference changed without generation advancement"
                );
            }
            None => ensure!(
                current.is_none(),
                "initial destination branch must be empty; arbitrary anchors are not accepted"
            ),
        }
        let job = uuid::Uuid::new_v4().simple().to_string();
        state.pending = Some(Pending {
            generation: selected.generation,
            native: native.clone(),
            expected_git: current,
            git: None,
            job,
            hold: format!("_tinytree/hold/v1/{}/{native}", hex(&state.client)),
            acquire_generation: None,
            release_generation: None,
            phase: Phase::Acquiring,
        });
        save(&owner, &state, deadline).await?;
    }
    acquire(&owner, &client, &config.repository_id, &mut state, deadline).await?;
    #[cfg(test)]
    if owner.options.fault == Some(crate::TestFault::BeforeObjects) {
        anyhow::bail!("injected interruption before objects");
    }
    let head = state.pending.as_ref().unwrap().native.clone();
    let mut converter = Converter::new(
        client.clone(),
        git.clone(),
        state.mappings.clone(),
        disk,
        config.limits.max_buffered_bytes as u64,
    );
    let oid = converter.commits(&head).await?;
    if let Some(previous) = &state.delivered {
        ensure!(
            converter.contains_ancestor(&previous.native, &head)?,
            "native public history diverged; no Git overwrite attempted"
        );
    }
    // Git fsck independently validates the newly produced public object graph
    // before a destination reference can make it reachable.
    git.run(
        &["fsck", "--strict", "--no-reflogs", "--no-dangling", &oid],
        Vec::new(),
        65536,
    )
    .await?;
    commits = converter.new_commits;
    file_bytes = converter.file_bytes;
    let reused = commits == 0;
    state.mappings = converter.mappings;
    drop(converter.client);
    let pending = state.pending.as_mut().unwrap();
    pending.git = Some(oid);
    pending.phase = Phase::Ready;
    save(&owner, &state, deadline).await?;
    #[cfg(test)]
    if owner.options.fault == Some(crate::TestFault::BeforeRef) {
        anyhow::bail!("injected interruption before ref publication");
    }
    finish_ready(&owner, &git, &mut state).await?;
    release(&owner, &git, &client, &config.repository_id, &mut state).await?;
    report(&state, commits, file_bytes, reused)
}
async fn save(owner: &Arc<Resources>, state: &State, deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "adapter checkpoint deadline exceeded; pending state retained"
    );
    owner.save(state).await
}
pub(crate) fn scope(
    config: &Config,
    owner: &Resources,
    trust: Option<&TrustSnapshot>,
) -> Result<([u8; 32], Option<Arc<[u8]>>)> {
    scope_for_destination(config, &owner.options, owner.destination.path(), trust)
}
pub(crate) fn scope_for_destination(
    config: &Config,
    options: &ExportOptions,
    destination: &std::path::Path,
    trust: Option<&TrustSnapshot>,
) -> Result<([u8; 32], Option<Arc<[u8]>>)> {
    if let Some(captured) = trust {
        ensure!(
            config.tls.is_some() == captured.ca.is_some(),
            "captured native trust configuration differs"
        );
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-public-git-export-v1\0");
    for value in [
        &config.endpoint,
        &config.repository_id,
        &options.source_reference,
        &options.destination_reference,
    ] {
        hash.update(&(value.len() as u64).to_le_bytes());
        hash.update(value.as_bytes());
    }
    let destination = serde_json::to_vec(destination)?;
    hash.update(&(destination.len() as u64).to_le_bytes());
    hash.update(&destination);
    hash.update(&[u8::from(config.token_env.is_some())]);
    if let Some(name) = &config.token_env {
        let token = std::env::var(name)
            .map_err(|_| anyhow::anyhow!("native credential environment unavailable"))?;
        hash.update(&(name.len() as u64).to_le_bytes());
        hash.update(name.as_bytes());
        hash.update(blake3::hash(token.as_bytes()).as_bytes());
    }
    hash.update(&[u8::from(config.tls.is_some())]);
    let mut ca = None;
    if let Some(tls) = &config.tls {
        use std::os::unix::fs::OpenOptionsExt;
        let encoded = serde_json::to_vec(tls)?;
        hash.update(&(encoded.len() as u64).to_le_bytes());
        hash.update(&encoded);
        let certificate = if let Some(captured) = trust {
            captured
                .ca
                .as_ref()
                .context("captured native trust is absent")?
                .as_ref()
                .to_vec()
        } else {
            let mut certificate = Vec::new();
            let file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
                .open(&tls.ca_certificate)?;
            ensure!(
                file.metadata()?.is_file(),
                "native TLS roots must be a regular file"
            );
            file.take(1024 * 1024 + 1).read_to_end(&mut certificate)?;
            ensure!(
                certificate.len() <= 1024 * 1024,
                "native TLS configuration exceeds identity budget"
            );
            certificate
        };
        hash.update(&certificate);
        ca = Some(Arc::<[u8]>::from(certificate));
    }
    Ok((*hash.finalize().as_bytes(), ca))
}
async fn hold(client: &NativeClient, name: &str, head: &str) -> Result<(u64, bool)> {
    let view = client
        .open(vec![name.into()])
        .await
        .map_err(|_| anyhow::anyhow!("native retention reconciliation failed"))?;
    let reference = view
        .references
        .first()
        .context("native retention reference absent")?;
    if let Some(commit) = &reference.commit {
        let key = commit.key.as_ref().context("native retention key absent")?;
        ensure!(
            key.kind == p::ObjectKind::Commit as i32 && hex(&key.native_id) == head,
            "native retention reference differs"
        );
        Ok((reference.generation, true))
    } else {
        Ok((reference.generation, false))
    }
}
async fn acquire(
    owner: &Arc<Resources>,
    client: &NativeClient,
    repository: &str,
    state: &mut State,
    deadline: Instant,
) -> Result<()> {
    let pending = state
        .pending
        .as_ref()
        .context("pending export absent")?
        .clone();
    let (generation, present) = hold(client, &pending.hold, &pending.native).await?;
    if !present {
        let expected = match pending.acquire_generation {
            Some(expected) => expected,
            None => {
                state.pending.as_mut().unwrap().acquire_generation = Some(generation);
                save(owner, state, deadline).await?;
                generation
            }
        };
        client
            .publish(p::PublishReferenceRequest {
                repository_id: repository.into(),
                reference_name: pending.hold.clone(),
                expected_generation: expected,
                new_commit: Some(p::ObjectKey {
                    kind: p::ObjectKind::Commit as i32,
                    native_id: decode::<32>(&pending.native)?.to_vec(),
                }),
                idempotency_key: format!("git_export_{}_hold", pending.job),
                staging_leases: vec![],
            })
            .await
            .map_err(|_| anyhow::anyhow!("native retention acquisition requires reconciliation"))?;
        ensure!(
            hold(client, &pending.hold, &pending.native).await?.1,
            "native hold is not currently retained"
        );
    }
    state.pending.as_mut().unwrap().phase = Phase::Held;
    save(owner, state, deadline).await
}
async fn finish_ready(owner: &Arc<Resources>, git: &Git, state: &mut State) -> Result<()> {
    let pending = state
        .pending
        .as_ref()
        .context("prepared export absent")?
        .clone();
    let oid = pending
        .git
        .as_ref()
        .context("prepared Git identity absent")?;
    let mapping = state
        .mappings
        .get(&pending.native)
        .context("prepared native/Git mapping absent")?;
    ensure!(&mapping.git == oid, "prepared export mapping differs");
    git.verify_object("commit", oid, mapping.payload_blake3, mapping.payload_size)
        .await?;
    git.run(
        &["fsck", "--strict", "--no-reflogs", "--no-dangling", oid],
        Vec::new(),
        65536,
    )
    .await?;
    git.deliver(oid, pending.expected_git.as_deref()).await?;
    #[cfg(test)]
    if owner.options.fault == Some(crate::TestFault::AfterRef) {
        anyhow::bail!("injected interruption after ref publication");
    }
    state.delivered = Some(Position {
        generation: pending.generation,
        native: pending.native,
        git: oid.clone(),
    });
    state.pending.as_mut().unwrap().phase = Phase::Release;
    save(owner, state, git.deadline).await
}
async fn release(
    owner: &Arc<Resources>,
    git: &Git,
    client: &NativeClient,
    repository: &str,
    state: &mut State,
) -> Result<()> {
    let pending = state
        .pending
        .as_ref()
        .context("retention release intent absent")?
        .clone();
    ensure!(
        pending.phase == Phase::Release,
        "native hold cannot release before delivery checkpoint"
    );
    let delivered = state
        .delivered
        .as_ref()
        .context("release lacks delivered checkpoint")?;
    ensure!(
        delivered.native == pending.native
            && delivered.generation == pending.generation
            && pending.git.as_deref() == Some(delivered.git.as_str()),
        "release checkpoint binding differs"
    );
    let mapping = state
        .mappings
        .get(&delivered.native)
        .context("release mapping absent")?;
    ensure!(mapping.git == delivered.git, "release mapping differs");
    ensure!(
        git.reference().await?.as_deref() == Some(delivered.git.as_str()),
        "receiver diverged before retention release; hold preserved"
    );
    git.verify_object(
        "commit",
        &mapping.git,
        mapping.payload_blake3,
        mapping.payload_size,
    )
    .await?;
    git.run(
        &[
            "fsck",
            "--strict",
            "--no-reflogs",
            "--no-dangling",
            &mapping.git,
        ],
        Vec::new(),
        65536,
    )
    .await?;
    #[cfg(test)]
    if owner.options.fault == Some(crate::TestFault::BeforeRelease) {
        anyhow::bail!("injected interruption before retention release");
    }
    let (generation, present) = hold(client, &pending.hold, &pending.native).await?;
    if present {
        let expected = if let Some(value) = pending.release_generation {
            value
        } else {
            state.pending.as_mut().unwrap().release_generation = Some(generation);
            owner.save(state).await?;
            generation
        };
        client
            .publish(p::PublishReferenceRequest {
                repository_id: repository.into(),
                reference_name: pending.hold.clone(),
                expected_generation: expected,
                new_commit: None,
                idempotency_key: format!("git_export_{}_release", pending.job),
                staging_leases: vec![],
            })
            .await
            .map_err(|_| {
                anyhow::anyhow!("delivered Git export retains a pending native release intent")
            })?;
        ensure!(
            !hold(client, &pending.hold, &pending.native).await?.1,
            "native retention release requires readback"
        );
    }
    state.pending = None;
    owner.save(state).await?;
    Ok(())
}
fn report(state: &State, commits: usize, file_bytes: u64, reused: bool) -> Result<Report> {
    let delivered = state
        .delivered
        .as_ref()
        .context("Git delivery checkpoint absent")?;
    Ok(Report {
        source_generation: delivered.generation,
        native_head: delivered.native.clone(),
        git_head: delivered.git.clone(),
        exported_commits: commits,
        file_bytes,
        reused,
        delivered: true,
        hold_released: state.pending.is_none(),
    })
}

pub(crate) fn admit_memory(config: &Config, limits: &crate::Limits, extra: u64) -> Result<()> {
    let reserve = (limits.max_file_bytes as u64) * 2
        + (limits.max_state_bytes as u64) * 4
        + (limits.max_metadata_bytes as u64) * 8
        + (limits.max_objects as u64) * 256
        + config.limits.max_buffered_bytes as u64;
    ensure!(
        reserve.saturating_add(extra) <= limits.max_memory_bytes,
        "configured SDK and adapter memory budgets do not fit"
    );
    Ok(())
}
