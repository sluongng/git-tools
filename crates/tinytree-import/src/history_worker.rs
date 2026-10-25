use super::{
    HistoryLimits, HistoryOptions, HistoryProof, HistoryReport, POLICY,
    graph::{Graph, Manifest, Raw, Source, Stored},
    io::Git,
    state::{Journal, Phase, Resources, hex, id, valid_hex},
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::BTreeMap,
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    sync::Arc,
};
use tinytree_client::{Config, MetadataOutcome, NativeClient, protocol as p};
use tinytree_model as m;
use tokio::time::Instant;

pub(super) async fn run(
    mut config: Config,
    options: HistoryOptions,
    deadline: Instant,
) -> Result<HistoryReport> {
    config.validate()?;
    let owner = Resources::open(options)?;
    let (scope, ca) = scope(&config, &owner.options)?;
    let mut journal = owner.load(scope)?;
    save(&owner, &journal, deadline).await?;
    let reserve = reservation(&config, &owner.options.limits)?;
    let client = connect(&mut config, ca, deadline, &owner.options.limits)
        .await?
        .with_lifetime_guard(owner.clone());
    let existing = if let (Some(manifest), Some(checkpoint), Some(hold)) =
        (&journal.manifest, &journal.checkpoint, &journal.hold)
    {
        ensure!(
            manifest.scope == journal.scope
                && manifest.job == journal.job
                && manifest.client == journal.client
                && manifest.repository == config.repository_id
                && manifest.reference == owner.options.reference
                && manifest.expected_generation == owner.options.expected_generation
                && manifest.git_head == owner.options.revision,
            "history journal/manifest binding differs"
        );
        if observe_hold(&client, hold, checkpoint).await?.1 {
            let proof = HistoryProof {
                retention_hold: hold.clone(),
                provenance_checkpoint: checkpoint.clone(),
            };
            let (verified, graph) =
                read_verified(&client, &owner, &proof, reserve, deadline).await?;
            ensure!(
                &verified == manifest,
                "retained provenance differs from private journal"
            );
            Some(graph)
        } else {
            None
        }
    } else {
        None
    };
    let graph = if let Some(graph) = existing {
        graph
    } else {
        let git = Git::new(owner.clone(), deadline)?;
        git.validate().await?;
        let mut graph = Graph::new(
            Source::Git(git),
            owner.options.limits.clone(),
            reserve,
            deadline,
        )?;
        graph.build(&owner.options.revision).await?;
        let estimate = graph
            .commits
            .values()
            .chain(graph.trees.values())
            .try_fold(4096usize, |n, r| {
                n.checked_add(r.raw.len().saturating_mul(4) + 512)
                    .context("provenance capacity overflow")
            })?
            .checked_add(graph.blobs.len().saturating_mul(320))
            .context("provenance capacity overflow")?;
        ensure!(
            estimate <= owner.options.limits.max_manifest_bytes,
            "provenance manifest capacity exceeded"
        );
        let manifest = graph.manifest(
            config.repository_id.clone(),
            owner.options.reference.clone(),
            owner.options.expected_generation,
            journal.scope,
            journal.job.clone(),
            journal.client,
            owner.options.revision.clone(),
        );
        graph.validate(&manifest)?;
        if let Some(previous) = &journal.manifest {
            ensure!(
                previous == &manifest,
                "source history differs from prepared migration"
            );
        }
        let artifacts = checkpoint(&manifest, owner.options.limits.max_manifest_bytes)?;
        let checkpoint_id = hex(&artifacts[2].descriptor.key.id.0);
        let manifest_id = hex(&artifacts[0].descriptor.key.id.0);
        let hold = format!("_tinytree/hold/v1/{}/{checkpoint_id}", hex(&journal.client));
        ensure!(
            journal
                .checkpoint
                .as_ref()
                .is_none_or(|v| v == &checkpoint_id)
                && journal
                    .manifest_id
                    .as_ref()
                    .is_none_or(|v| v == &manifest_id)
                && journal.hold.as_ref().is_none_or(|v| v == &hold),
            "prepared provenance identity differs"
        );
        journal.manifest = Some(manifest);
        journal.checkpoint = Some(checkpoint_id.clone());
        journal.manifest_id = Some(manifest_id);
        journal.hold = Some(hold.clone());
        journal.phase = Phase::Prepared;
        save(&owner, &journal, deadline).await?;
        for stored in graph.native.values() {
            upload(&client, &owner, stored, deadline).await?;
        }
        for artifact in &artifacts {
            upload(&client, &owner, artifact, deadline).await?;
        }
        let (generation, present) = observe_hold(&client, &hold, &checkpoint_id).await?;
        if !present {
            let expected = if let Some(n) = journal.hold_generation {
                n
            } else {
                journal.hold_generation = Some(generation);
                save(&owner, &journal, deadline).await?;
                generation
            };
            client
                .publish(p::PublishReferenceRequest {
                    repository_id: config.repository_id.clone(),
                    reference_name: hold.clone(),
                    expected_generation: expected,
                    new_commit: Some(wire(m::Kind::Commit, &checkpoint_id)?),
                    idempotency_key: format!("history_{}_hold", journal.job),
                    staging_leases: vec![],
                })
                .await
                .map_err(|_| {
                    anyhow::anyhow!("history provenance retention requires reconciliation")
                })?;
        }
        ensure!(
            observe_hold(&client, &hold, &checkpoint_id).await?.1,
            "history provenance is not freshly retained"
        );
        let proof = HistoryProof {
            retention_hold: hold,
            provenance_checkpoint: checkpoint_id,
        };
        // Independent native readback verifies every original Git preimage and
        // canonical native/tree/parent association before public publication.
        drop(graph);
        let (verified, reloaded) =
            read_verified(&client, &owner, &proof, reserve, deadline).await?;
        ensure!(
            Some(&verified) == journal.manifest.as_ref(),
            "remote provenance differs"
        );
        reloaded
    };
    let manifest = journal
        .manifest
        .as_ref()
        .context("prepared history manifest absent")?
        .clone();
    let checkpoint_id = journal
        .checkpoint
        .clone()
        .context("history checkpoint absent")?;
    let hold = journal.hold.clone().context("history hold absent")?;
    let artifacts = checkpoint(&manifest, owner.options.limits.max_manifest_bytes)?;
    ensure!(
        hex(&artifacts[2].descriptor.key.id.0) == checkpoint_id
            && journal.manifest_id.as_deref()
                == Some(hex(&artifacts[0].descriptor.key.id.0).as_str()),
        "journal provenance descriptors differ"
    );
    graph.validate(&manifest)?;
    drop(graph);
    ensure!(
        observe_hold(&client, &hold, &checkpoint_id).await?.1,
        "provenance hold changed before publication"
    );
    journal.phase = Phase::Held;
    save(&owner, &journal, deadline).await?;
    journal.phase = Phase::Publishing;
    save(&owner, &journal, deadline).await?;
    let receipt = match client.publication(&publication_key(&manifest)).await {
        Ok(receipt) => receipt,
        Err(error) if error.code() == tonic::Code::NotFound => {
            let current = client
                .open(vec![manifest.reference.clone()])
                .await
                .map_err(|_| {
                    anyhow::anyhow!("history target observation unavailable; provenance retained")
                })?;
            let reference = current
                .references
                .first()
                .context("history target observation absent")?;
            ensure!(
                reference.commit.is_none() && reference.generation == manifest.expected_generation,
                "history destination is not empty at the expected generation; provenance retained"
            );
            client
                .publish(p::PublishReferenceRequest {
                    repository_id: manifest.repository.clone(),
                    reference_name: manifest.reference.clone(),
                    expected_generation: manifest.expected_generation,
                    new_commit: Some(wire(m::Kind::Commit, &manifest.native_head)?),
                    idempotency_key: publication_key(&manifest),
                    staging_leases: vec![],
                })
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "history public CAS failed or requires reconciliation; provenance retained"
                    )
                })?
        }
        Err(_) => {
            anyhow::bail!("history publication reconciliation unavailable; provenance retained")
        }
    };

    let generation = validate_receipt(&manifest, &receipt)?;
    journal.publication_generation = Some(generation);
    journal.phase = Phase::Published;
    save(&owner, &journal, deadline).await?;
    report(
        &client,
        &manifest,
        &HistoryProof {
            retention_hold: hold,
            provenance_checkpoint: checkpoint_id,
        },
        Some(generation),
    )
    .await
}
fn reservation(config: &Config, l: &HistoryLimits) -> Result<u64> {
    let reserve = config.limits.max_buffered_bytes as u64
        + (l.max_file_bytes as u64) * 2
        + (l.max_metadata_bytes as u64) * 8
        + (l.max_manifest_bytes as u64) * 6
        + (l.max_objects as u64) * 256;
    ensure!(
        reserve <= l.max_memory_bytes,
        "SDK/history memory budgets do not fit"
    );
    Ok(reserve)
}
async fn save(owner: &Arc<Resources>, j: &Journal, deadline: Instant) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "history checkpoint deadline exceeded"
    );
    owner.save(j).await
}
async fn connect(
    config: &mut Config,
    ca: Option<Arc<[u8]>>,
    deadline: Instant,
    limits: &HistoryLimits,
) -> Result<NativeClient> {
    let maximum = limits.max_file_bytes.max(limits.max_manifest_bytes) as u64;
    config.limits.max_file_bytes = config.limits.max_file_bytes.min(maximum);
    config.limits.max_temporary_bytes = config
        .limits
        .max_temporary_bytes
        .min(u32::try_from(maximum.saturating_mul(2))?);
    config.limits.max_metadata_bytes = config
        .limits
        .max_metadata_bytes
        .min(limits.max_metadata_bytes);
    let remaining = deadline
        .saturating_duration_since(Instant::now())
        .as_millis();
    ensure!(remaining > 0, "history connection deadline exceeded");
    let remaining = u64::try_from(remaining)?;
    config.rpc.operation_timeout_ms = config.rpc.operation_timeout_ms.min(remaining);
    config.rpc.connect_timeout_ms = config.rpc.connect_timeout_ms.min(remaining);
    if let Some(ca) = ca {
        NativeClient::connect_with_ca_certificate(config.clone(), ca).await
    } else {
        NativeClient::connect(config.clone()).await
    }
    .map_err(|_| anyhow::anyhow!("history native connection failed"))
    .map(|c| c.with_operation_deadline(deadline))
}
fn scope(config: &Config, options: &HistoryOptions) -> Result<([u8; 32], Option<Arc<[u8]>>)> {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-history-job-v1\0");
    for part in [
        POLICY.as_bytes(),
        config.endpoint.as_bytes(),
        config.repository_id.as_bytes(),
        options.reference.as_bytes(),
        options.revision.as_bytes(),
        options.source_git.as_os_str().as_encoded_bytes(),
    ] {
        h.update(&(part.len() as u64).to_le_bytes());
        h.update(part);
    }
    h.update(&options.expected_generation.to_le_bytes());
    h.update(&[u8::from(config.token_env.is_some())]);
    if let Some(name) = &config.token_env {
        let token = std::env::var(name)
            .map_err(|_| anyhow::anyhow!("history credential environment unavailable"))?;
        h.update(&(name.len() as u64).to_le_bytes());
        h.update(name.as_bytes());
        h.update(blake3::hash(token.as_bytes()).as_bytes());
    }
    h.update(&[u8::from(config.tls.is_some())]);
    let mut ca = None;
    if let Some(tls) = &config.tls {
        let encoded = serde_json::to_vec(tls)?;
        h.update(&(encoded.len() as u64).to_le_bytes());
        h.update(&encoded);
        let f = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC)
            .open(&tls.ca_certificate)?;
        ensure!(f.metadata()?.is_file(), "TLS roots must be regular");
        let mut b = vec![];
        f.take(1024 * 1024 + 1).read_to_end(&mut b)?;
        ensure!(
            !b.is_empty() && b.len() <= 1024 * 1024,
            "TLS root capacity exceeded"
        );
        h.update(&b);
        ca = Some(Arc::from(b));
    }
    Ok((*h.finalize().as_bytes(), ca))
}
fn wire(kind: m::Kind, value: &str) -> Result<p::ObjectKey> {
    Ok(p::ObjectKey {
        kind: kind as i32,
        native_id: id(value)?.to_vec(),
    })
}
async fn observe_hold(client: &NativeClient, name: &str, head: &str) -> Result<(u64, bool)> {
    super::validate_proof(name, head)?;
    let view = client
        .open(vec![name.into()])
        .await
        .map_err(|_| anyhow::anyhow!("provenance hold observation failed"))?;
    let r = view
        .references
        .first()
        .context("provenance reference absent")?;
    if let Some(d) = &r.commit {
        let k = d.key.as_ref().context("hold descriptor absent")?;
        ensure!(
            k.kind == p::ObjectKind::Commit as i32 && hex(&k.native_id) == head,
            "provenance hold points elsewhere"
        );
        Ok((r.generation, true))
    } else {
        Ok((r.generation, false))
    }
}
fn checkpoint(manifest: &Manifest, cap: usize) -> Result<Vec<Stored>> {
    let bytes = serde_json::to_vec(manifest)?;
    ensure!(
        bytes.len() <= cap,
        "provenance manifest byte limit exceeded"
    );
    let descriptor = m::describe_file(&bytes);
    let file = Stored {
        descriptor,
        bytes: Arc::new(bytes),
    };
    let tree = m::encode(
        &m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "history-manifest-v1.json".into(),
                value: m::TreeValue::File {
                    id: m::FileId(descriptor.key.id.0),
                    executable: false,
                    size_bytes: descriptor.size_bytes,
                    copy_id: None,
                },
            }],
        }),
        Default::default(),
    )?;
    let signature = m::Signature {
        name: "History provenance".into(),
        email: "provenance@example.invalid".into(),
        timestamp: m::Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    let mut change = blake3::Hasher::new();
    change.update(b"tinytree-history-provenance-v1\0");
    change.update(&descriptor.key.id.0);
    let commit = m::encode(
        &m::Object::Commit(m::Commit {
            parents: vec![m::CommitId(id(&manifest.native_head)?)],
            predecessors: vec![],
            root_tree: m::TreeMerge {
                terms: vec![m::TreeId(tree.descriptor.key.id.0)],
                labels: vec![String::new()],
            },
            change_id: m::ChangeId(change.finalize().as_bytes()[..16].try_into()?),
            description: "Private verified history migration provenance".into(),
            author: signature.clone(),
            committer: signature,
            secure_signature: None,
        }),
        Default::default(),
    )?;
    Ok(vec![
        file,
        Stored {
            descriptor: tree.descriptor,
            bytes: Arc::new(tree.bytes),
        },
        Stored {
            descriptor: commit.descriptor,
            bytes: Arc::new(commit.bytes),
        },
    ])
}
async fn upload(
    client: &NativeClient,
    owner: &Arc<Resources>,
    stored: &Stored,
    deadline: Instant,
) -> Result<()> {
    ensure!(
        Instant::now() < deadline,
        "history upload deadline exceeded"
    );
    if stored.descriptor.key.kind == m::Kind::File {
        let physical = owner.clone();
        let bytes = stored.bytes.clone();
        let descriptor = stored.descriptor;
        let source = tokio::task::spawn_blocking(move || {
            let mut f = tempfile::NamedTempFile::new_in(physical.spool.path())?;
            f.write_all(&bytes)?;
            f.as_file().sync_all()?;
            Ok::<_, anyhow::Error>((f, physical))
        })
        .await??;
        let prepared = client
            .prepare_upload_file(source.0.path(), descriptor, owner.spool.path())
            .await
            .map_err(|_| anyhow::anyhow!("history FILE preparation failed"))?;
        drop(source);
        client
            .upload_file(prepared)
            .await
            .map_err(|_| anyhow::anyhow!("history FILE upload failed"))?;
    } else {
        let metadata = client
            .verify_metadata(stored.descriptor, stored.bytes.as_ref().clone())
            .await
            .map_err(|_| anyhow::anyhow!("native metadata verification failed"))?;
        client
            .upload_metadata(metadata)
            .await
            .map_err(|_| anyhow::anyhow!("history metadata upload failed"))?;
    }
    Ok(())
}
async fn metadata(
    client: &NativeClient,
    kind: m::Kind,
    name: &str,
) -> Result<(m::Object, Vec<u8>)> {
    let mut replies = client
        .read_objects(vec![m::ObjectKey {
            kind,
            id: m::ObjectId(id(name)?),
        }])
        .await
        .map_err(|_| anyhow::anyhow!("provenance native metadata read failed"))?;
    let MetadataOutcome::Object(value) = replies.pop().context("metadata reply absent")? else {
        anyhow::bail!("provenance metadata unavailable")
    };
    Ok((value.object().clone(), value.canonical_bytes().to_vec()))
}
async fn file(
    client: &NativeClient,
    owner: &Arc<Resources>,
    native: &str,
    cap: usize,
) -> Result<Arc<Vec<u8>>> {
    let expected = id(native)?;
    let f = client
        .download_file(m::ObjectId(expected), owner.spool.path())
        .await
        .map_err(|_| anyhow::anyhow!("provenance FILE unavailable"))?;
    ensure!(
        f.descriptor().size_bytes <= cap as u64,
        "provenance FILE byte limit exceeded"
    );
    let physical = owner.clone();
    tokio::task::spawn_blocking(move || {
        let _owner = physical;
        let mut b = vec![];
        f.open()?.take(cap as u64 + 1).read_to_end(&mut b)?;
        ensure!(
            b.len() <= cap && m::file_id(&b).0 == expected,
            "provenance FILE differs"
        );
        Ok::<_, anyhow::Error>(Arc::new(b))
    })
    .await?
}
async fn read_verified(
    client: &NativeClient,
    owner: &Arc<Resources>,
    proof: &HistoryProof,
    reserve: u64,
    deadline: Instant,
) -> Result<(Manifest, Graph)> {
    ensure!(
        observe_hold(client, &proof.retention_hold, &proof.provenance_checkpoint)
            .await?
            .1,
        "provenance hold is absent"
    );
    let (object, commit_bytes) =
        metadata(client, m::Kind::Commit, &proof.provenance_checkpoint).await?;
    let m::Object::Commit(commit) = object else {
        anyhow::bail!("provenance checkpoint is not a commit")
    };
    ensure!(
        commit.root_tree.terms.len() == 1 && commit.root_tree.labels == vec![String::new()],
        "provenance checkpoint tree unresolved"
    );
    let tree_id = hex(&commit.root_tree.terms[0].0);
    let (tree, tree_bytes) = metadata(client, m::Kind::Tree, &tree_id).await?;
    let m::Object::Tree(tree) = tree else {
        anyhow::bail!("provenance tree missing")
    };
    ensure!(
        tree.entries.len() == 1 && tree.entries[0].name == "history-manifest-v1.json",
        "provenance tree shape differs"
    );
    let m::TreeValue::File {
        id: manifest_id,
        size_bytes,
        executable: false,
        copy_id: None,
    } = tree.entries[0].value
    else {
        anyhow::bail!("provenance file shape differs")
    };
    ensure!(
        size_bytes <= owner.options.limits.max_manifest_bytes as u64,
        "provenance file exceeds limit"
    );
    let bytes = file(
        client,
        owner,
        &hex(&manifest_id.0),
        owner.options.limits.max_manifest_bytes,
    )
    .await?;
    ensure!(
        bytes.len() as u64 == size_bytes,
        "provenance declared size differs"
    );
    let manifest: Manifest =
        serde_json::from_slice(&bytes).context("invalid provenance manifest")?;
    ensure!(
        manifest.version == 1
            && manifest.policy == POLICY
            && manifest.object_format == "sha1"
            && manifest.commits.len() <= owner.options.limits.max_commits
            && manifest
                .commits
                .len()
                .saturating_add(manifest.trees.len())
                .saturating_add(manifest.blobs.len())
                <= owner.options.limits.max_objects * 2
            && valid_hex(&manifest.git_head, 40)
            && valid_hex(&manifest.native_head, 64)
            && valid_hex(&manifest.job, 32)
            && !manifest.reference.is_empty()
            && manifest.reference.len() <= 4096
            && !manifest.reference.starts_with("_tinytree")
            && manifest.expected_generation < u64::MAX,
        "unsupported/excessive provenance manifest"
    );
    ensure!(
        proof.retention_hold
            == format!(
                "_tinytree/hold/v1/{}/{}",
                hex(&manifest.client),
                proof.provenance_checkpoint
            ),
        "provenance hold/client binding differs"
    );
    let expected = checkpoint(&manifest, owner.options.limits.max_manifest_bytes)?;
    ensure!(
        expected[0].descriptor.key.id.0 == manifest_id.0
            && expected[0].bytes.as_slice() == bytes.as_slice()
            && expected[1].descriptor.key.id.0 == id(&tree_id)?
            && expected[1].bytes.as_slice() == tree_bytes
            && expected[2].descriptor.key.id.0 == id(&proof.provenance_checkpoint)?
            && expected[2].bytes.as_slice() == commit_bytes,
        "provenance checkpoint canonical binding differs"
    );
    let mut raw = BTreeMap::<String, Raw>::new();
    let mut charged = 0u64;
    let mut total = 0u64;
    for (kind, records) in [("commit", &manifest.commits), ("tree", &manifest.trees)] {
        for record in records {
            ensure!(
                valid_hex(&record.git, 40)
                    && valid_hex(&record.native, 64)
                    && record.raw.len() <= owner.options.limits.max_metadata_bytes,
                "invalid provenance metadata row"
            );
            super::io::verify(&record.git, kind, &record.raw)?;
            charged = charged
                .checked_add(record.raw.len() as u64 * 4 + 4096)
                .context("provenance memory overflow")?;
            ensure!(
                reserve.saturating_add(charged) <= owner.options.limits.max_memory_bytes,
                "provenance memory limit exceeded"
            );
            total = total
                .checked_add(record.raw.len() as u64)
                .context("provenance bytes overflow")?;
            ensure!(
                raw.insert(
                    record.git.clone(),
                    Raw {
                        kind: kind.into(),
                        bytes: Arc::new(record.raw.clone())
                    }
                )
                .is_none(),
                "duplicate provenance Git metadata"
            );
        }
    }
    for record in &manifest.blobs {
        ensure!(
            valid_hex(&record.git, 40)
                && valid_hex(&record.native, 64)
                && record.size <= owner.options.limits.max_file_bytes as u64,
            "invalid provenance blob row"
        );
        let payload = if record.kind == m::Kind::File as u8 {
            file(client, owner, &record.native, usize::try_from(record.size)?).await?
        } else if record.kind == m::Kind::Symlink as u8 {
            let (object, _) = metadata(client, m::Kind::Symlink, &record.native).await?;
            let m::Object::Symlink(target) = object else {
                anyhow::bail!("provenance symlink differs")
            };
            Arc::new(target.into_bytes())
        } else {
            anyhow::bail!("provenance blob native kind differs")
        };
        ensure!(
            payload.len() as u64 == record.size
                && *blake3::hash(&payload).as_bytes() == record.digest,
            "provenance blob size/digest differs"
        );
        super::io::verify(&record.git, "blob", &payload)?;
        if let Some(previous) = raw.get(&record.git) {
            ensure!(
                previous.kind == "blob" && previous.bytes.as_slice() == payload.as_slice(),
                "provenance duplicate Git identity differs"
            );
        } else {
            charged = charged
                .checked_add(payload.len() as u64 * 4 + 4096)
                .context("provenance memory overflow")?;
            ensure!(
                reserve.saturating_add(charged) <= owner.options.limits.max_memory_bytes,
                "provenance memory limit exceeded"
            );
            total = total
                .checked_add(payload.len() as u64)
                .context("provenance bytes overflow")?;
            ensure!(
                total <= owner.options.limits.max_total_bytes
                    && raw.len() < owner.options.limits.max_objects,
                "provenance aggregate limits exceeded"
            );
            raw.insert(
                record.git.clone(),
                Raw {
                    kind: "blob".into(),
                    bytes: payload,
                },
            );
        }
    }
    ensure!(
        total == manifest.source_bytes
            && total <= owner.options.limits.max_total_bytes
            && raw.len() <= owner.options.limits.max_objects,
        "provenance aggregate byte/object count differs"
    );
    let count = raw.len();
    let mut graph = Graph::new(
        Source::Memory(raw),
        owner.options.limits.clone(),
        reserve,
        deadline,
    )?;
    graph.build(&manifest.git_head).await?;
    graph.validate(&manifest)?;
    ensure!(
        graph.raw.len() == count,
        "provenance includes unselected Git objects"
    );
    for stored in graph
        .native
        .values()
        .filter(|s| s.descriptor.key.kind != m::Kind::File)
    {
        let (_, actual) = metadata(
            client,
            stored.descriptor.key.kind,
            &hex(&stored.descriptor.key.id.0),
        )
        .await?;
        ensure!(
            actual.as_slice() == stored.bytes.as_slice(),
            "canonical native association differs"
        );
    }
    ensure!(
        observe_hold(client, &proof.retention_hold, &proof.provenance_checkpoint)
            .await?
            .1,
        "provenance hold changed during verification"
    );
    Ok((manifest, graph))
}
fn publication_key(m: &Manifest) -> String {
    format!("history_{}_public", m.job)
}
fn validate_receipt(manifest: &Manifest, publication: &p::Publication) -> Result<u64> {
    let r = publication
        .reference
        .as_ref()
        .context("publication receipt reference absent")?;
    let key = r
        .commit
        .as_ref()
        .and_then(|d| d.key.as_ref())
        .context("publication receipt head absent")?;
    ensure!(
        publication.idempotency_key == publication_key(manifest)
            && r.name == manifest.reference
            && r.generation
                == manifest
                    .expected_generation
                    .checked_add(1)
                    .context("generation overflow")?
            && key.kind == p::ObjectKind::Commit as i32
            && hex(&key.native_id) == manifest.native_head,
        "historical publication receipt differs"
    );
    Ok(r.generation)
}
async fn report(
    client: &NativeClient,
    manifest: &Manifest,
    proof: &HistoryProof,
    generation: Option<u64>,
) -> Result<HistoryReport> {
    let view = client
        .open(vec![manifest.reference.clone()])
        .await
        .map_err(|_| anyhow::anyhow!("cannot observe current history target"))?;
    ensure!(
        view.repository_id == manifest.repository,
        "history repository identity differs"
    );
    let r = view
        .references
        .first()
        .context("current history target absent")?;
    let current = r
        .commit
        .as_ref()
        .and_then(|d| d.key.as_ref())
        .map(|k| hex(&k.native_id));
    Ok(HistoryReport {
        policy: POLICY.into(),
        git_head: manifest.git_head.clone(),
        native_head: manifest.native_head.clone(),
        publication_generation: generation,
        current_generation: r.generation,
        is_current: current.as_deref() == Some(manifest.native_head.as_str()),
        current_native_head: current,
        provenance_checkpoint: proof.provenance_checkpoint.clone(),
        provenance_manifest: hex(&checkpoint(manifest, usize::MAX)?[0].descriptor.key.id.0),
        retention_hold: proof.retention_hold.clone(),
        commits: manifest.commits.len(),
        git_objects: manifest.commits.len()
            + manifest.trees.len()
            + manifest
                .blobs
                .iter()
                .map(|b| &b.git)
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
        source_bytes: manifest.source_bytes,
    })
}
pub(super) async fn verify(
    mut config: Config,
    proof: HistoryProof,
    limits: HistoryLimits,
    deadline: Instant,
) -> Result<HistoryReport> {
    config.validate()?;
    let temp = Arc::new(
        tempfile::Builder::new()
            .prefix("history-verify-")
            .permissions(std::fs::Permissions::from_mode(0o700))
            .tempdir()?,
    );
    let options = HistoryOptions {
        source_git: temp.path().to_path_buf(),
        revision: "0".repeat(40),
        state_dir: temp.path().join("state"),
        reference: "verify".into(),
        expected_generation: 0,
        git_program: "git".into(),
        limits,
    };
    let owner = Resources::open(options)?;
    let (_, ca) = scope(&config, &owner.options)?;
    let reserve = reservation(&config, &owner.options.limits)?;
    let client = connect(&mut config, ca, deadline, &owner.options.limits)
        .await?
        .with_lifetime_guard(owner.clone())
        .with_lifetime_guard(temp);
    let (manifest, graph) = read_verified(&client, &owner, &proof, reserve, deadline).await?;
    drop(graph);
    ensure!(
        manifest.repository == config.repository_id,
        "provenance repository differs"
    );
    let publication = match client.publication(&publication_key(&manifest)).await {
        Ok(p) => Some(validate_receipt(&manifest, &p)?),
        Err(e) if e.code() == tonic::Code::NotFound => None,
        Err(_) => anyhow::bail!("history publication proof unavailable"),
    };
    report(&client, &manifest, &proof, publication).await
}
