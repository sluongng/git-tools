//! Explicit owner-private Legacy backup. No automatic capture or native publication.
use anyhow::{Context as _, Result, ensure};
use native_retention::{
    BundleDescriptor, CaptureBundle, CommandOwner, OwnedCaptureBundle, RootCheckpoint,
};
use native_vfs_rpc::PrivateBackupReceipt;
use native_workspace_admin::{capture, private_bundle as bundle};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
    sync::Arc,
};
use tinytree_client::private_capsule::{
    CapsuleAction, CapsuleBinding, CapsuleCall, CapsuleDescriptor, CapsuleSelection, CapsuleState,
    PrivateCapsuleClient, chain_chunk,
};
const MAX: u64 = 512 * 1024 * 1024;
const JOURNAL: usize = 65536;
fn hex(b: &[u8]) -> String {
    b.iter().map(|b| format!("{b:02x}")).collect()
}
fn random() -> [u8; 32] {
    let mut id = [0; 32];
    id[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    id[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    id
}
fn remaining_bundle_limits(deadline: tokio::time::Instant) -> Result<bundle::BundleLimits> {
    let mut limits = bundle::BundleLimits::default();
    limits.max_elapsed_ms = deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .as_millis()
        .min(300_000) as u64;
    ensure!(limits.max_elapsed_ms > 0, "private backup deadline");
    Ok(limits)
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pending {
    binding: CapsuleBinding,
    digest: [u8; 32],
    file: String,
    snapshot_commit: String,
    snapshot_operation: String,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreSeed {
    binding: CapsuleBinding,
    source_digest: [u8; 32],
    source_workspace: [u8; 32],
    exporter: [u8; 16],
    receipt: PrivateBackupReceipt,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema: u32,
    context: [u8; 32],
    workspace: [u8; 32],
    exporter: [u8; 16],
    generation: u64,
    pending: Option<Pending>,
    receipt: Option<PrivateBackupReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    restore_seed: Option<RestoreSeed>,
}
pub(crate) struct Context {
    directory: PrivateDir,
    config: tinytree_client::Config,
    trust: Option<[u8; 32]>,
    account: String,
    selection: CapsuleSelection,
    binding: [u8; 32],
    session: tinytree_forge_core::session::ForgeSession,
    ca: Option<Arc<[u8]>>,
    active: Arc<tokio::sync::Semaphore>,
}
impl Context {
    pub(crate) fn open(
        path: &Path,
        mut config: tinytree_client::Config,
        trust: Option<[u8; 32]>,
        account: String,
        selection: CapsuleSelection,
        binding: [u8; 32],
        session: tinytree_forge_core::session::ForgeSession,
        ca: Option<Arc<[u8]>>,
    ) -> Result<Arc<Self>> {
        selection.validate()?;
        session.validate()?;
        ensure!(
            session.account_id == account
                && matches!(
                    &session.identity,
                    tinytree_forge_core::session::ForgeSessionIdentity::Authenticated { .. }
                )
                && config.transport == tinytree_client::Transport::ForgeHttpsV1,
            "private backup requires the enrolled authenticated Forge context"
        );
        config.validate()?;
        config.rpc.operation_timeout_ms = 300_000;
        let directory = PrivateDir::create(path)?;
        let context = Arc::new(Self {
            directory,
            config,
            trust,
            account,
            selection,
            binding,
            session,
            ca,
            active: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        if !context.directory.path().join("journal.json").try_exists()? {
            context.save(&Journal {
                schema: 1,
                context: binding,
                workspace: random(),
                exporter: *uuid::Uuid::new_v4().as_bytes(),
                generation: 0,
                pending: None,
                receipt: None,
                restore_seed: None,
            })?;
        }
        context.load()?;
        Ok(context)
    }
    fn load(&self) -> Result<Journal> {
        let j: Journal = serde_json::from_slice(&self.directory.read("journal.json", JOURNAL)?)?;
        ensure!(
            j.schema == 1
                && j.context == self.binding
                && j.workspace != [0; 32]
                && j.generation <= 9_007_199_254_740_991,
            "private backup journal binding differs"
        );
        if let Some(p) = &j.pending {
            p.binding.validate()?;
            ensure!(
                p.binding.account_id == self.account
                    && p.binding.workspace_id == hex(&j.workspace)
                    && p.binding.selection == self.selection
                    && p.binding.generation == j.generation + 1
                    && p.file == format!("{}.private", hex(&p.digest)),
                "private backup intent differs"
            );
        }
        if let Some(r) = &j.receipt {
            r.validate()?;
            ensure!(
                r.workspace_id == hex(&j.workspace) && r.generation == j.generation,
                "private backup head differs"
            );
        }
        Ok(j)
    }
    fn save(&self, j: &Journal) -> Result<()> {
        let bytes = serde_json::to_vec(j)?;
        ensure!(bytes.len() <= JOURNAL, "private backup journal limit");
        self.directory.replace("journal.json", &bytes)?;
        Ok(())
    }
    /// Local diagnostic custody only. Never included in RPC replies or receipts.
    pub(crate) fn record_error(&self, stage: &str, error: &anyhow::Error) {
        struct Bounded(String);
        impl std::fmt::Write for Bounded {
            fn write_str(&mut self, value: &str) -> std::fmt::Result {
                let available = (16 * 1024usize).saturating_sub(self.0.len());
                let mut end = value.len().min(available);
                while !value.is_char_boundary(end) {
                    end -= 1;
                }
                self.0.push_str(&value[..end]);
                if end < value.len() {
                    Err(std::fmt::Error)
                } else {
                    Ok(())
                }
            }
        }
        use std::fmt::Write as _;
        let mut output = Bounded(String::new());
        let stage = if stage.len() <= 64
            && stage
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        {
            stage
        } else {
            "backup"
        };
        let _ = write!(&mut output, "{stage}: {error:#}");
        // Underlying transport errors are already sanitized. Additionally never
        // preserve a bearer value if a future error source includes a header.
        for prefix in ["Bearer ", "bearer "] {
            while let Some(start) = output.0.find(prefix) {
                let after = start + prefix.len();
                let end = output.0[after..]
                    .find(char::is_whitespace)
                    .map(|n| after + n)
                    .unwrap_or(output.0.len());
                output.0.replace_range(start..end, "[credential omitted]");
            }
        }
        if output.0.len() > 16 * 1024 {
            let mut end = 16 * 1024;
            while !output.0.is_char_boundary(end) {
                end -= 1;
            }
            output.0.truncate(end);
        }
        let _ = self
            .directory
            .replace("last-error.txt", output.0.as_bytes());
    }
    pub(crate) async fn admit(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<tokio::sync::OwnedSemaphorePermit>> {
        Ok(Arc::new(
            tokio::time::timeout_at(deadline, self.active.clone().acquire_owned())
                .await
                .context("private backup admission deadline")??,
        ))
    }
    pub(crate) async fn prepare(
        self: Arc<Self>,
        target: Arc<crate::native_capture::Target>,
        command: Arc<CommandOwner>,
        kernel: File,
        admission: Arc<tokio::sync::OwnedSemaphorePermit>,
        deadline: tokio::time::Instant,
    ) -> Result<OwnedCaptureBundle> {
        let owner: Arc<dyn std::any::Any + Send + Sync> = Arc::new((kernel, command, admission));
        let mut j = self.load()?;
        ensure!(
            j.generation < 9_007_199_254_740_991,
            "private backup generation exhausted"
        );
        let mut limits = capture::CaptureLimits::default();
        limits.max_elapsed_ms = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .as_millis()
            .min(300_000) as u64;
        ensure!(limits.max_elapsed_ms > 0, "private backup deadline");
        let (_, projection) = jj_vfs::native::read_retained_state_for_capture(
            &target.binding.projection(),
            limits
                .max_file_bytes
                .min(limits.max_decode_bytes.saturating_sub(4096) / 32),
        )?;
        let controller = target
            .controller
            .upgrade()
            .context("private backup controller unavailable")?;
        let frozen = controller
            .capture_for_backup_until(&projection, deadline)
            .await?;
        let proof = capture::TreeFreezePermit::from_controller(frozen, owner.clone())?;
        let mut files = 0usize;
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(self.directory.path())? {
            let entry = entry?;
            files += 1;
            ensure!(
                files <= 16,
                "private backup spool file limit; preserve pending bundles"
            );
            let metadata = entry.file_type()?;
            ensure!(metadata.is_file(), "private backup spool entry differs");
            bytes = bytes
                .checked_add(entry.metadata()?.len())
                .context("private backup spool bytes")?;
        }
        ensure!(
            bytes <= 2 * 1024 * 1024 * 1024 - MAX,
            "private backup spool byte limit; preserve pending bundles"
        );
        let temporary = format!("{}.part", hex(&random()));
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.directory.path().join(&temporary))?;
        let self2 = self.clone();
        let produced=tokio::task::spawn_blocking(move|| {
            let rt=tokio::runtime::Builder::new_current_thread().enable_all().max_blocking_threads(2).build()?;
            rt.block_on(async move {
                let mut bounds=bundle::BundleLimits::default();bounds.max_elapsed_ms=deadline.saturating_duration_since(tokio::time::Instant::now()).as_millis().min(300_000) as u64;
                ensure!(bounds.max_elapsed_ms>0,"private backup deadline");
                let mut writer=bundle::PrivateBundleWriter::new(file,bounds.clone())?;
                let input=capture::CaptureInput{registry:target.registry.clone(),binding:target.binding.clone(),index_config:target.index.clone(),local_objects:target.local.clone(),remote_scope:target.remote_scope.clone()};
                let candidate=capture::read_private_candidate(input,proof,limits,&mut writer).await?;
                let identity=tinytree_cloud::CaptureExportIdentity{workspace:j.workspace,exporter:j.exporter,sequence:j.generation+1,parents:vec![]};
                let produced=writer.finish(candidate,target.native.clone(),identity,owner,bounds).await?;
                let verified=produced.verified();
                self2.verify_scope(verified)?;
                if let Some(p)=&j.pending {
                    let old=bundle::verify_file(self2.directory.open_existing_file(&p.file,MAX)?,p.digest,remaining_bundle_limits(deadline)?)?;
                    ensure!(old.capture_data()==verified.capture_data()&&old.manifest().source_workspace_id==verified.manifest().source_workspace_id,"pending private backup differs from current snapshot; preserve and resume original intent");
                    // Do not replace the original immutable byte intent.
                    std::fs::remove_file(self2.directory.path().join(&temporary))?;
                    let checkpoint=checkpoint(&old)?;
                    return Ok(OwnedCaptureBundle::new(CaptureBundle{checkpoint,bundle:BundleDescriptor{blake3:p.digest,size_bytes:p.binding.descriptor.total_bytes}},Arc::new(produced)).with_verified_existing_bundle());
                }
                let digest=produced.blake3();let name=format!("{}.private",hex(&digest));
                match std::fs::hard_link(self2.directory.path().join(&temporary),self2.directory.path().join(&name)) {
                    Ok(())=>{},
                    Err(error) if error.kind()==std::io::ErrorKind::AlreadyExists=>{
                        let existing=bundle::verify_file(self2.directory.open_existing_file(&name,MAX)?,digest,remaining_bundle_limits(deadline)?)?;
                        ensure!(existing.manifest()==verified.manifest()&&existing.size_bytes()==produced.size_bytes(),"existing private backup bundle differs");
                    },
                    Err(error)=>return Err(error.into()),
                }
                File::open(self2.directory.path())?.sync_all()?;
                std::fs::remove_file(self2.directory.path().join(&temporary))?;
                let mut input=produced.file()?;input.seek(SeekFrom::Start(0))?;
                let mut buffer=[0;65536];let mut chain="0".repeat(64);let mut total=0;let mut chunks=0;
                while total<produced.size_bytes() {let n=(produced.size_bytes()-total).min(65536) as usize;input.read_exact(&mut buffer[..n])?;chain=chain_chunk(&chain,chunks,&buffer[..n])?;chunks+=1;total+=n as u64;ensure!(total<=MAX,"private backup byte limit");}
                ensure!(total==produced.size_bytes(),"private backup file changed");
                let p=Pending{binding:CapsuleBinding{account_id:self2.account.clone(),capsule_id:hex(&random()),workspace_id:hex(&j.workspace),generation:j.generation+1,selection:self2.selection.clone(),descriptor:CapsuleDescriptor{format:"tinytree-private-capture-v1".into(),total_bytes:total,chunk_count:chunks,chain_root:chain}},digest,file:name,snapshot_commit:hex(&verified.working_copy().snapshot_commit),snapshot_operation:verified.working_copy().wc_operation.clone()};
                p.binding.validate()?;let checkpoint=checkpoint(verified)?;j.pending=Some(p);self2.save(&j)?;
                Ok::<_,anyhow::Error>(OwnedCaptureBundle::new(CaptureBundle{checkpoint,bundle:BundleDescriptor{blake3:digest,size_bytes:total}},Arc::new(produced)))
            })
        }).await.context("private backup worker failed")??;
        Ok(produced)
    }
    fn verify_scope(&self, verified: &bundle::VerifiedPrivateBundle) -> Result<()> {
        if let Some(base) = verified.review_base() {
            ensure!(
                hex(&base.view_version) == self.selection.view_version
                    && base.policy_epoch == self.selection.policy_epoch,
                "private backup review scope differs from enrolled client"
            );
        }
        Ok(())
    }
    async fn verify_session(&self, deadline: tokio::time::Instant) -> Result<()> {
        let remaining = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .as_millis()
            .min(300_000) as u64;
        ensure!(remaining > 0, "private backup deadline");
        let mut config = self.config.clone();
        config.rpc.operation_timeout_ms = remaining;
        let transport = match &self.ca {
            Some(ca) => {
                tinytree_client::NativeClient::connect_with_ca_certificate(config, ca.clone())
                    .await?
            }
            None => tinytree_client::NativeClient::connect(config).await?,
        };
        ensure!(
            transport
                .forge_get_session(&self.account, &self.selection.view)
                .await?
                == self.session,
            "private backup session differs from enrolled client"
        );
        Ok(())
    }
    /// Trusted restore integration supplies both SDK binding and semantically verified bytes.
    /// The caller must retain the independent sealed-capsule source authorization.
    pub(crate) fn seed_restored(
        &self,
        source: &bundle::VerifiedPrivateBundle,
        binding: &CapsuleBinding,
    ) -> Result<()> {
        binding.validate()?;
        self.verify_scope(source)?;
        let mut j = self.load()?;
        ensure!(
            binding.account_id == self.account
                && binding.selection == self.selection
                && binding.workspace_id == hex(&source.manifest().workspace_id)
                && binding.generation == source.manifest().generation
                && binding.descriptor.total_bytes == source.size_bytes(),
            "restored private backup source differs"
        );
        let receipt = PrivateBackupReceipt {
            schema: 1,
            capsule_id: binding.capsule_id.clone(),
            workspace_id: binding.workspace_id.clone(),
            generation: binding.generation,
            bytes: binding.descriptor.total_bytes,
            chain_root: binding.descriptor.chain_root.clone(),
            snapshot_commit: hex(&source.working_copy().snapshot_commit),
            snapshot_operation: source.working_copy().wc_operation.clone(),
        };
        receipt.validate()?;
        let seed = RestoreSeed {
            binding: binding.clone(),
            source_digest: source.blake3(),
            source_workspace: source.manifest().source_workspace_id,
            exporter: source.manifest().exporter,
            receipt: receipt.clone(),
        };
        ensure!(
            j.pending.is_none(),
            "restored private backup has a pending upload intent"
        );
        if let Some(saved) = &j.restore_seed {
            // An interrupted install may retry this exact immutable source. It
            // must never rewind a later successful backup or replace any intent.
            ensure!(
                saved == &seed
                    && j.workspace == source.manifest().workspace_id
                    && j.exporter == seed.exporter
                    && j.generation == binding.generation
                    && j.receipt.as_ref() == Some(&receipt),
                "restored private backup journal differs"
            );
            return Ok(());
        }
        ensure!(
            j.generation == 0 && j.receipt.is_none(),
            "restored private backup journal is not fresh"
        );
        j.workspace = source.manifest().workspace_id;
        j.exporter = seed.exporter;
        j.generation = binding.generation;
        j.receipt = Some(receipt);
        j.restore_seed = Some(seed);
        self.save(&j)
    }
    pub(crate) async fn upload(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<PrivateBackupReceipt> {
        let mut j = self.load()?;
        let p = j
            .pending
            .clone()
            .context("private backup intent unavailable")?;
        let mut file = self.directory.open_existing_file(&p.file, MAX)?;
        // Fresh semantic and full-byte verification precedes network replay.
        let digest = p.digest;
        let verified = tokio::task::spawn_blocking(move || {
            bundle::verify_file(file, digest, remaining_bundle_limits(deadline)?)
        })
        .await??;
        self.verify_scope(&verified)?;
        ensure!(
            verified.size_bytes() == p.binding.descriptor.total_bytes
                && hex(&verified.working_copy().snapshot_commit) == p.snapshot_commit
                && verified.working_copy().wc_operation == p.snapshot_operation,
            "private backup spool differs"
        );
        file = verified.file()?;
        file.seek(SeekFrom::Start(0))?;
        ensure!(
            tokio::time::Instant::now() < deadline,
            "private backup deadline"
        );
        // One immutable authorization/header and HTTP pool for this owned upload.
        // Current server authority is still rechecked for EVERY capsule action.
        self.verify_session(deadline).await?;
        let client = PrivateCapsuleClient::connect(self.config.clone(), self.trust).await?;
        let mut reply = client
            .call_until(CapsuleCall::begin(&p.binding)?, Some(&p.binding), deadline)
            .await?;
        let mut buffer = [0; 65536];
        let mut sequence = 0;
        while sequence < p.binding.descriptor.chunk_count {
            let n = p.binding.descriptor.chunk_size(sequence)?;
            file.read_exact(&mut buffer[..n])?;
            if sequence >= reply.next_sequence {
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    "private backup deadline"
                );
                reply = client
                    .call_until(
                        CapsuleCall::append(&p.binding, sequence, &buffer[..n])?,
                        Some(&p.binding),
                        deadline,
                    )
                    .await?;
            }
            sequence += 1;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "private backup deadline"
        );
        let reply = client
            .call_until(
                CapsuleCall::new(&self.account, &p.binding.capsule_id, CapsuleAction::Finish),
                Some(&p.binding),
                deadline,
            )
            .await?;
        self.verify_session(deadline).await?;
        ensure!(
            reply.state == CapsuleState::Sealed,
            "private backup is not sealed"
        );
        let receipt = PrivateBackupReceipt {
            schema: 1,
            capsule_id: p.binding.capsule_id,
            workspace_id: p.binding.workspace_id,
            generation: p.binding.generation,
            bytes: p.binding.descriptor.total_bytes,
            chain_root: p.binding.descriptor.chain_root,
            snapshot_commit: p.snapshot_commit,
            snapshot_operation: p.snapshot_operation,
        };
        receipt.validate()?;
        j.generation = receipt.generation;
        j.pending = None;
        j.receipt = Some(receipt.clone());
        self.save(&j)?;
        Ok(receipt)
    }
}
fn checkpoint(v: &bundle::VerifiedPrivateBundle) -> Result<RootCheckpoint> {
    let c = v.capture_data();
    let op = |s: &str| -> Result<Vec<u8>> {
        ensure!(s.len() == 128, "private backup operation identity");
        (0..s.len())
            .step_by(2)
            .map(|i| Ok(u8::from_str_radix(&s[i..i + 2], 16)?))
            .collect()
    };
    Ok(RootCheckpoint {
        roots: c
            .roots
            .iter()
            .map(|r| {
                Ok(tinytree_model::ObjectKey {
                    kind: tinytree_model::Kind::try_from(r.kind)?,
                    id: tinytree_model::ObjectId(r.id),
                })
            })
            .collect::<Result<_>>()?,
        op_heads: c
            .operation_heads
            .iter()
            .map(|s| op(s))
            .collect::<Result<_>>()?,
        working_copy_operation: Some(op(&c.working_copy.wc_operation)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owner_diagnostic_is_bounded_utf8_and_omits_bearer_values() -> Result<()> {
        let parent = tempfile::tempdir()?;
        let config = tinytree_client::Config {
            transport: tinytree_client::Transport::ForgeHttpsV1,
            endpoint: "https://example.invalid".into(),
            repository_id: "forge-example".into(),
            token_env: Some("FIXTURE_FORGE_TOKEN".into()),
            ..Default::default()
        };
        let selection = CapsuleSelection {
            view: "public".into(),
            view_version: "1".repeat(64),
            policy_epoch: 1,
            projected_commit: "2".repeat(64),
            projected_tree: "3".repeat(64),
        };
        let session = tinytree_forge_core::session::ForgeSession {
            schema_version: 1,
            account_id: "example".into(),
            identity: tinytree_forge_core::session::ForgeSessionIdentity::Authenticated {
                principal: "fixture-owner".into(),
                credential_id: "4".repeat(64),
            },
        };
        let context = Context::open(
            &parent.path().join("backups"),
            config,
            None,
            "example".into(),
            selection,
            [5; 32],
            session,
            None,
        )?;
        context.record_error(
            "upload",
            &anyhow::anyhow!("Bearer fixture-sensitive-value {}", "é".repeat(9000)),
        );
        let bytes = context.directory.read("last-error.txt", 16 * 1024)?;
        assert!(bytes.len() <= 16 * 1024);
        let text = std::str::from_utf8(&bytes)?;
        assert!(text.starts_with("upload: [credential omitted] "));
        assert!(!text.contains("fixture-sensitive-value"));
        assert!(text.contains('é'));
        Ok(())
    }
}
