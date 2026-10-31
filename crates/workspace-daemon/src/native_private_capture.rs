//! Explicit owner-private Legacy backup. No automatic capture or native publication.
use crate::native_private_coverage as coverage;
use crate::native_private_retention as retention;
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
    local: Option<retention::LocalCaptureHandle>,
    intent: Option<retention::ExactFile>,
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
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    binding: CapsuleBinding,
    receipt: PrivateBackupReceipt,
    bundle: retention::ExactFile,
    intent: retention::ExactFile,
    seal: retention::ExactFile,
    local: Option<retention::LocalCaptureHandle>,
}
impl Head {
    fn files(&self) -> retention::CleanupFiles {
        retention::CleanupFiles {
            bundle: self.bundle.clone(),
            intent: self.intent.clone(),
            seal: self.seal.clone(),
        }
    }
}
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PrunePhase {
    Remote,
    Local,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PruneOutbox {
    old_capsule: String,
    successor_capsule: String,
    phase: PrunePhase,
    obligation: Option<native_retention::CaptureCleanupObligation>,
    pending_intent: Option<[u8; 32]>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResumeAck {
    head: Head,
    pending_source: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize)]
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
    head: Option<Head>,
    retained: Vec<Head>,
    prune: Option<PruneOutbox>,
    preserved_restored: Option<Head>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resume_ack: Option<ResumeAck>,
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
                schema: 2,
                context: binding,
                workspace: random(),
                exporter: *uuid::Uuid::new_v4().as_bytes(),
                generation: 0,
                pending: None,
                receipt: None,
                restore_seed: None,
                head: None,
                retained: vec![],
                prune: None,
                preserved_restored: None,
                resume_ack: None,
            })?;
        }
        context.load()?;
        Ok(context)
    }
    fn load(&self) -> Result<Journal> {
        let j: Journal = serde_json::from_slice(&self.directory.read("journal.json", JOURNAL)?)?;
        ensure!(
            j.schema == 2
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
        ensure!(j.retained.len() <= 16, "private backup history limit");
        let mut seen = std::collections::BTreeSet::new();
        for head in j
            .head
            .iter()
            .chain(j.retained.iter())
            .chain(j.preserved_restored.iter())
        {
            head.binding.validate()?;
            head.receipt.validate()?;
            ensure!(
                head.binding.account_id == self.account
                    && head.binding.selection == self.selection
                    && head.binding.workspace_id == hex(&j.workspace)
                    && head.binding.generation <= j.generation
                    && head.receipt.capsule_id == head.binding.capsule_id
                    && head.receipt.workspace_id == head.binding.workspace_id
                    && head.receipt.generation == head.binding.generation
                    && head.receipt.bytes == head.binding.descriptor.total_bytes
                    && head.receipt.chain_root == head.binding.descriptor.chain_root
                    && seen.insert(head.binding.capsule_id.clone())
                    && head.bundle.name
                        == format!("{}.private", hex(&head.bundle.descriptor.blake3))
                    && head.bundle.descriptor.size_bytes == head.binding.descriptor.total_bytes
                    && head.intent.name == format!("{}.intent", head.binding.capsule_id)
                    && head.seal.name == format!("{}.seal", head.binding.capsule_id),
                "private backup retained head differs"
            );
            if let Some(local) = &head.local {
                ensure!(
                    local.bundle == head.bundle.descriptor,
                    "private backup local custody differs"
                );
            }
        }
        match &j.head {
            Some(head) => ensure!(
                head.binding.generation == j.generation
                    && j.receipt.as_ref() == Some(&head.receipt),
                "private backup head differs"
            ),
            None => ensure!(
                j.generation == 0 && j.receipt.is_none() && j.retained.is_empty(),
                "private backup head unavailable"
            ),
        }
        if let Some(ack) = &j.resume_ack {
            ensure!(
                j.head.as_ref() == Some(&ack.head)
                    && j.receipt.as_ref() == Some(&ack.head.receipt)
                    && ack.pending_source == Self::completed_pending_binding(&ack.head)?,
                "private resume acknowledgement head differs"
            );
        }
        if let Some(preserved) = &j.preserved_restored {
            ensure!(
                preserved.local.is_none()
                    && j.restore_seed
                        .as_ref()
                        .is_some_and(|seed| seed.binding == preserved.binding
                            && seed.source_digest == preserved.bundle.descriptor.blake3),
                "preserved restored source differs"
            );
        }
        if let Some(outbox) = &j.prune {
            ensure!(
                j.head
                    .as_ref()
                    .is_some_and(|h| h.binding.capsule_id == outbox.successor_capsule)
                    && j.retained
                        .iter()
                        .any(|h| h.binding.capsule_id == outbox.old_capsule
                            && h.binding.capsule_id != outbox.successor_capsule),
                "private backup prune binding differs"
            );
            ensure!(
                (outbox.phase != PrunePhase::Remote || outbox.obligation.is_none())
                    && outbox.pending_intent == Self::pending_binding(&j)?,
                "private prune phase differs"
            );
        }
        Ok(j)
    }
    fn completed_pending_binding(head: &Head) -> Result<[u8; 32]> {
        let pending = Pending {
            binding: head.binding.clone(),
            digest: head.bundle.descriptor.blake3,
            file: head.bundle.name.clone(),
            snapshot_commit: head.receipt.snapshot_commit.clone(),
            snapshot_operation: head.receipt.snapshot_operation.clone(),
            local: head.local.clone(),
            intent: Some(head.intent.clone()),
        };
        Ok(*blake3::hash(&serde_json::to_vec(&pending)?).as_bytes())
    }
    fn pending_binding(j: &Journal) -> Result<Option<[u8; 32]>> {
        j.pending
            .as_ref()
            .map(|pending| Ok(*blake3::hash(&serde_json::to_vec(pending)?).as_bytes()))
            .transpose()
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
    fn admit_spool(&self) -> Result<()> {
        let mut files = 0usize;
        let mut bytes = 0u64;
        for entry in std::fs::read_dir(self.directory.path())? {
            let entry = entry?;
            files += 1;
            ensure!(
                files <= 60,
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
        Ok(())
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
            j.prune.is_none() && j.retained.len() < 16,
            "private backup history requires explicit prune"
        );
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
        self.admit_spool()?;
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
                let p=Pending{binding:CapsuleBinding{account_id:self2.account.clone(),capsule_id:hex(&random()),workspace_id:hex(&j.workspace),generation:j.generation+1,selection:self2.selection.clone(),descriptor:CapsuleDescriptor{format:"tinytree-private-capture-v1".into(),total_bytes:total,chunk_count:chunks,chain_root:chain}},digest,file:name,snapshot_commit:hex(&verified.working_copy().snapshot_commit),snapshot_operation:verified.working_copy().wc_operation.clone(),local:None,intent:None};
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
    pub(crate) async fn seed_restored(
        self: Arc<Self>,
        source: &bundle::VerifiedPrivateBundle,
        binding: &CapsuleBinding,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(300);
        let admission = self.admit(deadline).await?;
        let mut input = source.file()?;
        let digest = source.blake3();
        let expected_size = source.size_bytes();
        let binding = binding.clone();
        tokio::task::spawn_blocking(move || {
            let _admission = admission;
            ensure!(expected_size <= MAX, "private seed size limit");
            self.admit_spool()?;
            let temporary = format!("{}.part", hex(&random()));
            use std::io::Write as _;
            use std::os::unix::fs::OpenOptionsExt;
            let mut output = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
                .open(self.directory.path().join(&temporary))?;
            input.seek(SeekFrom::Start(0))?;
            let mut buffer = [0; 65536];
            let mut total = 0u64;
            while total < expected_size {
                ensure!(
                    tokio::time::Instant::now() < deadline,
                    "private seed copy deadline"
                );
                let n = (expected_size - total).min(65536) as usize;
                input.read_exact(&mut buffer[..n])?;
                output.write_all(&buffer[..n])?;
                total += n as u64;
                ensure!(total <= MAX, "private seed copy limit");
            }
            output.sync_all()?;
            let verified = bundle::verify_file(output, digest, remaining_bundle_limits(deadline)?)?;
            let name = format!("{}.private", hex(&digest));
            match std::fs::hard_link(
                self.directory.path().join(&temporary),
                self.directory.path().join(&name),
            ) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    bundle::verify_file(
                        self.directory.open_existing_file(&name, MAX)?,
                        digest,
                        remaining_bundle_limits(deadline)?,
                    )?;
                }
                Err(error) => return Err(error.into()),
            }
            File::open(self.directory.path())?.sync_all()?;
            std::fs::remove_file(self.directory.path().join(&temporary))?;
            self.seed_verified(&verified, &binding)
        })
        .await
        .context("private seed copy worker failed")?
    }
    fn seed_verified(
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
                    && j.receipt.as_ref() == Some(&receipt)
                    && j.head.as_ref().is_some_and(|head| head.binding == *binding
                        && head.bundle.descriptor.blake3 == source.blake3()
                        && head.local.is_none()),
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
        let intent_bytes = serde_json::to_vec(
            &serde_json::json!({"schema":2,"source":"restore","context":self.binding,"binding":binding,"digest":source.blake3(),"source_workspace":source.manifest().source_workspace_id}),
        )?;
        let intent = retention::write_artifact(
            &self.directory,
            &format!("{}.intent", binding.capsule_id),
            &intent_bytes,
        )?;
        let seal = retention::write_artifact(
            &self.directory,
            &format!("{}.seal", binding.capsule_id),
            &serde_json::to_vec(&receipt)?,
        )?;
        j.head = Some(Head {
            binding: binding.clone(),
            receipt: receipt.clone(),
            bundle: retention::ExactFile {
                name: format!("{}.private", hex(&source.blake3())),
                descriptor: BundleDescriptor {
                    blake3: source.blake3(),
                    size_bytes: source.size_bytes(),
                },
            },
            intent,
            seal,
            local: None,
        });
        j.receipt = Some(receipt);
        j.restore_seed = Some(seed);
        self.save(&j)
    }
    pub(crate) async fn record_capture(
        &self,
        registry: &native_retention::Registry,
        registration: native_retention::Registration,
        pending: native_retention::PendingCapture,
        deadline: tokio::time::Instant,
    ) -> Result<()> {
        let mut j = self.load()?;
        let p = j
            .pending
            .as_mut()
            .context("private capture intent unavailable")?;
        let file = self.directory.open_existing_file(&p.file, MAX)?;
        let digest = p.digest;
        let verified = tokio::task::spawn_blocking(move || {
            bundle::verify_file(file, digest, remaining_bundle_limits(deadline)?)
        })
        .await??;
        ensure!(
            pending.registration == registration,
            "private capture registration differs"
        );
        let actual = retention::record(&pending, &verified)?;
        let handle = if p.local.is_none() {
            retention::reconcile(registry, registration, &verified).await?
        } else {
            actual.clone()
        };
        ensure!(handle == actual, "private capture recovery ticket differs");
        // Reconciliation reads the durable registry, not merely the returned DTO.
        let durable = retention::resolve(registry, &handle, &verified).await?;
        ensure!(durable == pending, "private capture durable ticket differs");
        if let Some(old) = &p.local {
            ensure!(old == &handle, "private capture replay ticket differs");
        }
        p.local = Some(handle);
        let mut immutable = p.clone();
        immutable.intent = None;
        let intent = retention::write_artifact(
            &self.directory,
            &format!("{}.intent", p.binding.capsule_id),
            &serde_json::to_vec(&immutable)?,
        )?;
        if let Some(old) = &p.intent {
            ensure!(old == &intent, "private capture intent artifact differs");
        }
        p.intent = Some(intent);
        self.save(&j)
    }
    async fn verify_head(
        &self,
        head: &Head,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<bundle::VerifiedPrivateBundle>> {
        let file = self.directory.open_existing_file(&head.bundle.name, MAX)?;
        let digest = head.bundle.descriptor.blake3;
        let verified = tokio::task::spawn_blocking(move || {
            bundle::verify_file(file, digest, remaining_bundle_limits(deadline)?)
        })
        .await??;
        self.verify_scope(&verified)?;
        ensure!(
            verified.size_bytes() == head.bundle.descriptor.size_bytes
                && hex(&verified.manifest().workspace_id) == head.binding.workspace_id
                && verified.manifest().generation == head.binding.generation
                && hex(&verified.working_copy().snapshot_commit) == head.receipt.snapshot_commit
                && verified.working_copy().wc_operation == head.receipt.snapshot_operation,
            "private prune semantic source differs"
        );
        Ok(Arc::new(verified))
    }
    pub(crate) async fn prune(
        self: Arc<Self>,
        registry: native_retention::Registry,
        deadline: tokio::time::Instant,
    ) -> Result<crate::control::NativeCapsulesPruned> {
        let admission = self.admit(deadline).await?;
        let j = self.load()?;
        let original_pending = Self::pending_binding(&j)?;
        let current = j
            .head
            .as_ref()
            .context("private prune has no acknowledged head")?
            .binding
            .capsule_id
            .clone();
        self.verify_session(deadline).await?;
        let client = PrivateCapsuleClient::connect(self.config.clone(), self.trust).await?;
        let selected = j.head.as_ref().context("private prune head unavailable")?;
        let status = client
            .call_until(
                CapsuleCall::new(&self.account, &current, CapsuleAction::Status),
                Some(&selected.binding),
                deadline,
            )
            .await?;
        ensure!(
            status.state == CapsuleState::Sealed,
            "private prune acknowledged head is not sealed"
        );
        let mut retired = 0u32;
        let mut local_files = 0u32;
        for _ in 0..16 {
            let mut j = self.load()?;
            let successor = j.head.clone().context("private prune head unavailable")?;
            ensure!(
                successor.binding.capsule_id == current
                    && Self::pending_binding(&j)? == original_pending,
                "private prune successor changed"
            );
            if j.retained.is_empty() {
                break;
            }
            let result: Result<u32> = async {
                let status = client
                    .call_until(
                        CapsuleCall::new(&self.account, &current, CapsuleAction::Status),
                        Some(&successor.binding),
                        deadline,
                    )
                    .await?;
                ensure!(
                    status.state == CapsuleState::Sealed,
                    "private prune successor is not sealed"
                );
                let old_id = j
                    .prune
                    .as_ref()
                    .map(|p| p.old_capsule.clone())
                    .unwrap_or_else(|| j.retained[0].binding.capsule_id.clone());
                let old = j
                    .retained
                    .iter()
                    .find(|h| h.binding.capsule_id == old_id)
                    .cloned()
                    .context("private prune predecessor unavailable")?;
                ensure!(
                    old.binding.capsule_id != current
                        && old.binding.generation < successor.binding.generation,
                    "private prune target is not superseded"
                );
                if let Some(pending) = &j.pending {
                    ensure!(
                        old.binding.capsule_id != pending.binding.capsule_id
                            && old.bundle.name != pending.file
                            && pending
                                .intent
                                .as_ref()
                                .is_none_or(|intent| intent.name != old.intent.name
                                    && intent.name != old.seal.name),
                        "private prune target aliases pending custody"
                    );
                }
                if j.prune.is_none()
                    || j.prune
                        .as_ref()
                        .is_some_and(|outbox| outbox.phase == PrunePhase::Remote)
                {
                    let older = self.verify_head(&old, deadline).await?;
                    let newer = self.verify_head(&successor, deadline).await?;
                    let owner: Arc<dyn std::any::Any + Send + Sync> = admission.clone();
                    let _proof = coverage::prove_successor(&older, &newer, owner).await?;
                    if j.prune.is_none() {
                        j.prune = Some(PruneOutbox {
                            old_capsule: old_id.clone(),
                            successor_capsule: current.clone(),
                            phase: PrunePhase::Remote,
                            obligation: None,
                            pending_intent: original_pending,
                        });
                        self.save(&j)?;
                    }
                    let mut deleted = false;
                    for _ in 0..513 {
                        ensure!(
                            tokio::time::Instant::now() < deadline,
                            "private prune deadline"
                        );
                        let reply = client
                            .call_until(
                                CapsuleCall::new(&self.account, &old_id, CapsuleAction::Retire),
                                Some(&old.binding),
                                deadline,
                            )
                            .await?;
                        if reply.state == CapsuleState::Deleted {
                            deleted = true;
                            break;
                        }
                        ensure!(
                            reply.state == CapsuleState::Retiring,
                            "private prune predecessor state differs"
                        );
                    }
                    ensure!(deleted, "private prune remote pages remain pending");
                    self.verify_session(deadline).await?;
                    j.prune.as_mut().unwrap().phase = PrunePhase::Local;
                    self.save(&j)?;
                }
                let count = if let Some(old_handle) = &old.local {
                    let new_handle = successor
                        .local
                        .as_ref()
                        .context("private prune successor local custody unavailable")?;
                    if j.prune.as_ref().unwrap().obligation.is_none() {
                        let existing = registry.capture_cleanup().await?;
                        let obligation = if let Some(obligation) = existing {
                            ensure!(
                                obligation.retired.id == old_handle.capture_id
                                    && obligation.successor.id == new_handle.capture_id
                                    && obligation.retired_binding == old_handle.ticket_binding
                                    && obligation.successor_binding == new_handle.ticket_binding
                                    && obligation.upload.job == Some(old.intent.descriptor)
                                    && obligation.upload.published == Some(old.seal.descriptor)
                                    && obligation.basis.is_none(),
                                "private prune cleanup slot differs"
                            );
                            obligation
                        } else {
                            let older = self.verify_head(&old, deadline).await?;
                            let newer = self.verify_head(&successor, deadline).await?;
                            let owner: Arc<dyn std::any::Any + Send + Sync> = admission.clone();
                            let proof = coverage::prove_successor(&older, &newer, owner).await?;
                            let old_ticket =
                                retention::resolve(&registry, old_handle, &older).await?;
                            let new_ticket =
                                retention::resolve(&registry, new_handle, &newer).await?;
                            retention::retire(
                                &registry,
                                old_ticket,
                                new_ticket,
                                old.intent.descriptor,
                                old.seal.descriptor,
                                proof,
                            )
                            .await
                            .context("private_prune_native_retire")?
                        };
                        j.prune.as_mut().unwrap().obligation = Some(obligation);
                        self.save(&j)?;
                    }
                    retention::cleanup(
                        &registry,
                        j.prune.as_ref().unwrap().obligation.as_ref().unwrap(),
                        &self.directory,
                        old.files(),
                        successor.files(),
                    )
                    .await
                    .context("private_prune_guarded_cleanup")?
                } else {
                    ensure!(
                        j.preserved_restored
                            .as_ref()
                            .is_none_or(|saved| saved == &old),
                        "private prune has another preserved restored source"
                    );
                    j.preserved_restored = Some(old.clone());
                    0
                };
                j.retained.retain(|h| h.binding.capsule_id != old_id);
                j.prune = None;
                self.save(&j)?;
                Ok(count)
            }
            .await;
            match result {
                Ok(count) => {
                    retired += 1;
                    local_files += count;
                }
                Err(error) => {
                    self.record_error("prune", &error);
                    break;
                }
            }
        }
        let remaining = self.load()?;
        Ok(crate::control::NativeCapsulesPruned {
            current_capsule: current,
            retired,
            local_files,
            pending: remaining.retained.len() as u32,
        })
    }
    pub(crate) async fn resume(
        self: Arc<Self>,
        registry: native_retention::Registry,
        registration: Option<(native_retention::Registration, [u8; 32])>,
        deadline: tokio::time::Instant,
    ) -> Result<crate::control::NativePrivateBackupResumed> {
        let _admission = self.admit(deadline).await?;
        let j = self.load()?;
        ensure!(
            j.prune.is_none(),
            "private resume requires completion of saved prune work"
        );
        if let Some(pending) = &j.pending {
            let (registration, source_workspace) =
                registration.context("private resume source registration unavailable")?;
            let file = self.directory.open_existing_file(&pending.file, MAX)?;
            let digest = pending.digest;
            let verified = tokio::task::spawn_blocking(move || {
                bundle::verify_file(file, digest, remaining_bundle_limits(deadline)?)
            })
            .await??;
            self.verify_scope(&verified)?;
            ensure!(
                verified.manifest().source_workspace_id == source_workspace
                    && verified.manifest().workspace_id == j.workspace
                    && verified.manifest().exporter == j.exporter
                    && verified.manifest().generation == pending.binding.generation
                    && verified.size_bytes() == pending.binding.descriptor.total_bytes
                    && hex(&verified.working_copy().snapshot_commit) == pending.snapshot_commit
                    && verified.working_copy().wc_operation == pending.snapshot_operation,
                "private resume saved semantic source differs"
            );
            let handle = if let Some(handle) = &pending.local {
                ensure!(
                    handle.registration == registration,
                    "private resume registration changed"
                );
                handle.clone()
            } else {
                retention::reconcile(&registry, registration, &verified).await?
            };
            let actual = retention::resolve(&registry, &handle, &verified).await?;
            // Only actual durable rows can fill the registry-commit/host-save gap.
            self.record_capture(&registry, registration, actual, deadline)
                .await?;
            let receipt = self.upload_pending(deadline, true).await?;
            return Ok(crate::control::NativePrivateBackupResumed {
                receipt,
                replayed: false,
            });
        }
        let ack = j
            .resume_ack
            .as_ref()
            .context("no saved pending private backup or explicit resume acknowledgement")?;
        let head = j
            .head
            .as_ref()
            .context("private resume acknowledged head unavailable")?;
        ensure!(
            head == &ack.head,
            "private resume acknowledged head changed"
        );
        let _verified = self.verify_head(head, deadline).await?;
        self.verify_session(deadline).await?;
        let client = PrivateCapsuleClient::connect(self.config.clone(), self.trust).await?;
        let status = client
            .call_until(
                CapsuleCall::new(
                    &self.account,
                    &head.binding.capsule_id,
                    CapsuleAction::Status,
                ),
                Some(&head.binding),
                deadline,
            )
            .await?;
        ensure!(
            status.state == CapsuleState::Sealed,
            "private resume acknowledged capsule is not sealed"
        );
        self.verify_session(deadline).await?;
        Ok(crate::control::NativePrivateBackupResumed {
            receipt: ack.head.receipt.clone(),
            replayed: true,
        })
    }
    pub(crate) async fn upload(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<PrivateBackupReceipt> {
        self.upload_pending(deadline, false).await
    }
    async fn upload_pending(
        &self,
        deadline: tokio::time::Instant,
        explicit_resume: bool,
    ) -> Result<PrivateBackupReceipt> {
        let mut j = self.load()?;
        let pending_source =
            Self::pending_binding(&j)?.context("private backup pending source unavailable")?;
        let p = j
            .pending
            .clone()
            .context("private backup intent unavailable")?;
        ensure!(
            p.local.is_some() && p.intent.is_some() && j.retained.len() < 16 && j.prune.is_none(),
            "private backup durable capture/history admission unavailable"
        );
        let predicted = PrivateBackupReceipt {
            schema: 1,
            capsule_id: p.binding.capsule_id.clone(),
            workspace_id: p.binding.workspace_id.clone(),
            generation: p.binding.generation,
            bytes: p.binding.descriptor.total_bytes,
            chain_root: p.binding.descriptor.chain_root.clone(),
            snapshot_commit: p.snapshot_commit.clone(),
            snapshot_operation: p.snapshot_operation.clone(),
        };
        predicted.validate()?;
        let seal_bytes = serde_json::to_vec(&predicted)?;
        let predicted_seal = retention::ExactFile {
            name: format!("{}.seal", p.binding.capsule_id),
            descriptor: BundleDescriptor {
                blake3: *blake3::hash(&seal_bytes).as_bytes(),
                size_bytes: seal_bytes.len() as u64,
            },
        };
        let mut admitted = j.clone();
        let predicted_head = Head {
            binding: p.binding.clone(),
            receipt: predicted.clone(),
            bundle: retention::ExactFile {
                name: p.file.clone(),
                descriptor: BundleDescriptor {
                    blake3: p.digest,
                    size_bytes: p.binding.descriptor.total_bytes,
                },
            },
            intent: p.intent.clone().context("private intent unavailable")?,
            seal: predicted_seal,
            local: p.local.clone(),
        };
        admitted.resume_ack = if explicit_resume {
            Some(ResumeAck {
                head: predicted_head.clone(),
                pending_source,
            })
        } else {
            None
        };
        if let Some(old) = admitted.head.replace(predicted_head) {
            admitted.retained.push(old);
        }
        admitted.generation = predicted.generation;
        admitted.receipt = Some(predicted);
        admitted.pending = None;
        ensure!(
            serde_json::to_vec(&admitted)?.len() <= JOURNAL,
            "private backup history metadata admission limit"
        );
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
            capsule_id: p.binding.capsule_id.clone(),
            workspace_id: p.binding.workspace_id.clone(),
            generation: p.binding.generation,
            bytes: p.binding.descriptor.total_bytes,
            chain_root: p.binding.descriptor.chain_root.clone(),
            snapshot_commit: p.snapshot_commit.clone(),
            snapshot_operation: p.snapshot_operation.clone(),
        };
        receipt.validate()?;
        let seal = retention::write_artifact(
            &self.directory,
            &format!("{}.seal", p.binding.capsule_id),
            &serde_json::to_vec(&receipt)?,
        )?;
        let head = Head {
            binding: p.binding.clone(),
            receipt: receipt.clone(),
            bundle: retention::ExactFile {
                name: p.file.clone(),
                descriptor: BundleDescriptor {
                    blake3: p.digest,
                    size_bytes: p.binding.descriptor.total_bytes,
                },
            },
            intent: p
                .intent
                .context("private backup intent artifact unavailable")?,
            seal,
            local: p.local,
        };
        j.resume_ack = if explicit_resume {
            Some(ResumeAck {
                head: head.clone(),
                pending_source,
            })
        } else {
            None
        };
        if let Some(old) = j.head.replace(head) {
            j.retained.push(old);
        }
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
