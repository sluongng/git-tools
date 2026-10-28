//! Retained-version admission is separate from reference holds and index import.
use crate::{
    Inner, RemoteObjectIo, RemoteRepositoryControl, io,
    ledger::{Authority, CloudVersionAuthority},
};
use anyhow::{Context, Result, ensure};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tinytree_client::{CloudRawDigest, RetainedVersionSelector, VerifiedRetainedVersion};
use tinytree_model as m;

pub(crate) fn captured_manifest(bytes: &[u8]) -> bool {
    bytes.get(..8) == Some(b"TTCLOUD\0".as_slice()) && bytes.get(8..10) == Some([3u8, 0].as_slice())
}
pub(crate) fn retained_native_roots(
    config: &crate::Config,
    selector: RetainedVersionSelector,
    bytes: &[u8],
) -> Result<Vec<m::ObjectKey>> {
    let mut limits = tinytree_cloud::ManifestV2Limits::default();
    limits.recovery.max_manifest_bytes = config.remote.cloud.max_manifest_bytes;
    limits.recovery.max_decode_bytes = config
        .closure
        .max_metadata_bytes
        .checked_mul(8)
        .context("cloud manifest decode budget overflow")?;
    limits.recovery.max_native_roots = config.max_admitted_roots;

    ensure!(
        bytes.len() >= 10 && &bytes[..8] == b"TTCLOUD\0",
        "unrecognized canonical cloud manifest"
    );
    let roots = match u16::from_le_bytes(bytes[8..10].try_into().unwrap()) {
        1 => {
            let value = tinytree_cloud::decode_manifest(
                bytes,
                tinytree_cloud::ManifestId(selector.manifest_id.0),
                limits.recovery,
            )?;
            ensure!(
                tinytree_cloud::encode_manifest(&value, limits.recovery)? == bytes,
                "cloud manifest is not canonical"
            );
            value.native_roots
        }
        2 => {
            let value = tinytree_cloud::decode_manifest_v2(
                bytes,
                tinytree_cloud::ManifestId(selector.manifest_id.0),
                limits,
            )?;
            ensure!(
                tinytree_cloud::encode_manifest_v2(&value, limits)? == bytes,
                "cloud manifest is not canonical"
            );
            value.recovery.native_roots
        }
        3 => {
            let mut captured = tinytree_cloud::ManifestV3Limits {
                history: limits,
                ..Default::default()
            };
            captured.capture.max_bytes = config.remote.cloud.max_capture_bytes;
            let value = tinytree_cloud::decode_manifest_v3(
                bytes,
                tinytree_cloud::ManifestId(selector.manifest_id.0),
                captured,
            )?;
            ensure!(
                tinytree_cloud::encode_manifest_v3(&value, captured)? == bytes,
                "cloud manifest is not canonical"
            );
            // Declared capture policy does not prove a live
            // mount, restore completeness or index installation.
            value.history.recovery.native_roots
        }
        _ => anyhow::bail!("unsupported canonical cloud manifest version"),
    };
    Ok(roots)
}

/// Receipt of canonical manifest-root admission only. It does not prove radix
/// coverage, import an index, publish a mount or grant snapshot query authority.
pub struct AdmittedCloudVersion {
    selector: RetainedVersionSelector,
    selected: CloudRawDigest,
    roots: usize,
}
impl AdmittedCloudVersion {
    pub fn selector(&self) -> RetainedVersionSelector {
        self.selector
    }
    pub fn manifest_selected(&self) -> CloudRawDigest {
        self.selected
    }
    pub fn native_root_count(&self) -> usize {
        self.roots
    }
}
struct CloudJob {
    inner: Arc<Inner>,
    _owner: Arc<dyn Any + Send + Sync>,
    scope: Option<Arc<io::ControlScope>>,
    _completion: io::WorkTicket,
}
impl RemoteRepositoryControl {
    /// Reauthorize through THIS provider's current connector; supplied selectors
    /// and raw descriptors only pin expected bytes, never transfer client grants.
    pub async fn admit_retained_version(
        &self,
        selector: RetainedVersionSelector,
        selected: CloudRawDigest,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<AdmittedCloudVersion> {
        ensure!(
            selected.size_bytes <= self.inner.config.remote.cloud.max_manifest_bytes as u64,
            "cloud manifest byte limit exceeded"
        );
        let deadline = io::scope().map_or_else(
            || self.inner.deadline(),
            |scope| scope.deadline.min(self.inner.deadline()),
        );
        let job = Arc::new(CloudJob {
            inner: self.inner.clone(),
            _owner: owner,
            scope: io::scope(),
            _completion: self.inner.metadata_work.enter(),
        });
        io::owned(async move {
            let operation = async {
                let client = job
                    .inner
                    .connector
                    .get()
                    .await?
                    .with_lifetime_guard(job.clone())
                    .with_operation_deadline(deadline);
                let proof = Arc::new(
                    io::network(client.cloud_retained_version_selected(selector, selected)).await?,
                );
                let inner = job.inner.clone();
                let held = proof.clone();
                let roots = io::blocking_owned(job.clone(), move || {
                    retained_native_roots(&inner.config, selector, held.manifest())
                })
                .await?;
                let authority = if captured_manifest(proof.manifest()) {
                    crate::retained_offload::adopt_file_source_pin(
                        &job.inner,
                        job.clone(),
                        &client,
                        proof,
                        roots,
                    )
                    .await?
                } else {
                    let inner = job.inner.clone();
                    io::blocking_owned(job.clone(), move || {
                        inner.ledger.install_cloud_authority(&proof, &roots)
                    })
                    .await?
                };
                Ok(AdmittedCloudVersion {
                    selector,
                    selected,
                    roots: authority.root_count(),
                })
            };
            let bounded = async {
                tokio::time::timeout_at(deadline, operation)
                    .await
                    .map_err(|_| anyhow::anyhow!("native cloud admission deadline exceeded"))?
            };
            match job.scope.clone() {
                Some(scope) => io::control_scope(scope, bounded).await,
                None => bounded.await,
            }
        })
        .await
    }
}
impl RemoteObjectIo {
    async fn cloud_proof(
        &self,
        authority: &CloudVersionAuthority,
        requested: m::ObjectKey,
        parents: &HashMap<m::ObjectKey, m::ObjectKey>,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<(m::ObjectKey, Vec<m::ObjectKey>)> {
        let mut current = requested;
        let mut suffix = Vec::new();
        let mut seen = HashSet::new();
        loop {
            ensure!(
                seen.insert(current) && seen.len() <= 256,
                "cloud copy provenance exceeds bounds"
            );
            let ledger = self.inner.ledger.clone();
            let captured = authority.clone();
            if let Some((terminal, mut path)) =
                io::blocking_owned(owner.clone(), move || ledger.cloud_path(&captured, current))
                    .await?
            {
                ensure!(
                    path.len()
                        .checked_add(suffix.len())
                        .is_some_and(|n| n <= 256),
                    "cloud provenance path exceeds bounds"
                );
                path.extend(suffix.into_iter().rev());
                return Ok((terminal, path));
            }
            suffix.push(current);
            current = *parents
                .get(&current)
                .context("cloud metadata has no canonical retained-version provenance")?;
        }
    }
    async fn fresh_cloud_version(
        &self,
        authority: &CloudVersionAuthority,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<(tinytree_client::NativeClient, VerifiedRetainedVersion)> {
        let client = self
            .inner
            .connector
            .get()
            .await?
            .with_lifetime_guard(Arc::new(owner.clone()));
        let client = match io::scope() {
            Some(scope) => client.with_operation_deadline(scope.deadline),
            None => client,
        };
        let proof = io::network(
            client.cloud_retained_version_selected(authority.selector(), authority.selected()),
        )
        .await?;
        ensure!(
            authority.agrees(&proof),
            "cloud version owner or selected binding changed"
        );
        if captured_manifest(proof.manifest()) {
            crate::retained_offload::ensure_file_source_pin(
                &self.inner,
                owner,
                &client,
                authority.clone(),
            )
            .await?;
        }
        Ok((client, proof))
    }
    pub(crate) async fn cloud_metadata(
        &self,
        authority: &CloudVersionAuthority,
        requested: m::ObjectKey,
        parents: &HashMap<m::ObjectKey, m::ObjectKey>,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<tinytree_client::VerifiedMetadata> {
        let ledger = self.inner.ledger.clone();
        let selected = authority.clone();
        let read =
            io::blocking_owned(owner.clone(), move || ledger.retain_cloud_read(&selected)).await?;
        let owner: Arc<dyn Any + Send + Sync> = Arc::new((owner, read));
        let (terminal, path) = self
            .cloud_proof(authority, requested, parents, owner.clone())
            .await?;
        let (client, proof) = self.fresh_cloud_version(authority, owner).await?;
        Ok(
            io::network(client.cloud_read_native_metadata(&proof, terminal, &path, requested))
                .await?,
        )
    }
    pub(crate) async fn terminal_cloud_file_descriptor(
        &self,
        id: m::FileId,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<m::Descriptor>> {
        let ledger = self.inner.ledger.clone();
        io::blocking_owned(owner, move || ledger.terminal_cloud_file_descriptor(id)).await
    }
    pub(crate) async fn cloud_file_authority(
        &self,
        id: m::FileId,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<CloudVersionAuthority>> {
        let ledger = self.inner.ledger.clone();
        io::blocking_owned(owner, move || ledger.cloud_file_authority(id)).await
    }
    /// Raw, nonrecursive delivery with ordinary FILE limits and owned temporary
    /// storage. A caller must retain the complete VerifiedFile through its reader.
    pub(crate) async fn download_cloud_file_owned(
        &self,
        authority: CloudVersionAuthority,
        id: m::FileId,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<tinytree_client::VerifiedFile> {
        let deadline = io::scope().map_or_else(
            || self.inner.deadline(),
            |scope| scope.deadline.min(self.inner.deadline()),
        );
        tokio::time::timeout_at(
            deadline,
            self.download_cloud_file_inner(authority, id, owner),
        )
        .await
        .map_err(|_| anyhow::anyhow!("native cloud file deadline exceeded"))?
    }
    async fn download_cloud_file_inner(
        &self,
        authority: CloudVersionAuthority,
        id: m::FileId,
        owner: Arc<dyn Any + Send + Sync>,
    ) -> Result<tinytree_client::VerifiedFile> {
        let ledger = self.inner.ledger.clone();
        let selected = authority.clone();
        let read =
            io::blocking_owned(owner.clone(), move || ledger.retain_cloud_read(&selected)).await?;
        let owner: Arc<dyn Any + Send + Sync> = Arc::new((owner, read));
        let requested = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(id.0),
        };
        let (terminal, path) = self
            .cloud_proof(&authority, requested, &HashMap::new(), owner.clone())
            .await?;
        let (client, proof) = self.fresh_cloud_version(&authority, owner.clone()).await?;
        let verified = io::network(client.cloud_download_native_file(
            &proof,
            terminal,
            &path,
            id,
            &self.inner.staging,
        ))
        .await?;
        if authority.terminal(requested)? {
            let ledger = self.inner.ledger.clone();
            return io::blocking_owned(owner, move || {
                ledger.verified_cloud_file_terminal(&authority, &verified)?;
                Ok(verified)
            })
            .await;
        }
        Ok(verified)
    }
}
