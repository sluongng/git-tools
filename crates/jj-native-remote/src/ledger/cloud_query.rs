//! Exact selected-snapshot query provenance, never a live hold or manifest root.
use super::*;
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CloudQueryAuthority {
    version: u8,
    pub(super) id: String,
    binding: String,
    provider_identity: [u8; 32],
    workspace: [u8; 32],
    generation: u64,
    manifest: [u8; 32],
    raw: [u8; 32],
    size: u64,
    selected: Vec<[u8; 32]>,
    anchors: Vec<[u8; 32]>,
    owner: [u8; 32],
    native_binding: [u8; 32],
}
impl CloudQueryAuthority {
    pub fn key(&self) -> String {
        format!("cq/{}", self.id)
    }
    pub fn source(&self) -> native_broker::CloudQuerySource {
        native_broker::CloudQuerySource {
            provider_identity: self.provider_identity,
            workspace_id: self.workspace,
            generation: self.generation,
            manifest_id: self.manifest,
            manifest_blake3: self.raw,
            manifest_size_bytes: self.size,
            index_root_ids: self.selected.clone(),
            expected_anchors: self.anchors.iter().copied().map(CommitId).collect(),
        }
    }
    pub fn digest(&self) -> [u8; 32] {
        self.source().digest()
    }
    pub fn selector(&self) -> tinytree_client::RetainedVersionSelector {
        tinytree_client::RetainedVersionSelector {
            workspace_id: self.workspace,
            generation: self.generation,
            manifest_id: tinytree_client::CloudManifestId(self.manifest),
        }
    }
    pub fn selected_raw(&self) -> tinytree_client::CloudRawDigest {
        tinytree_client::CloudRawDigest {
            blake3: self.raw,
            size_bytes: self.size,
        }
    }
    pub fn indexes(&self) -> &[[u8; 32]] {
        &self.selected
    }
    pub fn agrees(&self, proof: &tinytree_client::VerifiedRetainedVersion) -> bool {
        proof.selector() == self.selector()
            && proof.manifest_selected() == self.selected_raw()
            && proof.owner_binding() == self.owner
            && proof.binding_digest() == self.native_binding
    }
    fn identity(&self) -> Result<String> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-cloud-query-authority-v1\0");
        hash.update(&serde_json::to_vec(&(
            self.version,
            &self.binding,
            self.source().digest(),
            self.owner,
            self.native_binding,
        ))?);
        Ok(hash.finalize().to_hex().to_string())
    }
}
impl Ledger {
    pub fn cloud_query_authority(
        &self,
        source: native_broker::CloudQuerySource,
        proof: &tinytree_client::VerifiedRetainedVersion,
    ) -> Result<CloudQueryAuthority> {
        ensure!(
            source.workspace_id == proof.selector().workspace_id
                && source.generation == proof.selector().generation
                && source.manifest_id == proof.selector().manifest_id.0
                && source.manifest_blake3 == proof.manifest_selected().blake3
                && source.manifest_size_bytes == proof.manifest_selected().size_bytes,
            "cloud query selected version changed"
        );
        let mut authority = CloudQueryAuthority {
            version: 1,
            id: String::new(),
            binding: self.binding.scope.clone(),
            provider_identity: source.provider_identity,
            workspace: source.workspace_id,
            generation: source.generation,
            manifest: source.manifest_id,
            raw: source.manifest_blake3,
            size: source.manifest_size_bytes,
            selected: source.index_root_ids,
            anchors: source.expected_anchors.into_iter().map(|id| id.0).collect(),
            owner: proof.owner_binding(),
            native_binding: proof.binding_digest(),
        };
        authority.id = authority.identity()?;
        ensure!(
            self.valid_cloud_query(&authority)?,
            "invalid cloud query source"
        );
        self.enable_cloud_queries()?;
        Ok(authority)
    }
    pub(super) fn valid_cloud_query(&self, authority: &CloudQueryAuthority) -> Result<bool> {
        self.check()?;
        Ok(authority.version == 1
            && authority.binding == self.binding.scope
            && authority.provider_identity != [0; 32]
            && authority.generation > 0
            && authority.size > 0
            && authority.size <= self.config.remote.cloud.max_manifest_bytes as u64
            && !authority.selected.is_empty()
            && authority.selected.len() <= 256
            && authority.selected.windows(2).all(|p| p[0] < p[1])
            && !authority.anchors.is_empty()
            && authority.anchors.len() <= self.config.max_admitted_roots
            && authority.anchors.windows(2).all(|p| p[0] < p[1])
            && authority.id == authority.identity()?)
    }
    pub(super) fn cloud_query(&self, id: &str) -> Result<Option<CloudQueryAuthority>> {
        let _: [u8; 32] = unhex(id)?;
        let authority = self.get::<CloudQueryAuthority>(&self.cloud_queries, id.as_bytes())?;
        if let Some(authority) = &authority {
            ensure!(authority.id == id, "cloud query index identity changed");
        }
        Ok(authority)
    }
    pub(crate) fn cloud_query_authorities(&self) -> Result<Vec<CloudQueryAuthority>> {
        self.list(&self.cloud_queries, self.config.query.max_authorities)
    }
    pub(super) fn authority_count(&self) -> Result<usize> {
        let mut count = 0usize;
        for space in [
            &self.query_authorities,
            &self.cloud_authorities,
            &self.cloud_queries,
        ] {
            for item in self.db.read_tx().iter(space) {
                item.into_inner()?;
                count = count
                    .checked_add(1)
                    .context("query authority count overflow")?;
                ensure!(
                    count <= self.config.query.max_authorities,
                    "query authority count exceeded"
                );
            }
        }
        Ok(count)
    }
    pub fn cloud_query_file(
        &self,
        id: tinytree_model::FileId,
    ) -> Result<Option<CloudQueryAuthority>> {
        let Some(claim) = self.claim(id.0)? else {
            return Ok(None);
        };
        let file = id;
        let Some(id) = claim.root.strip_prefix("cq/") else {
            return Ok(None);
        };
        // Current exact terminals precede stale TREE claims, but never borrow
        // an old claim's size or otherwise widen selected-snapshot authority.
        if self.cloud_file_authority(file)?.is_some() {
            return Ok(None);
        }
        let authority = self
            .cloud_query(id)?
            .context("cloud query file authority missing")?;
        ensure!(
            self.valid_cloud_query(&authority)?,
            "cloud query file binding is not current"
        );
        Ok(Some(authority))
    }
    /// Follow only witnessed canonical edges to an authenticated snapshot COMMIT.
    pub fn cloud_query_path(
        &self,
        authority: &CloudQueryAuthority,
        requested: ObjectKey,
        parents: &std::collections::HashMap<ObjectKey, ObjectKey>,
    ) -> Result<(CommitId, Vec<ObjectKey>)> {
        self.validate_authority(&Authority::CloudQuery(authority.clone()))?;
        let mut current = requested;
        let mut reverse = Vec::new();
        let mut seen = std::collections::HashSet::new();
        loop {
            ensure!(
                seen.insert(current) && seen.len() <= 256,
                "cloud query proof path exceeds bounds"
            );
            if self.query_terminal(&Authority::CloudQuery(authority.clone()), current)? {
                ensure!(
                    current.kind == Kind::Commit,
                    "cloud query terminal is not a commit"
                );
                reverse.reverse();
                return Ok((CommitId(current.id.0), reverse));
            }
            let parent = if current.kind == Kind::File {
                let claim = self
                    .claim(current.id.0)?
                    .context("cloud query file claim missing")?;
                ensure!(
                    claim.root == authority.key(),
                    "cloud query file witness changed source"
                );
                ObjectKey {
                    kind: Kind::Tree,
                    id: ObjectId(unhex(&claim.tree)?),
                }
            } else if let Some(parent) = parents.get(&current) {
                *parent
            } else {
                self.reference(&authority.key(), current)?
                    .and_then(|r| r.parent)
                    .context("cloud query object has no canonical parent witness")?
                    .descriptor()?
                    .key
            };
            reverse.push(current);
            current = parent;
        }
    }
}
