//! Durable retained-version witnesses, separate from root holds and query grants.
use super::*;
use std::collections::HashSet;
use tinytree_model::FileId;
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct CloudKey {
    kind: u8,
    id: String,
}
impl CloudKey {
    fn new(key: ObjectKey) -> Self {
        Self {
            kind: key.kind as u8,
            id: hex(&key.id.0),
        }
    }
    fn key(&self) -> Result<ObjectKey> {
        Ok(ObjectKey {
            kind: Kind::try_from(self.kind)?,
            id: ObjectId(unhex(&self.id)?),
        })
    }
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct CloudVersionAuthority {
    version: u8,
    id: String,
    pub(super) binding: String,
    workspace: [u8; 32],
    generation: u64,
    manifest: [u8; 32],
    manifest_raw: [u8; 32],
    manifest_size: u64,
    owner_binding: [u8; 32],
    source_binding: [u8; 32],
    roots: Vec<CloudKey>,
}
impl CloudVersionAuthority {
    /// Saved keys are selection hints only until a fresh selected proof agrees.
    pub fn native_root_hints(&self) -> impl Iterator<Item = Result<ObjectKey>> + '_ {
        self.roots.iter().map(|root| root.key())
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn root_count(&self) -> usize {
        self.roots.len()
    }
    pub fn key(&self) -> String {
        format!("v/{}", self.id)
    }
    pub fn selector(&self) -> tinytree_client::RetainedVersionSelector {
        tinytree_client::RetainedVersionSelector {
            workspace_id: self.workspace,
            generation: self.generation,
            manifest_id: tinytree_client::CloudManifestId(self.manifest),
        }
    }
    pub fn selected(&self) -> tinytree_client::CloudRawDigest {
        tinytree_client::CloudRawDigest {
            blake3: self.manifest_raw,
            size_bytes: self.manifest_size,
        }
    }
    pub fn agrees(&self, proof: &tinytree_client::VerifiedRetainedVersion) -> bool {
        self.selector() == proof.selector()
            && self.selected() == proof.manifest_selected()
            && self.owner_binding == proof.owner_binding()
            && self.source_binding == proof.binding_digest()
    }
    pub fn terminal(&self, key: ObjectKey) -> Result<bool> {
        Ok(self.roots.binary_search(&CloudKey::new(key)).is_ok())
    }
    pub(super) fn identity(&self) -> Result<String> {
        let mut h = blake3::Hasher::new();
        h.update(b"native-cloud-version-authority-v1\0");
        h.update(&serde_json::to_vec(&(
            self.version,
            &self.binding,
            self.workspace,
            self.generation,
            self.manifest,
            self.manifest_raw,
            self.manifest_size,
            self.owner_binding,
            self.source_binding,
            &self.roots,
        ))?);
        Ok(h.finalize().to_hex().to_string())
    }
}
impl Ledger {
    pub fn install_cloud_authority(
        &self,
        proof: &tinytree_client::VerifiedRetainedVersion,
        roots: &[ObjectKey],
    ) -> Result<CloudVersionAuthority> {
        ensure!(
            roots.len() <= self.config.max_admitted_roots
                && roots.len() <= self.config.closure.max_edges,
            "cloud native root count exceeded"
        );
        ensure!(
            roots
                .windows(2)
                .all(|p| { (p[0].kind as u8, p[0].id.0) < (p[1].kind as u8, p[1].id.0) }),
            "cloud native roots are not canonical"
        );
        self.install_cloud_record(self.make_cloud_authority(proof, roots)?)
    }
    pub(super) fn make_cloud_authority(
        &self,
        proof: &tinytree_client::VerifiedRetainedVersion,
        roots: &[ObjectKey],
    ) -> Result<CloudVersionAuthority> {
        ensure!(
            roots.len() <= self.config.max_admitted_roots
                && roots.len() <= self.config.closure.max_edges,
            "cloud native root count exceeded"
        );
        ensure!(
            roots
                .windows(2)
                .all(|p| (p[0].kind as u8, p[0].id.0) < (p[1].kind as u8, p[1].id.0)),
            "cloud native roots are not canonical"
        );
        let selector = proof.selector();
        let selected = proof.manifest_selected();
        let mut authority = CloudVersionAuthority {
            version: 1,
            id: String::new(),
            binding: self.binding.scope.clone(),
            workspace: selector.workspace_id,
            generation: selector.generation,
            manifest: selector.manifest_id.0,
            manifest_raw: selected.blake3,
            manifest_size: selected.size_bytes,
            owner_binding: proof.owner_binding(),
            source_binding: proof.binding_digest(),
            roots: roots.iter().copied().map(CloudKey::new).collect(),
        };
        authority.id = authority.identity()?;
        Ok(authority)
    }
    fn install_cloud_record(
        &self,
        authority: CloudVersionAuthority,
    ) -> Result<CloudVersionAuthority> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        if let Some(pin) =
            self.get::<super::offload::SourcePin>(&self.source_pins, authority.id.as_bytes())?
        {
            ensure!(
                pin.authority == super::offload::SourcePinAuthority::File(authority.clone())
                    && pin.phase != super::offload::SourcePinPhase::ReleasePending,
                "retained source release must finish before generic readmission"
            );
        }
        if let Some(existing) = self.cloud_authority(&authority.id)? {
            ensure!(
                existing == authority,
                "cloud version admission identity changed"
            );
            return Ok(authority);
        }
        ensure!(
            self.authority_count()? < self.config.query.max_authorities,
            "native authority count exceeded"
        );
        self.write_locked(vec![(
            &self.cloud_authorities,
            authority.id.as_bytes().to_vec(),
            Some(serde_json::to_vec(&authority)?),
        )])?;
        Ok(authority)
    }
    #[cfg(test)]
    pub fn install_catalog_test_authority(
        &self,
        terminal: ObjectKey,
    ) -> Result<CloudVersionAuthority> {
        // The race fixture bypasses only the already RPC-tested proof capture;
        // insertion still uses the production identity/quota/SyncAll path.
        let mut authority = CloudVersionAuthority {
            version: 1,
            id: String::new(),
            binding: self.binding.scope.clone(),
            workspace: [2; 32],
            generation: 1,
            manifest: [3; 32],
            manifest_raw: [4; 32],
            manifest_size: 1,
            owner_binding: [5; 32],
            source_binding: tinytree_model::cloud_native_binding_digest(
                &self.config.remote.repository_id,
                [5; 32],
                [2; 32],
                1,
                [3; 32],
                [4; 32],
                1,
            ),
            roots: vec![CloudKey::new(terminal)],
        };
        authority.id = authority.identity()?;
        self.install_cloud_record(authority)
    }
    pub fn cloud_authority(&self, id: &str) -> Result<Option<CloudVersionAuthority>> {
        let _: [u8; 32] = unhex(id)?;
        let value = self.get::<CloudVersionAuthority>(&self.cloud_authorities, id.as_bytes())?;
        if let Some(v) = &value {
            ensure!(
                v.id == id && v.identity()? == id,
                "cloud version admission corrupt"
            );
        }
        Ok(value)
    }
    pub fn cloud_authorities(&self) -> Result<Vec<CloudVersionAuthority>> {
        self.list(&self.cloud_authorities, self.config.query.max_authorities)
    }
    pub fn capture_cloud_authorities(
        &self,
        max_count: usize,
        max_bytes: usize,
    ) -> Result<(Vec<CloudVersionAuthority>, usize)> {
        self.list_capture_bounded(
            &self.cloud_authorities,
            max_count.min(self.config.query.max_authorities),
            max_bytes,
        )
    }
    /// Recheck the exact bounded snapshot without decoding a concurrently
    /// replaced ledger value outside the capture reservation.
    pub fn capture_valid_cloud_authority(
        &self,
        authority: &CloudVersionAuthority,
        max_bytes: usize,
    ) -> Result<bool> {
        self.check()?;
        ensure!(
            authority.version == 1
                && authority.id == authority.identity()?
                && authority.generation > 0
                && authority.roots.len() <= self.config.max_admitted_roots
                && authority.roots.windows(2).all(|p| p[0] < p[1]),
            "cloud version admission corrupt"
        );
        for root in &authority.roots {
            root.key()?;
        }
        let Some(raw) = self
            .db
            .read_tx()
            .get(&self.cloud_authorities, authority.id.as_bytes())?
        else {
            return Ok(false);
        };
        ensure!(
            raw.len() <= max_bytes,
            "capture cloud authority exceeds byte budget"
        );
        Ok(authority.binding == self.binding.scope
            && raw.as_ref() == serde_json::to_vec(authority)?.as_slice())
    }
    pub fn valid_cloud_authority(&self, authority: &CloudVersionAuthority) -> Result<bool> {
        ensure!(
            authority.version == 1
                && authority.id == authority.identity()?
                && authority.generation > 0
                && authority.roots.len() <= self.config.max_admitted_roots
                && authority.roots.windows(2).all(|p| p[0] < p[1]),
            "cloud version admission corrupt"
        );
        for root in &authority.roots {
            root.key()?;
        }
        Ok(authority.binding == self.binding.scope
            && self.cloud_authority(&authority.id)?.as_ref() == Some(authority))
    }
    /// Returns an explicit terminal and REAL canonical child path. No fabricated
    /// parent=None reference or query-membership witness is installed.
    pub fn cloud_path(
        &self,
        authority: &CloudVersionAuthority,
        requested: ObjectKey,
    ) -> Result<Option<(ObjectKey, Vec<ObjectKey>)>> {
        ensure!(
            self.valid_cloud_authority(authority)?,
            "cloud version binding is not current"
        );
        let name = authority.key();
        let mut current = requested;
        let mut reversed = Vec::new();
        let mut seen = HashSet::new();
        loop {
            ensure!(
                seen.insert(current) && seen.len() <= 256,
                "cloud native provenance path exceeds bounds"
            );
            if authority.terminal(current)? {
                reversed.reverse();
                return Ok(Some((current, reversed)));
            }
            let parent = if current.kind == Kind::File {
                let Some(claim) = self.claim(current.id.0)? else {
                    return Ok(None);
                };
                if claim.root != name {
                    return Ok(None);
                }
                let key = ObjectKey {
                    kind: Kind::Tree,
                    id: ObjectId(unhex(&claim.tree)?),
                };
                self.get::<StoredDescriptor>(&self.members, member_key(&name, key))?
            } else {
                self.reference(&name, current)?.and_then(|r| r.parent)
            };
            let Some(parent) = parent else {
                return Ok(None);
            };
            let parent = parent.descriptor()?;
            ensure!(
                self.member(&name, parent.key)?,
                "cloud native parent is not verified"
            );
            reversed.push(current);
            current = parent.key;
        }
    }
    pub fn verified_cloud_file_terminal(
        &self,
        authority: &CloudVersionAuthority,
        file: &tinytree_client::VerifiedFile,
    ) -> Result<()> {
        let descriptor = file.descriptor();
        ensure!(
            descriptor.key.kind == Kind::File
                && descriptor.format_version == tinytree_model::FORMAT_VERSION
                && authority.terminal(descriptor.key)?,
            "verified file is not an exact cloud manifest terminal"
        );
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        ensure!(
            self.valid_cloud_authority(authority)?,
            "cloud file binding is not current"
        );
        let name = authority.key();
        let key = member_key(&name, descriptor.key);
        if let Some(previous) = self.get::<StoredDescriptor>(&self.members, &key)? {
            ensure!(
                previous.descriptor()? == descriptor,
                "cloud terminal file descriptor changed"
            );
            return Ok(());
        }
        let maximum = authority
            .roots
            .iter()
            .filter(|key| key.kind == Kind::File as u8)
            .count();
        let mut count = 0usize;
        for row in self
            .db
            .read_tx()
            .prefix(&self.members, format!("{name}/").as_bytes())
        {
            let (_, bytes) = row.into_inner()?;
            let stored: StoredDescriptor = serde_json::from_slice(&bytes)?;
            if stored.descriptor()?.key.kind == Kind::File {
                count += 1;
            }
            ensure!(count < maximum, "cloud terminal file count exceeded");
        }
        let mut progress: Progress = self
            .get(&self.progress, name.as_bytes())?
            .unwrap_or_default();
        progress.objects = progress
            .objects
            .checked_add(1)
            .context("cloud terminal count overflow")?;
        ensure!(
            progress.objects <= self.config.closure.max_objects,
            "cloud terminal object count exceeded"
        );
        // Only a descriptor is stored. FILE payload bytes use CAS/disk quotas,
        // not the metadata-byte progress counter or an invented TREE claim.
        self.write_locked(vec![
            (
                &self.members,
                key,
                Some(serde_json::to_vec(&StoredDescriptor::new(descriptor))?),
            ),
            (
                &self.progress,
                name.into_bytes(),
                Some(serde_json::to_vec(&progress)?),
            ),
        ])
    }
    pub fn terminal_cloud_file_descriptor(&self, id: FileId) -> Result<Option<Descriptor>> {
        let key = ObjectKey {
            kind: Kind::File,
            id: ObjectId(id.0),
        };
        let mut stale = false;
        let mut current_terminal = false;
        for authority in self.cloud_authorities()? {
            if !authority.terminal(key)? {
                continue;
            }
            let current = self.valid_cloud_authority(&authority)?;
            current_terminal |= current;
            let Some(stored) =
                self.get::<StoredDescriptor>(&self.members, member_key(&authority.key(), key))?
            else {
                continue;
            };
            let descriptor = stored.descriptor()?;
            ensure!(
                descriptor.key == key,
                "cloud terminal file descriptor corrupt"
            );
            if current {
                return Ok(Some(descriptor));
            }
            stale = true;
        }
        // Fresh admission can authorize a network refill, but must not borrow
        // an old binding's descriptor to serve resident bytes offline.
        if stale && !current_terminal {
            let independently_current = match self.claim(id.0)? {
                Some(claim) if !claim.root.starts_with("v/") => {
                    self.authority(&claim.root)?.is_some()
                }
                _ => false,
            };
            ensure!(
                independently_current,
                "cloud terminal file binding is not current"
            );
        }
        Ok(None)
    }
    pub fn cloud_file_authority(&self, id: FileId) -> Result<Option<CloudVersionAuthority>> {
        let key = ObjectKey {
            kind: Kind::File,
            id: ObjectId(id.0),
        };
        // A newly authenticated exact terminal takes precedence over any old
        // TREE-derived claim; its missing descriptor requires a fresh transfer.
        for authority in self.cloud_authorities()? {
            if self.valid_cloud_authority(&authority)? && authority.terminal(key)? {
                return Ok(Some(authority));
            }
        }
        if let Some(claim) = self.claim(id.0)? {
            if claim.root.starts_with("v/") {
                let Some(Authority::CloudVersion(version)) = self.authority(&claim.root)? else {
                    anyhow::bail!("cloud file binding is not current");
                };
                return Ok(Some(version));
            }
            // Preserve independently admitted root/query authority; this is not
            // a generic-read fallback from a retained-version-only claim.
            return Ok(None);
        }
        Ok(None)
    }
}
