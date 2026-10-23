//! Authenticated union-membership witnesses are separate from canonical edges.
use super::*;
use std::collections::{HashMap, HashSet};

#[derive(Clone)]
pub(crate) enum Authority {
    Root(Root),
    Query(QueryAuthority),
}
impl Authority {
    pub fn key(&self) -> String {
        match self {
            Self::Root(root) => root.head.clone(),
            Self::Query(query) => format!("q/{}", query.id),
        }
    }
    pub fn root(&self) -> Option<&Root> {
        match self {
            Self::Root(root) => Some(root),
            Self::Query(_) => None,
        }
    }
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Anchor {
    head: String,
    hold_name: String,
    generation: u64,
    binding: String,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct QueryAuthority {
    version: u8,
    id: String,
    binding: String,
    anchors: Vec<Anchor>,
}
#[derive(Serialize, Deserialize)]
struct Grant {
    descriptor: StoredDescriptor,
    change_id: [u8; 16],
    scope_digest: [u8; 32],
    index_epoch: [u8; 16],
}
impl QueryAuthority {
    pub fn key(&self) -> String {
        format!("q/{}", self.id)
    }
    pub fn roots(&self) -> Result<Vec<CommitId>> {
        self.anchors
            .iter()
            .map(|anchor| Ok(CommitId(unhex(&anchor.head)?)))
            .collect()
    }
}
impl Ledger {
    /// Capture only already-confirmed holds. A query pin is not a durable hold.
    pub fn query_authority(&self, roots: &[CommitId]) -> Result<QueryAuthority> {
        ensure!(
            roots.len() <= self.config.max_admitted_roots,
            "query anchor count exceeded"
        );
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        let mut anchors = Vec::new();
        let mut heads = roots
            .iter()
            .copied()
            .filter(|id| *id != CommitId::ROOT)
            .collect::<Vec<_>>();
        heads.sort_unstable();
        heads.dedup();
        for head in heads {
            let root = self
                .root(head)?
                .context("query root has no durable admission")?;
            ensure!(
                root.retained() && root.binding == self.binding.scope,
                "query root retention binding is not current"
            );
            anchors.push(Anchor {
                head: root.head,
                hold_name: root.hold_name,
                generation: root.generation.context("query root generation missing")?,
                binding: root.binding,
            });
        }
        let binding = self.binding.scope.clone();
        let identity = serde_json::to_vec(&(1u8, &binding, &anchors))?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-query-union-authority-v1\0");
        hash.update(&identity);
        Ok(QueryAuthority {
            version: 1,
            id: hash.finalize().to_hex().to_string(),
            binding,
            anchors,
        })
    }
    fn valid_query_authority(&self, query: &QueryAuthority) -> Result<bool> {
        if query.version != 1
            || query.binding != self.binding.scope
            || query.anchors.len() > self.config.max_admitted_roots
        {
            return Ok(false);
        }
        let mut previous: Option<&str> = None;
        for anchor in &query.anchors {
            if previous.is_some_and(|last| last >= anchor.head.as_str()) {
                return Ok(false);
            }
            previous = Some(&anchor.head);
            let Some(root) = self.root(CommitId(unhex(&anchor.head)?))? else {
                return Ok(false);
            };
            if !root.retained()
                || root.hold_name != anchor.hold_name
                || root.generation != Some(anchor.generation)
                || root.binding != anchor.binding
                || root.binding != query.binding
            {
                return Ok(false);
            }
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-query-union-authority-v1\0");
        hash.update(&serde_json::to_vec(&(
            query.version,
            &query.binding,
            &query.anchors,
        ))?);
        Ok(query.id == hash.finalize().to_hex().as_str())
    }
    pub fn validate_authority(&self, authority: &Authority) -> Result<()> {
        match authority {
            Authority::Root(root) => ensure!(
                self.root(CommitId(unhex(&root.head)?))?
                    .is_some_and(|root| root.retained()),
                "native metadata root is not pinned"
            ),
            Authority::Query(query) => ensure!(
                self.valid_query_authority(query)?,
                "native query anchors are no longer retained"
            ),
        }
        Ok(())
    }
    pub fn authority(&self, key: &str) -> Result<Option<Authority>> {
        if let Some(id) = key.strip_prefix("q/") {
            let _: [u8; 32] = unhex(id)?;
            let Some(query) = self.get::<QueryAuthority>(&self.query_authorities, id.as_bytes())?
            else {
                return Ok(None);
            };
            ensure!(query.id == id, "query authority index is corrupt");
            if self.valid_query_authority(&query)? {
                Ok(Some(Authority::Query(query)))
            } else {
                Ok(None)
            }
        } else {
            Ok(self
                .root(CommitId(unhex(key)?))?
                .filter(Root::retained)
                .map(Authority::Root))
        }
    }
    pub fn retained_authorities(&self) -> Result<Vec<Authority>> {
        let mut authorities = self
            .roots()?
            .into_iter()
            .filter(Root::retained)
            .map(Authority::Root)
            .collect::<Vec<_>>();
        for query in
            self.list::<QueryAuthority>(&self.query_authorities, self.config.query.max_authorities)?
        {
            if self.valid_query_authority(&query)? {
                authorities.push(Authority::Query(query));
            }
        }
        Ok(authorities)
    }
    pub fn metadata_authority(&self, key: ObjectKey) -> Result<Option<Authority>> {
        if let Some(root) = self.metadata_root(key)? {
            return Ok(Some(Authority::Root(root)));
        }
        for query in
            self.list::<QueryAuthority>(&self.query_authorities, self.config.query.max_authorities)?
        {
            if !self.valid_query_authority(&query)? {
                continue;
            }
            let authority = query.key();
            if self.reference(&authority, key)?.is_some()
                || self
                    .get::<Grant>(&self.query_grants, member_key(&authority, key))?
                    .is_some()
            {
                return Ok(Some(Authority::Query(query)));
            }
        }
        Ok(None)
    }
    /// A terminal is an explicit authenticated membership witness, never a fake parent.
    pub fn query_terminal(&self, authority: &Authority, key: ObjectKey) -> Result<bool> {
        let Authority::Query(query) = authority else {
            return Ok(false);
        };
        self.validate_authority(authority)?;
        let Some(grant) = self.get::<Grant>(&self.query_grants, member_key(&query.key(), key))?
        else {
            return Ok(false);
        };
        ensure!(
            grant.descriptor.descriptor()?.key == key && key.kind == Kind::Commit,
            "query membership witness is corrupt"
        );
        let Some(member) =
            self.get::<StoredDescriptor>(&self.members, member_key(&query.key(), key))?
        else {
            return Ok(false);
        };
        ensure!(
            member.descriptor()? == grant.descriptor.descriptor()?,
            "query witness membership descriptor changed"
        );
        Ok(true)
    }
    pub fn check_verified_authority_batch(
        &self,
        authority: &Authority,
        nodes: &[(Descriptor, tinytree_model::Object)],
    ) -> Result<()> {
        match authority {
            Authority::Root(root) => self.check_verified_batch(CommitId(unhex(&root.head)?), nodes),
            Authority::Query(query) => {
                let _gate = self
                    .gate
                    .lock()
                    .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
                let changes = self.query_changes(query, nodes, None)?;
                self.check_changes(&changes)
            }
        }
    }
    pub fn verified_authority(
        &self,
        authority: &Authority,
        descriptor: Descriptor,
        object: &tinytree_model::Object,
    ) -> Result<()> {
        match authority {
            Authority::Root(root) => {
                self.verified(CommitId(unhex(&root.head)?), descriptor, object)
            }
            Authority::Query(query) => {
                let _gate = self
                    .gate
                    .lock()
                    .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
                self.write_locked(self.query_changes(
                    query,
                    &[(descriptor, object.clone())],
                    None,
                )?)
            }
        }
    }
    pub fn check_query_page(
        &self,
        authority: &QueryAuthority,
        nodes: &[(Descriptor, tinytree_model::Object)],
        scope: [u8; 32],
        epoch: [u8; 16],
    ) -> Result<()> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        let changes = self.query_changes(authority, nodes, Some((scope, epoch)))?;
        self.check_changes(&changes)
    }
    pub fn admit_query_page(
        &self,
        authority: &QueryAuthority,
        nodes: &[(Descriptor, tinytree_model::Object)],
        scope: [u8; 32],
        epoch: [u8; 16],
    ) -> Result<()> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.write_locked(self.query_changes(authority, nodes, Some((scope, epoch)))?)
    }
    fn check_changes(
        &self,
        changes: &[(&fjall::SingleWriterTxKeyspace, Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<()> {
        let tx = self.db.read_tx();
        let mut total: u64 = tx
            .get(&self.meta, b"bytes")?
            .map(|v| serde_json::from_slice(&v))
            .transpose()?
            .context("native ledger byte counter missing")?;
        for (space, key, value) in changes {
            if let Some(old) = tx.get(space, key)? {
                total = total
                    .checked_sub((key.len() + old.len()) as u64)
                    .context("native ledger byte counter corrupt")?;
            }
            if let Some(value) = value {
                total = total
                    .checked_add((key.len() + value.len()) as u64)
                    .context("native ledger byte count overflow")?;
            }
            ensure!(
                total <= self.config.max_ledger_bytes as u64,
                "native admission ledger byte budget exceeded"
            );
        }
        Ok(())
    }
    /// Simulate only real canonical references reachable from existing grants.
    /// This supports top-first CopyHistory dependency installation without
    /// inventing references or requiring every child to be published already.
    fn query_batch_authorized(
        &self,
        query: &QueryAuthority,
        nodes: &[(Descriptor, tinytree_model::Object)],
    ) -> Result<HashSet<ObjectKey>> {
        let name = query.key();
        let mut positions = HashMap::new();
        let mut permitted = HashSet::new();
        let mut pending = Vec::new();
        let mut edges = 0usize;
        for (index, (descriptor, object)) in nodes.iter().enumerate() {
            ensure!(
                positions.insert(descriptor.key, index).is_none(),
                "duplicate native metadata batch object"
            );
            let mut seed = self.reference(&name, descriptor.key)?.is_some();
            if let Some(grant) =
                self.get::<Grant>(&self.query_grants, member_key(&name, descriptor.key))?
            {
                let tinytree_model::Object::Commit(commit) = object else {
                    anyhow::bail!("query membership witness has non-commit content")
                };
                ensure!(
                    grant.descriptor.descriptor()? == *descriptor
                        && grant.change_id == commit.change_id.0
                        && self.member(&name, descriptor.key)?,
                    "query membership witness changed identity"
                );
                seed = true;
            }
            if seed {
                permitted.insert(descriptor.key);
                pending.push(index);
            }
        }
        while let Some(index) = pending.pop() {
            let references = nodes[index].1.references();
            edges = edges
                .checked_add(references.len())
                .context("native batch reference overflow")?;
            ensure!(
                edges <= self.config.closure.max_edges,
                "native batch reference limit exceeded"
            );
            for key in references {
                if let Some(index) = positions.get(&key) {
                    if permitted.insert(key) {
                        pending.push(*index);
                    }
                }
            }
        }
        Ok(permitted)
    }
    fn query_changes<'a>(
        &'a self,
        query: &QueryAuthority,
        nodes: &[(Descriptor, tinytree_model::Object)],
        provenance: Option<([u8; 32], [u8; 16])>,
    ) -> Result<Vec<(&'a fjall::SingleWriterTxKeyspace, Vec<u8>, Option<Vec<u8>>)>> {
        ensure!(
            self.valid_query_authority(query)?,
            "native query anchors are no longer retained"
        );
        ensure!(
            !query.anchors.is_empty() || nodes.is_empty(),
            "intrinsic-only scope cannot grant remote metadata"
        );
        ensure!(
            nodes.len() <= self.config.closure.max_objects,
            "native query page object budget exceeded"
        );
        let name = query.key();
        let existing: Option<QueryAuthority> =
            self.get(&self.query_authorities, query.id.as_bytes())?;
        let mut changes = Vec::new();
        if let Some(existing) = existing {
            ensure!(
                existing == *query,
                "native query authority identity changed"
            );
        } else {
            ensure!(
                provenance.is_some(),
                "native query authority is not installed"
            );
            ensure!(
                self.list::<QueryAuthority>(
                    &self.query_authorities,
                    self.config.query.max_authorities
                )?
                .len()
                    < self.config.query.max_authorities,
                "native query authority count exceeded"
            );
            if !nodes.is_empty() {
                changes.push((
                    &self.query_authorities,
                    query.id.as_bytes().to_vec(),
                    Some(serde_json::to_vec(query)?),
                ));
            }
        }
        let mut progress: Progress = self
            .get(&self.progress, name.as_bytes())?
            .unwrap_or_default();
        let mut grant_count = 0usize;
        for item in self.db.read_tx().iter(&self.query_grants) {
            item.into_inner()?;
            grant_count += 1;
            ensure!(
                grant_count <= self.config.query.max_grants,
                "native query grant count exceeded"
            );
        }
        let mut file_count = 0usize;
        for item in self.db.read_tx().iter(&self.files) {
            item.into_inner()?;
            file_count += 1;
            ensure!(
                file_count <= self.config.max_file_index_entries,
                "native derived file index count exceeded"
            );
        }
        let permitted = if provenance.is_none() {
            self.query_batch_authorized(query, nodes)?
        } else {
            HashSet::new()
        };
        let mut inserted_members = HashSet::new();
        let mut inserted_refs = HashSet::new();
        let mut inserted_files = HashMap::new();
        for (descriptor, object) in nodes {
            let member_key = member_key(&name, descriptor.key);
            let member: Option<StoredDescriptor> = self.get(&self.members, &member_key)?;
            if let Some(previous) = &member {
                ensure!(
                    previous.descriptor()? == *descriptor,
                    "native verified metadata descriptor changed"
                );
            }
            if let Some((scope, epoch)) = provenance {
                let tinytree_model::Object::Commit(commit) = object else {
                    anyhow::bail!("query membership can only grant canonical commits");
                };
                ensure!(
                    descriptor.key.kind == Kind::Commit,
                    "query canonical kind mismatch"
                );
                let previous: Option<Grant> = self.get(&self.query_grants, &member_key)?;
                if let Some(previous) = previous {
                    ensure!(
                        previous.descriptor.descriptor()? == *descriptor
                            && previous.change_id == commit.change_id.0,
                        "native query membership identity changed"
                    );
                } else if inserted_members.insert(descriptor.key) {
                    grant_count += 1;
                    ensure!(
                        grant_count <= self.config.query.max_grants,
                        "native query grant count exceeded"
                    );
                    changes.push((
                        &self.query_grants,
                        member_key.clone(),
                        Some(serde_json::to_vec(&Grant {
                            descriptor: StoredDescriptor::new(*descriptor),
                            change_id: commit.change_id.0,
                            scope_digest: scope,
                            index_epoch: epoch,
                        })?),
                    ));
                }
            } else {
                ensure!(
                    permitted.contains(&descriptor.key),
                    "native metadata has no canonical query reference or exact query witness"
                );
            }
            if member.is_some() {
                continue;
            }
            // Batch input IDs are unique; duplicates must not overwrite progress.
            ensure!(
                nodes
                    .iter()
                    .filter(|(d, _)| d.key == descriptor.key)
                    .count()
                    == 1,
                "duplicate native query page object"
            );
            progress.objects = progress
                .objects
                .checked_add(1)
                .context("native metadata count overflow")?;
            progress.bytes = progress
                .bytes
                .checked_add(descriptor.size_bytes)
                .context("native metadata bytes overflow")?;
            let references = object.references();
            progress.edges = progress
                .edges
                .checked_add(references.len())
                .context("native metadata edges overflow")?;
            let parent = StoredDescriptor::new(*descriptor);
            changes.push((
                &self.members,
                member_key,
                Some(serde_json::to_vec(&parent)?),
            ));
            for key in references.into_iter().filter(|key| key.kind != Kind::File) {
                if self.reference(&name, key)?.is_none() && inserted_refs.insert(key) {
                    progress.known = progress
                        .known
                        .checked_add(1)
                        .context("native known-reference count overflow")?;
                    changes.push((
                        &self.references,
                        super::member_key(&name, key),
                        Some(serde_json::to_vec(&KnownReference {
                            parent: Some(parent.clone()),
                        })?),
                    ));
                }
            }
            if let tinytree_model::Object::Tree(tree) = object {
                for entry in &tree.entries {
                    if let tinytree_model::TreeValue::File { id, size_bytes, .. } = entry.value {
                        if let Some(previous) = inserted_files.insert(id, size_bytes) {
                            ensure!(
                                previous == size_bytes,
                                "conflicting admitted native file lengths"
                            );
                            continue;
                        }
                        if let Some(existing) = self.claim(id.0)? {
                            ensure!(
                                existing.size == size_bytes,
                                "conflicting admitted native file lengths"
                            );
                            // Preserve an independently valid witness instead of
                            // replacing it with this potentially narrower union.
                            if self.authority(&existing.root)?.is_some() {
                                continue;
                            }
                        } else {
                            file_count += 1;
                        }
                        ensure!(
                            file_count <= self.config.max_file_index_entries,
                            "native derived file index count exceeded"
                        );
                        changes.push((
                            &self.files,
                            hex(&id.0).into_bytes(),
                            Some(serde_json::to_vec(&FileClaim {
                                size: size_bytes,
                                root: name.clone(),
                                tree: hex(&descriptor.key.id.0),
                            })?),
                        ));
                    }
                }
            }
        }
        ensure!(
            progress.objects <= self.config.closure.max_objects
                && progress.bytes <= self.config.closure.max_metadata_bytes as u64
                && progress.edges <= self.config.closure.max_edges
                && progress.known <= self.config.closure.max_edges,
            "native query metadata budget exceeded"
        );
        if !nodes.is_empty() {
            changes.push((
                &self.progress,
                name.as_bytes().to_vec(),
                Some(serde_json::to_vec(&progress)?),
            ));
        }
        Ok(changes)
    }
}
