//! Fresh complete-retention cutoffs, never local-residency or caller assertions.
use crate::{NativeTransferLimits, RemoteRepositoryControl, io, ledger::Root};
use anyhow::{Context, Result, ensure};
use jj_native_backend::NativeObjectIo;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tinytree_client::{NativeClient, VerifiedRetainedVersion};
use tinytree_model as m;
use tokio::time::Instant;

type Owner = Arc<dyn Any + Send + Sync>;
/// Construction is private to this provider. A caller cannot promote a root,
/// descriptor hit, local-complete bit, or query membership into a cutoff.
pub(crate) struct CaptureBoundaries {
    keys: HashSet<m::ObjectKey>,
    resident: usize,
    edges: usize,
    objects: usize,
    _holds: Vec<HoldProof>,
    _cloud: Vec<Arc<VerifiedRetainedVersion>>,
    _client: NativeClient,
    _owner: Owner,
}
struct HoldProof {
    _name: String,
    _generation: u64,
    _commit: jj_native_backend::VerifiedMetadata,
}
impl CaptureBoundaries {
    pub(crate) fn covers(&self, key: &m::ObjectKey) -> bool {
        self.keys.contains(key)
    }
    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident
    }
    pub(crate) fn count(&self) -> usize {
        self.objects
    }
    pub(crate) fn edge_count(&self) -> usize {
        self.edges
    }
}
struct Budget {
    used: usize,
    maximum: usize,
    count: usize,
}
impl Budget {
    fn charge(&mut self, n: usize) -> Result<()> {
        self.used = self
            .used
            .checked_add(n)
            .filter(|n| *n <= self.maximum)
            .context("capture boundary byte budget exceeded")?;
        Ok(())
    }
    fn remaining(&self) -> usize {
        self.maximum - self.used
    }
    fn key(&mut self, keys: &mut HashSet<m::ObjectKey>, key: m::ObjectKey) -> Result<()> {
        if key.kind == m::Kind::Commit && key.id.0 == [0; 32] {
            return Ok(());
        }
        if !keys.contains(&key) {
            ensure!(
                keys.len() < self.count,
                "capture boundary count exceeds budget"
            );
            // Hash-table growth and both old/new allocations during resize.
            self.charge(4 * std::mem::size_of::<m::ObjectKey>() + 64)?;
            keys.insert(key);
        }
        Ok(())
    }
}
fn proof_cost(bytes: usize) -> Result<usize> {
    bytes
        .checked_mul(16)
        .and_then(|n| n.checked_add(4096))
        .context("capture boundary proof budget overflow")
}
impl RemoteRepositoryControl {
    /// Creation readiness uses only an already-owned complete hold. It neither
    /// installs a hold nor promotes query scope into retained capture authority.
    pub(crate) async fn capture_anchor_ready(
        &self,
        root: m::CommitId,
        owner: Owner,
        originaldeadline: Instant,
    ) -> Result<()> {
        ensure!(
            root != m::CommitId::ROOT,
            "intrinsic root needs no capture anchor"
        );
        let deadline = io::bounded_deadline(originaldeadline);
        let ledger = self.inner.ledger.clone();
        let maximum = self.inner.config.closure.max_metadata_bytes;
        let existing =
            io::blocking_owned(owner.clone(), move || ledger.capture_root(root, maximum))
                .await?
                .context("capture requires an existing retained anchor")?;
        let expected = self.existing_capture_root(&existing)?;
        ensure!(
            expected.id.0 == root.0,
            "capture anchor immutable identity differs"
        );
        let client = self.client_for_owned_work(owner, deadline).await?;
        self.fresh_capture_hold(&client, &existing, expected)
            .await?;
        ensure!(
            Instant::now() < deadline,
            "capture anchor readiness deadline exceeded"
        );
        Ok(())
    }
    fn existing_capture_root(&self, root: &Root) -> Result<m::ObjectKey> {
        ensure!(
            root.retained()
                && (root.ready || root.complete)
                && root.binding == self.inner.ledger.binding.scope,
            "capture hold binding/readiness is not current"
        );
        let head = m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(crate::unhex(&root.head)?),
        };
        ensure!(
            head.id.0 != [0; 32]
                && root.hold_name
                    == format!(
                        "_tinytree/hold/v1/{}/{}",
                        crate::hex(&self.inner.ledger.client),
                        root.head
                    ),
            "capture hold is not owned by this provider"
        );
        Ok(head)
    }
    async fn fresh_capture_hold(
        &self,
        client: &NativeClient,
        root: &Root,
        head: m::ObjectKey,
    ) -> Result<(u64, u64)> {
        let view = io::network(client.open(vec![root.hold_name.clone()])).await?;
        ensure!(
            view.repository_id == self.inner.config.remote.repository_id
                && view.references.len() == 1,
            "capture hold repository/reference identity differs"
        );
        let reference = &view.references[0];
        let descriptor = reference
            .commit
            .as_ref()
            .context("capture hold was revoked")?;
        let key = descriptor
            .key
            .as_ref()
            .context("capture hold target absent")?;
        ensure!(
            reference.name == root.hold_name
                && reference.generation > 0
                && Some(reference.generation) == root.generation
                && key.kind == m::Kind::Commit as i32
                && key.native_id.as_slice() == head.id.0,
            "capture hold target or generation differs"
        );
        Ok((reference.generation, descriptor.size_bytes))
    }
    /// Read-only proof refresh under the caller's independently owned background
    /// admission. No foreground lease, new hold, query grant, ledger mutation or
    /// public-history hydration is performed. The original deadline is shared
    /// with every ledger/SDK/decode worker and remains the caller's work bound.
    pub(crate) async fn capture_boundaries(
        &self,
        roots: &[m::ObjectKey],
        owner: Owner,
        limits: &NativeTransferLimits,
        originaldeadline: Instant,
    ) -> Result<CaptureBoundaries> {
        ensure!(
            !roots.is_empty() && roots.len() <= limits.max_roots,
            "capture boundary root count exceeds budget"
        );
        let scope = io::scope().context("capture boundaries require owned background admission")?;
        ensure!(
            !scope.has_control(),
            "capture proof refresh must not retain foreground control"
        );
        let deadline = io::bounded_deadline(originaldeadline).min(scope.deadline);
        let mut budget = Budget {
            used: std::mem::size_of::<CaptureBoundaries>(),
            maximum: limits
                .closure
                .max_metadata_bytes
                .min(usize::try_from(limits.max_memory_bytes).unwrap_or(usize::MAX)),
            count: limits.closure.max_objects.min(limits.closure.max_edges),
        };
        ensure!(
            budget.used <= budget.maximum,
            "capture boundary byte budget exceeded"
        );
        let ledger = self.inner.ledger.clone();
        let max_count = limits.closure.max_objects;
        let max_bytes = budget.remaining();
        let (records, charged) = io::blocking_owned(owner.clone(), move || {
            ledger.capture_roots(max_count, max_bytes)
        })
        .await?;
        budget.charge(charged)?;
        let ledger = self.inner.ledger.clone();
        let max_bytes = budget.remaining();
        let (authorities, charged) = io::blocking_owned(owner.clone(), move || {
            ledger.capture_cloud_authorities(max_count, max_bytes)
        })
        .await?;
        budget.charge(charged)?;
        let (selected_holds, selected_cloud, selection_edges, selection_objects) = select_relevant(
            self,
            roots,
            &records,
            &authorities,
            owner.clone(),
            limits,
            &mut budget,
        )
        .await?;
        budget.count = budget.count.checked_sub(selection_objects).ok_or_else(|| {
            tonic::Status::resource_exhausted("capture selection object budget exceeded")
        })?;
        io::check_deadline()?;
        let client = self
            .physical_client()
            .await?
            .with_operation_deadline(deadline)
            .with_rpc_lifetime_guard(Arc::new(owner.clone()));
        let mut keys = HashSet::new();
        let mut holds = Vec::new();
        let mut cloud = Vec::new();
        let mut proof_edges = selection_edges;
        for (index, root) in records.into_iter().enumerate() {
            if !selected_holds.contains(&index) {
                continue;
            }
            io::check_deadline()?;
            // `complete` describes LOCAL metadata only. This predicate merely
            // selects existing records; fresh server hold proof below is mandatory.
            if !root.retained() || !(root.ready || root.complete) {
                continue;
            }
            let head = self.existing_capture_root(&root)?;
            // Reserve every kept witness container/name before the SDK request.
            budget.charge(2 * std::mem::size_of::<HoldProof>() + root.hold_name.len())?;
            let (generation, size) = self.fresh_capture_hold(&client, &root, head).await?;
            let descriptor = self
                .inner
                .local
                .descriptor_with_owner(head, owner.clone())
                .await
                .map_err(crate::backend)?
                .context("capture anchor canonical commit is not locally resident")?;
            ensure!(
                descriptor.key == head && descriptor.size_bytes == size,
                "capture anchor canonical descriptor differs"
            );
            let anticipated = usize::try_from(descriptor.size_bytes)?;
            ensure!(
                anticipated <= self.inner.config.remote.limits.max_metadata_bytes,
                "capture anchor commit exceeds metadata budget"
            );
            budget.charge(proof_cost(anticipated)?)?;
            let commit = self
                .inner
                .local
                .read_metadata_with_owner(head, owner.clone())
                .await
                .map_err(crate::backend)?;
            ensure!(
                commit.descriptor() == descriptor,
                "capture anchor canonical descriptor changed"
            );
            // The direct provider validates bytes/hash before delivery. Decode a
            // separately precharged temporary view without cloning the witness.
            let object = m::decode(commit.bytes(), self.inner.model_limits())?;
            let m::Object::Commit(value) = &object else {
                anyhow::bail!("capture anchor is not a canonical commit")
            };
            let edges = value
                .parents
                .len()
                .checked_add(value.predecessors.len())
                .and_then(|n| n.checked_add(value.root_tree.terms.len()))
                .context("capture anchor edge budget overflow")?;
            proof_edges = proof_edges
                .checked_add(edges)
                .filter(|n| *n <= limits.closure.max_edges)
                .context("capture anchor edge count exceeds budget")?;
            budget.key(&mut keys, head)?;
            // Count and charge before constructing any direct-reference vector.
            for parent in value.parents.iter().chain(&value.predecessors) {
                budget.key(
                    &mut keys,
                    m::ObjectKey {
                        kind: m::Kind::Commit,
                        id: m::ObjectId(parent.0),
                    },
                )?;
            }
            for tree in &value.root_tree.terms {
                budget.key(
                    &mut keys,
                    m::ObjectKey {
                        kind: m::Kind::Tree,
                        id: m::ObjectId(tree.0),
                    },
                )?;
            }
            holds.push(HoldProof {
                _name: root.hold_name,
                _generation: generation,
                _commit: commit,
            });
        }
        for (index, authority) in authorities.into_iter().enumerate() {
            if !selected_cloud.contains(&index) {
                continue;
            }
            io::check_deadline()?;
            let ledger = self.inner.ledger.clone();
            let checked = authority.clone();
            let maximum = budget.maximum;
            ensure!(
                io::blocking_owned(owner.clone(), move || ledger
                    .capture_valid_cloud_authority(&checked, maximum))
                .await?,
                "capture cloud authority binding is not current"
            );
            let selected = authority.selected();
            let bytes = usize::try_from(selected.size_bytes)?;
            ensure!(
                bytes <= self.inner.config.remote.cloud.max_manifest_bytes,
                "capture manifest exceeds provider budget"
            );
            // V2 shape validation charges sixteen wire bytes plus up to
            // 512 bytes per member/index entry (each has at least 32 wire bytes).
            // V3 additionally retains a prefix and encoding copy. Reserve all
            // of these before SDK proof retention or canonical model allocation.
            let cost = bytes
                .checked_mul(64)
                .and_then(|n| n.checked_add(16384))
                .context("capture cloud decode budget overflow")?;
            budget.charge(
                cost.checked_add(2 * std::mem::size_of::<Arc<VerifiedRetainedVersion>>())
                    .context("capture cloud witness budget overflow")?,
            )?;
            let proof = Arc::new(
                io::network(client.cloud_retained_version_selected(authority.selector(), selected))
                    .await?,
            );
            ensure!(
                authority.agrees(&proof),
                "capture cloud selected owner/source binding changed"
            );
            let held = proof.clone();
            let maximum = limits
                .closure
                .max_objects
                .min(self.inner.config.max_admitted_roots);
            let raw_limit = self.inner.config.remote.cloud.max_manifest_bytes;
            let capture_limit = self.inner.config.remote.cloud.max_capture_bytes;
            // Decoders enforce their allocation budget before model allocation;
            // the budget has already been reserved above, separately from SDK pools.
            let native = io::blocking_owned(owner.clone(), move || {
                canonical_roots(&held, maximum, raw_limit, capture_limit, cost)
            })
            .await?;
            proof_edges = proof_edges
                .checked_add(native.len())
                .filter(|n| *n <= limits.closure.max_edges)
                .context("capture cloud edge count exceeds budget")?;
            ensure!(
                native.len() == authority.root_count(),
                "capture cloud canonical root count differs"
            );
            for key in native {
                ensure!(
                    authority.terminal(key)?,
                    "capture cloud canonical roots differ"
                );
                budget.key(&mut keys, key)?;
            }
            cloud.push(proof);
        }
        io::check_deadline()?;
        ensure!(
            Instant::now() < deadline,
            "capture boundary deadline exceeded"
        );
        let objects = keys.len() + selection_objects;
        Ok(CaptureBoundaries {
            keys,
            resident: budget.used,
            edges: proof_edges,
            objects,
            _holds: holds,
            _cloud: cloud,
            _client: client,
            _owner: owner,
        })
    }
}
#[derive(Clone, Copy, Default)]
struct Hint {
    hold: Option<usize>,
    cloud: Option<usize>,
}
fn hint<'a>(
    hints: &'a mut HashMap<m::ObjectKey, Hint>,
    key: m::ObjectKey,
    budget: &mut Budget,
    max: usize,
) -> Result<&'a mut Hint> {
    if !hints.contains_key(&key) {
        ensure!(
            hints.len() < max,
            "capture selection hint count exceeds budget"
        );
        budget.charge(256)?;
        hints.insert(key, Hint::default());
    }
    Ok(hints.get_mut(&key).unwrap())
}
async fn select_relevant(
    control: &RemoteRepositoryControl,
    roots: &[m::ObjectKey],
    records: &[Root],
    authorities: &[crate::ledger::CloudVersionAuthority],
    owner: Owner,
    limits: &NativeTransferLimits,
    budget: &mut Budget,
) -> Result<(HashSet<usize>, HashSet<usize>, usize, usize)> {
    let mut hints = HashMap::<m::ObjectKey, Hint>::new();
    for (index, root) in records.iter().enumerate() {
        if root.retained() && (root.ready || root.complete) {
            let key = m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(crate::unhex(&root.head)?),
            };
            hint(&mut hints, key, budget, limits.closure.max_objects)?.hold = Some(index);
        }
    }
    for (index, authority) in authorities.iter().enumerate() {
        for key in authority.native_root_hints() {
            let entry = hint(&mut hints, key?, budget, limits.closure.max_objects)?;
            let newer = entry.cloud.is_none_or(|old| {
                let old = authorities[old].selector();
                let new = authority.selector();
                (new.generation, new.workspace_id, new.manifest_id.0)
                    > (old.generation, old.workspace_id, old.manifest_id.0)
            });
            if newer {
                entry.cloud = Some(index);
            }
        }
    }
    let mut selected_holds = HashSet::new();
    let mut selected_cloud = HashSet::new();
    // Exact captured cloud terminals win before looking at local ancestry. A
    // restored immutable root must not depend on an older unrelated live hold.
    for key in roots {
        if let Some(index) = hints.get(key).and_then(|h| h.cloud) {
            if !selected_cloud.contains(&index) {
                budget.charge(128)?;
                selected_cloud.insert(index);
            }
        }
    }
    let mut pending = Vec::new();
    let mut active = HashSet::new();
    let mut done = HashSet::new();
    let mut edges = 0usize;
    budget.charge(
        roots
            .len()
            .checked_mul(2 * std::mem::size_of::<(m::ObjectKey, bool)>())
            .context("capture selection roots overflow")?,
    )?;
    pending.extend(roots.iter().copied().map(|key| (key, false)));
    while let Some((key, exit)) = pending.pop() {
        io::check_deadline()?;
        if key.kind == m::Kind::Commit && key.id.0 == [0; 32] {
            continue;
        }
        if exit {
            active.remove(&key);
            done.insert(key);
            continue;
        }
        if done.contains(&key) {
            continue;
        }
        let mut covered_cloud = false;
        for index in &selected_cloud {
            if authorities[*index].terminal(key)? {
                covered_cloud = true;
                break;
            }
        }
        if covered_cloud {
            continue;
        }
        if let Some(candidate) = hints.get(&key) {
            if let Some(index) = candidate
                .cloud
                .filter(|index| selected_cloud.contains(index))
            {
                selected_cloud.insert(index);
                continue;
            }
            if let Some(index) = candidate.hold {
                if !selected_holds.contains(&index) {
                    budget.charge(128)?;
                    selected_holds.insert(index);
                }
                continue;
            }
            if let Some(index) = candidate.cloud {
                if !selected_cloud.contains(&index) {
                    budget.charge(128)?;
                    selected_cloud.insert(index);
                }
                continue;
            }
        }
        // TREE selection never walks checkout contents. Only locally resident
        // COMMITs may bridge authored history to a candidate retained anchor.
        if key.kind != m::Kind::Commit {
            continue;
        }
        ensure!(
            !active.contains(&key)
                && active.len().saturating_add(done.len()) < limits.closure.max_objects,
            "capture selection commit cycle/count exceeds budget"
        );
        let descriptor = control
            .inner
            .local
            .descriptor_with_owner(key, owner.clone())
            .await
            .map_err(crate::backend)?
            .ok_or_else(|| {
                tonic::Status::unimplemented(
                    "capture requires a retained boundary before remote commit ancestry",
                )
            })?;
        budget.charge(
            proof_cost(usize::try_from(descriptor.size_bytes)?)?
                .checked_add(512)
                .context("capture selection byte budget overflow")?,
        )?;
        let value = control
            .inner
            .local
            .read_metadata_with_owner(key, owner.clone())
            .await
            .map_err(crate::backend)?;
        ensure!(
            value.descriptor() == descriptor,
            "capture selection local commit descriptor changed"
        );
        let m::Object::Commit(commit) = value.into_object() else {
            anyhow::bail!("capture selection expected canonical local commit")
        };
        let count = commit
            .parents
            .len()
            .checked_add(commit.predecessors.len())
            .and_then(|n| n.checked_add(commit.root_tree.terms.len()))
            .context("capture selection edge overflow")?;
        edges = edges
            .checked_add(count)
            .filter(|n| *n <= limits.closure.max_edges)
            .context("capture selection edge budget exceeded")?;
        budget.charge(
            count
                .checked_add(1)
                .and_then(|n| n.checked_mul(4 * std::mem::size_of::<(m::ObjectKey, bool)>()))
                .context("capture selection pending budget overflow")?,
        )?;
        active.insert(key);
        pending.push((key, true));
        for parent in commit.parents.iter().chain(&commit.predecessors) {
            pending.push((
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(parent.0),
                },
                false,
            ));
        }
        for tree in &commit.root_tree.terms {
            pending.push((
                m::ObjectKey {
                    kind: m::Kind::Tree,
                    id: m::ObjectId(tree.0),
                },
                false,
            ));
        }
    }
    Ok((selected_holds, selected_cloud, edges, done.len()))
}
fn canonical_roots(
    proof: &VerifiedRetainedVersion,
    max_roots: usize,
    max_bytes: usize,
    max_capture: usize,
    decode: usize,
) -> Result<Vec<m::ObjectKey>> {
    let bytes = proof.manifest();
    ensure!(
        bytes.len() >= 10 && &bytes[..8] == b"TTCLOUD\0",
        "capture manifest is not canonical"
    );
    let mut limits = tinytree_cloud::ManifestV2Limits::default();
    ensure!(
        bytes.len() <= max_bytes,
        "capture manifest byte budget exceeded"
    );
    limits.recovery.max_manifest_bytes = bytes.len();
    limits.recovery.max_decode_bytes = decode;
    limits.recovery.max_native_roots = max_roots;
    let id = tinytree_cloud::ManifestId(proof.selector().manifest_id.0);
    Ok(match u16::from_le_bytes(bytes[8..10].try_into().unwrap()) {
        1 => {
            let value = tinytree_cloud::decode_manifest(bytes, id, limits.recovery)?;
            ensure!(
                tinytree_cloud::encode_manifest(&value, limits.recovery)? == bytes,
                "capture manifest is not canonical"
            );
            value.native_roots
        }
        2 => {
            let value = tinytree_cloud::decode_manifest_v2(bytes, id, limits)?;
            ensure!(
                tinytree_cloud::encode_manifest_v2(&value, limits)? == bytes,
                "capture manifest is not canonical"
            );
            value.recovery.native_roots
        }
        3 => {
            let mut v3 = tinytree_cloud::ManifestV3Limits {
                history: limits,
                ..Default::default()
            };
            v3.capture.max_bytes = max_capture;
            let value = tinytree_cloud::decode_manifest_v3(bytes, id, v3)?;
            ensure!(
                tinytree_cloud::encode_manifest_v3(&value, v3)? == bytes,
                "capture manifest is not canonical"
            );
            value.history.recovery.native_roots
        }
        _ => anyhow::bail!("unsupported capture manifest version"),
    })
}
