//! Durable retained-source cleanup pins and bounded provenance-preserving reuse.
use super::*;
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Weak},
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedOffloadWitness {
    pub capture: [u8; 32],
    pub authority: [u8; 32],
    pub pin: [u8; 32],
    pub binding: [u8; 32],
    pub roots: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct OffloadPin {
    version: u8,
    witness: RetainedOffloadWitness,
}
pub(crate) struct OffloadPinLease {
    ledger: Arc<Ledger>,
    pin: OffloadPin,
}
impl OffloadPinLease {
    pub fn witness(&self) -> RetainedOffloadWitness {
        self.pin.witness.clone()
    }
    /// Immutable pins cannot be removed while this owner is registered. This
    /// is RAM/atomic-only: safe under a foreign registry maintenance fence.
    pub fn validate(&self, expected: &RetainedOffloadWitness) -> Result<()> {
        self.ledger.check()?;
        ensure!(
            &self.pin.witness == expected
                && expected.binding == unhex::<32>(&self.ledger.binding.scope)?,
            "retained offload protected owner changed"
        );
        Ok(())
    }
}
pub(crate) struct CloudReadLease {
    _ledger: Arc<Ledger>,
    _authority: String,
}
pub(crate) fn root_digest(roots: &[ObjectKey]) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"native-retained-offload-roots-v1\0");
    hash.update(&(roots.len() as u64).to_le_bytes());
    for root in roots {
        hash.update(&[root.kind as u8]);
        hash.update(&root.id.0);
    }
    *hash.finalize().as_bytes()
}
pub(crate) fn intrinsic(root: ObjectKey) -> bool {
    root.kind == Kind::Commit && root.id.0 == [0; 32]
}
fn pin_identity(
    capture: [u8; 32],
    authority: [u8; 32],
    binding: [u8; 32],
    roots: [u8; 32],
) -> [u8; 32] {
    let mut hash = blake3::Hasher::new();
    hash.update(b"native-retained-offload-pin-v1\0");
    for value in [capture, authority, binding, roots] {
        hash.update(&value);
    }
    *hash.finalize().as_bytes()
}
/// Validate every v6 pin before the opener updates application state. Fjall may
/// itself replay its journal before this check; this is not a no-write opener.
pub(super) fn validate_saved_pins(
    db: &fjall::SingleWriterTxDatabase,
    pins: &fjall::SingleWriterTxKeyspace,
    authorities: &fjall::SingleWriterTxKeyspace,
    config: &Config,
) -> Result<()> {
    let mut captures = HashSet::new();
    for item in db.read_tx().iter(pins) {
        ensure!(
            captures.len() < config.max_pending_operations,
            "retained pin count exceeded"
        );
        let (key, bytes) = item.into_inner()?;
        ensure!(bytes.len() <= 4096, "retained pin encoding exceeds bound");
        let pin: OffloadPin = serde_json::from_slice(&bytes)?;
        let w = &pin.witness;
        ensure!(
            pin.version == 1
                && w.capture != [0; 32]
                && captures.insert(w.capture)
                && w.pin == pin_identity(w.capture, w.authority, w.binding, w.roots)
                && key.as_ref() == hex(&w.pin).as_bytes(),
            "retained pin corrupt or unsupported"
        );
        let id = hex(&w.authority);
        let source = db
            .read_tx()
            .get(authorities, id.as_bytes())?
            .context("retained pin source missing")?;
        ensure!(
            source.len()
                <= config
                    .max_admitted_roots
                    .saturating_mul(128)
                    .saturating_add(4096),
            "retained source encoding exceeds bound"
        );
        let source: CloudVersionAuthority = serde_json::from_slice(&source)?;
        ensure!(
            source.id() == id && source.identity()? == id && source.binding == hex(&w.binding),
            "retained pin source identity changed"
        );
    }
    Ok(())
}
impl Ledger {
    fn enable_offload_locked(&self) -> Result<()> {
        self.check()?;
        let format = self
            .db
            .read_tx()
            .get(&self.meta, b"format")?
            .context("native feature gate missing")?;
        if format.as_ref() == OFFLOAD_FORMAT {
            return Ok(());
        }
        ensure!(
            format.as_ref() == FORMAT || format.as_ref() == CLOUD_QUERY_FORMAT,
            "retained offload feature gate unsupported"
        );
        ensure!(
            self.db.read_tx().iter(&self.offload_pins).next().is_none(),
            "ungated retained offload pins"
        );
        let mut tx = self
            .db
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        tx.insert(&self.meta, b"format", OFFLOAD_FORMAT);
        if let Err(error) = tx.commit() {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error.into());
        }
        if let Err(error) = self._directory.replace(MARKER, OFFLOAD_FORMAT) {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error.into());
        }
        Ok(())
    }
    pub(crate) fn pinned_cloud_authority(
        &self,
        capture: [u8; 32],
        roots: &[ObjectKey],
    ) -> Result<Option<CloudVersionAuthority>> {
        for pin in self.offload_pins()? {
            if pin.witness.capture == capture {
                ensure!(
                    pin.witness.roots == root_digest(roots),
                    "retained offload capture obligation changed"
                );
                return self.offload_pin_authority(&pin.witness).map(Some);
            }
        }
        Ok(None)
    }
    pub(crate) fn reusable_cloud_authority(
        &self,
        roots: &[ObjectKey],
    ) -> Result<Option<CloudVersionAuthority>> {
        for authority in self.cloud_authorities()? {
            if !self.valid_cloud_authority(&authority)? {
                continue;
            }
            let mut covers = true;
            for root in roots {
                covers &= intrinsic(*root) || authority.terminal(*root)?;
            }
            if covers {
                return Ok(Some(authority));
            }
        }
        Ok(None)
    }
    pub(crate) fn retain_cloud_read(
        self: &Arc<Self>,
        authority: &CloudVersionAuthority,
    ) -> Result<Arc<CloudReadLease>> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        ensure!(
            self.valid_cloud_authority(authority)?,
            "retained read source changed"
        );
        let mut owners = self
            .cloud_read_owners
            .try_lock()
            .map_err(|_| anyhow::anyhow!("retained read ownership busy"))?;
        owners.retain(|_, owner| owner.strong_count() != 0);
        if let Some(owner) = owners.get(authority.id()).and_then(Weak::upgrade) {
            return Ok(owner);
        }
        let owner = Arc::new(CloudReadLease {
            _ledger: self.clone(),
            _authority: authority.id().into(),
        });
        owners.insert(authority.id().into(), Arc::downgrade(&owner));
        Ok(owner)
    }
    fn offload_pins(&self) -> Result<Vec<OffloadPin>> {
        let pins =
            self.list::<OffloadPin>(&self.offload_pins, self.config.max_pending_operations)?;
        let mut captures = HashSet::new();
        for pin in &pins {
            let w = &pin.witness;
            ensure!(
                captures.insert(w.capture),
                "duplicate retained offload capture pin"
            );
            ensure!(
                pin.version == 1
                    && w.capture != [0; 32]
                    && w.pin == pin_identity(w.capture, w.authority, w.binding, w.roots),
                "retained offload pin corrupt"
            );
        }
        Ok(pins)
    }
    /// The new fresh source may replace sources whose DIRECT roots it includes.
    /// All canonical child paths, descriptors and FILE claims migrate atomically.
    /// Pinned cleanup sources and physically active reads are never victims.
    pub(crate) fn install_offload_authority(
        &self,
        proof: &tinytree_client::VerifiedRetainedVersion,
        roots: &[ObjectKey],
    ) -> Result<CloudVersionAuthority> {
        let authority = self.make_cloud_authority(proof, roots)?;
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.enable_offload_locked()?;
        if let Some(existing) = self.cloud_authority(authority.id())? {
            ensure!(existing == authority, "retained authority identity changed");
            return Ok(existing);
        }
        let pins = self.offload_pins()?;
        let reads = self
            .cloud_read_owners
            .try_lock()
            .map_err(|_| anyhow::anyhow!("retained read ownership busy"))?;
        let mut victims = Vec::new();
        for old in self.cloud_authorities()? {
            if !self.valid_cloud_authority(&old)?
                || pins.iter().any(|p| hex(&p.witness.authority) == old.id())
                || reads.get(old.id()).and_then(Weak::upgrade).is_some()
            {
                continue;
            }
            let old_roots = old.native_root_hints().collect::<Result<Vec<_>>>()?;
            if old_roots.iter().all(|root| {
                roots
                    .binary_search_by_key(&(root.kind as u8, root.id.0), |r| (r.kind as u8, r.id.0))
                    .is_ok()
            }) {
                victims.push(old);
            }
        }
        ensure!(
            self.authority_count()?.saturating_sub(victims.len())
                < self.config.query.max_authorities,
            "native authority count exceeded"
        );
        let name = authority.key();
        let mut changes = Vec::new();
        let mut copied: BTreeMap<(u8, Vec<u8>), Vec<u8>> = BTreeMap::new();
        let mut charged = 0usize;
        let mut rows = 0usize;
        let mut progress = Progress::default();
        let mut sum = |value: Progress| -> Result<()> {
            progress.objects = progress
                .objects
                .checked_add(value.objects)
                .context("retained compaction count overflow")?;
            progress.bytes = progress
                .bytes
                .checked_add(value.bytes)
                .context("retained compaction byte overflow")?;
            progress.edges = progress
                .edges
                .checked_add(value.edges)
                .context("retained compaction edge overflow")?;
            progress.known = progress
                .known
                .checked_add(value.known)
                .context("retained compaction reference overflow")?;
            ensure!(
                progress.objects <= self.config.closure.max_objects
                    && progress.bytes <= self.config.closure.max_metadata_bytes as u64
                    && progress.edges <= self.config.closure.max_edges
                    && progress.known <= self.config.closure.max_edges,
                "retained compaction closure budget exceeded"
            );
            Ok(())
        };
        for old in &victims {
            let old_name = old.key();
            let prefix = format!("{old_name}/").into_bytes();
            for (number, space) in [
                (0, &self.members),
                (1, &self.references),
                (2, &self.query_grants),
            ] {
                for item in self.db.read_tx().prefix(space, &prefix) {
                    let (key, value) = item.into_inner()?;
                    rows = rows
                        .checked_add(1)
                        .context("retained compaction row overflow")?;
                    charged = charged
                        .checked_add(
                            (key.len() + value.len())
                                .checked_mul(4)
                                .and_then(|n| n.checked_add(256))
                                .context("retained compaction allocation overflow")?,
                        )
                        .context("retained compaction byte overflow")?;
                    ensure!(
                        rows <= self.config.closure.max_edges && charged <= 128 * 1024 * 1024,
                        "retained compaction budget exceeded"
                    );
                    let mut new_key = format!("{name}/").into_bytes();
                    new_key.extend_from_slice(&key[prefix.len()..]);
                    if let Some(previous) = copied.get(&(number, new_key.clone())) {
                        // Two real canonical paths can name different parents.
                        // Keep the first path; descriptors/grants must agree.
                        ensure!(
                            number == 1 || previous.as_slice() == value.as_ref(),
                            "retained compaction descriptor mismatch"
                        );
                    } else {
                        copied.insert((number, new_key), value.to_vec());
                    }
                    changes.push((space, key.to_vec(), None));
                }
            }
            if let Some(value) = self.get::<Progress>(&self.progress, old_name.as_bytes())? {
                sum(value)?;
                changes.push((&self.progress, old_name.as_bytes().to_vec(), None));
            }
            changes.push((&self.cloud_authorities, old.id().as_bytes().to_vec(), None));
        }
        drop(sum);
        changes.push((
            &self.cloud_authorities,
            authority.id().as_bytes().to_vec(),
            Some(serde_json::to_vec(&authority)?),
        ));
        for ((number, key), value) in copied {
            let space = match number {
                0 => &self.members,
                1 => &self.references,
                _ => &self.query_grants,
            };
            changes.push((space, key, Some(value)));
        }
        if !victims.is_empty() {
            changes.push((
                &self.progress,
                name.as_bytes().to_vec(),
                Some(serde_json::to_vec(&progress)?),
            ));
        }
        let victims = victims.iter().map(|a| a.key()).collect::<HashSet<_>>();
        for item in self.db.read_tx().iter(&self.files) {
            let (key, value) = item.into_inner()?;
            let mut claim: FileClaim = serde_json::from_slice(&value)?;
            rows = rows
                .checked_add(1)
                .context("retained compaction row overflow")?;
            ensure!(
                rows <= self.config.max_file_index_entries + self.config.closure.max_edges,
                "retained compaction FILE budget exceeded"
            );
            if victims.contains(&claim.root) {
                charged = charged
                    .checked_add(
                        (key.len() + value.len())
                            .checked_mul(4)
                            .and_then(|n| n.checked_add(256))
                            .context("retained compaction allocation overflow")?,
                    )
                    .context("retained compaction allocation overflow")?;
                ensure!(
                    charged <= 128 * 1024 * 1024,
                    "retained compaction budget exceeded"
                );
                claim.root = name.clone();
                changes.push((&self.files, key.to_vec(), Some(serde_json::to_vec(&claim)?)));
            }
        }
        self.write_locked(changes)?;
        drop(reads);
        Ok(authority)
    }
    pub(crate) fn install_offload_pin(
        self: &Arc<Self>,
        authority: &CloudVersionAuthority,
        capture: [u8; 32],
        roots: &[ObjectKey],
    ) -> Result<Arc<OffloadPinLease>> {
        ensure!(
            capture != [0; 32],
            "retained offload capture identity missing"
        );
        for root in roots {
            ensure!(
                intrinsic(*root) || authority.terminal(*root)?,
                "retained offload roots not directly admitted"
            );
        }
        let binding = unhex::<32>(&self.binding.scope)?;
        let id = unhex::<32>(authority.id())?;
        let roots = root_digest(roots);
        let witness = RetainedOffloadWitness {
            capture,
            authority: id,
            pin: pin_identity(capture, id, binding, roots),
            binding,
            roots,
        };
        let pin = OffloadPin {
            version: 1,
            witness,
        };
        let key = hex(&pin.witness.pin);
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.enable_offload_locked()?;
        ensure!(
            self.offload_pins()?
                .iter()
                .all(|old| old.witness.capture != capture || old == &pin),
            "retained offload capture pin changed"
        );
        ensure!(
            self.valid_cloud_authority(authority)?,
            "retained offload source changed"
        );
        if let Some(existing) = self.get::<OffloadPin>(&self.offload_pins, key.as_bytes())? {
            ensure!(existing == pin, "retained offload pin identity changed");
        } else {
            ensure!(
                self.offload_pins()?.len() < self.config.max_pending_operations,
                "retained offload pin count exceeded"
            );
            self.write_locked(vec![(
                &self.offload_pins,
                key.as_bytes().to_vec(),
                Some(serde_json::to_vec(&pin)?),
            )])?;
        }
        self.pin_owner_locked(pin)
    }
    fn pin_owner_locked(self: &Arc<Self>, pin: OffloadPin) -> Result<Arc<OffloadPinLease>> {
        let key = hex(&pin.witness.pin);
        let mut owners = self
            .offload_owners
            .try_lock()
            .map_err(|_| anyhow::anyhow!("retained offload ownership busy"))?;
        if let Some(owner) = owners.get(&key).and_then(Weak::upgrade) {
            ensure!(owner.pin == pin, "retained offload owner changed");
            return Ok(owner);
        }
        let owner = Arc::new(OffloadPinLease {
            ledger: self.clone(),
            pin,
        });
        owners.insert(key, Arc::downgrade(&owner));
        Ok(owner)
    }
    pub(crate) fn offload_pin_authority(
        &self,
        witness: &RetainedOffloadWitness,
    ) -> Result<CloudVersionAuthority> {
        let pin = self
            .get::<OffloadPin>(&self.offload_pins, hex(&witness.pin).as_bytes())?
            .context("retained offload pin missing")?;
        ensure!(
            pin.version == 1
                && pin.witness.pin
                    == pin_identity(
                        pin.witness.capture,
                        pin.witness.authority,
                        pin.witness.binding,
                        pin.witness.roots
                    )
                && &pin.witness == witness
                && witness.binding == unhex::<32>(&self.binding.scope)?,
            "retained offload pin binding changed"
        );
        let authority = self
            .cloud_authority(&hex(&witness.authority))?
            .context("retained offload source missing")?;
        ensure!(
            self.valid_cloud_authority(&authority)?,
            "retained offload source not current"
        );
        Ok(authority)
    }
    pub(crate) fn resume_offload_pin(
        self: &Arc<Self>,
        witness: &RetainedOffloadWitness,
    ) -> Result<Arc<OffloadPinLease>> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.offload_pin_authority(witness)?;
        let pin = self
            .get::<OffloadPin>(&self.offload_pins, hex(&witness.pin).as_bytes())?
            .context("retained offload pin missing")?;
        self.pin_owner_locked(pin)
    }
    /// Called only outside foreign maintenance after ACK, or with the trusted
    /// host's complete protected pending/cleanup bindings under its maintenance.
    /// Live opaque owners always win over a requested release/reconciliation.
    pub(crate) fn release_offload_pins(
        &self,
        protected: &[[u8; 32]],
        only: Option<&RetainedOffloadWitness>,
    ) -> Result<usize> {
        ensure!(
            // Complete host inventory has an independent existing registry
            // ceiling (100000 pending + one cleanup), not this provider's PIN
            // quota. Other providers' captures do not consume local pin slots.
            protected.len() <= 100_001 && protected.windows(2).all(|p| p[0] < p[1]),
            "retained offload protected inventory bound or ordering exceeded"
        );
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        let mut owners = self
            .offload_owners
            .try_lock()
            .map_err(|_| anyhow::anyhow!("retained offload ownership busy"))?;
        let mut changes = Vec::new();
        for pin in self.offload_pins()? {
            let w = &pin.witness;
            let key = hex(&w.pin);
            if only.is_some_and(|expected| expected != w)
                || protected.binary_search(&w.capture).is_ok()
                || owners.get(&key).and_then(Weak::upgrade).is_some()
            {
                continue;
            }
            if let Some(expected) = only {
                ensure!(
                    expected.binding == unhex::<32>(&self.binding.scope)?,
                    "retained offload release binding changed"
                );
            }
            changes.push((&self.offload_pins, key.as_bytes().to_vec(), None));
            owners.remove(&key);
        }
        let count = changes.len();
        if count > 0 {
            self.write_locked(changes)?;
        }
        Ok(count)
    }
}
