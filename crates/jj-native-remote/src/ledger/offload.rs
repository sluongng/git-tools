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
    pub(super) _ledger: Arc<Ledger>,
    pub(super) _authority: String,
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

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum SourcePinPhase {
    AcquirePending,
    Active,
    ReleasePending,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum SourcePinAuthority {
    File(CloudVersionAuthority),
    Query(CloudQueryAuthority),
}
impl SourcePinAuthority {
    pub(crate) fn id(&self) -> String {
        match self {
            Self::File(a) => a.id().into(),
            Self::Query(a) => a.key(),
        }
    }
    fn identity(&self) -> Result<String> {
        match self {
            Self::File(a) => a.identity(),
            Self::Query(a) => Ok(format!("cq/{}", a.identity()?)),
        }
    }
    fn binding(&self) -> &str {
        match self {
            Self::File(a) => &a.binding,
            Self::Query(a) => a.binding(),
        }
    }
    pub(crate) fn selector(&self) -> tinytree_client::RetainedVersionSelector {
        match self {
            Self::File(a) => a.selector(),
            Self::Query(a) => a.selector(),
        }
    }
    pub(crate) fn selected(&self) -> tinytree_client::CloudRawDigest {
        match self {
            Self::File(a) => a.selected(),
            Self::Query(a) => a.selected_raw(),
        }
    }
    pub(crate) fn agrees(&self, proof: &tinytree_client::VerifiedRetainedVersion) -> bool {
        match self {
            Self::File(a) => a.agrees(proof),
            Self::Query(a) => a.agrees(proof),
        }
    }
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourcePin {
    version: u8,
    pub authority: SourcePinAuthority,
    pub nonce: [u8; 16],
    pub acquire_not_after_unix_seconds: i64,
    pub ordinal: u64,
    pub phase: SourcePinPhase,
}
impl SourcePin {
    pub(crate) fn identity(&self) -> tinytree_client::CloudVersionPinIdentity {
        tinytree_client::CloudVersionPinIdentity {
            pin_id: self.nonce,
            acquire_not_after_unix_seconds: self.acquire_not_after_unix_seconds,
        }
    }
}
fn source_pin_admission_deadline() -> Result<i64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs()
        .try_into()
        .ok()
        .and_then(|now: i64| now.checked_add(120))
        .context("retained source pin admission clock invalid")
}
fn source_pin_limit(config: &Config) -> usize {
    config
        .query
        .max_authorities
        .saturating_add(config.max_pending_operations)
}
pub(super) fn validate_saved_source_pins(
    db: &fjall::SingleWriterTxDatabase,
    meta: &fjall::SingleWriterTxKeyspace,
    pins: &fjall::SingleWriterTxKeyspace,
    authorities: &fjall::SingleWriterTxKeyspace,
    query_authorities: &fjall::SingleWriterTxKeyspace,
    config: &Config,
) -> Result<()> {
    let sequence: u64 = db
        .read_tx()
        .get(meta, b"source-pin-sequence")?
        .map(|v| serde_json::from_slice(&v))
        .transpose()?
        .unwrap_or(0);
    let mut nonces = HashSet::new();
    let mut ordinals = HashSet::new();
    let mut pending = 0usize;
    let mut active = 0usize;
    for entry in db.read_tx().iter(pins) {
        ensure!(
            nonces.len() < source_pin_limit(config),
            "retained source pin count exceeded"
        );
        let (key, raw) = entry.into_inner()?;
        ensure!(
            raw.len()
                <= config
                    .max_admitted_roots
                    .saturating_add(256)
                    .saturating_mul(256)
                    .saturating_add(16_384),
            "retained source pin encoding exceeds bound"
        );
        let pin: SourcePin = serde_json::from_slice(&raw)?;
        ensure!(
            pin.version == 2
                && pin.acquire_not_after_unix_seconds > 0
                && pin.nonce != [0; 16]
                && nonces.insert(pin.nonce)
                && pin.ordinal > 0
                && pin.ordinal <= sequence
                && ordinals.insert(pin.ordinal)
                && pin.authority.id() == pin.authority.identity()?
                && key.as_ref() == pin.authority.id().as_bytes(),
            "retained source pin corrupt"
        );
        if pin.phase == SourcePinPhase::Active {
            active += 1;
            let space = match &pin.authority {
                SourcePinAuthority::File(_) => authorities,
                SourcePinAuthority::Query(_) => query_authorities,
            };
            let authority_key = match &pin.authority {
                SourcePinAuthority::File(a) => a.id(),
                SourcePinAuthority::Query(a) => a.id(),
            };
            let authority = db
                .read_tx()
                .get(space, authority_key.as_bytes())?
                .context("active source authority missing")?;
            ensure!(
                authority.as_ref()
                    == match &pin.authority {
                        SourcePinAuthority::File(a) => serde_json::to_vec(a)?,
                        SourcePinAuthority::Query(a) => serde_json::to_vec(a)?,
                    }
                    .as_slice(),
                "active source authority changed"
            );
        } else {
            pending += 1;
            if pin.phase == SourcePinPhase::ReleasePending {
                ensure!(
                    match &pin.authority {
                        SourcePinAuthority::File(a) =>
                            db.read_tx().get(authorities, a.id().as_bytes())?,
                        SourcePinAuthority::Query(a) =>
                            db.read_tx().get(query_authorities, a.id().as_bytes())?,
                    }
                    .is_none(),
                    "released source authority remains installed"
                );
            }
        }
        ensure!(
            active <= config.query.max_authorities && pending <= config.max_pending_operations,
            "retained source pin state count exceeded"
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
        if format.as_ref() == OFFLOAD_FORMAT || format.as_ref() == SOURCE_PIN_FORMAT {
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
    pub(crate) fn source_pins(&self) -> Result<Vec<SourcePin>> {
        let rows = self.list::<SourcePin>(&self.source_pins, source_pin_limit(&self.config))?;
        for pin in &rows {
            ensure!(
                pin.version == 2
                    && pin.acquire_not_after_unix_seconds > 0
                    && pin.nonce != [0; 16]
                    && pin.ordinal > 0
                    && pin.authority.identity()? == pin.authority.id()
                    && pin.authority.binding() == self.binding.scope,
                "retained source pin binding changed"
            );
        }
        Ok(rows)
    }
    /// Snapshot only installed sources in this publication workspace. The final
    /// pass rechecks all still-installed authorities under the same ledger gate;
    /// a changed acquisition row is not an acknowledgement of protection.
    pub(crate) fn source_pins_before_publish(
        &self,
        workspace: [u8; 32],
        require_active: bool,
    ) -> Result<Vec<SourcePin>> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.check()?;
        let authorities = self
            .cloud_authorities()?
            .into_iter()
            .map(SourcePinAuthority::File)
            .chain(
                self.cloud_query_authorities()?
                    .into_iter()
                    .map(SourcePinAuthority::Query),
            );
        let mut pending = Vec::new();
        for authority in authorities {
            if authority.selector().workspace_id != workspace {
                continue;
            }
            let saved = self.get::<SourcePin>(&self.source_pins, authority.id().as_bytes())?;
            if require_active {
                ensure!(
                    saved.as_ref().is_some_and(|pin| {
                        pin.authority == authority && pin.phase == SourcePinPhase::Active
                    }),
                    "publication source authority has no confirmed backend pin"
                );
            } else if let Some(pin) = saved {
                ensure!(pin.authority == authority, "publication source pin changed");
                if pin.phase == SourcePinPhase::AcquirePending {
                    pending.push(pin);
                }
            }
        }
        pending.sort_by_key(|pin| std::cmp::Reverse(pin.ordinal));
        Ok(pending)
    }
    pub(crate) fn prepare_offload_source_pin(
        &self,
        proof: &tinytree_client::VerifiedRetainedVersion,
        roots: &[ObjectKey],
    ) -> Result<SourcePin> {
        self.prepare_source_pin(&SourcePinAuthority::File(
            self.make_cloud_authority(proof, roots)?,
        ))
    }
    pub(crate) fn source_pin_unchanged(&self, expected: &SourcePin) -> Result<bool> {
        self.check()?;
        Ok(self
            .get::<SourcePin>(&self.source_pins, expected.authority.id().as_bytes())?
            .as_ref()
            == Some(expected))
    }
    pub(crate) fn prepare_source_pin(&self, authority: &SourcePinAuthority) -> Result<SourcePin> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.enable_offload_locked()?;
        ensure!(
            authority.identity()? == authority.id() && authority.binding() == self.binding.scope,
            "retained source authority binding changed"
        );
        if let Some(saved) = self.get::<SourcePin>(&self.source_pins, authority.id().as_bytes())? {
            ensure!(
                saved.authority == *authority && saved.phase != SourcePinPhase::ReleasePending,
                "retained source release must finish before reacquisition"
            );
            return Ok(saved);
        }
        let rows = self.source_pins()?;
        ensure!(
            rows.len() < source_pin_limit(&self.config)
                && rows
                    .iter()
                    .filter(|p| p.phase != SourcePinPhase::Active)
                    .count()
                    < self.config.max_pending_operations,
            "retained source pin outbox full"
        );
        let sequence: u64 = self.get(&self.meta, b"source-pin-sequence")?.unwrap_or(0);
        let ordinal = sequence
            .checked_add(1)
            .context("retained source pin sequence exhausted")?;
        let nonce = *uuid::Uuid::new_v4().as_bytes();
        ensure!(
            nonce != [0; 16] && rows.iter().all(|p| p.nonce != nonce),
            "retained source nonce collision"
        );
        let pin = SourcePin {
            version: 2,
            authority: authority.clone(),
            nonce,
            acquire_not_after_unix_seconds: source_pin_admission_deadline()?,
            ordinal,
            phase: SourcePinPhase::AcquirePending,
        };
        self.write_locked_with_format(
            vec![
                (
                    &self.meta,
                    b"source-pin-sequence".to_vec(),
                    Some(serde_json::to_vec(&ordinal)?),
                ),
                (
                    &self.source_pins,
                    authority.id().as_bytes().to_vec(),
                    Some(serde_json::to_vec(&pin)?),
                ),
            ],
            Some(SOURCE_PIN_FORMAT),
        )?;
        if let Err(error) = self._directory.replace(MARKER, SOURCE_PIN_FORMAT) {
            self.poisoned
                .store(true, std::sync::atomic::Ordering::Release);
            return Err(error.into());
        }
        Ok(pin)
    }
    /// Called only after Acquire proves the old identity durably terminal. An
    /// installed legacy authority keeps all provenance and gets a new pending
    /// intent for a later request; an unreferenced intent may instead be cleared.
    /// Active pins and uncertain identities are never renewed here.
    pub(crate) fn acknowledge_expired_acquisition(&self, expected: &SourcePin) -> Result<bool> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.check()?;
        if expected.phase != SourcePinPhase::AcquirePending {
            return Ok(false);
        }
        let key = expected.authority.id();
        if self
            .get::<SourcePin>(&self.source_pins, key.as_bytes())?
            .as_ref()
            != Some(expected)
        {
            return Ok(false);
        }
        let installed = match &expected.authority {
            SourcePinAuthority::File(authority) => self
                .cloud_authority(authority.id())?
                .map(|saved| SourcePinAuthority::File(saved)),
            SourcePinAuthority::Query(authority) => self
                .cloud_query(authority.id())?
                .map(|saved| SourcePinAuthority::Query(saved)),
        };
        if let Some(installed) = installed {
            ensure!(
                installed == expected.authority,
                "terminal acquisition authority changed"
            );
            let mut next = expected.clone();
            next.nonce = *uuid::Uuid::new_v4().as_bytes();
            ensure!(
                next.nonce != [0; 16]
                    && self
                        .source_pins()?
                        .iter()
                        .all(|pin| pin.nonce != next.nonce),
                "retained source nonce collision"
            );
            next.acquire_not_after_unix_seconds = source_pin_admission_deadline()?;
            // Retain the original source ordinal: retrying an old adoption does
            // not make it a newer source eligible to supersede later captures.
            self.write_locked(vec![(
                &self.source_pins,
                key.into_bytes(),
                Some(serde_json::to_vec(&next)?),
            )])?;
            return Ok(true);
        }
        let (id, name) = match &expected.authority {
            SourcePinAuthority::File(a) => (a.id(), a.key()),
            SourcePinAuthority::Query(a) => (a.id(), a.key()),
        };
        if self.cloud_authority(id)?.is_some()
            || self.cloud_query(id)?.is_some()
            || self
                .offload_pins()?
                .iter()
                .any(|pin| hex(&pin.witness.authority) == id)
            || self
                .db
                .read_tx()
                .get(&self.progress, name.as_bytes())?
                .is_some()
        {
            return Ok(false);
        }
        let reads = self
            .cloud_read_owners
            .try_lock()
            .map_err(|_| anyhow::anyhow!("retained read ownership busy"))?;
        if reads.get(&key).and_then(Weak::upgrade).is_some() {
            return Ok(false);
        }
        let prefix = format!("{name}/");
        for space in [&self.members, &self.references, &self.query_grants] {
            if let Some(entry) = self.db.read_tx().prefix(space, prefix.as_bytes()).next() {
                entry.into_inner()?;
                return Ok(false);
            }
        }
        for (count, entry) in self.db.read_tx().iter(&self.files).enumerate() {
            ensure!(
                count < self.config.max_file_index_entries,
                "retained FILE claim count exceeded"
            );
            let (_, raw) = entry.into_inner()?;
            let claim: FileClaim = serde_json::from_slice(&raw)?;
            if claim.root == name {
                return Ok(false);
            }
        }
        self.write_locked(vec![(&self.source_pins, key.into_bytes(), None)])?;
        drop(reads);
        Ok(true)
    }
    pub(crate) fn acknowledge_source_release(&self, expected: &SourcePin) -> Result<bool> {
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        ensure!(
            expected.phase == SourcePinPhase::ReleasePending
                && expected.authority.binding() == self.binding.scope,
            "retained source release binding changed"
        );
        let current =
            self.get::<SourcePin>(&self.source_pins, expected.authority.id().as_bytes())?;
        if current.as_ref() != Some(expected) {
            return Ok(false);
        }
        self.write_locked(vec![(
            &self.source_pins,
            expected.authority.id().as_bytes().to_vec(),
            None,
        )])?;
        Ok(true)
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
        self.retain_source_read_locked(authority.id().into())
    }
    pub(super) fn retain_source_read_locked(
        self: &Arc<Self>,
        key: String,
    ) -> Result<Arc<CloudReadLease>> {
        let mut owners = self
            .cloud_read_owners
            .try_lock()
            .map_err(|_| anyhow::anyhow!("retained read ownership busy"))?;
        owners.retain(|_, owner| owner.strong_count() != 0);
        if let Some(owner) = owners.get(&key).and_then(Weak::upgrade) {
            return Ok(owner);
        }
        let owner = Arc::new(CloudReadLease {
            _ledger: self.clone(),
            _authority: key.clone(),
        });
        owners.insert(key, Arc::downgrade(&owner));
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
        pin: &SourcePin,
    ) -> Result<CloudVersionAuthority> {
        let authority = self.make_cloud_authority(proof, roots)?;
        let _gate = self
            .gate
            .lock()
            .map_err(|_| anyhow::anyhow!("native ledger requires recovery"))?;
        self.enable_offload_locked()?;
        let existing = self.cloud_authority(authority.id())?;
        ensure!(
            existing.as_ref().is_none_or(|a| *a == authority),
            "retained authority identity changed"
        );
        let saved = self
            .get::<SourcePin>(&self.source_pins, authority.id().as_bytes())?
            .context("retained source acquisition intent missing")?;
        ensure!(
            &saved == pin
                && pin.authority == SourcePinAuthority::File(authority.clone())
                && pin.phase != SourcePinPhase::ReleasePending,
            "retained source acquisition intent changed"
        );
        let source_pins = self.source_pins()?;
        let pins = self.offload_pins()?;
        let reads = self
            .cloud_read_owners
            .try_lock()
            .map_err(|_| anyhow::anyhow!("retained read ownership busy"))?;
        let mut victims = Vec::new();
        for old in self.cloud_authorities()? {
            let Some(_) = source_pins.iter().find(|p| {
                p.authority == SourcePinAuthority::File(old.clone())
                    && p.phase == SourcePinPhase::Active
                    && p.ordinal < pin.ordinal
            }) else {
                continue;
            };
            if old.id() == authority.id()
                || !self.valid_cloud_authority(&old)?
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
            self.authority_count()?
                .saturating_sub(victims.len())
                .saturating_add(usize::from(existing.is_none()))
                <= self.config.query.max_authorities,
            "native authority count exceeded"
        );
        let pending = source_pins
            .iter()
            .filter(|p| p.phase != SourcePinPhase::Active)
            .count();
        ensure!(
            pending
                .saturating_sub(usize::from(pin.phase == SourcePinPhase::AcquirePending))
                .saturating_add(victims.len())
                <= self.config.max_pending_operations,
            "retained source release outbox full"
        );
        let name = authority.key();
        let mut changes = Vec::new();
        let mut copied: BTreeMap<(u8, Vec<u8>), Vec<u8>> = BTreeMap::new();
        let mut charged = 0usize;
        let mut rows = 0usize;
        let mut progress = self
            .get::<Progress>(&self.progress, name.as_bytes())?
            .unwrap_or_default();
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
                    let destination = self.db.read_tx().get(space, &new_key)?;
                    if let Some(previous) = destination
                        .as_ref()
                        .map(|v| v.as_ref())
                        .or_else(|| copied.get(&(number, new_key.clone())).map(Vec::as_slice))
                    {
                        // Two real canonical paths can name different parents.
                        // Keep the first path; descriptors/grants must agree.
                        ensure!(
                            number == 1 || previous == value.as_ref(),
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
            let mut released = source_pins
                .iter()
                .find(|p| p.authority == SourcePinAuthority::File(old.clone()))
                .context("retained victim pin missing")?
                .clone();
            released.phase = SourcePinPhase::ReleasePending;
            changes.push((
                &self.source_pins,
                old.id().as_bytes().to_vec(),
                Some(serde_json::to_vec(&released)?),
            ));
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
        let mut active = pin.clone();
        active.phase = SourcePinPhase::Active;
        changes.push((
            &self.source_pins,
            authority.id().as_bytes().to_vec(),
            Some(serde_json::to_vec(&active)?),
        ));
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
