//! Optional owned catalog memory. Cumulative validation work is deliberately
//! separate: revisiting an immutable row consumes work, not another live row.
use super::*;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CloudResidentLimits {
    pub max_bytes: u64,
    pub max_entries: u64,
}
impl CloudResidentLimits {
    /// Validate numeric policy before opening authoritative storage.
    pub fn validate(self) -> Result<()> {
        if self.max_bytes < 65536 || self.max_entries < 16 {
            return Err(Error::Invalid("cloud resident bounds"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Catalog {
    pub records: u64,
    pub metadata: u64,
    pub namespaces: u64,
    pub typed: u64,
}
#[derive(Clone, Copy, Default)]
pub(crate) struct Charge {
    pub bytes: u64,
    pub entries: u64,
}
impl Catalog {
    pub(crate) fn charge(self) -> Result<Charge> {
        let add = |a: u64, b: u64| {
            a.checked_add(b)
                .ok_or(Error::Limit("cloud resident memory"))
        };
        let mul = |a: u64, b: u64| {
            a.checked_mul(b)
                .ok_or(Error::Limit("cloud resident memory"))
        };
        // Precharge one complete operation arena, not a row-at-a-time live-len
        // counter. Unique rows dominate every retained catalog container:
        //
        // * version/current/manifest and work/descriptor maps retain bounded copies
        //   of each version key/record, never every repeated validation read;
        // * native/cloud/variant/artifact/blob sets deduplicate catalog IDs;
        //   native-by-repository and quota unions add simultaneous set copies;
        // * reopen expected/actual/blob-reference/repository maps and namespace
        //   vectors coexist with the retained plan;
        // * GC object/primary/mark/pending sets, marked.clone(), grouped encoded
        //   changes, path vectors and apply's encoded copies coexist too.
        //
        // 128 raw-row bytes and 4096 bytes per row include decoded scalar/key
        // expansion, simultaneous copies, hash-bucket slack and path ownership.
        // Typed references additionally cover the root vectors/sets even when
        // their targets are intrinsic and have no cloud object catalog row.
        // Capacity is retained for the entire arena, including after partial
        // moves, clear/remove and encoded apply copies. A second physical call
        // or returned version guard owns a separately charged arena.
        // Native closure/path/model codec scratch retains separate configured
        // admission terms; those limits are not replaced by this arena.
        Ok(Charge {
            bytes: add(
                add(
                    add(mul(self.metadata, 128)?, mul(self.records, 4096)?)?,
                    mul(self.typed, 1024)?,
                )?,
                add(mul(self.namespaces, 8192)?, 65536)?,
            )?,
            entries: add(
                add(mul(self.records, 16)?, mul(self.typed, 8)?)?,
                add(mul(self.namespaces, 4)?, 16)?,
            )?,
        })
    }
}
fn within(limits: CloudResidentLimits, charge: Charge) -> Result<()> {
    if charge.bytes > limits.max_bytes || charge.entries > limits.max_entries {
        return Err(Error::Limit("cloud resident memory"));
    }
    Ok(())
}

// Recovery allocation is independent of retained-work counters. The same
// policy is enforced by normal writers before WAL emission and strict readers.
// Historical physical journal bytes are streamed, not used as a heap quota.
pub(crate) fn recovery_limits(config: &Config) -> Option<fjall::RecoveryAllocationLimits> {
    config
        .cloud
        .resident
        .map(|_| fjall::RecoveryAllocationLimits {
            max_record_bytes: CLOUD_BINARY_ROW_BYTES + 65536,
            max_batch_bytes: 256 * 1024 * 1024,
            max_batch_items: 1_000_000,
            max_journals: 1024,
        })
}

/// Read-only resource preflight, not a replacement for semantic reopen checks.
/// Only existing keyspaces are opened; dropping the strict handle authorizes no
/// recovery writes. The caller retains the owner lock across ordinary reopen.
pub(crate) fn preflight(root: &Path, config: &Config) -> Result<Catalog> {
    let Some(limits) = config.cloud.resident else {
        return Ok(Catalog::default());
    };
    let db = SingleWriterTxDatabase::builder(root.join("metadata"))
        .manual_journal_persist(true)
        .strict_existing_recovery(true)
        .recovery_allocation_limits(recovery_limits(config))
        .strict_recovery_max_scan_bytes(u64::MAX)
        .open()?;
    let mut catalog = Catalog::default();
    let mut accounting_repositories = 0u64;
    let mut accounting_globals = 0u64;
    let view = db.read_tx();
    for (slot, name) in TABLES.iter().enumerate() {
        if !db.keyspace_exists(name) {
            continue;
        }
        let space = db.keyspace(*name, KeyspaceCreateOptions::default)?;
        for item in view.iter(&space) {
            let (key, bytes) = item.into_inner()?;
            if bytes.len()
                > if [3, 6, 11].contains(&slot) {
                    CLOUD_BINARY_ROW_BYTES
                } else {
                    CLOUD_JSON_ROW_BYTES
                }
            {
                return Err(Error::Corrupt("cloud metadata row size"));
            }
            if slot == 13 && bytes.len() > 4096 {
                return Err(Error::Corrupt("cloud resident lifecycle row"));
            }
            if slot == 8 {
                // Normal writes encode Accounting's fixed u64 scalar fields;
                // even maximal decimal values fit well below this bound. This
                // also bounds transient JSON field-name decoding on refusal.
                if bytes.len() > 4096 {
                    return Err(Error::Corrupt("cloud resident accounting row"));
                }
                if key.as_ref() == [0, 0] {
                    accounting_globals = accounting_globals
                        .checked_add(1)
                        .ok_or(Error::Limit("cloud resident memory"))?;
                } else {
                    let size = u16::from_be_bytes(
                        key.get(..2)
                            .ok_or(Error::Corrupt("cloud resident accounting key"))?
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let end = size + 2;
                    let repo = std::str::from_utf8(
                        key.get(2..end)
                            .ok_or(Error::Corrupt("cloud resident accounting key"))?,
                    )
                    .map_err(|_| Error::Corrupt("cloud resident accounting key"))?;
                    crate::validate_repository_id(repo)
                        .map_err(|_| Error::Corrupt("cloud resident accounting key"))?;
                    if key.len() == end {
                        accounting_repositories = accounting_repositories
                            .checked_add(1)
                            .ok_or(Error::Limit("cloud resident memory"))?;
                    } else if key.len() == end + 32 {
                        catalog.namespaces = catalog
                            .namespaces
                            .checked_add(1)
                            .ok_or(Error::Limit("cloud resident memory"))?;
                    } else {
                        return Err(Error::Corrupt("cloud resident accounting key"));
                    }
                }
                if accounting_repositories > limits.max_entries || accounting_globals > 1 {
                    return Err(Error::Limit("cloud resident memory"));
                }
            } else {
                catalog.records = catalog
                    .records
                    .checked_add(1)
                    .ok_or(Error::Limit("cloud resident memory"))?;
                catalog.metadata = catalog
                    .metadata
                    .checked_add((key.len() + bytes.len()) as u64)
                    .ok_or(Error::Limit("cloud resident memory"))?;
                if slot == 6 {
                    catalog.typed = catalog
                        .typed
                        .checked_add(root_count(&bytes)?)
                        .ok_or(Error::Limit("cloud resident memory"))?;
                }
            }
            within(limits, catalog.charge()?)?;
        }
    }
    // cloud_changes atomically writes owner, repository and global totals;
    // GC only updates them and never removes an owner namespace row. Therefore
    // valid states, including emptied owners and a fresh zero catalog, have at
    // most one repository row per observed owner row and one global row. Check
    // this before ordinary recovery: reopen retains all accounting rows, not
    // just owner rows. Bounded repository IDs plus fixed scalar values make the
    // existing per-namespace arena allowance dominate those maps and copies.
    if accounting_repositories > catalog.namespaces {
        return Err(Error::Corrupt("cloud resident accounting namespaces"));
    }
    within(limits, catalog.charge()?)?;
    Ok(catalog)
}

/// Inspect framing/counts without allocating typed-root vectors. Semantic
/// identity, order, proof policy and manifest equality remain reopen's job.
pub(crate) fn root_count(bytes: &[u8]) -> Result<u64> {
    if bytes.len() < 44 {
        return Err(Error::Corrupt("cloud resident root framing"));
    }
    let native = u32::from_le_bytes(bytes[40..44].try_into().unwrap()) as usize;
    let end = native
        .checked_mul(33)
        .and_then(|n| n.checked_add(44))
        .ok_or(Error::Corrupt("cloud resident root framing"))?;
    if &bytes[..8] == b"TTROOT01" && end == bytes.len() {
        return Ok(native as u64);
    }
    if &bytes[..8] != b"TTROOT02" || end.checked_add(30).is_none_or(|n| n > bytes.len()) {
        return Err(Error::Corrupt("cloud resident root framing"));
    }
    let artifacts = u32::from_le_bytes(bytes[end + 26..end + 30].try_into().unwrap()) as usize;
    if artifacts
        .checked_mul(73)
        .and_then(|n| n.checked_add(end + 30))
        != Some(bytes.len())
    {
        return Err(Error::Corrupt("cloud resident root framing"));
    }
    (native as u64)
        .checked_add(artifacts as u64)
        .ok_or(Error::Limit("cloud resident memory"))
}

#[derive(Default)]
struct State {
    catalog: Catalog,
    pending: Option<Catalog>,
    used: Charge,
    active: Weak<Reservation>,
}
pub(crate) struct Budget {
    limits: Option<CloudResidentLimits>,
    state: Mutex<State>,
}
impl Budget {
    pub(crate) fn new(limits: Option<CloudResidentLimits>, catalog: Catalog) -> Arc<Self> {
        Arc::new(Self {
            limits,
            state: Mutex::new(State {
                catalog,
                ..State::default()
            }),
        })
    }
    pub(crate) fn arena(self: &Arc<Self>) -> Result<Option<Arc<Reservation>>> {
        if self.limits.is_none() {
            return Ok(None);
        }
        let charge = self
            .state
            .lock()
            .map_err(|_| Error::Poisoned)?
            .catalog
            .charge()?;
        let reservation = Arc::new(self.reserve(charge)?);
        self.state.lock().map_err(|_| Error::Poisoned)?.active = Arc::downgrade(&reservation);
        Ok(Some(reservation))
    }
    /// Reenter the same physical operation after releasing the Store gate.
    /// Catalog growth during validation must be admitted without reserving a
    /// second arena or refunding capacity still retained by the caller.
    pub(crate) fn reenter(
        self: &Arc<Self>,
        retained: Option<&Arc<Reservation>>,
    ) -> Result<Option<Arc<Reservation>>> {
        let Some(limits) = self.limits else {
            if retained.is_some() {
                return Err(Error::Poisoned);
            }
            return Ok(None);
        };
        let arena = retained.ok_or(Error::Poisoned)?;
        if !Arc::ptr_eq(self, &arena.budget) {
            return Err(Error::Poisoned);
        }
        let mut state = self.state.lock().map_err(|_| Error::Poisoned)?;
        if state.pending.is_some() {
            return Err(Error::Poisoned);
        }
        let current = state.catalog.charge()?;
        let old_bytes = arena.bytes.load(Ordering::Relaxed);
        let old_entries = arena.entries.load(Ordering::Relaxed);
        let bytes = old_bytes.max(current.bytes);
        let entries = old_entries.max(current.entries);
        let used = Charge {
            bytes: state
                .used
                .bytes
                .checked_add(bytes - old_bytes)
                .ok_or(Error::Limit("cloud resident memory"))?,
            entries: state
                .used
                .entries
                .checked_add(entries - old_entries)
                .ok_or(Error::Limit("cloud resident memory"))?,
        };
        within(limits, used)?;
        state.used = used;
        arena.bytes.store(bytes, Ordering::Relaxed);
        arena.entries.store(entries, Ordering::Relaxed);
        state.active = Arc::downgrade(arena);
        Ok(Some(arena.clone()))
    }
    pub(crate) fn lease(self: &Arc<Self>) -> Result<Option<Arc<Reservation>>> {
        if self.limits.is_none() {
            return Ok(None);
        }
        self.reserve(Charge {
            bytes: 4096,
            entries: 1,
        })
        .map(|value| Some(Arc::new(value)))
    }
    pub(crate) fn reserve(self: &Arc<Self>, charge: Charge) -> Result<Reservation> {
        let mut state = self.state.lock().map_err(|_| Error::Poisoned)?;
        if let Some(limits) = self.limits {
            let next = Charge {
                bytes: state
                    .used
                    .bytes
                    .checked_add(charge.bytes)
                    .ok_or(Error::Limit("cloud resident memory"))?,
                entries: state
                    .used
                    .entries
                    .checked_add(charge.entries)
                    .ok_or(Error::Limit("cloud resident memory"))?,
            };
            within(limits, next)?;
            state.used = next;
        }
        Ok(Reservation {
            budget: self.clone(),
            bytes: AtomicU64::new(charge.bytes),
            entries: AtomicU64::new(charge.entries),
        })
    }
    pub(super) fn prepare(&self, global: &Accounting, typed_delta: i128) -> Result<()> {
        let Some(limits) = self.limits else {
            return Ok(());
        };
        let mut state = self.state.lock().map_err(|_| Error::Poisoned)?;
        let mut typed = state.catalog.typed;
        adjust(&mut typed, typed_delta)?;
        let prospective = Catalog {
            records: global.records,
            metadata: global.metadata_bytes,
            namespaces: global.namespaces,
            typed,
        };
        let charge = prospective.charge()?;
        within(limits, charge)?;
        let arena = state.active.upgrade().ok_or(Error::Poisoned)?;
        let bytes = arena.bytes.load(Ordering::Relaxed).max(charge.bytes);
        let entries = arena.entries.load(Ordering::Relaxed).max(charge.entries);
        let used = Charge {
            bytes: state
                .used
                .bytes
                .checked_add(bytes - arena.bytes.load(Ordering::Relaxed))
                .ok_or(Error::Limit("cloud resident memory"))?,
            entries: state
                .used
                .entries
                .checked_add(entries - arena.entries.load(Ordering::Relaxed))
                .ok_or(Error::Limit("cloud resident memory"))?,
        };
        within(limits, used)?;
        state.used = used;
        arena.bytes.store(bytes, Ordering::Relaxed);
        arena.entries.store(entries, Ordering::Relaxed);
        state.pending = Some(prospective);
        Ok(())
    }
    pub(crate) fn discard_pending(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.pending = None;
        }
    }
    pub(crate) fn committed(&self) -> Result<()> {
        if self.limits.is_none() {
            return Ok(());
        }
        let mut state = self.state.lock().map_err(|_| Error::Poisoned)?;
        if let Some(catalog) = state.pending.take() {
            state.catalog = catalog;
        }
        Ok(())
    }
}
/// This reservation is declared after the models it protects. Sharing it moves
/// ownership across gate release without refunding any arena capacity early.
pub(crate) struct Reservation {
    budget: Arc<Budget>,
    bytes: AtomicU64,
    entries: AtomicU64,
}
impl Reservation {
    pub(crate) fn discard_pending(&self) {
        self.budget.discard_pending();
    }
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if self.budget.limits.is_some() {
            if let Ok(mut state) = self.budget.state.lock() {
                state.used.bytes -= self.bytes.load(Ordering::Relaxed);
                state.used.entries -= self.entries.load(Ordering::Relaxed);
            }
        }
    }
}
