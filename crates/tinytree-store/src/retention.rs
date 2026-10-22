//! Durable owner-scoped roots. Counters and references share one transaction;
//! limits reject new promises, never expire or evict an existing promise.
use super::*;

const PREFIX: &str = "_tinytree/holds/v1/";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct RetentionOwner {
    pub principal: [u8; 32],
    pub client: [u8; 16],
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionLimits {
    pub max_known_clients_per_repository: u64,
    pub max_known_clients_per_principal: u64,
    pub max_root_records_per_repository: u64,
    pub max_root_records_per_principal: u64,
    pub max_root_records_per_client: u64,
    pub max_receipts_per_repository: u64,
    pub max_receipts_per_principal: u64,
    pub max_receipts_per_client: u64,
    pub max_live_roots_per_client: u64,
    pub max_unique_bytes_per_client: u64,
    pub max_unique_bytes_per_principal: u64,
    pub max_quota_scan_roots: usize,
}
impl Default for RetentionLimits {
    fn default() -> Self {
        Self {
            max_known_clients_per_repository: 4096,
            max_known_clients_per_principal: 128,
            max_root_records_per_repository: 100_000,
            max_root_records_per_principal: 16_384,
            max_root_records_per_client: 4096,
            max_receipts_per_repository: 400_000,
            max_receipts_per_principal: 65_536,
            max_receipts_per_client: 16_384,
            max_live_roots_per_client: 4096,
            max_unique_bytes_per_client: 8 * 1024 * 1024 * 1024,
            max_unique_bytes_per_principal: 16 * 1024 * 1024 * 1024,
            max_quota_scan_roots: 100_000,
        }
    }
}
impl RetentionLimits {
    pub fn validate(&self) -> Result<()> {
        for value in [
            self.max_known_clients_per_repository,
            self.max_known_clients_per_principal,
            self.max_root_records_per_repository,
            self.max_root_records_per_principal,
            self.max_root_records_per_client,
            self.max_live_roots_per_client,
        ] {
            if !(1..=1_000_000).contains(&value) {
                return Err(Error::Invalid("retention namespace/root limits"));
            }
        }
        for value in [
            self.max_receipts_per_repository,
            self.max_receipts_per_principal,
            self.max_receipts_per_client,
        ] {
            if !(1..=10_000_000).contains(&value) {
                return Err(Error::Invalid("retention receipt limits"));
            }
        }
        if self.max_unique_bytes_per_client == 0
            || self.max_unique_bytes_per_principal == 0
            || !(1..=1_000_000).contains(&self.max_quota_scan_roots)
        {
            return Err(Error::Invalid("retention byte/work limits"));
        }
        Ok(())
    }
}

pub fn retention_reference(owner: RetentionOwner, commit: CommitId) -> Result<String> {
    if commit == CommitId::ROOT {
        return Err(Error::Invalid("synthetic root cannot be retained"));
    }
    Ok(format!(
        "{PREFIX}{}/{}/{}",
        hex(&owner.principal),
        hex(&owner.client),
        hex(&commit.0)
    ))
}

pub fn parse_retention_reference(value: &str) -> Option<(RetentionOwner, CommitId)> {
    let mut parts = value.strip_prefix(PREFIX)?.split('/');
    let principal = unhex(parts.next()?)?;
    let client = unhex(parts.next()?)?;
    let commit = CommitId(unhex(parts.next()?)?);
    if parts.next().is_some() || commit == CommitId::ROOT {
        return None;
    }
    Some((RetentionOwner { principal, client }, commit))
}
fn unhex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    fn nibble(b: u8) -> Option<u8> {
        match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        }
    }
    let mut result = [0; N];
    for (pair, output) in value.as_bytes().chunks_exact(2).zip(&mut result) {
        *output = nibble(pair[0])? * 16 + nibble(pair[1])?;
    }
    Some(result)
}
fn principal_prefix(owner: RetentionOwner) -> String {
    format!("{PREFIX}{}/", hex(&owner.principal))
}
fn client_prefix(owner: RetentionOwner) -> String {
    format!("{}{}/", principal_prefix(owner), hex(&owner.client))
}
fn accounting_keys(repo: &str, owner: RetentionOwner) -> [Vec<u8>; 3] {
    [
        key(repo, b"retention/v1/repository"),
        key(
            repo,
            format!("retention/v1/principal/{}", hex(&owner.principal)).as_bytes(),
        ),
        key(
            repo,
            format!(
                "retention/v1/client/{}/{}",
                hex(&owner.principal),
                hex(&owner.client)
            )
            .as_bytes(),
        ),
    ]
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Accounting {
    clients: u64,
    roots: u64,
    receipts: u64,
    live: u64,
}
impl Accounting {
    fn validate(&self) -> Result<()> {
        if self.live > self.roots || self.live > self.receipts {
            return Err(Error::Corrupt("retention accounting"));
        }
        Ok(())
    }
}
pub(super) struct RetentionUpdate {
    records: [(Vec<u8>, Accounting); 3],
}
impl RetentionUpdate {
    pub(super) fn write(
        self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        space: &SingleWriterTxKeyspace,
    ) -> Result<()> {
        for (key, record) in self.records {
            tx.insert(space, key, encode(&record)?);
        }
        Ok(())
    }
}
fn add(value: &mut u64, count: u64) -> Result<()> {
    *value = value
        .checked_add(count)
        .ok_or(Error::Limit("retention accounting overflow"))?;
    Ok(())
}

impl Inner {
    /// Called only under the final publication/GC gate, after replay and CAS.
    pub(super) fn retention_update(
        &self,
        repo: &str,
        owner: RetentionOwner,
        request: &PublishRequest,
        current: &Reference,
        limits: &RetentionLimits,
    ) -> Result<RetentionUpdate> {
        let acquire = request.new_commit.is_some();
        if acquire == current.commit.is_some() {
            return Err(Error::Invalid(if acquire {
                "retention reference already held"
            } else {
                "retention reference is not held"
            }));
        }
        let keys = accounting_keys(repo, owner);
        let view = self.db.read_tx();
        let mut records = [Accounting::default(); 3];
        let mut present = [false; 3];
        let prefixes = [
            PREFIX.to_owned(),
            principal_prefix(owner),
            client_prefix(owner),
        ];
        for index in 0..3 {
            if let Some(bytes) = view.get(&self.spaces.meta, &keys[index])? {
                records[index] = decode(&bytes)?;
                records[index].validate()?;
                present[index] = true;
            } else if view
                .prefix(&self.spaces.refs, key(repo, prefixes[index].as_bytes()))
                .next()
                .is_some()
            {
                return Err(Error::Corrupt("missing retention accounting"));
            }
        }
        if present[2] && (!present[1] || !present[0]) {
            return Err(Error::Corrupt("retention accounting scope"));
        }
        if current.generation != 0 && !present[2] {
            return Err(Error::Corrupt("retention reference accounting missing"));
        }
        if !present[2] {
            add(&mut records[0].clients, 1)?;
            add(&mut records[1].clients, 1)?;
        }
        let new_root = u64::from(current.generation == 0);
        for record in &mut records {
            add(&mut record.roots, new_root)?;
            add(&mut record.receipts, 1)?;
            if acquire {
                add(&mut record.live, 1)?;
            } else {
                record.live = record
                    .live
                    .checked_sub(1)
                    .ok_or(Error::Corrupt("retention live accounting"))?;
            }
        }
        if acquire {
            if records[0].clients > limits.max_known_clients_per_repository
                || records[1].clients > limits.max_known_clients_per_principal
                || records[0].roots > limits.max_root_records_per_repository
                || records[1].roots > limits.max_root_records_per_principal
                || records[2].roots > limits.max_root_records_per_client
                || records[2].live > limits.max_live_roots_per_client
            {
                return Err(Error::Limit("retention namespace/root quota"));
            }
            // One future release receipt per live hold remains available even
            // when admission fills the cap. Lowered policies never block release.
            for (record, maximum) in records.iter().zip([
                limits.max_receipts_per_repository,
                limits.max_receipts_per_principal,
                limits.max_receipts_per_client,
            ]) {
                if record
                    .receipts
                    .checked_add(record.live)
                    .is_none_or(|n| n > maximum)
                {
                    return Err(Error::Limit("retention receipt quota"));
                }
            }
            let mut principal_roots = Vec::new();
            let mut client_roots = Vec::new();
            let prefix = key(repo, principal_prefix(owner).as_bytes());
            for (count, item) in view.prefix(&self.spaces.refs, &prefix).enumerate() {
                if count >= limits.max_quota_scan_roots {
                    return Err(Error::Limit("retention quota scan"));
                }
                let (stored_key, bytes) = item.into_inner()?;
                let suffix = stored_key
                    .as_ref()
                    .strip_prefix(key(repo, b"").as_slice())
                    .ok_or(Error::Corrupt("retention key scope"))?;
                let reference_name = std::str::from_utf8(suffix)
                    .map_err(|_| Error::Corrupt("retention reference name"))?;
                let (actual_owner, named_commit) = parse_retention_reference(reference_name)
                    .ok_or(Error::Corrupt("retention reference name"))?;
                if actual_owner.principal != owner.principal {
                    return Err(Error::Corrupt("retention principal scope"));
                }
                let reference: StoredReference = decode(&bytes)?;
                if let Some(descriptor) = reference.commit {
                    let descriptor = descriptor.descriptor()?;
                    if descriptor.key.kind != Kind::Commit || descriptor.key.id.0 != named_commit.0
                    {
                        return Err(Error::Corrupt("retention reference target"));
                    }
                    principal_roots.push(descriptor.key);
                    if actual_owner.client == owner.client {
                        client_roots.push(descriptor.key);
                    }
                }
            }
            let target = ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(request.new_commit.unwrap().0),
            };
            principal_roots.push(target);
            client_roots.push(target);
            for (roots, maximum) in [
                (client_roots, limits.max_unique_bytes_per_client),
                (principal_roots, limits.max_unique_bytes_per_principal),
            ] {
                let closure = self.closure(repo, roots, true, false)?;
                let mut size = 0u64;
                for object in closure {
                    let descriptor = self
                        .descriptor(repo, object)?
                        .ok_or(Error::MissingObject(object))?;
                    size = size
                        .checked_add(descriptor.size_bytes)
                        .ok_or(Error::Limit("retention closure byte quota"))?;
                    if size > maximum {
                        return Err(Error::Limit("retention closure byte quota"));
                    }
                }
            }
        }
        Ok(RetentionUpdate {
            records: [
                (keys[0].clone(), records[0]),
                (keys[1].clone(), records[1]),
                (keys[2].clone(), records[2]),
            ],
        })
    }
}

struct ReopenBudget {
    records: usize,
    bytes: u64,
    maximum_records: usize,
    maximum_bytes: u64,
}
impl ReopenBudget {
    fn visit(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.records = self
            .records
            .checked_add(1)
            .ok_or(Error::Limit("retention validation records"))?;
        self.bytes = self
            .bytes
            .checked_add(key.len() as u64)
            .and_then(|n| n.checked_add(value.len() as u64))
            .ok_or(Error::Limit("retention validation bytes"))?;
        if self.records > self.maximum_records || self.bytes > self.maximum_bytes {
            return Err(Error::Limit("retention reopen validation"));
        }
        Ok(())
    }
}
struct HoldState {
    scope_keys: [Vec<u8>; 3],
    named_commit: CommitId,
    generation: u64,
    target: Option<Descriptor>,
    receipts: u64,
}
fn physical_parts(value: &[u8]) -> Result<(&str, &[u8])> {
    let delimiter = value
        .iter()
        .position(|&b| b == 0)
        .ok_or(Error::Corrupt("metadata repository key"))?;
    let repo = std::str::from_utf8(&value[..delimiter])
        .map_err(|_| Error::Corrupt("metadata repository key"))?;
    validate_repository_id(repo).map_err(|_| Error::Corrupt("metadata repository key"))?;
    Ok((repo, &value[delimiter + 1..]))
}
fn reserved(value: &str) -> bool {
    value == "_tinytree" || value.starts_with("_tinytree/")
}
fn private_target(reference: &StoredReference, named: CommitId) -> Result<Option<Descriptor>> {
    if reference.generation == 0
        || reference.generation % 2 != u64::from(reference.commit.is_some())
    {
        return Err(Error::Corrupt("retention generation state"));
    }
    let descriptor = reference
        .commit
        .as_ref()
        .map(StoredDescriptor::descriptor)
        .transpose()?;
    if descriptor.is_some_and(|d| d.key.kind != Kind::Commit || d.key.id.0 != named.0) {
        return Err(Error::Corrupt("retention immutable target"));
    }
    Ok(descriptor)
}
impl Inner {
    /// Rebuild exact scope totals from authoritative names and replay history.
    /// The streaming budget includes public records, because a receipt cannot
    /// be classified as private without reading its stored reference name.
    pub(super) fn validate_retention_on_reopen(&self) -> Result<()> {
        let mut budget = ReopenBudget {
            records: 0,
            bytes: 0,
            maximum_records: self.config.max_retention_validation_records,
            maximum_bytes: self.config.max_retention_validation_bytes,
        };
        let view = self.db.read_tx();
        let mut expected: HashMap<Vec<u8>, Accounting> = HashMap::new();
        let mut index: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut holds: Vec<HoldState> = Vec::new();
        for item in view.prefix(&self.spaces.refs, b"") {
            let (physical, bytes) = item.into_inner()?;
            budget.visit(&physical, &bytes)?;
            let (repo, suffix) = physical_parts(&physical)?;
            let name = std::str::from_utf8(suffix).map_err(|_| Error::Corrupt("reference name"))?;
            if !reserved(name) {
                continue;
            }
            let (owner, named_commit) = parse_retention_reference(name)
                .ok_or(Error::Corrupt("retention reference name"))?;
            match self.repository(repo) {
                Ok(_) => {}
                Err(Error::UnknownRepository) => {
                    return Err(Error::Corrupt("retention repository missing"));
                }
                Err(error) => return Err(error),
            }
            let stored: StoredReference = decode(&bytes)?;
            let target = private_target(&stored, named_commit)?;
            let keys = accounting_keys(repo, owner);
            let new_client = !expected.contains_key(&keys[2]);
            for (scope, key) in keys.iter().enumerate() {
                let record = expected.entry(key.clone()).or_default();
                if scope < 2 && new_client {
                    add(&mut record.clients, 1)?;
                }
                add(&mut record.roots, 1)?;
                add(&mut record.live, u64::from(target.is_some()))?;
            }
            index.insert(physical.to_vec(), holds.len());
            holds.push(HoldState {
                scope_keys: keys,
                named_commit,
                generation: stored.generation,
                target,
                receipts: 0,
            });
        }
        let mut generations: HashSet<(usize, u64)> = HashSet::new();
        for item in view.prefix(&self.spaces.receipts, b"") {
            let (physical, bytes) = item.into_inner()?;
            budget.visit(&physical, &bytes)?;
            let (repo, suffix) = physical_parts(&physical)?;
            let id = std::str::from_utf8(suffix).map_err(|_| Error::Corrupt("receipt key"))?;
            request_token(id).map_err(|_| Error::Corrupt("receipt key"))?;
            let receipt: ReceiptRecord = decode(&bytes)?;
            if !reserved(&receipt.reference_name) {
                continue;
            }
            let (_owner, named) = parse_retention_reference(&receipt.reference_name)
                .ok_or(Error::Corrupt("retention receipt name"))?;
            let key = key(repo, receipt.reference_name.as_bytes());
            let number = *index
                .get(&key)
                .ok_or(Error::Corrupt("retention receipt without reference"))?;
            let state = &mut holds[number];
            if state.named_commit != named
                || receipt.reference.generation > state.generation
                || !generations.insert((number, receipt.reference.generation))
            {
                return Err(Error::Corrupt("retention receipt generation"));
            }
            let target = private_target(&receipt.reference, named)?;
            let canonical_request = PublishRequest {
                idempotency_key: id.to_owned(),
                reference_name: receipt.reference_name.clone(),
                expected_generation: receipt.reference.generation - 1,
                new_commit: target.map(|_| named),
                staging_leases: vec![],
            };
            if receipt.fingerprint != fingerprint(&canonical_request) {
                return Err(Error::Corrupt("retention receipt fingerprint"));
            }
            if let Some(target) = target {
                if state.target.is_some_and(|expected| expected != target) {
                    return Err(Error::Corrupt("retention historical target"));
                }
                state.target = Some(target);
            }
            add(&mut state.receipts, 1)?;
            for key in &state.scope_keys {
                add(
                    &mut expected
                        .get_mut(key)
                        .ok_or(Error::Corrupt("retention receipt scope"))?
                        .receipts,
                    1,
                )?;
            }
        }
        if holds.iter().any(|state| state.receipts != state.generation) {
            return Err(Error::Corrupt("retention replay history incomplete"));
        }
        for item in view.prefix(&self.spaces.meta, b"") {
            let (physical, bytes) = item.into_inner()?;
            budget.visit(&physical, &bytes)?;
            // The ordinary store-format marker is not repository-scoped.
            if physical.as_ref() == b"format" {
                continue;
            }
            let (_repo, suffix) = physical_parts(&physical)?;
            if !suffix.starts_with(b"retention/") {
                continue;
            }
            let actual: Accounting = decode(&bytes)?;
            let derived = expected
                .remove(physical.as_ref())
                .ok_or(Error::Corrupt("unexpected retention accounting"))?;
            if actual != derived {
                return Err(Error::Corrupt("retention accounting totals"));
            }
        }
        if !expected.is_empty() {
            return Err(Error::Corrupt("retention accounting missing"));
        }
        Ok(())
    }
}
