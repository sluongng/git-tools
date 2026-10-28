//! Explicit, owner-scoped retirement. No automatic expiration policy lives here.
//! A retained successor proves conservative undo coverage; exact retired capsules
//! expire. Durable jobs must pin their exact selector before admitting work.
use super::*;

pub(super) fn zero(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloudPublicationIdentity {
    pub workspace: [u8; 32],
    pub generation: u64,
    pub nonce: [u8; 16],
}
impl CloudPublicationIdentity {
    /// Display token only. Durable lookup uses a binary domain that legacy
    /// ASCII request tokens cannot occupy, even if their text looks identical.
    pub fn token(self) -> String {
        let hex = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        format!(
            "r2-{}-{:016x}-{}",
            hex(&self.workspace),
            self.generation,
            hex(&self.nonce)
        )
    }
    fn suffix(self) -> Vec<u8> {
        let mut key = vec![0];
        key.extend_from_slice(&self.workspace);
        key.extend_from_slice(&self.generation.to_be_bytes());
        key.extend_from_slice(&self.nonce);
        key
    }
    fn key(self, repo: &str, owner: CloudOwner) -> Vec<u8> {
        let mut key = scope(repo, owner);
        key.extend(self.suffix());
        key
    }
    pub(super) fn parse(suffix: &[u8]) -> Result<Self> {
        if suffix.len() != 57 || suffix[0] != 0 {
            return Err(Error::Corrupt("cloud publication identity"));
        }
        let value = Self {
            workspace: suffix[1..33].try_into().unwrap(),
            generation: u64::from_be_bytes(suffix[33..41].try_into().unwrap()),
            nonce: suffix[41..57].try_into().unwrap(),
        };
        if value.generation == 0 {
            return Err(Error::Corrupt("cloud publication generation"));
        }
        Ok(value)
    }
}
pub(super) fn publication_receipt_key(
    repo: &str,
    owner: CloudOwner,
    request: &PublishCloudRequest,
    identity: Option<CloudPublicationIdentity>,
) -> Result<Vec<u8>> {
    Ok(match identity {
        Some(identity)
            if identity.generation != 0 && request.idempotency_key == identity.token() =>
        {
            identity.key(repo, owner)
        }
        Some(_) => return Err(Error::Invalid("cloud publication identity token")),
        None => receipt_key(repo, owner, &request.idempotency_key),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloudRetentionState {
    pub retired_through: u64,
    pub retention_revision: u64,
    pub retained_versions: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloudRetirementRequest {
    pub expected_current: CloudVersionSelector,
    pub expected_floor: u64,
    pub expected_revision: u64,
    pub retire_through: u64,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CloudRetirementReceipt {
    pub retired_through: u64,
    pub retention_revision: u64,
}
impl CloudRetirementRequest {
    fn fingerprint(self) -> [u8; 32] {
        let mut h = blake3::Hasher::new();
        h.update(b"tinytree-cloud-retirement-v1\0");
        h.update(&self.expected_current.workspace);
        h.update(&self.expected_current.generation.to_be_bytes());
        h.update(&self.expected_current.manifest.0);
        h.update(&self.expected_current.selected.blake3);
        h.update(&self.expected_current.selected.size_bytes.to_be_bytes());
        h.update(&self.expected_floor.to_be_bytes());
        h.update(&self.expected_revision.to_be_bytes());
        h.update(&self.retire_through.to_be_bytes());
        *h.finalize().as_bytes()
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Pin {
    acquire_not_after_unix_seconds: i64,
    workspace: [u8; 32],
    generation: u64,
    manifest: [u8; 32],
    digest: [u8; 32],
    size: u64,
}
impl Pin {
    fn new(selector: CloudVersionSelector, identity: CloudVersionPinIdentity) -> Self {
        Self {
            acquire_not_after_unix_seconds: identity.acquire_not_after_unix_seconds,
            workspace: selector.workspace,
            generation: selector.generation,
            manifest: selector.manifest.0,
            digest: selector.selected.blake3,
            size: selector.selected.size_bytes,
        }
    }
    fn selector(&self) -> CloudVersionSelector {
        CloudVersionSelector {
            workspace: self.workspace,
            generation: self.generation,
            manifest: ManifestId(self.manifest),
            selected: BlobDigest {
                blake3: self.digest,
                size_bytes: self.size,
            },
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum LifecycleRecord {
    LegacyClosed,
    Pin(Pin),
    Released(Pin),
}
fn closed_key(repo: &str, owner: CloudOwner) -> Vec<u8> {
    let mut key = scope(repo, owner);
    key.push(0);
    key
}
pub(super) fn replay_close_record(repo: &str, owner: CloudOwner) -> Result<(Vec<u8>, Vec<u8>)> {
    Ok((
        closed_key(repo, owner),
        encode(&LifecycleRecord::LegacyClosed)?,
    ))
}
fn pin_prefix(repo: &str, owner: CloudOwner, workspace: [u8; 32]) -> Vec<u8> {
    let mut key = scope(repo, owner);
    key.push(1);
    key.extend_from_slice(&workspace);
    key
}
fn pin_key(
    repo: &str,
    owner: CloudOwner,
    workspace: [u8; 32],
    identity: CloudVersionPinIdentity,
) -> Vec<u8> {
    let mut key = pin_prefix(repo, owner, workspace);
    key.extend_from_slice(&identity.pin_id);
    key.extend_from_slice(&identity.acquire_not_after_unix_seconds.to_be_bytes());
    key
}
const PIN_ADMISSION_FLOOR: &[u8] = b"cloud-pin-admission-floor";
fn pin_record_matches(suffix: &[u8], pin: &Pin) -> bool {
    suffix.len() == 57
        && suffix[0] == 1
        && suffix[1..33] == pin.workspace
        && pin.acquire_not_after_unix_seconds > 0
        && suffix[49..57] == pin.acquire_not_after_unix_seconds.to_be_bytes()
        && suffix[33..49] != [0; 16]
}
struct PinAdmission {
    now: i64,
    expired: Vec<Vec<u8>>,
    records: u64,
}
impl WorkspaceRecord {
    pub(super) fn validate_lifecycle(&self) -> Result<()> {
        if self.retired_through >= self.generation && self.generation != 0
            || (self.retired_through == 0) != self.boundary_manifest.is_none()
            || (self.retired_through == 0) != (self.retention_revision == 0)
            || (self.retired_through == 0) != self.last_retirement.is_none()
            || (self.generation == 0 && self.retired_through != 0)
        {
            return Err(Error::Corrupt("cloud workspace retention anchor"));
        }
        Ok(())
    }
}
impl Inner {
    /// Caller owns the Store gate. Released admission fences are metadata, not
    /// roots; active source holds remain live independently of their deadline.
    pub(super) fn active_version_pin_selectors(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        scan: &mut Scan,
    ) -> Result<Vec<CloudVersionSelector>> {
        let view = self.db.read_tx();
        let mut selected = Vec::new();
        let mut records = 0u64;
        for item in view.prefix(
            &self.spaces.cloud.lifecycle,
            pin_prefix(repo, owner, workspace),
        ) {
            let (key, bytes) = item.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            records += 1;
            if records > self.config.cloud.max_pin_lifecycle_records_per_workspace {
                return Err(Error::Limit("cloud pin lifecycle records"));
            }
            let (_, _, suffix) = parse_scope(&key)?;
            match decode::<LifecycleRecord>(&bytes)? {
                LifecycleRecord::Pin(pin) if pin_record_matches(suffix, &pin) => {
                    scan.visit(self, &key, &[])?;
                    selected.push(pin.selector());
                }
                LifecycleRecord::Released(pin) if pin_record_matches(suffix, &pin) => {}
                _ => return Err(Error::Corrupt("cloud version pin identity")),
            }
        }
        Ok(selected)
    }
    pub(super) fn workspace_lifecycle(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
    ) -> Result<WorkspaceRecord> {
        let record = self
            .db
            .read_tx()
            .get(
                &self.spaces.cloud.workspaces,
                workspace_key(repo, owner, workspace),
            )?
            .map(|bytes| decode::<WorkspaceRecord>(&bytes))
            .transpose()?
            .unwrap_or(WorkspaceRecord {
                generation: 0,
                manifest: [0; 32],
                retired_through: 0,
                boundary_manifest: None,
                retention_revision: 0,
                last_retirement: None,
            });
        record.validate_lifecycle()?;
        // A smaller rolling policy is a target for the next atomic publication,
        // not permission to truncate or reject an otherwise valid catalog here.
        if !self.config.cloud.rolling_retention_enabled
            && record.generation - record.retired_through
                > self.config.cloud.max_versions_per_workspace
        {
            return Err(Error::Limit("cloud retained version quota"));
        }
        Ok(record)
    }
    pub(super) fn legacy_replay_closed(&self, repo: &str, owner: CloudOwner) -> Result<bool> {
        self.db
            .read_tx()
            .get(&self.spaces.cloud.lifecycle, closed_key(repo, owner))?
            .map(|bytes| match decode::<LifecycleRecord>(&bytes)? {
                LifecycleRecord::LegacyClosed => Ok(true),
                _ => Err(Error::Corrupt("cloud replay namespace")),
            })
            .transpose()
            .map(|value| value.unwrap_or(false))
    }
    pub(super) fn check_publication_identity(
        &self,
        repo: &str,
        owner: CloudOwner,
        request: &PublishCloudRequest,
        identity: Option<CloudPublicationIdentity>,
    ) -> Result<()> {
        match identity {
            None if self.legacy_replay_closed(repo, owner)? => Err(Error::CloudReplayExpired),
            None => Ok(()),
            Some(id) => {
                if id.workspace != request.workspace
                    || request.expected_generation.checked_add(1) != Some(id.generation)
                {
                    return Err(Error::Invalid("cloud publication identity binding"));
                }
                if id.generation
                    <= self
                        .workspace_lifecycle(repo, owner, id.workspace)?
                        .retired_through
                {
                    return Err(Error::CloudReplayExpired);
                }
                Ok(())
            }
        }
    }
    fn selected_version(
        &self,
        repo: &str,
        owner: CloudOwner,
        selector: CloudVersionSelector,
        scan: &mut Scan,
        gate: &Runtime,
    ) -> Result<(VersionRecord, RecoveryManifest)> {
        let view = self.db.read_tx();
        let workspace_key = workspace_key(repo, owner, selector.workspace);
        let workspace_bytes = view
            .get(&self.spaces.cloud.workspaces, &workspace_key)?
            .ok_or(Error::Invalid("cloud workspace unavailable"))?;
        scan.visit(self, &workspace_key, &workspace_bytes)?;
        let state: WorkspaceRecord = decode(&workspace_bytes)?;
        state.validate_lifecycle()?;
        let protection = if selector.generation != 0 && selector.generation <= state.retired_through
        {
            let protection = self.version_protection(
                repo,
                owner,
                selector.workspace,
                selector.generation,
                gate,
                scan,
            )?;
            if !protection.held() {
                return Err(Error::CloudVersionExpired);
            }
            Some(protection)
        } else {
            None
        };
        let key = version_key(repo, owner, selector.workspace, selector.generation);
        let record_bytes =
            view.get(&self.spaces.cloud.versions, &key)?
                .ok_or(if protection.is_some() {
                    Error::Corrupt("cloud protected version missing")
                } else {
                    Error::Invalid("cloud version unavailable")
                })?;
        scan.visit(self, &key, &record_bytes)?;
        let record: VersionRecord = decode(&record_bytes)?;
        if record.workspace != selector.workspace
            || record.generation != selector.generation
            || record.manifest != selector.manifest.0
        {
            return Err(Error::Invalid("cloud exact version selection"));
        }
        let bytes = view
            .get(
                &self.spaces.cloud.manifests,
                manifest_key(repo, owner, selector.manifest),
            )?
            .ok_or(Error::Corrupt("cloud version manifest missing"))?;
        scan.visit(self, &manifest_key(repo, owner, selector.manifest), &bytes)?;
        if let Some(protection) = protection {
            protection.verify(&record, BlobDigest::of(&bytes))?;
        }
        if BlobDigest::of(&bytes) != selector.selected {
            return Err(Error::Invalid("cloud exact version digest"));
        }
        let manifest =
            self.decode_recovery(&bytes, selector.manifest, self.config.cloud.manifest_limits)?;
        if manifest.format() != record.format {
            return Err(Error::Corrupt("cloud version manifest format"));
        }
        Ok((record, manifest))
    }
    pub(super) fn retained_receipt_target(
        &self,
        repo: &str,
        owner: CloudOwner,
        suffix: &[u8],
        bytes: &[u8],
        plan: &RetainedPlan,
    ) -> Result<Vec<u8>> {
        let record: ReceiptRecord = decode(bytes)?;
        match record.format {
            1 => {
                let name =
                    std::str::from_utf8(suffix).map_err(|_| Error::Corrupt("cloud receipt key"))?;
                request_token(name)?;
            }
            2 => {
                let identity = CloudPublicationIdentity::parse(suffix)?;
                if identity.workspace != record.version.workspace
                    || identity.generation != record.version.generation
                    || store_feature(
                        self.db
                            .read_tx()
                            .get(&self.spaces.meta, b"format")?
                            .as_deref(),
                    )? < 12
                {
                    return Err(Error::Corrupt("cloud structured receipt identity"));
                }
            }
            _ => return Err(Error::Corrupt("cloud receipt format")),
        }
        let target = version_key(
            repo,
            owner,
            record.version.workspace,
            record.version.generation,
        );
        let actual = plan
            .versions
            .get(&target)
            .ok_or(Error::Corrupt("cloud receipt version missing"))?;
        if encode(actual)? != encode(&record.version)? {
            return Err(Error::Corrupt("cloud receipt replay history"));
        }
        Ok(target)
    }
    pub(super) fn validate_lifecycle_on_reopen(
        &self,
        expected: &mut HashMap<Vec<u8>, Accounting>,
        scan: &mut Scan,
        plan: &RetainedPlan,
    ) -> Result<()> {
        let view = self.db.read_tx();
        let activated = view.get(&self.spaces.meta, b"cloud-retirement-activated")?;
        if activated.as_deref().is_some_and(|bytes| bytes != [1]) {
            return Err(Error::Corrupt("cloud retirement activation marker"));
        }
        if let Some(bytes) = view.get(&self.spaces.meta, PIN_ADMISSION_FLOOR)? {
            scan.visit(self, PIN_ADMISSION_FLOOR, &bytes)?;
            let value: [u8; 8] = bytes
                .as_ref()
                .try_into()
                .map_err(|_| Error::Corrupt("cloud pin admission floor"))?;
            if i64::from_be_bytes(value) < 0 {
                return Err(Error::Corrupt("cloud pin admission floor"));
            }
        }
        let mut closed = false;
        let mut pins = HashMap::<Vec<u8>, usize>::new();
        for item in view.iter(&self.spaces.cloud.lifecycle) {
            let (key, bytes) = item.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (repo, owner, suffix) = parse_scope(&key)?;
            self.repository(repo)?;
            metadata_account(
                expected.entry(scope(repo, owner)).or_default(),
                &key,
                &bytes,
            )?;
            match decode::<LifecycleRecord>(&bytes)? {
                LifecycleRecord::LegacyClosed if suffix == [0] => {
                    closed = true;
                    let mut anchored = false;
                    for item in view.prefix(&self.spaces.cloud.workspaces, scope(repo, owner)) {
                        let (key, bytes) = item.into_inner()?;
                        scan.visit(self, &key, &bytes)?;
                        let record: WorkspaceRecord = decode(&bytes)?;
                        anchored |= record.retired_through != 0;
                    }
                    if !anchored {
                        return Err(Error::Corrupt(
                            "cloud replay close without retention anchor",
                        ));
                    }
                }
                LifecycleRecord::Pin(pin) if pin_record_matches(suffix, &pin) => {
                    let key = workspace_key(repo, owner, pin.workspace);
                    let count = pins.entry(key).or_default();
                    *count += 1;
                    if *count > self.config.cloud.max_pin_lifecycle_records_per_workspace as usize {
                        return Err(Error::Limit("cloud durable version pins"));
                    }
                    // The plan has already verified this immutable version and
                    // manifest. Check exact selectors from admitted descriptors,
                    // never decode the same large manifest once per durable pin.
                    let record = plan
                        .versions
                        .get(&version_key(repo, owner, pin.workspace, pin.generation))
                        .ok_or(Error::Corrupt("cloud durable pin version missing"))?;
                    let descriptor = plan
                        .manifest_descriptors
                        .get(&manifest_key(repo, owner, ManifestId(pin.manifest)))
                        .ok_or(Error::Corrupt("cloud durable pin manifest missing"))?;
                    if record.manifest != pin.manifest || *descriptor != pin.selector().selected {
                        return Err(Error::Corrupt("cloud durable pin selection"));
                    }
                }
                LifecycleRecord::Released(pin) if pin_record_matches(suffix, &pin) => {
                    let key = workspace_key(repo, owner, pin.workspace);
                    let count = pins.entry(key).or_default();
                    *count += 1;
                    if *count > self.config.cloud.max_pin_lifecycle_records_per_workspace as usize {
                        return Err(Error::Limit("cloud pin lifecycle records"));
                    }
                }
                _ => return Err(Error::Corrupt("cloud lifecycle record identity")),
            }
        }
        if closed != activated.is_some() {
            return Err(Error::Corrupt("cloud retirement activation mismatch"));
        }
        for item in view.iter(&self.spaces.cloud.workspaces) {
            let (key, bytes) = item.into_inner()?;
            scan.visit(self, &key, &bytes)?;
            let (repo, owner, _) = parse_scope(&key)?;
            let record: WorkspaceRecord = decode(&bytes)?;
            record.validate_lifecycle()?;
            if record.retired_through != 0 && !self.legacy_replay_closed(repo, owner)? {
                return Err(Error::Corrupt("cloud retention replay namespace missing"));
            }
        }
        Ok(())
    }
}

impl Store {
    pub fn publish_cloud_version_v2(
        &self,
        repo: &str,
        owner: CloudOwner,
        identity: CloudPublicationIdentity,
        request: &PublishCloudRequest,
        now: i64,
        limits: &CloudPublicationLimits,
    ) -> Result<CloudPublication> {
        self.publish_cloud_version_key(repo, owner, request, None, now, limits, Some(identity))
    }
    pub fn publish_captured_cloud_version_v2(
        &self,
        repo: &str,
        owner: CloudOwner,
        identity: CloudPublicationIdentity,
        request: &PublishCapturedCloudRequest,
        now: i64,
        limits: &CloudPublicationLimits,
    ) -> Result<CloudPublication> {
        self.publish_cloud_version_key(
            repo,
            owner,
            &request.publication,
            Some(&request.capture_data),
            now,
            limits,
            Some(identity),
        )
    }
    pub fn cloud_publication_v2(
        &self,
        repo: &str,
        owner: CloudOwner,
        identity: CloudPublicationIdentity,
    ) -> Result<Option<CloudPublication>> {
        if identity.generation == 0 {
            return Err(Error::Invalid("cloud publication generation"));
        }
        let _gate = self.inner.lock_read()?;
        self.inner.repository(repo)?;
        if identity.generation
            <= self
                .inner
                .workspace_lifecycle(repo, owner, identity.workspace)?
                .retired_through
        {
            return Err(Error::CloudReplayExpired);
        }
        let row = self
            .inner
            .db
            .read_tx()
            .get(&self.inner.spaces.cloud.receipts, identity.key(repo, owner))?;
        if let Some(row) = row {
            let record: ReceiptRecord = decode(&row)?;
            if record.format != 2
                || record.version.workspace != identity.workspace
                || record.version.generation != identity.generation
            {
                return Err(Error::Corrupt("cloud publication identity receipt"));
            }
            return Ok(Some(publication(&identity.token(), &record.version)));
        }
        Ok(None)
    }
    pub fn cloud_retention_state(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
    ) -> Result<CloudRetentionState> {
        let _gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let state = self.inner.workspace_lifecycle(repo, owner, workspace)?;
        Ok(CloudRetentionState {
            retired_through: state.retired_through,
            retention_revision: state.retention_revision,
            retained_versions: state.generation - state.retired_through,
        })
    }
    /// Admission identities never renew uncertain work. A successful hold stays
    /// active until explicit release, even after its acquisition deadline.
    pub fn pin_cloud_version(
        &self,
        repo: &str,
        owner: CloudOwner,
        identity: CloudVersionPinIdentity,
        selector: CloudVersionSelector,
        clock: impl FnOnce() -> Result<i64>,
    ) -> Result<()> {
        identity.validate()?;
        if selector.generation == 0 {
            return Err(Error::Invalid("cloud pin selector"));
        }
        let gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        if self.replay_version_pin(repo, owner, identity, selector)? {
            return Ok(());
        }
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let _plan = self
            .inner
            .retained_cloud_plan(Some(repo), Some(owner), true, &mut scan)?;
        self.inner
            .selected_version(repo, owner, selector, &mut scan, &gate)?;
        self.write_version_pin(repo, owner, identity, selector, &gate, &mut scan, clock)
    }
    /// Transfer an already checked read while its physical source remains held.
    /// The authoritative clock is sampled only after proof and under the gate.
    pub fn pin_cloud_version_from_read_guard(
        &self,
        repo: &str,
        owner: CloudOwner,
        identity: CloudVersionPinIdentity,
        read: &CloudVersionReadGuard,
        clock: impl FnOnce() -> Result<i64>,
    ) -> Result<CloudVersionSelector> {
        identity.validate()?;
        let version = read.version();
        if !Arc::ptr_eq(&self.inner, &read._guard.inner)
            || read._guard.repo != repo
            || read._guard.owner != owner
            || read._guard.version != Some((version.workspace, version.generation))
            || version.generation == 0
        {
            return Err(Error::Invalid("cloud version pin read guard scope"));
        }
        let gate = self
            .inner
            .lock_reusing_arena(read._guard._resident.as_ref())?;
        self.inner.repository(repo)?;
        let reader = (
            repo.to_owned(),
            owner.principal,
            version.workspace,
            version.generation,
        );
        if gate
            .cloud_version_readers
            .get(&reader)
            .copied()
            .unwrap_or(0)
            == 0
        {
            return Err(Error::Corrupt("cloud version read guard pin missing"));
        }
        let selector = CloudVersionSelector {
            workspace: version.workspace,
            generation: version.generation,
            manifest: version.manifest,
            selected: BlobDigest::of(read.manifest_bytes()),
        };
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let key = version_key(repo, owner, version.workspace, version.generation);
        let bytes = self
            .inner
            .db
            .read_tx()
            .get(&self.inner.spaces.cloud.versions, &key)?
            .ok_or(Error::Corrupt("cloud pinned read version missing"))?;
        scan.visit(&self.inner, &key, &bytes)?;
        let record: VersionRecord = decode(&bytes)?;
        if record.format != read.format || super::version(&record) != *version {
            return Err(Error::Corrupt("cloud pinned read version changed"));
        }
        if !self.replay_version_pin(repo, owner, identity, selector)? {
            self.write_version_pin(repo, owner, identity, selector, &gate, &mut scan, clock)?;
        }
        Ok(selector)
    }
    fn replay_version_pin(
        &self,
        repo: &str,
        owner: CloudOwner,
        identity: CloudVersionPinIdentity,
        selector: CloudVersionSelector,
    ) -> Result<bool> {
        let key = pin_key(repo, owner, selector.workspace, identity);
        self.inner
            .db
            .read_tx()
            .get(&self.inner.spaces.cloud.lifecycle, &key)?
            .map(|bytes| match decode::<LifecycleRecord>(&bytes)? {
                LifecycleRecord::Pin(pin)
                    if pin.selector() == selector
                        && pin.acquire_not_after_unix_seconds
                            == identity.acquire_not_after_unix_seconds =>
                {
                    Ok(true)
                }
                LifecycleRecord::Released(pin)
                    if pin.workspace == selector.workspace
                        && pin.generation != 0
                        && pin.acquire_not_after_unix_seconds
                            == identity.acquire_not_after_unix_seconds =>
                {
                    Err(Error::CloudReplayExpired)
                }
                _ => Err(Error::IdempotencyConflict),
            })
            .transpose()
            .map(|result| result.unwrap_or(false))
    }
    fn prepare_pin_admission(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        clock: impl FnOnce() -> Result<i64>,
        scan: &mut Scan,
    ) -> Result<PinAdmission> {
        let wall = clock()?;
        if wall < 0 {
            return Err(Error::Invalid("cloud pin clock"));
        }
        let view = self.inner.db.read_tx();
        let floor = view
            .get(&self.inner.spaces.meta, PIN_ADMISSION_FLOOR)?
            .map(|bytes| {
                let raw: [u8; 8] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::Corrupt("cloud pin admission floor"))?;
                let value = i64::from_be_bytes(raw);
                if value < 0 {
                    return Err(Error::Corrupt("cloud pin admission floor"));
                }
                Ok(value)
            })
            .transpose()?
            .unwrap_or(0);
        let now = wall.max(floor);
        let mut expired = Vec::new();
        let mut records = 0u64;
        for item in view.prefix(
            &self.inner.spaces.cloud.lifecycle,
            pin_prefix(repo, owner, workspace),
        ) {
            let (key, bytes) = item.into_inner()?;
            scan.visit(&self.inner, &key, &bytes)?;
            let (_, _, suffix) = parse_scope(&key)?;
            match decode::<LifecycleRecord>(&bytes)? {
                LifecycleRecord::Released(pin)
                    if pin_record_matches(suffix, &pin)
                        && pin.acquire_not_after_unix_seconds <= now =>
                {
                    // One bounded catalog row per key; charge before retaining it.
                    scan.visit(&self.inner, &key, &[])?;
                    expired.push(key.to_vec());
                }
                LifecycleRecord::Pin(pin) | LifecycleRecord::Released(pin)
                    if pin_record_matches(suffix, &pin) =>
                {
                    records += 1;
                }
                _ => return Err(Error::Corrupt("cloud version pin identity")),
            }
            if records + expired.len() as u64
                > self
                    .inner
                    .config
                    .cloud
                    .max_pin_lifecycle_records_per_workspace
            {
                return Err(Error::Limit("cloud pin lifecycle records"));
            }
        }
        Ok(PinAdmission {
            now,
            expired,
            records,
        })
    }
    fn commit_pin_admission(
        &self,
        repo: &str,
        owner: CloudOwner,
        gate: &RuntimeGuard<'_>,
        admission: PinAdmission,
        change: Option<(Vec<u8>, Option<LifecycleRecord>)>,
    ) -> Result<()> {
        self.enable_external_checks_locked(gate)?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        advance_feature(&mut tx, &self.inner.spaces.meta, 12)?;
        tx.insert(
            &self.inner.spaces.meta,
            PIN_ADMISSION_FLOOR,
            admission.now.to_be_bytes(),
        );
        let mut expired = admission.expired;
        if let Some((key, _)) = &change {
            expired.retain(|candidate| candidate != key);
        }
        let mut changes = expired
            .into_iter()
            .map(|key| (&self.inner.spaces.cloud.lifecycle, key, None))
            .collect::<Vec<_>>();
        if let Some((key, value)) = change {
            changes.push((
                &self.inner.spaces.cloud.lifecycle,
                key,
                value.map(|record| encode(&record)).transpose()?,
            ));
        }
        if !changes.is_empty() {
            self.inner
                .cloud_changes(&mut tx, repo, owner, changes, Delta::default())?;
        }
        self.inner.commit(tx)
    }
    fn write_version_pin(
        &self,
        repo: &str,
        owner: CloudOwner,
        identity: CloudVersionPinIdentity,
        selector: CloudVersionSelector,
        gate: &RuntimeGuard<'_>,
        scan: &mut Scan,
        clock: impl FnOnce() -> Result<i64>,
    ) -> Result<()> {
        let admission = self.prepare_pin_admission(repo, owner, selector.workspace, clock, scan)?;
        if identity.acquire_not_after_unix_seconds <= admission.now {
            self.commit_pin_admission(repo, owner, gate, admission, None)?;
            return Err(Error::CloudReplayExpired);
        }
        if identity.acquire_not_after_unix_seconds - admission.now
            > self.inner.config.cloud.max_pin_acquisition_seconds
        {
            return Err(Error::Invalid("cloud pin admission horizon"));
        }
        if admission.records
            >= self
                .inner
                .config
                .cloud
                .max_pin_lifecycle_records_per_workspace
        {
            self.commit_pin_admission(repo, owner, gate, admission, None)?;
            return Err(Error::Limit("cloud pin lifecycle records"));
        }
        self.commit_pin_admission(
            repo,
            owner,
            gate,
            admission,
            Some((
                pin_key(repo, owner, selector.workspace, identity),
                Some(LifecycleRecord::Pin(Pin::new(selector, identity))),
            )),
        )
    }
    fn existing_version_pin(
        &self,
        key: &[u8],
        workspace: [u8; 32],
        identity: CloudVersionPinIdentity,
    ) -> Result<Option<CloudVersionSelector>> {
        self.inner
            .db
            .read_tx()
            .get(&self.inner.spaces.cloud.lifecycle, key)?
            .map(|bytes| match decode::<LifecycleRecord>(&bytes)? {
                LifecycleRecord::Pin(pin)
                    if pin.workspace == workspace
                        && pin.generation != 0
                        && pin.acquire_not_after_unix_seconds
                            == identity.acquire_not_after_unix_seconds =>
                {
                    Ok(pin.selector())
                }
                LifecycleRecord::Released(pin)
                    if pin.workspace == workspace
                        && pin.generation != 0
                        && pin.acquire_not_after_unix_seconds
                            == identity.acquire_not_after_unix_seconds =>
                {
                    Err(Error::CloudReplayExpired)
                }
                _ => Err(Error::Corrupt("cloud version pin identity")),
            })
            .transpose()
    }
    /// Lookup is not source authority. Existing Active/Released rows are scalar
    /// reads and do not admit another catalog arena beside a verified reader.
    /// Expired-missing is terminal only after a durable clock-floor mutation.
    pub fn cloud_version_pin(
        &self,
        repo: &str,
        owner: CloudOwner,
        workspace: [u8; 32],
        identity: CloudVersionPinIdentity,
        clock: impl FnOnce() -> Result<i64>,
    ) -> Result<Option<CloudVersionSelector>> {
        identity.validate()?;
        let key = pin_key(repo, owner, workspace, identity);
        {
            let _gate = self.inner.lock_read()?;
            self.inner.repository(repo)?;
            if let Some(selector) = self.existing_version_pin(&key, workspace, identity)? {
                return Ok(Some(selector));
            }
        }
        let gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        // Release/Acquire may have committed after the read gate was dropped.
        // Recheck before sampling time or making a terminal missing decision.
        if let Some(selector) = self.existing_version_pin(&key, workspace, identity)? {
            return Ok(Some(selector));
        }
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let admission = self.prepare_pin_admission(repo, owner, workspace, clock, &mut scan)?;
        if identity.acquire_not_after_unix_seconds <= admission.now {
            self.commit_pin_admission(repo, owner, &gate, admission, None)?;
            return Err(Error::CloudReplayExpired);
        }
        if identity.acquire_not_after_unix_seconds - admission.now
            > self.inner.config.cloud.max_pin_acquisition_seconds
        {
            return Err(Error::Invalid("cloud pin admission horizon"));
        }
        Ok(None)
    }
    pub fn release_cloud_version_pin(
        &self,
        repo: &str,
        owner: CloudOwner,
        identity: CloudVersionPinIdentity,
        selector: CloudVersionSelector,
        clock: impl FnOnce() -> Result<i64>,
    ) -> Result<()> {
        identity.validate()?;
        if selector.generation == 0 {
            return Err(Error::Invalid("cloud pin selector"));
        }
        let gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let key = pin_key(repo, owner, selector.workspace, identity);
        let existing = self
            .inner
            .db
            .read_tx()
            .get(&self.inner.spaces.cloud.lifecycle, &key)?;
        if let Some(bytes) = &existing {
            match decode::<LifecycleRecord>(bytes)? {
                LifecycleRecord::Pin(pin) | LifecycleRecord::Released(pin)
                    if pin.selector() == selector
                        && pin.acquire_not_after_unix_seconds
                            == identity.acquire_not_after_unix_seconds => {}
                _ => return Err(Error::IdempotencyConflict),
            }
        }
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        let admission =
            self.prepare_pin_admission(repo, owner, selector.workspace, clock, &mut scan)?;
        if identity.acquire_not_after_unix_seconds > admission.now {
            if identity.acquire_not_after_unix_seconds - admission.now
                > self.inner.config.cloud.max_pin_acquisition_seconds
            {
                return Err(Error::Invalid("cloud pin admission horizon"));
            }
            if existing.is_none()
                && admission.records
                    >= self
                        .inner
                        .config
                        .cloud
                        .max_pin_lifecycle_records_per_workspace
            {
                self.commit_pin_admission(repo, owner, &gate, admission, None)?;
                return Err(Error::Limit("cloud pin lifecycle records"));
            }
            self.commit_pin_admission(
                repo,
                owner,
                &gate,
                admission,
                Some((
                    key,
                    Some(LifecycleRecord::Released(Pin::new(selector, identity))),
                )),
            )
        } else {
            // Once fenced by the persisted floor no row is needed, including
            // when Release overtakes an accepted worker before its first write.
            let change = existing.map(|_| (key, None));
            self.commit_pin_admission(repo, owner, &gate, admission, change)
        }
    }
}

fn remote_intents_covered(
    old: Option<&tinytree_cloud::PortableRemoteState>,
    new: Option<&tinytree_cloud::PortableRemoteState>,
    inner: &Inner,
    scan: &mut Scan,
) -> Result<bool> {
    let Some(old) = old else {
        return Ok(true);
    };
    let Some(new) = new else {
        return Ok(false);
    };
    // Pending/uncertain intent transitions are not inferred from observation
    // advancement. A later integration may prove additional terminal cases.
    if old.archived_pushes != new.archived_pushes || old.landing != new.landing {
        return Ok(false);
    }
    for observation in &old.observations {
        let mut replacement = None;
        for candidate in &new.observations {
            scan.visit(
                inner,
                candidate.reference.as_bytes(),
                candidate.bookmark.as_bytes(),
            )?;
            if candidate.reference == observation.reference
                && candidate.bookmark == observation.bookmark
            {
                replacement = Some(candidate);
                break;
            }
        }
        let Some(replacement) = replacement else {
            return Ok(false);
        };
        if replacement.generation < observation.generation
            || (replacement.generation == observation.generation
                && replacement.head != observation.head)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn contains_all<T: Eq>(
    old: &[T],
    new: &[T],
    compare: impl Fn(&T, &T) -> std::cmp::Ordering,
    inner: &Inner,
    scan: &mut Scan,
) -> Result<bool> {
    for value in old {
        // Precharge bounded binary-search/equality work before doing it. Capture
        // framing separately bounds every key, parent vector and root vector.
        for _ in 0..=new.len().checked_ilog2().unwrap_or(0) + 1 {
            scan.visit(inner, &[], &[])?;
        }
        if !new
            .binary_search_by(|candidate| compare(candidate, value))
            .is_ok_and(|position| new[position] == *value)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

impl Store {
    /// Retire a superseded contiguous prefix only after a verified V3 successor
    /// covers every old operation/index/raw selection. This deliberately refuses
    /// exporters which omit old private undo or snapshot-operation bindings.
    /// Exact old WC/capsule bytes expire, rather than being replaced by latest.
    pub fn retire_cloud_versions(
        &self,
        repo: &str,
        owner: CloudOwner,
        request: CloudRetirementRequest,
    ) -> Result<CloudRetirementReceipt> {
        if !self.inner.config.cloud.retirement_enabled {
            return Err(Error::Invalid("cloud retirement is disabled"));
        }
        let gate = self.inner.lock()?;
        self.inner.repository(repo)?;
        let selector = request.expected_current;
        let mut state = self
            .inner
            .workspace_lifecycle(repo, owner, selector.workspace)?;
        let fingerprint = request.fingerprint();
        if state.last_retirement == Some(fingerprint) {
            return Ok(CloudRetirementReceipt {
                retired_through: state.retired_through,
                retention_revision: state.retention_revision,
            });
        }
        if request.expected_floor != state.retired_through
            || request.expected_revision != state.retention_revision
            || selector.generation != state.generation
            || selector.manifest.0 != state.manifest
        {
            return Err(Error::CloudRetentionChanged);
        }
        if request.retire_through <= state.retired_through
            || request.retire_through >= state.generation
        {
            return Err(Error::Invalid(
                "cloud retirement must be a superseded prefix",
            ));
        }
        let mut scan = Scan {
            records: 0,
            bytes: 0,
            payload: 0,
        };
        // The gate remains held throughout bounded proof and the single atomic
        // update. This is one physical arena, not a second validation admission.
        let plan = self
            .inner
            .retained_cloud_plan(Some(repo), Some(owner), true, &mut scan)?;
        let (_, successor) = self
            .inner
            .selected_version(repo, owner, selector, &mut scan, &gate)?;
        let RecoveryManifest::V3(successor) = successor else {
            return Err(Error::Invalid(
                "cloud retirement requires captured successor",
            ));
        };
        let current_key = version_key(repo, owner, selector.workspace, selector.generation);
        let capture = self
            .inner
            .capture_record(&current_key, Some(successor.capture))?
            .ok_or(Error::Corrupt("cloud retirement successor capture missing"))?;
        scan.visit(&self.inner, &current_key, &capture.bytes)?;
        let successor_data = tinytree_cloud::decode_data(
            &capture.bytes,
            tinytree_cloud::data_digest(&capture.bytes),
            &Default::default(),
        )
        .map_err(|_| Error::Invalid("cloud retirement successor data"))?;
        let successor_native = self.inner.closure(
            repo,
            successor.history.recovery.native_roots.iter().copied(),
            true,
            false,
        )?;
        let view = self.inner.db.read_tx();
        let mut retiring = HashSet::new();
        let mut retiring_ids = HashSet::new();
        let mut boundary = None;
        for generation in state.retired_through + 1..=request.retire_through {
            let key = version_key(repo, owner, selector.workspace, generation);
            let record = plan
                .versions
                .get(&key)
                .ok_or(Error::Corrupt("cloud retirement version gap"))?;
            let reader = (
                repo.to_owned(),
                owner.principal,
                selector.workspace,
                generation,
            );
            if gate
                .cloud_version_readers
                .get(&reader)
                .copied()
                .unwrap_or(0)
                != 0
            {
                return Err(Error::Invalid("cloud version has active readers"));
            }
            let manifest_bytes = view
                .get(
                    &self.inner.spaces.cloud.manifests,
                    manifest_key(repo, owner, ManifestId(record.manifest)),
                )?
                .ok_or(Error::Corrupt("cloud retirement manifest missing"))?;
            scan.visit(
                &self.inner,
                &manifest_key(repo, owner, ManifestId(record.manifest)),
                &manifest_bytes,
            )?;
            let RecoveryManifest::V3(old) = self.inner.decode_recovery(
                &manifest_bytes,
                ManifestId(record.manifest),
                self.inner.config.cloud.manifest_limits,
            )?
            else {
                return Err(Error::Invalid("cloud retirement requires captured history"));
            };
            let capture = self
                .inner
                .capture_record(&key, Some(old.capture))?
                .ok_or(Error::Corrupt("cloud retirement capture missing"))?;
            scan.visit(&self.inner, &key, &capture.bytes)?;
            let old_data = tinytree_cloud::decode_data(
                &capture.bytes,
                tinytree_cloud::data_digest(&capture.bytes),
                &Default::default(),
            )
            .map_err(|_| Error::Invalid("cloud retirement capture data"))?;
            if !contains_all(
                &old_data.operations,
                &successor_data.operations,
                |a, b| a.operation.cmp(&b.operation),
                &self.inner,
                &mut scan,
            )? || !contains_all(
                &old_data.blobs,
                &successor_data.blobs,
                |a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)),
                &self.inner,
                &mut scan,
            )? || !contains_all(
                &old_data.indexes,
                &successor_data.indexes,
                |a, b| a.operation.cmp(&b.operation),
                &self.inner,
                &mut scan,
            )? || !contains_all(
                &old.history.recovery.members,
                &successor.history.recovery.members,
                |a, b| a.id.cmp(&b.id),
                &self.inner,
                &mut scan,
            )? || !contains_all(
                &old.history.operation_indexes,
                &successor.history.operation_indexes,
                |a, b| a.operation.cmp(&b.operation),
                &self.inner,
                &mut scan,
            )? || !contains_all(
                &old.history.index_roots,
                &successor.history.index_roots,
                |a, b| a.id.cmp(&b.id),
                &self.inner,
                &mut scan,
            )? || !remote_intents_covered(old_data.remote.as_ref(), successor_data.remote.as_ref(), &self.inner, &mut scan)?
                // Both captures were verified against their OWN DATA above.
                // Intent wrapper IDs contain capture.id, so compare supported
                // portable semantics and covered native roots, not wrapper IDs.
                || old.history.recovery.native_roots.iter().chain(old.history.recovery.intent_roots.iter().flat_map(|intent| intent.roots.iter())).any(|key| {
                    !successor_native.contains(key)
                        && !(key.kind == Kind::Commit && key.id.0 == CommitId::ROOT.0)
                })
            {
                return Err(Error::Invalid(
                    "cloud retirement successor coverage incomplete",
                ));
            }
            retiring.insert(key);
            retiring_ids.insert(record.manifest);
            boundary = Some(record.manifest);
        }
        for pin in
            self.inner
                .active_version_pin_selectors(repo, owner, selector.workspace, &mut scan)?
        {
            if pin.generation <= request.retire_through {
                return Err(Error::Invalid("cloud version has durable pins"));
            }
        }
        let mut kept_manifests = HashSet::new();
        for (key, record) in &plan.versions {
            if retiring.contains(key) {
                continue;
            }
            kept_manifests.insert(record.manifest);
            let bytes = view
                .get(
                    &self.inner.spaces.cloud.manifests,
                    manifest_key(repo, owner, ManifestId(record.manifest)),
                )?
                .ok_or(Error::Corrupt("cloud retained manifest missing"))?;
            scan.visit(&self.inner, key, &bytes)?;
            let manifest = self.inner.decode_recovery(
                &bytes,
                ManifestId(record.manifest),
                self.inner.config.cloud.manifest_limits,
            )?;
            for parent in &manifest.parents {
                if retiring_ids.contains(&parent.0)
                    && !(record.workspace == selector.workspace
                        && record.generation == request.retire_through + 1
                        && Some(parent.0) == boundary)
                {
                    return Err(Error::Invalid(
                        "cloud retirement retained parent crosses prefix",
                    ));
                }
            }
        }
        let mut changes = Vec::new();
        let mut delta = Delta::default();
        for key in &retiring {
            changes.push((&self.inner.spaces.cloud.versions, key.clone(), None));
            changes.push((&self.inner.spaces.cloud.roots, key.clone(), None));
            changes.push((&self.inner.spaces.cloud.captures, key.clone(), None));
            delta.versions -= 1;
            delta.version_payload_bytes -= *plan
                .payload_work
                .get(key)
                .ok_or(Error::Corrupt("cloud retirement payload accounting"))?
                as i128;
            delta.version_metadata_bytes -= *plan
                .metadata_work
                .get(key)
                .ok_or(Error::Corrupt("cloud retirement metadata accounting"))?
                as i128;
            delta.version_records -= *plan
                .record_work
                .get(key)
                .ok_or(Error::Corrupt("cloud retirement record accounting"))?
                as i128;
        }
        let mut receipt_targets = HashSet::new();
        for item in view.prefix(&self.inner.spaces.cloud.receipts, scope(repo, owner)) {
            let (key, bytes) = item.into_inner()?;
            scan.visit(&self.inner, &key, &bytes)?;
            let (_, _, suffix) = parse_scope(&key)?;
            let target = self
                .inner
                .retained_receipt_target(repo, owner, suffix, &bytes, &plan)?;
            if !receipt_targets.insert(target.clone()) {
                return Err(Error::Corrupt("cloud receipt replay history"));
            }
            if retiring.contains(&target) {
                changes.push((&self.inner.spaces.cloud.receipts, key.to_vec(), None));
                delta.receipts -= 1;
            }
        }
        if receipt_targets.len() != plan.versions.len() {
            return Err(Error::Corrupt("cloud version receipt missing"));
        }
        if delta.receipts != delta.versions {
            return Err(Error::Corrupt("cloud retirement receipt history"));
        }
        for id in retiring_ids.difference(&kept_manifests) {
            changes.push((
                &self.inner.spaces.cloud.manifests,
                manifest_key(repo, owner, ManifestId(*id)),
                None,
            ));
        }
        state.retired_through = request.retire_through;
        state.boundary_manifest = boundary;
        state.retention_revision = state
            .retention_revision
            .checked_add(1)
            .ok_or(Error::Limit("cloud retention revision"))?;
        state.last_retirement = Some(fingerprint);
        changes.push((
            &self.inner.spaces.cloud.workspaces,
            workspace_key(repo, owner, selector.workspace),
            Some(encode(&state)?),
        ));
        changes.push((
            &self.inner.spaces.cloud.lifecycle,
            closed_key(repo, owner),
            Some(encode(&LifecycleRecord::LegacyClosed)?),
        ));
        self.enable_external_checks_locked(&gate)?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        advance_feature(&mut tx, &self.inner.spaces.meta, 12)?;
        tx.insert(&self.inner.spaces.meta, b"cloud-retirement-activated", [1]);
        self.inner
            .cloud_changes(&mut tx, repo, owner, changes, delta)?;
        self.inner.commit(tx)?;
        Ok(CloudRetirementReceipt {
            retired_through: state.retired_through,
            retention_revision: state.retention_revision,
        })
    }
}
