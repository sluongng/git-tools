//! Durable, actor-owned sequential upload assembly. Partial bytes are never
//! published as native objects or acknowledged as completed object receipts.
use super::*;
use std::collections::BTreeSet;
pub use tinytree_forge_core::upload::{ForgeUploadState, ForgeUploadedObject, ForgeUploadStatus};
use tinytree_forge_core::upload::{session_id, validate_request_id};

const PREFIX: &[u8] = b"forge/upload-sessions/v1/";
const DIRECTORY: &str = "forge-upload-sessions";
const CHUNK: usize = tinytree_forge_core::upload::MAX_CHUNK_BYTES;
const FILE_LIMIT: u64 = 64 * 1024 * 1024;
const METADATA_LIMIT: u64 = 1024 * 1024;
const MAX_RECORDS: usize = 4096;
const MAX_ACTIVE: usize = 64;
const MAX_ACCOUNT_ACTIVE: usize = 8;
const GLOBAL_BYTES: u64 = 1024 * 1024 * 1024;
const ACCOUNT_BYTES: u64 = 256 * 1024 * 1024;
const TTL: i64 = 24 * 60 * 60;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    account: AccountId,
    principal: String,
    view_name: String,
    view: ViewVersion,
    epoch: PolicyEpoch,
    path: String,
    descriptor: StoredDescriptor,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    staging_lease: String,
    expires_at: i64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Session {
    id: [u8; 32],
    fingerprint: [u8; 32],
    binding: Binding,
    committed_len: u64,
    expires_at: i64,
    expired: bool,
    receipt: Option<Receipt>,
}
impl Session {
    fn status(&self) -> Result<ForgeUploadStatus> {
        let d = self.binding.descriptor.descriptor()?;
        Ok(ForgeUploadStatus {
            session_id: self.id,
            offset: self.committed_len,
            state: if self.receipt.is_some() {
                ForgeUploadState::Committed
            } else {
                ForgeUploadState::Pending
            },
            receipt: self.receipt.as_ref().map(|r| ForgeUploadedObject {
                kind: d.key.kind as u8,
                id: d.key.id.0,
                size_bytes: d.size_bytes,
                format_version: d.format_version,
                staging_lease: r.staging_lease.clone(),
                expires_at: r.expires_at,
            }),
        })
    }
}
fn hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
fn session_key(id: &[u8; 32]) -> Vec<u8> {
    [PREFIX, hex(id).as_bytes()].concat()
}
fn fingerprint(binding: &Binding) -> Result<[u8; 32]> {
    let mut h = blake3::Hasher::new();
    h.update(b"tinytree-forge-upload-binding-v1\0");
    h.update(&encode(binding)?);
    Ok(*h.finalize().as_bytes())
}
fn descriptor_limit(d: Descriptor) -> Result<()> {
    if d.format_version != tinytree_model::FORMAT_VERSION {
        return Err(Error::Invalid("upload format"));
    }
    let maximum = if d.key.kind == Kind::File {
        FILE_LIMIT
    } else {
        METADATA_LIMIT
    };
    if d.size_bytes > maximum {
        return Err(Error::Limit("upload object size"));
    }
    Ok(())
}
fn allows(view: &ResolvedView, path: &str, kind: Kind) -> Result<()> {
    if path.is_empty() && matches!(kind, Kind::Commit | Kind::Tree) {
        return Ok(());
    }
    if !view.allows_path(path) {
        return Err(Error::ForgeAccessDenied);
    }
    Ok(())
}

impl Inner {
    fn forge_upload_directory(&self) -> Result<PathBuf> {
        let directory = self.root.join(DIRECTORY);
        match fs::symlink_metadata(&directory) {
            Ok(m) if m.is_dir() => {}
            Ok(_) => return Err(Error::Corrupt("upload assembly directory")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&directory)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
                }
                sync_dir(&directory)?;
                sync_dir(&self.root)?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(directory)
    }
    fn forge_check_upload_directory(&self) -> Result<()> {
        let metadata = fs::symlink_metadata(self.root.join(DIRECTORY))?;
        if !metadata.is_dir() {
            return Err(Error::Corrupt("upload assembly directory"));
        }
        Ok(())
    }
    fn forge_upload_path(&self, id: &[u8; 32]) -> PathBuf {
        self.root.join(DIRECTORY).join(hex(id))
    }
    fn forge_upload_session(&self, tx: &impl fjall::Readable, id: &[u8; 32]) -> Result<Session> {
        let bytes = tx
            .get(&self.spaces.meta, session_key(id))?
            .ok_or(Error::Invalid("unknown upload session"))?;
        if bytes.len() > 16 * 1024 {
            return Err(Error::Corrupt("upload session record size"));
        }
        let s: Session = decode(&bytes)?;
        if &s.id != id || fingerprint(&s.binding)? != s.fingerprint {
            return Err(Error::Corrupt("upload session identity"));
        }
        Ok(s)
    }
    fn forge_check_upload_in(
        &self,
        tx: &impl fjall::Readable,
        account: &AccountId,
        actor: &ForgeActor,
        view_name: &str,
        s: &Session,
        now: i64,
    ) -> Result<ResolvedView> {
        if s.binding.account != *account
            || s.binding.principal != actor.principal
            || (!view_name.is_empty() && s.binding.view_name != view_name)
        {
            return Err(Error::ForgeAccessDenied);
        }
        self.forge_authorize_in(
            tx,
            account,
            actor,
            ForgeAction::Write,
            Some(s.binding.epoch),
        )?;
        let view = self.forge_resolve_view_in(tx, account, Some(actor), s.binding.view)?;
        if view.name() != s.binding.view_name || view.epoch() != s.binding.epoch {
            return Err(Error::StaleForgePolicy);
        }
        allows(
            &view,
            &s.binding.path,
            s.binding.descriptor.descriptor()?.key.kind,
        )?;
        if s.expired
            || now >= s.expires_at
            || s.receipt.as_ref().is_some_and(|r| now >= r.expires_at)
        {
            return Err(Error::Invalid("upload session expired"));
        }
        if s.receipt.is_some() {
            self.require_descriptor(view.repository(), s.binding.descriptor.descriptor()?)?;
        }
        Ok(view)
    }
    /// Discard bytes ahead of the metadata receipt after a crash. Missing bytes
    /// below the committed offset are corruption, never an invitation to ACK.
    fn forge_open_upload(&self, s: &Session) -> Result<File> {
        self.forge_check_upload_directory()?;
        let path = self.forge_upload_path(&s.id);
        require_regular_file(&path)?;
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        let length = file.metadata()?.len();
        if length < s.committed_len {
            return Err(Error::Corrupt("upload assembly truncated"));
        }
        if length != s.committed_len {
            file.set_len(s.committed_len)?;
            file.sync_all()?;
        }
        Ok(file)
    }
    fn forge_reap_uploads(&self, now: i64) -> Result<Vec<Session>> {
        let view = self.db.read_tx();
        let mut records = Vec::new();
        for item in view.prefix(&self.spaces.meta, PREFIX) {
            let (key, bytes) = item.into_inner()?;
            if records.len() >= MAX_RECORDS || bytes.len() > 16 * 1024 {
                return Err(Error::Limit("upload session records"));
            }
            let mut s: Session = decode(&bytes)?;
            if key.as_ref() != session_key(&s.id) || fingerprint(&s.binding)? != s.fingerprint {
                return Err(Error::Corrupt("upload session identity"));
            }
            if !s.expired
                && (now >= s.expires_at || s.receipt.as_ref().is_some_and(|r| now >= r.expires_at))
            {
                s.expired = true;
                let mut tx = self.db.write_tx().durability(Some(PersistMode::SyncAll));
                tx.insert(&self.spaces.meta, key, encode(&s)?);
                self.commit(tx)?;
            }
            if s.expired || s.receipt.is_some() {
                self.forge_check_upload_directory()?;
                match fs::remove_file(self.forge_upload_path(&s.id)) {
                    Ok(()) => {
                        sync_dir(&self.root.join(DIRECTORY))?;
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.into()),
                }
            }
            records.push(s);
        }
        Ok(records)
    }
}
impl Store {
    pub fn forge_begin_upload(
        &self,
        view: &ResolvedView,
        actor: &ForgeActor,
        request: &str,
        canonical_path: &str,
        descriptor: Descriptor,
        now: i64,
    ) -> Result<ForgeUploadStatus> {
        validate_request_id(request)?;
        descriptor_limit(descriptor)?;
        allows(view, canonical_path, descriptor.key.kind)?;
        let binding = Binding {
            account: view.account().clone(),
            principal: actor.principal.clone(),
            view_name: view.name().into(),
            view: view.version(),
            epoch: view.epoch(),
            path: canonical_path.into(),
            descriptor: descriptor.into(),
        };
        let id = session_id(view.account(), actor, request)?;
        let fp = fingerprint(&binding)?;
        let _gate = self.inner.lock()?;
        let tx = self.inner.db.read_tx();
        self.inner.forge_check_view_in(&tx, view)?;
        self.inner.forge_authorize_in(
            &tx,
            view.account(),
            actor,
            ForgeAction::Write,
            Some(view.epoch()),
        )?;
        let actor_view =
            self.inner
                .forge_resolve_view_in(&tx, view.account(), Some(actor), view.version())?;
        if actor_view.epoch() != view.epoch() {
            return Err(Error::StaleForgePolicy);
        }
        allows(&actor_view, canonical_path, descriptor.key.kind)?;
        self.inner.forge_upload_directory()?;
        let records = self.inner.forge_reap_uploads(now)?;
        if let Some(s) = records.iter().find(|s| s.id == id) {
            if s.fingerprint != fp {
                return Err(Error::IdempotencyConflict);
            }
            self.inner
                .forge_check_upload_in(&tx, view.account(), actor, view.name(), s, now)?;
            if s.receipt.is_none() {
                self.inner.forge_open_upload(s)?;
            }
            return s.status();
        }
        if records.len() >= MAX_RECORDS {
            return Err(Error::Limit("upload session records"));
        }
        let active: Vec<_> = records
            .iter()
            .filter(|s| !s.expired && s.receipt.is_none())
            .collect();
        let account_active: Vec<_> = active
            .iter()
            .filter(|s| s.binding.account == *view.account())
            .collect();
        let total: u64 = active.iter().map(|s| s.binding.descriptor.size).sum();
        let account_total: u64 = account_active
            .iter()
            .map(|s| s.binding.descriptor.size)
            .sum();
        if active.len() >= MAX_ACTIVE
            || account_active.len() >= MAX_ACCOUNT_ACTIVE
            || total.saturating_add(descriptor.size_bytes) > GLOBAL_BYTES
            || account_total.saturating_add(descriptor.size_bytes) > ACCOUNT_BYTES
        {
            return Err(Error::Limit("upload active quota"));
        }
        let directory = self.inner.forge_upload_directory()?;
        let path = self.inner.forge_upload_path(&id);
        // A crash before the intent transaction may leave only this empty file.
        // No record means no committed bytes, so the bounded orphan is discarded.
        if path.try_exists()? {
            require_regular_file(&path)?;
            fs::remove_file(&path)?;
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(path)?;
        file.sync_all()?;
        sync_dir(&directory)?;
        let s = Session {
            id,
            fingerprint: fp,
            binding,
            committed_len: 0,
            expires_at: now
                .checked_add(TTL)
                .ok_or(Error::Invalid("upload expiry"))?,
            expired: false,
            receipt: None,
        };
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(&self.inner.spaces.meta, session_key(&id), encode(&s)?);
        self.inner.commit(tx)?;
        s.status()
    }
    pub fn forge_append_upload(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        view_name: &str,
        id: [u8; 32],
        offset: u64,
        bytes: &[u8],
        now: i64,
    ) -> Result<ForgeUploadStatus> {
        if bytes.is_empty() || bytes.len() > CHUNK {
            return Err(Error::Limit("upload chunk size"));
        }
        let _gate = self.inner.lock()?;
        let tx = self.inner.db.read_tx();
        let mut s = self.inner.forge_upload_session(&tx, &id)?;
        self.inner
            .forge_check_upload_in(&tx, account, actor, view_name, &s, now)?;
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(Error::Invalid("upload offset"))?;
        if end > s.binding.descriptor.size || offset > s.committed_len {
            return Err(Error::Invalid("upload noncontiguous offset"));
        }
        if s.receipt.is_some() {
            let descriptor = s.binding.descriptor.descriptor()?;
            let mut file = self
                .inner
                .open_backing(&account.repository_id(), descriptor)?;
            let mut previous = vec![0; bytes.len()];
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut previous)?;
            if previous != bytes {
                return Err(Error::IdempotencyConflict);
            }
            return s.status();
        }
        let mut file = self.inner.forge_open_upload(&s)?;
        if offset < s.committed_len {
            if end > s.committed_len {
                return Err(Error::IdempotencyConflict);
            }
            let mut previous = vec![0; bytes.len()];
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut previous)?;
            if previous != bytes {
                return Err(Error::IdempotencyConflict);
            }
            return s.status();
        }
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        s.committed_len = end;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(&self.inner.spaces.meta, session_key(&id), encode(&s)?);
        self.inner.commit(tx)?;
        s.status()
    }
    pub fn forge_upload_status(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        view_name: &str,
        id: [u8; 32],
        now: i64,
    ) -> Result<ForgeUploadStatus> {
        let _gate = self.inner.lock()?;
        let tx = self.inner.db.read_tx();
        let s = self.inner.forge_upload_session(&tx, &id)?;
        self.inner
            .forge_check_upload_in(&tx, account, actor, view_name, &s, now)?;
        if s.receipt.is_none() {
            self.inner.forge_open_upload(&s)?;
        }
        s.status()
    }
    pub fn forge_finalize_upload(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        view_name: &str,
        id: [u8; 32],
        now: i64,
    ) -> Result<ForgeUploadStatus> {
        let (s, view, mut file) = {
            let _gate = self.inner.lock()?;
            let tx = self.inner.db.read_tx();
            let s = self.inner.forge_upload_session(&tx, &id)?;
            let view = self
                .inner
                .forge_check_upload_in(&tx, account, actor, view_name, &s, now)?;
            if s.receipt.is_some() {
                return s.status();
            }
            if s.committed_len != s.binding.descriptor.size {
                return Err(Error::Invalid("upload incomplete"));
            }
            let file = self.inner.forge_open_upload(&s)?;
            (s, view, file)
        };
        file.seek(SeekFrom::Start(0))?;
        let receipt = self.forge_upload_object_stream(
            &view,
            actor,
            &s.binding.path,
            s.binding.descriptor.descriptor()?,
            &mut file,
            now,
        )?;
        let _gate = self.inner.lock()?;
        let tx = self.inner.db.read_tx();
        let mut current = self.inner.forge_upload_session(&tx, &id)?;
        self.inner
            .forge_check_upload_in(&tx, account, actor, view_name, &current, now)?;
        if current.receipt.is_some() {
            return current.status();
        }
        current.receipt = Some(Receipt {
            staging_lease: receipt.staging_lease,
            expires_at: receipt.expires_at,
        });
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        tx.insert(&self.inner.spaces.meta, session_key(&id), encode(&current)?);
        self.inner.commit(tx)?;
        // Completion is already durable; cleanup failure cannot undo its receipt.
        let path = self.inner.forge_upload_path(&id);
        let _ = fs::remove_file(path);
        let _ = sync_dir(&self.inner.root.join(DIRECTORY));
        current.status()
    }
    /// Reopen truncates uncommitted trailing bytes and rejects missing committed
    /// assembly bytes. Native completed objects remain owned by normal staging
    /// receipts, not by a second object-retention ledger.
    pub fn forge_validate_uploads_on_reopen(&self) -> Result<()> {
        let _gate = self.inner.lock()?;
        let tx = self.inner.db.read_tx();
        let mut expected = BTreeSet::new();
        let mut count = 0;
        let mut total = 0u64;
        for row in tx.prefix(&self.inner.spaces.meta, PREFIX) {
            let (key, bytes) = row.into_inner()?;
            count += 1;
            total = total
                .checked_add((key.len() + bytes.len()) as u64)
                .ok_or(Error::Limit("upload reopen bytes"))?;
            if count > MAX_RECORDS
                || count > self.inner.config.max_retention_validation_records
                || total > self.inner.config.max_retention_validation_bytes
                || bytes.len() > 16 * 1024
            {
                return Err(Error::Limit("upload reopen records"));
            }
            let s: Session = decode(&bytes)?;
            s.binding.account.validate()?;
            let descriptor = s.binding.descriptor.descriptor()?;
            descriptor_limit(descriptor)?;
            if key.as_ref() != session_key(&s.id)
                || fingerprint(&s.binding)? != s.fingerprint
                || s.committed_len > descriptor.size_bytes
                || (s.receipt.is_some() && s.committed_len != descriptor.size_bytes)
            {
                return Err(Error::Corrupt("upload session record"));
            }
            if self
                .inner
                .forge_account_for_repository_in(&tx, &s.binding.account.repository_id())?
                .as_ref()
                != Some(&s.binding.account)
            {
                return Err(Error::Corrupt("upload account missing"));
            }
            if !s.expired && s.receipt.is_none() {
                self.inner.forge_open_upload(&s)?;
                expected.insert(hex(&s.id));
            }
        }
        let directory = self.inner.root.join(DIRECTORY);
        match fs::symlink_metadata(&directory) {
            Ok(m) if m.is_dir() => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            _ => return Err(Error::Corrupt("upload assembly directory")),
        }
        let mut files = 0;
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            files += 1;
            if files > MAX_RECORDS + 1 {
                return Err(Error::Limit("upload assembly files"));
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::Corrupt("upload assembly name"))?;
            if name.len() != 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                || !entry.file_type()?.is_file()
            {
                return Err(Error::Corrupt("upload assembly entry"));
            }
            if !expected.contains(&name) {
                fs::remove_file(entry.path())?;
            }
        }
        sync_dir(&directory)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinytree_model::{FileId, Object, Tree, TreeEntry, TreeValue};
    fn fixture() -> (
        tempfile::TempDir,
        Store,
        AccountId,
        ForgeActor,
        ResolvedView,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path(), Config::default()).unwrap();
        let account = AccountId("upload-fixture".into());
        let actor = ForgeActor {
            principal: "fixture-owner".into(),
        };
        let state = store.forge_provision_account(&account, &actor).unwrap();
        let revision = store
            .references(&account.repository_id(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap()
            .key
            .id
            .0;
        let view = store
            .forge_publish_view(
                &account,
                &actor,
                ViewSpec {
                    name: "workspace".into(),
                    root_prefix: "".into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec![],
                    visibility: ForgeViewVisibility::Public,
                    revision,
                    export_allowed: true,
                },
                state.epoch,
                true,
            )
            .unwrap();
        (directory, store, account, actor, view)
    }
    fn transfer(
        store: &Store,
        view: &ResolvedView,
        actor: &ForgeActor,
        token: &str,
        path: &str,
        descriptor: Descriptor,
        bytes: &[u8],
    ) -> ForgeUploadStatus {
        let begin = store
            .forge_begin_upload(view, actor, token, path, descriptor, 0)
            .unwrap();
        let mut offset = 0;
        for chunk in bytes.chunks(CHUNK) {
            store
                .forge_append_upload(
                    view.account(),
                    actor,
                    view.name(),
                    begin.session_id,
                    offset,
                    chunk,
                    0,
                )
                .unwrap();
            offset += chunk.len() as u64;
        }
        store
            .forge_finalize_upload(view.account(), actor, view.name(), begin.session_id, 0)
            .unwrap()
    }
    #[test]
    fn chunk_upload_resumes_after_reopen_discards_uncommitted_bytes_and_finalizes_once() {
        let (directory, store, account, actor, view) = fixture();
        let bytes = vec![37; 1024 * 1024 + 17];
        let descriptor = tinytree_model::describe_file(&bytes);
        let begin = store
            .forge_begin_upload(&view, &actor, "large-file", "README.md", descriptor, 0)
            .unwrap();
        assert!(begin.receipt.is_none());
        let first = store
            .forge_append_upload(
                &account,
                &actor,
                "workspace",
                begin.session_id,
                0,
                &bytes[..CHUNK],
                0,
            )
            .unwrap();
        assert_eq!(first.offset, CHUNK as u64);
        let retry = store
            .forge_append_upload(
                &account,
                &actor,
                "workspace",
                begin.session_id,
                0,
                &bytes[..CHUNK],
                1,
            )
            .unwrap();
        assert_eq!(retry.offset, first.offset);
        assert!(
            store
                .descriptor(&account.repository_id(), descriptor.key)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            store.forge_finalize_upload(&account, &actor, "workspace", begin.session_id, 1),
            Err(Error::Invalid("upload incomplete"))
        ));
        // Simulate fsync completing but the offset transaction not committing.
        let mut ahead = OpenOptions::new()
            .append(true)
            .open(store.inner.forge_upload_path(&begin.session_id))
            .unwrap();
        ahead.write_all(b"uncommitted").unwrap();
        ahead.sync_all().unwrap();
        drop(ahead);
        drop(store);
        let store = Store::open_existing(directory.path(), Config::default()).unwrap();
        store.forge_validate_uploads_on_reopen().unwrap();
        let status = store
            .forge_upload_status(&account, &actor, "workspace", begin.session_id, 2)
            .unwrap();
        assert_eq!(status.offset, CHUNK as u64);
        assert_eq!(
            fs::metadata(store.inner.forge_upload_path(&begin.session_id))
                .unwrap()
                .len(),
            CHUNK as u64
        );
        let mut offset = CHUNK;
        while offset < bytes.len() {
            let end = (offset + CHUNK).min(bytes.len());
            store
                .forge_append_upload(
                    &account,
                    &actor,
                    "workspace",
                    begin.session_id,
                    offset as u64,
                    &bytes[offset..end],
                    2,
                )
                .unwrap();
            offset = end;
        }
        let done = store
            .forge_finalize_upload(&account, &actor, "workspace", begin.session_id, 2)
            .unwrap();
        let replay = store
            .forge_finalize_upload(&account, &actor, "workspace", begin.session_id, 3)
            .unwrap();
        assert_eq!(
            serde_json::to_value(&done).unwrap(),
            serde_json::to_value(&replay).unwrap()
        );
        assert!(
            store
                .forge_append_upload(
                    &account,
                    &actor,
                    "workspace",
                    begin.session_id,
                    0,
                    &bytes[..CHUNK],
                    3
                )
                .unwrap()
                .receipt
                .is_some()
        );
        assert!(matches!(
            store.forge_append_upload(
                &account,
                &actor,
                "workspace",
                begin.session_id,
                0,
                b"wrong",
                3
            ),
            Err(Error::IdempotencyConflict)
        ));
        let receipt = done.receipt.unwrap();
        assert_eq!(receipt.id, descriptor.key.id.0);
        assert_eq!(receipt.size_bytes, bytes.len() as u64);
        let mut stored = store
            .read(&account.repository_id(), descriptor.key)
            .unwrap();
        let mut actual = Vec::new();
        stored.read_to_end(&mut actual).unwrap();
        assert_eq!(actual, bytes);
        drop(stored);
        store.gc(&account.repository_id(), 3).unwrap();
        assert!(
            store
                .descriptor(&account.repository_id(), descriptor.key)
                .unwrap()
                .is_some()
        );
        drop(store);
        let store = Store::open_existing(directory.path(), Config::default()).unwrap();
        store.forge_validate_uploads_on_reopen().unwrap();
        assert!(
            store
                .forge_upload_status(&account, &actor, "workspace", begin.session_id, 3)
                .unwrap()
                .receipt
                .is_some()
        );
    }
    #[test]
    fn sessions_reject_rebinding_wrong_actor_gaps_conflicts_hash_failure_and_policy_changes() {
        let (_directory, store, account, actor, view) = fixture();
        let bytes = b"verified file";
        let descriptor = tinytree_model::describe_file(bytes);
        let begin = store
            .forge_begin_upload(&view, &actor, "binding", "README.md", descriptor, 0)
            .unwrap();
        assert!(matches!(
            store.forge_begin_upload(&view, &actor, "binding", "other.md", descriptor, 0),
            Err(Error::IdempotencyConflict)
        ));
        let stranger = ForgeActor {
            principal: "other-owner".into(),
        };
        assert!(matches!(
            store.forge_upload_status(&account, &stranger, "workspace", begin.session_id, 0),
            Err(Error::ForgeAccessDenied)
        ));
        assert!(matches!(
            store.forge_upload_status(&account, &actor, "other-view", begin.session_id, 0),
            Err(Error::ForgeAccessDenied)
        ));
        assert!(
            store
                .forge_append_upload(&account, &actor, "workspace", begin.session_id, 1, b"x", 0)
                .is_err()
        );
        store
            .forge_append_upload(
                &account,
                &actor,
                "workspace",
                begin.session_id,
                0,
                b"xxxx",
                0,
            )
            .unwrap();
        assert!(matches!(
            store.forge_append_upload(
                &account,
                &actor,
                "workspace",
                begin.session_id,
                0,
                b"yyyy",
                0
            ),
            Err(Error::IdempotencyConflict)
        ));
        store
            .forge_append_upload(
                &account,
                &actor,
                "workspace",
                begin.session_id,
                4,
                &bytes[4..],
                0,
            )
            .unwrap();
        assert!(
            store
                .forge_finalize_upload(&account, &actor, "workspace", begin.session_id, 0)
                .is_err()
        );
        assert!(
            store
                .forge_upload_status(&account, &actor, "workspace", begin.session_id, 0)
                .unwrap()
                .receipt
                .is_none()
        );
        assert!(
            store
                .descriptor(&account.repository_id(), descriptor.key)
                .unwrap()
                .is_none()
        );
        let epoch = store.forge_get_account(&account, &actor).unwrap().epoch;
        store
            .forge_publish_view(
                &account,
                &actor,
                ViewSpec {
                    name: "workspace".into(),
                    root_prefix: "".into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec!["README.md".into()],
                    visibility: ForgeViewVisibility::Public,
                    revision: view.revision().0,
                    export_allowed: true,
                },
                epoch,
                true,
            )
            .unwrap();
        assert!(matches!(
            store.forge_upload_status(&account, &actor, "workspace", begin.session_id, 0),
            Err(Error::StaleForgePolicy)
        ));
    }
    #[test]
    fn sessions_bound_active_quota_expire_and_validate_native_metadata_dependencies() {
        let (_directory, store, account, actor, view) = fixture();
        let bytes = b"content";
        let d = tinytree_model::describe_file(bytes);
        let mut sessions = Vec::new();
        for i in 0..MAX_ACCOUNT_ACTIVE {
            sessions.push(
                store
                    .forge_begin_upload(&view, &actor, &format!("quota-{i}"), "README.md", d, 0)
                    .unwrap(),
            );
        }
        assert!(matches!(
            store.forge_begin_upload(&view, &actor, "quota-overflow", "README.md", d, 0),
            Err(Error::Limit("upload active quota"))
        ));
        store
            .forge_begin_upload(&view, &actor, "after-expiry", "README.md", d, TTL + 1)
            .unwrap();
        assert!(matches!(
            store.forge_upload_status(
                &account,
                &actor,
                "workspace",
                sessions[0].session_id,
                TTL + 1
            ),
            Err(Error::Invalid("upload session expired"))
        ));
        // Use a fresh fixture so metadata dependency checks are not quota tests.
        let (_other, store, _account, actor, view) = fixture();
        let unknown = FileId([200; 32]);
        let tree = |file| {
            Object::Tree(Tree {
                entries: vec![TreeEntry {
                    name: "README.md".into(),
                    value: TreeValue::File {
                        id: file,
                        executable: false,
                        size_bytes: d.size_bytes,
                        copy_id: None,
                    },
                }],
            })
        };
        let bad =
            tinytree_model::encode(&tree(unknown), tinytree_model::Limits::default()).unwrap();
        let pending = store
            .forge_begin_upload(&view, &actor, "bad-tree", "", bad.descriptor, 0)
            .unwrap();
        store
            .forge_append_upload(
                view.account(),
                &actor,
                view.name(),
                pending.session_id,
                0,
                &bad.bytes,
                0,
            )
            .unwrap();
        assert!(
            store
                .forge_finalize_upload(view.account(), &actor, view.name(), pending.session_id, 0)
                .is_err()
        );
        assert!(
            store
                .forge_upload_status(view.account(), &actor, view.name(), pending.session_id, 0)
                .unwrap()
                .receipt
                .is_none()
        );
        transfer(&store, &view, &actor, "leaf", "README.md", d, bytes);
        let valid =
            tinytree_model::encode(&tree(FileId(d.key.id.0)), tinytree_model::Limits::default())
                .unwrap();
        assert!(
            transfer(
                &store,
                &view,
                &actor,
                "tree",
                "",
                valid.descriptor,
                &valid.bytes
            )
            .receipt
            .is_some()
        );
    }
    #[test]
    fn upload_cannot_borrow_another_actors_locked_view_capability() {
        let (_directory, store, account, owner, initial) = fixture();
        let writer = ForgeActor {
            principal: "fixture-writer".into(),
        };
        let epoch = store.forge_get_account(&account, &owner).unwrap().epoch;
        let updated = store
            .forge_set_member(
                &account,
                &owner,
                ForgeMember {
                    principal: writer.principal.clone(),
                    can_write: true,
                    can_review: false,
                    is_admin: false,
                    roles: vec![],
                },
                epoch,
            )
            .unwrap();
        let private = store
            .forge_publish_view(
                &account,
                &owner,
                ViewSpec {
                    name: "locked".into(),
                    root_prefix: "".into(),
                    include_prefixes: vec!["".into()],
                    exclude_prefixes: vec![],
                    visibility: ForgeViewVisibility::Roles(vec!["maintainer".into()]),
                    revision: initial.revision().0,
                    export_allowed: false,
                },
                updated.epoch,
                false,
            )
            .unwrap();
        assert!(matches!(
            store.forge_begin_upload(
                &private,
                &writer,
                "borrowed-view",
                "README.md",
                tinytree_model::describe_file(b"x"),
                0
            ),
            Err(Error::ForgeAccessDenied)
        ));
    }
}
