//! Account-owned canonical repositories and immutable, access-controlled views.
//!
//! The embedding authenticates actors. No caller-supplied account/view identifier
//! is a credential, and projected reads never expose canonical tree envelopes.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use tinytree_model::{Object, TreeValue};

pub(super) use tinytree_forge_core::accounts::validate_path as valid_path;
pub use tinytree_forge_core::accounts::{
    AccountId, ForgeAccess, ForgeAccount, ForgeAccountMutation, ForgeAction, ForgeActor,
    ForgeMember, ForgeViewVisibility, PolicyEpoch, ViewSpec, ViewVersion,
};
use tinytree_forge_core::accounts::{
    MAX_VIEW_BYTES, validate_name as valid_name, validate_principal as valid_principal,
    view_version,
};

impl From<tinytree_forge_core::Error> for Error {
    fn from(error: tinytree_forge_core::Error) -> Self {
        match error {
            tinytree_forge_core::Error::Invalid(reason) => Self::Invalid(reason),
            tinytree_forge_core::Error::Limit(reason) => Self::Limit(reason),
            tinytree_forge_core::Error::Corrupt(reason) => Self::Corrupt(reason),
        }
    }
}

const ACCOUNT_PREFIX: &[u8] = b"forge/accounts/v1/";
const VIEW_PREFIX: &[u8] = b"forge/views/v1/";
const MAX_ACCOUNTS: u64 = 1024;
const MAX_MEMBERS: usize = 64;
const MAX_VIEWS: usize = 64;
const MAX_VERSIONS: u64 = 1024;
const MAX_ENTRIES: usize = 100_000;
const MAX_TREE_BYTES: usize = 32 * 1024 * 1024;
const MAX_VIEW_PATH_BYTES: usize = 4 * 1024 * 1024;
const MAX_ACCOUNT_BYTES: usize = 256 * 1024;
const MAX_REPLAY_BYTES: usize = 64 * 1024;

/// Only Store resolution constructs this capability. Every Store read rechecks
/// its current policy epoch; cloning a context never extends authorization.
#[derive(Clone, Debug)]
pub struct ResolvedView {
    account: AccountId,
    repository: String,
    version: ViewVersion,
    epoch: PolicyEpoch,
    actor: Option<ForgeActor>,
    spec: ViewSpec,
    policy_spec: ViewSpec,
}
impl ResolvedView {
    pub(crate) fn at_revision(&self, revision: CommitId) -> Self {
        let mut view = self.clone();
        view.spec.revision = revision.0;
        view
    }
    pub fn account(&self) -> &AccountId {
        &self.account
    }
    pub fn repository(&self) -> &str {
        &self.repository
    }
    pub fn name(&self) -> &str {
        &self.spec.name
    }
    pub fn version(&self) -> ViewVersion {
        self.version
    }
    pub fn revision(&self) -> CommitId {
        CommitId(self.spec.revision)
    }
    pub fn epoch(&self) -> PolicyEpoch {
        self.epoch
    }
    pub fn root_prefix(&self) -> &str {
        &self.spec.root_prefix
    }
    pub fn allows_path(&self, canonical: &str) -> bool {
        valid_path(canonical, false).is_ok()
            && self.spec.allows(canonical)
            && self.policy_spec.allows(canonical)
    }
    pub fn canonical_path(&self, relative: &str) -> Result<String> {
        valid_path(relative, false)?;
        let path = if self.root_prefix().is_empty() {
            relative.to_owned()
        } else {
            format!("{}/{relative}", self.root_prefix())
        };
        if !self.allows_path(&path) {
            return Err(Error::ForgeAccessDenied);
        }
        Ok(path)
    }
}
#[derive(Clone, Debug)]
pub struct ForgeFile {
    pub path: String,
    pub canonical_path: String,
    pub descriptor: Descriptor,
    pub executable: bool,
    pub copy_id: Option<[u8; 32]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountRecord {
    id: AccountId,
    owner: String,
    members: BTreeMap<String, ForgeMember>,
    epoch: u64,
    views: BTreeMap<String, ViewVersion>,
    #[serde(default)]
    follow_main: BTreeSet<String>,
    public_view: Option<ViewVersion>,
    versions: u64,
}
impl AccountRecord {
    fn wire(&self) -> ForgeAccount {
        ForgeAccount {
            id: self.id.clone(),
            repository: self.id.repository_id(),
            epoch: PolicyEpoch(self.epoch),
            public_view: self.public_view,
        }
    }
    fn bump(&mut self) -> Result<()> {
        self.epoch = self
            .epoch
            .checked_add(1)
            .ok_or(Error::Limit("forge policy epoch"))?;
        Ok(())
    }
}

fn account_key(id: &AccountId) -> Vec<u8> {
    [ACCOUNT_PREFIX, id.0.as_bytes()].concat()
}
fn view_key(id: &AccountId, version: ViewVersion) -> Vec<u8> {
    [
        VIEW_PREFIX,
        id.0.as_bytes(),
        b"/",
        hex(&version.0).as_bytes(),
    ]
    .concat()
}
fn retired_key(id: &AccountId, version: ViewVersion) -> Vec<u8> {
    [
        b"forge/view-retired/v1/".as_slice(),
        id.0.as_bytes(),
        b"/",
        hex(&version.0).as_bytes(),
    ]
    .concat()
}
fn view_reference(version: ViewVersion) -> String {
    format!("_tinytree/forge/views/v1/{}", hex(&version.0))
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountReplay {
    account: AccountId,
    principal: String,
    request_id: String,
    digest: [u8; 32],
    result: serde_json::Value,
}
fn replay_key(account: &AccountId, actor: &ForgeActor, request_id: &str) -> Vec<u8> {
    format!(
        "forge/account-replays/v1/{}/{}/{}",
        account.0,
        blake3::hash(actor.principal.as_bytes()).to_hex(),
        blake3::hash(request_id.as_bytes()).to_hex()
    )
    .into_bytes()
}
fn decode_owned<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    limit: usize,
    reason: &'static str,
) -> Result<T> {
    if bytes.len() > limit {
        return Err(Error::Corrupt(reason));
    }
    serde_json::from_slice(bytes).map_err(|_| Error::Corrupt(reason))
}
fn encode_owned<T: Serialize>(value: &T, limit: usize, reason: &'static str) -> Result<Vec<u8>> {
    let bytes = encode(value)?;
    if bytes.len() > limit {
        return Err(Error::Limit(reason));
    }
    Ok(bytes)
}
fn validate_account(record: &AccountRecord) -> Result<()> {
    record.id.validate()?;
    valid_principal(&record.owner)?;
    if record.epoch == 0
        || record.members.len() > MAX_MEMBERS
        || record.views.len() > MAX_VIEWS
        || record.follow_main.len() > MAX_VIEWS
        || record.versions > MAX_VERSIONS
        || record
            .follow_main
            .iter()
            .any(|n| !record.views.contains_key(n))
        || record
            .public_view
            .is_some_and(|v| !record.views.values().any(|current| *current == v))
    {
        return Err(Error::Corrupt("forge account shape"));
    }
    for (key, member) in &record.members {
        valid_principal(key)?;
        if key != &member.principal || key == &record.owner || member.roles.len() > 16 {
            return Err(Error::Corrupt("forge member shape"));
        }
        for role in &member.roles {
            valid_name(role)?;
        }
    }
    for name in record.views.keys() {
        valid_name(name)?;
    }
    Ok(())
}
fn decode_account(bytes: &[u8]) -> Result<AccountRecord> {
    let record = decode_owned(bytes, MAX_ACCOUNT_BYTES, "forge account encoding")?;
    validate_account(&record)?;
    Ok(record)
}
fn encode_account(record: &AccountRecord) -> Result<Vec<u8>> {
    validate_account(record)?;
    encode_owned(record, MAX_ACCOUNT_BYTES, "forge account record")
}
fn decode_view(bytes: &[u8]) -> Result<ViewSpec> {
    let spec: ViewSpec = decode_owned(bytes, MAX_VIEW_BYTES, "forge view encoding")?;
    spec.validate()
        .map_err(|_| Error::Corrupt("forge view shape"))?;
    Ok(spec)
}
fn encode_view(spec: &ViewSpec) -> Result<Vec<u8>> {
    spec.validate()?;
    encode_owned(spec, MAX_VIEW_BYTES, "forge view record")
}
fn validate_replay(replay: &AccountReplay) -> Result<()> {
    replay.account.validate()?;
    valid_principal(&replay.principal)?;
    if replay.request_id.is_empty()
        || replay.request_id.len() > 128
        || !replay
            .request_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        || replay
            .result
            .get("account_id")
            .and_then(serde_json::Value::as_str)
            != Some(replay.account.0.as_str())
    {
        return Err(Error::Corrupt("forge account replay binding"));
    }
    Ok(())
}
fn decode_replay(bytes: &[u8]) -> Result<AccountReplay> {
    let replay = decode_owned(bytes, MAX_REPLAY_BYTES, "forge replay encoding")?;
    validate_replay(&replay)?;
    Ok(replay)
}
fn encode_replay(replay: &AccountReplay) -> Result<Vec<u8>> {
    validate_replay(replay)?;
    encode_owned(replay, MAX_REPLAY_BYTES, "forge replay record")
}
impl Store {
    /// Acknowledgment and account/view change commit in one durable transaction.
    pub fn forge_mutate_account(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        request_id: &str,
        mutation: ForgeAccountMutation,
    ) -> Result<serde_json::Value> {
        account.validate()?;
        valid_principal(&actor.principal)?;
        if request_id.is_empty()
            || request_id.len() > 128
            || !request_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(Error::Invalid("forge request identifier"));
        }
        let digest = *blake3::hash(&encode(&mutation)?).as_bytes();
        let rk = replay_key(account, actor, request_id);
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        if !matches!(&mutation, ForgeAccountMutation::Provision) {
            self.inner
                .forge_authorize_in(&tx, account, actor, ForgeAction::Admin, None)?;
        }
        if let Some(bytes) = tx.get(&self.inner.spaces.meta, &rk)? {
            let replay: AccountReplay = decode_replay(&bytes)?;
            if replay.account != *account
                || replay.principal != actor.principal
                || replay.request_id != request_id
                || replay.digest != digest
            {
                return Err(Error::Invalid("forge request replay mismatch"));
            }
            return Ok(replay.result);
        }
        let provisioning = matches!(&mutation, ForgeAccountMutation::Provision);
        let result = match mutation {
            ForgeAccountMutation::Provision => {
                serde_json::to_value(self.forge_provision_account_in(&mut tx, account, actor)?)
                    .map_err(|_| Error::Corrupt("forge receipt encoding"))?
            }
            ForgeAccountMutation::RetireView {
                version,
                expected_epoch,
                view,
            } => {
                let result = self.forge_retire_view_in(
                    &mut tx,
                    account,
                    actor,
                    version,
                    expected_epoch,
                    Some(&view),
                )?;
                let mut result = serde_json::to_value(result)
                    .map_err(|_| Error::Corrupt("forge receipt encoding"))?;
                result["retired_version"] = serde_json::json!(version.0);
                result
            }
            ForgeAccountMutation::SetMember {
                member,
                expected_epoch,
            } => serde_json::to_value(self.forge_set_member_in(
                &mut tx,
                account,
                actor,
                member,
                expected_epoch,
            )?)
            .map_err(|_| Error::Corrupt("forge receipt encoding"))?,
            ForgeAccountMutation::PublishView {
                spec,
                expected_epoch,
                make_default,
                follow_main,
            } => {
                let name = spec.name.clone();
                let revision = spec.revision;
                let root_prefix = spec.root_prefix.clone();
                let version = self.forge_publish_view_in(
                    &mut tx,
                    account,
                    actor,
                    spec,
                    expected_epoch,
                    make_default,
                    follow_main,
                )?;
                let record = self.inner.forge_account_in(&tx, account)?;
                serde_json::json!({"account_id":account.0,"view":name,"view_version":version.0,"revision":revision,"policy_epoch":record.epoch,"root_prefix":root_prefix})
            }
        };
        let result = if provisioning {
            self.forge_account_snapshot_in(&tx, account, result)?
        } else {
            result
        };
        let ck = format!("forge/account-replay-count/v1/{}", account.0);
        let count = tx
            .get(&self.inner.spaces.meta, &ck)?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        if count >= 4096 {
            return Err(Error::Limit("forge account replay receipts"));
        }
        tx.insert(
            &self.inner.spaces.meta,
            rk,
            encode_replay(&AccountReplay {
                account: account.clone(),
                principal: actor.principal.clone(),
                request_id: request_id.into(),
                digest,
                result: result.clone(),
            })?,
        );
        tx.insert(&self.inner.spaces.meta, ck, encode(&(count + 1))?);
        self.inner.commit(tx)?;
        Ok(result)
    }

    /// Trusted embedding lookup; a matching raw repository name alone is not an
    /// account ownership record or a checker read grant.
    pub fn forge_has_account_repository(&self, repository: &str) -> Result<bool> {
        let _gate = self.inner.lock_read()?;
        Ok(self
            .inner
            .forge_account_for_repository_in(&self.inner.db.read_tx(), repository)?
            .is_some())
    }
    /// Trusted embedding discovery for account-scoped background workers.
    pub fn forge_account_repositories(&self, max: usize) -> Result<Vec<String>> {
        if max == 0 || max > 1024 {
            return Err(Error::Limit("forge account discovery"));
        }
        let _gate = self.inner.lock_read()?;
        let tx = self.inner.db.read_tx();
        let mut repositories = Vec::new();
        for entry in tx.prefix(&self.inner.spaces.meta, ACCOUNT_PREFIX) {
            let (_, bytes) = entry.into_inner()?;
            let r: AccountRecord = decode_account(&bytes)?;
            r.id.validate()?;
            repositories.push(r.id.repository_id());
            if repositories.len() > max {
                return Err(Error::Limit("forge account discovery"));
            }
        }
        Ok(repositories)
    }
    /// Provision one canonical repository and deterministic non-root main in the
    /// same transaction as ownership. Interrupted payload writes are only orphans.
    pub fn forge_provision_account(
        &self,
        account: &AccountId,
        owner: &ForgeActor,
    ) -> Result<ForgeAccount> {
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        let result = self.forge_provision_account_in(&mut tx, account, owner)?;
        self.inner.commit(tx)?;
        Ok(result)
    }
    fn forge_provision_account_in(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        account: &AccountId,
        owner: &ForgeActor,
    ) -> Result<ForgeAccount> {
        account.validate()?;
        valid_principal(&owner.principal)?;
        if let Some(bytes) = tx.get(&self.inner.spaces.meta, account_key(account))? {
            let record: AccountRecord = decode_account(&bytes)?;
            if record.owner != owner.principal {
                return Err(Error::ForgeAccessDenied);
            }
            self.inner.repository(&account.repository_id())?;
            return Ok(record.wire());
        }
        let count_key = b"forge/account-count/v1";
        let count = tx
            .get(&self.inner.spaces.meta, count_key)?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        if count >= MAX_ACCOUNTS {
            return Err(Error::Limit("forge accounts"));
        }
        let repo = account.repository_id();
        if tx.get(&self.inner.spaces.repos, &repo)?.is_some() {
            return Err(Error::Invalid("unowned forge repository"));
        }
        let tinytree_forge_core::account_commands::Genesis { empty, commit } =
            tinytree_forge_core::account_commands::genesis(
                account,
                self.inner.config.model_limits,
            )?;
        let directory = self.inner.root.join("repos").join(&repo);
        for kind in [
            Kind::File,
            Kind::Tree,
            Kind::Commit,
            Kind::Symlink,
            Kind::CopyHistory,
        ] {
            let p = directory.join((kind as u8).to_string());
            fs::create_dir_all(&p)?;
            sync_dir(&p)?;
        }
        sync_dir(&directory)?;
        sync_dir(&self.inner.root.join("repos"))?;
        let bytes = empty
            .descriptor
            .size_bytes
            .checked_add(commit.descriptor.size_bytes)
            .ok_or(Error::Limit("forge genesis"))?;
        if bytes > self.inner.config.max_repository_bytes
            || self.inner.config.max_objects_per_repository < 2
        {
            return Err(Error::Limit("repository quota"));
        }
        for object in [&empty, &commit] {
            let target = self.inner.path(&repo, object.descriptor.key);
            let mut file = NamedTempFile::new_in(self.inner.root.join("staging"))?;
            file.write_all(&object.bytes)?;
            file.as_file().sync_all()?;
            match file.persist_noclobber(&target) {
                Ok(_) => (),
                Err(e) if e.error.kind() == io::ErrorKind::AlreadyExists => {
                    let mut old = File::open(&target)?;
                    verify_stream(&mut old, object.descriptor, self.inner.config.model_limits)?;
                }
                Err(e) => return Err(e.error.into()),
            }
            sync_dir(
                target
                    .parent()
                    .ok_or(Error::Corrupt("forge genesis path"))?,
            )?;
            tx.insert(
                &self.inner.spaces.objects,
                object_key(&repo, object.descriptor.key),
                encode(&StoredDescriptor::from(object.descriptor))?,
            );
        }
        let record = AccountRecord {
            id: account.clone(),
            owner: owner.principal.clone(),
            members: BTreeMap::new(),
            epoch: 1,
            views: BTreeMap::new(),
            follow_main: BTreeSet::new(),
            public_view: None,
            versions: 0,
        };
        super::activate_forge_feature(tx, &self.inner.spaces.meta)?;
        tx.insert(
            &self.inner.spaces.repos,
            &repo,
            encode(&RepositoryRecord { bytes, objects: 2 })?,
        );
        tx.insert(
            &self.inner.spaces.refs,
            key(&repo, b"main"),
            encode(&StoredReference {
                generation: 1,
                commit: Some(commit.descriptor.into()),
            })?,
        );
        tx.insert(
            &self.inner.spaces.meta,
            account_key(account),
            encode_account(&record)?,
        );
        tx.insert(&self.inner.spaces.meta, count_key, encode(&(count + 1))?);
        Ok(record.wire())
    }

    pub fn forge_authorize(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        action: ForgeAction,
        expected: Option<PolicyEpoch>,
    ) -> Result<ForgeAccess> {
        let _gate = self.inner.lock_read()?;
        self.inner
            .forge_authorize_in(&self.inner.db.read_tx(), account, actor, action, expected)
    }

    /// Authenticated account/bootstrap snapshot; main and policy are read under
    /// one Store gate. The object ID conveys no raw native read capability.
    pub fn forge_get_account_snapshot(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
    ) -> Result<serde_json::Value> {
        let _gate = self.inner.lock_read()?;
        let tx = self.inner.db.read_tx();
        self.inner
            .forge_authorize_in(&tx, account, actor, ForgeAction::Read, None)?;
        let value = serde_json::to_value(self.inner.forge_account_in(&tx, account)?.wire())
            .map_err(|_| Error::Corrupt("forge account encoding"))?;
        self.forge_account_snapshot_in(&tx, account, value)
    }
    fn forge_account_snapshot_in(
        &self,
        tx: &impl Readable,
        account: &AccountId,
        mut value: serde_json::Value,
    ) -> Result<serde_json::Value> {
        let reference: StoredReference = decode(
            &tx.get(
                &self.inner.spaces.refs,
                key(&account.repository_id(), b"main"),
            )?
            .ok_or(Error::Corrupt("forge canonical main missing"))?,
        )?;
        let commit = reference
            .commit
            .ok_or(Error::Corrupt("forge canonical main missing"))?
            .descriptor()?;
        if reference.generation == 0 || commit.key.kind != Kind::Commit {
            return Err(Error::Corrupt("forge canonical main identity"));
        }
        value["main_revision"] = serde_json::json!(commit.key.id.0);
        value["main_generation"] = serde_json::json!(reference.generation);
        Ok(value)
    }
    pub fn forge_get_account(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
    ) -> Result<ForgeAccount> {
        let _gate = self.inner.lock_read()?;
        let tx = self.inner.db.read_tx();
        self.inner
            .forge_authorize_in(&tx, account, actor, ForgeAction::Read, None)?;
        Ok(self.inner.forge_account_in(&tx, account)?.wire())
    }

    pub fn forge_list_views(
        &self,
        account: &AccountId,
        actor: Option<&ForgeActor>,
    ) -> Result<Vec<ResolvedView>> {
        let _gate = self.inner.lock_read()?;
        let tx = self.inner.db.read_tx();
        let record = self.inner.forge_account_in(&tx, account)?;
        let mut views = Vec::new();
        for version in record.views.values() {
            match self
                .inner
                .forge_resolve_view_in(&tx, account, actor, *version)
            {
                Ok(view) => views.push(view),
                Err(Error::ForgeAccessDenied) => (),
                Err(error) => return Err(error),
            }
        }
        Ok(views)
    }

    pub fn forge_set_member(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        member: ForgeMember,
        expected: PolicyEpoch,
    ) -> Result<ForgeAccount> {
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        let result = self.forge_set_member_in(&mut tx, account, actor, member, expected)?;
        self.inner.commit(tx)?;
        Ok(result)
    }
    fn forge_set_member_in(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        account: &AccountId,
        actor: &ForgeActor,
        member: ForgeMember,
        expected: PolicyEpoch,
    ) -> Result<ForgeAccount> {
        tinytree_forge_core::account_commands::validate_member(&member)?;
        self.inner
            .forge_authorize_in(&*tx, account, actor, ForgeAction::Admin, Some(expected))?;
        let mut record = self.inner.forge_account_in(&*tx, account)?;
        if !tinytree_forge_core::account_commands::member_change(
            &record.owner,
            record.members.get(&member.principal),
            &member,
            record.members.len(),
        )? {
            return Ok(record.wire());
        }
        record.members.insert(member.principal.clone(), member);
        record.bump()?;
        let bytes = encode_account(&record)?;
        if bytes.len() > MAX_ACCOUNT_BYTES {
            return Err(Error::Limit("forge account record"));
        }
        tx.insert(&self.inner.spaces.meta, account_key(account), bytes);
        Ok(record.wire())
    }

    pub fn forge_remove_member(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        principal: &str,
        expected: PolicyEpoch,
    ) -> Result<ForgeAccount> {
        valid_principal(principal)?;
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        self.inner
            .forge_authorize_in(&tx, account, actor, ForgeAction::Admin, Some(expected))?;
        let mut record = self.inner.forge_account_in(&tx, account)?;
        if principal == record.owner {
            return Err(Error::Invalid("forge owner membership"));
        }
        if record.members.remove(principal).is_some() {
            record.bump()?;
        }
        tx.insert(
            &self.inner.spaces.meta,
            account_key(account),
            encode_account(&record)?,
        );
        self.inner.commit(tx)?;
        Ok(record.wire())
    }

    pub fn forge_publish_view(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        spec: ViewSpec,
        expected: PolicyEpoch,
        make_default: bool,
    ) -> Result<ResolvedView> {
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        let version = self.forge_publish_view_in(
            &mut tx,
            account,
            actor,
            spec,
            expected,
            make_default,
            false,
        )?;
        self.inner.commit(tx)?;
        self.inner
            .forge_resolve_view_in(&self.inner.db.read_tx(), account, Some(actor), version)
    }
    fn forge_publish_view_in(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        account: &AccountId,
        actor: &ForgeActor,
        spec: ViewSpec,
        expected: PolicyEpoch,
        make_default: bool,
        follow_main: bool,
    ) -> Result<ViewVersion> {
        spec.validate()?;
        let version = view_version(account, &spec)?;
        let access = self.inner.forge_authorize_in(
            &*tx,
            account,
            actor,
            ForgeAction::Admin,
            Some(expected),
        )?;
        let mut record = self.inner.forge_account_in(&*tx, account)?;
        if tx
            .get(&self.inner.spaces.meta, retired_key(account, version))?
            .is_some()
        {
            return Err(Error::Invalid("forge view version retired"));
        }
        let descriptor = self
            .inner
            .descriptor(
                &access.repository,
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(spec.revision),
                },
            )?
            .ok_or(Error::ForgeAccessDenied)?;
        self.inner
            .closure(&access.repository, [descriptor.key], true, false)?;
        if !record.views.contains_key(&spec.name) && record.views.len() >= MAX_VIEWS {
            return Err(Error::Limit("forge views"));
        }
        let new_version = tx
            .get(&self.inner.spaces.meta, view_key(account, version))?
            .is_none();
        if new_version && record.versions >= MAX_VERSIONS {
            return Err(Error::Limit("forge view versions"));
        }
        if make_default && spec.visibility != ForgeViewVisibility::Public {
            return Err(Error::Invalid("forge default view must be public"));
        }
        if let Some(old) = record.public_view {
            let old_spec = self.inner.forge_view_in(&*tx, account, old)?;
            if old_spec.name == spec.name && spec.visibility != ForgeViewVisibility::Public {
                record.public_view = None;
            }
        }
        record.views.insert(spec.name.clone(), version);
        if follow_main {
            record.follow_main.insert(spec.name.clone());
        } else {
            record.follow_main.remove(&spec.name);
        }
        if make_default {
            record.public_view = Some(version);
        } else if let Some(old) = record.public_view {
            if self.inner.forge_view_in(&*tx, account, old)?.name == spec.name {
                record.public_view = Some(version);
            }
        }
        if new_version {
            record.versions += 1;
        }
        record.bump()?;
        let bytes = encode_account(&record)?;
        if bytes.len() > MAX_ACCOUNT_BYTES {
            return Err(Error::Limit("forge account record"));
        }
        tx.insert(
            &self.inner.spaces.meta,
            view_key(account, version),
            encode_view(&spec)?,
        );
        tx.insert(&self.inner.spaces.meta, account_key(account), bytes);
        // Hidden native reference makes every acknowledged immutable view a GC root.
        tx.insert(
            &self.inner.spaces.refs,
            key(&access.repository, view_reference(version).as_bytes()),
            encode(&StoredReference {
                generation: 1,
                commit: Some(descriptor.into()),
            })?,
        );
        Ok(version)
    }

    pub fn forge_resolve_view(
        &self,
        account: &AccountId,
        actor: Option<&ForgeActor>,
        view: &str,
        version: Option<ViewVersion>,
    ) -> Result<ResolvedView> {
        let _gate = self.inner.lock_read()?;
        let tx = self.inner.db.read_tx();
        let record = self.inner.forge_account_in(&tx, account)?;
        let selected = match version {
            Some(v) => v,
            None if view.is_empty() => record.public_view.ok_or(Error::ForgeAccessDenied)?,
            None => *record.views.get(view).ok_or(Error::ForgeAccessDenied)?,
        };
        let resolved = self
            .inner
            .forge_resolve_view_in(&tx, account, actor, selected)?;
        if !view.is_empty() && view != resolved.name() {
            return Err(Error::ForgeAccessDenied);
        }
        Ok(resolved)
    }

    /// Explicitly retire a historical view's retention obligation. Current or
    /// public versions cannot be retired; immutable metadata remains a tombstone.
    pub fn forge_retire_view(
        &self,
        account: &AccountId,
        actor: &ForgeActor,
        version: ViewVersion,
        expected: PolicyEpoch,
    ) -> Result<ForgeAccount> {
        let _gate = self.inner.lock()?;
        let mut tx = self
            .inner
            .db
            .write_tx()
            .durability(Some(PersistMode::SyncAll));
        let result = self.forge_retire_view_in(&mut tx, account, actor, version, expected, None)?;
        self.inner.commit(tx)?;
        Ok(result)
    }

    fn forge_retire_view_in(
        &self,
        tx: &mut fjall::SingleWriterWriteTx,
        account: &AccountId,
        actor: &ForgeActor,
        version: ViewVersion,
        expected: PolicyEpoch,
        expected_name: Option<&str>,
    ) -> Result<ForgeAccount> {
        self.inner
            .forge_authorize_in(tx, account, actor, ForgeAction::Admin, Some(expected))?;
        let mut record = self.inner.forge_account_in(tx, account)?;
        let spec = self.inner.forge_view_in(tx, account, version)?;
        if expected_name.is_some_and(|name| !name.is_empty() && name != spec.name) {
            return Err(Error::Invalid("forge view binding mismatch"));
        }
        if record.public_view == Some(version) || record.views.values().any(|v| *v == version) {
            return Err(Error::Invalid("forge current view cannot retire"));
        }
        self.inner
            .forge_retire_projection_roots_in(tx, account, version)?;
        if tx
            .get(&self.inner.spaces.meta, retired_key(account, version))?
            .is_none()
        {
            tx.insert(&self.inner.spaces.meta, retired_key(account, version), [1]);
            tx.remove(
                &self.inner.spaces.refs,
                key(&account.repository_id(), view_reference(version).as_bytes()),
            );
            record.bump()?;
            tx.insert(
                &self.inner.spaces.meta,
                account_key(account),
                encode_account(&record)?,
            );
        }
        Ok(record.wire())
    }

    pub fn forge_check_view(&self, view: &ResolvedView) -> Result<()> {
        let _gate = self.inner.lock_read()?;
        self.inner
            .forge_check_view_in(&self.inner.db.read_tx(), view)
    }
    pub fn forge_authorize_view_export(&self, view: &ResolvedView) -> Result<()> {
        self.forge_check_view(view)?;
        if !view.spec.export_allowed || !view.policy_spec.export_allowed {
            return Err(Error::ForgeAccessDenied);
        }
        Ok(())
    }

    /// Enumerate only visible paths. No canonical tree/commit payload is returned.
    /// Symlinks remain typed entries; file reads never follow or resolve them.
    pub fn forge_view_files(
        &self,
        view: &ResolvedView,
        max_entries: usize,
    ) -> Result<Vec<ForgeFile>> {
        self.forge_check_view(view)?;
        if max_entries == 0 || max_entries > MAX_ENTRIES {
            return Err(Error::Limit("forge file count"));
        }
        let _pin = self.pin_root(
            &view.repository,
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(view.spec.revision),
            },
        )?;
        let (_, bytes) = self.read_metadata(
            &view.repository,
            ObjectKey {
                kind: Kind::Commit,
                id: ObjectId(view.spec.revision),
            },
        )?;
        let commit = match tinytree_model::decode(&bytes, self.inner.config.model_limits)? {
            Object::Commit(commit) if commit.root_tree.terms.len() == 1 => commit,
            _ => return Err(Error::Invalid("forge view requires resolved revision")),
        };
        let mut stack = vec![(String::new(), commit.root_tree.terms[0], 0)];
        let mut files = Vec::new();
        let mut read_bytes = bytes.len();
        let mut path_bytes = 0usize;
        let mut entries = 0usize;
        while let Some((prefix, id, depth)) = stack.pop() {
            if depth > 64 {
                return Err(Error::Limit("forge view depth"));
            }
            let (_, bytes) = self.read_metadata(
                &view.repository,
                ObjectKey {
                    kind: Kind::Tree,
                    id: ObjectId(id.0),
                },
            )?;
            read_bytes = read_bytes
                .checked_add(bytes.len())
                .ok_or(Error::Limit("forge tree read"))?;
            if read_bytes > MAX_TREE_BYTES {
                return Err(Error::Limit("forge tree read"));
            }
            let tree = match tinytree_model::decode(&bytes, self.inner.config.model_limits)? {
                Object::Tree(tree) => tree,
                _ => return Err(Error::Corrupt("forge tree type")),
            };
            for entry in tree.entries {
                entries += 1;
                if entries > MAX_ENTRIES {
                    return Err(Error::Limit("forge tree entries"));
                }
                let canonical = if prefix.is_empty() {
                    entry.name
                } else {
                    format!("{prefix}/{}", entry.name)
                };
                valid_path(&canonical, false)?;
                path_bytes = path_bytes
                    .checked_add(canonical.len())
                    .ok_or(Error::Limit("forge projected path bytes"))?;
                if path_bytes > MAX_VIEW_PATH_BYTES {
                    return Err(Error::Limit("forge projected path bytes"));
                }
                match entry.value {
                    TreeValue::Tree(child) => {
                        if view.spec.could_contain(&canonical)
                            && view.policy_spec.could_contain(&canonical)
                        {
                            stack.push((canonical, child, depth + 1));
                        }
                    }
                    TreeValue::File {
                        id,
                        size_bytes,
                        executable,
                        copy_id,
                    } if view.allows_path(&canonical) => {
                        let descriptor = self
                            .descriptor(
                                &view.repository,
                                ObjectKey {
                                    kind: Kind::File,
                                    id: ObjectId(id.0),
                                },
                            )?
                            .ok_or(Error::Corrupt("forge file missing"))?;
                        if descriptor.size_bytes != size_bytes {
                            return Err(Error::Corrupt("forge file size"));
                        }
                        let relative = relative_path(view.root_prefix(), &canonical)?;
                        path_bytes = path_bytes
                            .checked_add(relative.len())
                            .ok_or(Error::Limit("forge projected path bytes"))?;
                        if path_bytes > MAX_VIEW_PATH_BYTES {
                            return Err(Error::Limit("forge projected path bytes"));
                        }
                        files.push(ForgeFile {
                            path: relative,
                            canonical_path: canonical,
                            descriptor,
                            executable,
                            copy_id: copy_id.map(|id| id.0),
                        });
                    }
                    TreeValue::Symlink { id, target_bytes } if view.allows_path(&canonical) => {
                        let descriptor = self
                            .descriptor(
                                &view.repository,
                                ObjectKey {
                                    kind: Kind::Symlink,
                                    id: ObjectId(id.0),
                                },
                            )?
                            .ok_or(Error::Corrupt("forge symlink missing"))?;
                        let _ = target_bytes;
                        let relative = relative_path(view.root_prefix(), &canonical)?;
                        path_bytes = path_bytes
                            .checked_add(relative.len())
                            .ok_or(Error::Limit("forge projected path bytes"))?;
                        if path_bytes > MAX_VIEW_PATH_BYTES {
                            return Err(Error::Limit("forge projected path bytes"));
                        }
                        files.push(ForgeFile {
                            path: relative,
                            canonical_path: canonical,
                            descriptor,
                            executable: false,
                            copy_id: None,
                        });
                    }
                    _ => (),
                }
                if files.len() > max_entries {
                    return Err(Error::Limit("forge file count"));
                }
            }
        }
        files.sort_by(|a, b| a.path.cmp(&b.path));
        self.forge_check_view(view)?;
        Ok(files)
    }

    pub fn forge_read_view_file(
        &self,
        view: &ResolvedView,
        relative: &str,
        max_bytes: usize,
    ) -> Result<Vec<u8>> {
        view.canonical_path(relative)?;
        if max_bytes == 0 || max_bytes > 32 * 1024 * 1024 {
            return Err(Error::Limit("forge file read"));
        }
        let file = self
            .forge_view_files(view, MAX_ENTRIES)?
            .into_iter()
            .find(|f| f.path == relative)
            .ok_or(Error::ForgeAccessDenied)?;
        if file.descriptor.key.kind != Kind::File {
            return Err(Error::Invalid("forge regular file required"));
        }
        if file.descriptor.size_bytes > max_bytes as u64 {
            return Err(Error::Limit("forge file read"));
        }
        let mut lease = self.read(&view.repository, file.descriptor.key)?;
        let mut bytes = Vec::with_capacity(file.descriptor.size_bytes as usize);
        lease.read_to_end(&mut bytes)?;
        if bytes.len() != file.descriptor.size_bytes as usize {
            return Err(Error::Corrupt("forge file length"));
        }
        self.forge_check_view(view)?;
        Ok(bytes)
    }
}

fn relative_path(root: &str, canonical: &str) -> Result<String> {
    if root.is_empty() {
        return Ok(canonical.to_owned());
    }
    canonical
        .strip_prefix(root)
        .and_then(|tail| tail.strip_prefix('/'))
        .map(str::to_owned)
        .ok_or(Error::Corrupt("forge projected path"))
}

impl Inner {
    fn forge_following_view_plan_in(
        &self,
        tx: &impl Readable,
        account: &AccountId,
        new_main: CommitId,
    ) -> Result<(AccountRecord, Vec<(ViewSpec, ViewVersion)>)> {
        if new_main == CommitId::ROOT {
            return Err(Error::Invalid("forge canonical main"));
        }
        let mut record = self.forge_account_in(tx, account)?;
        let mut updates = Vec::new();
        let mut additional = 0u64;
        for name in record.follow_main.clone() {
            let old = *record
                .views
                .get(&name)
                .ok_or(Error::Corrupt("forge following view policy"))?;
            let mut spec = self.forge_view_in(tx, account, old)?;
            if spec.revision == new_main.0 {
                continue;
            }
            spec.revision = new_main.0;
            spec.validate()?;
            let version = view_version(account, &spec)?;
            if tx
                .get(&self.spaces.meta, retired_key(account, version))?
                .is_some()
            {
                return Err(Error::Invalid("forge followed version retired"));
            }
            if tx
                .get(&self.spaces.meta, view_key(account, version))?
                .is_none()
            {
                additional = additional
                    .checked_add(1)
                    .ok_or(Error::Limit("forge view versions"))?;
            }
            record.views.insert(name, version);
            if record.public_view == Some(old) {
                record.public_view = Some(version);
            }
            updates.push((spec, version));
        }
        record.versions = record
            .versions
            .checked_add(additional)
            .ok_or(Error::Limit("forge view versions"))?;
        if record.versions > MAX_VERSIONS {
            return Err(Error::Limit("forge view versions"));
        }
        if encode_account(&record)?.len() > MAX_ACCOUNT_BYTES {
            return Err(Error::Limit("forge account record"));
        }
        Ok((record, updates))
    }
    pub(crate) fn forge_check_following_view_capacity_in(
        &self,
        tx: &impl Readable,
        account: &AccountId,
        new_main: CommitId,
    ) -> Result<()> {
        self.forge_following_view_plan_in(tx, account, new_main)
            .map(|_| ())
    }
    /// Trusted canonical finalization hook: update named current-monorepo views
    /// in the same transaction as main/review status, without changing ACL epoch.
    pub(crate) fn forge_advance_main_views_in(
        &self,
        tx: &mut fjall::SingleWriterWriteTx<'_>,
        repo: &str,
        new_main: CommitId,
    ) -> Result<()> {
        let Some(account) = self.forge_account_for_repository_in(&*tx, repo)? else {
            return Ok(());
        };
        let (record, updates) = self.forge_following_view_plan_in(&*tx, &account, new_main)?;
        if updates.is_empty() {
            return Ok(());
        }
        let descriptor = self
            .descriptor(
                repo,
                ObjectKey {
                    kind: Kind::Commit,
                    id: ObjectId(new_main.0),
                },
            )?
            .ok_or(Error::Corrupt("forge canonical main object"))?;
        for (spec, version) in updates {
            tx.insert(
                &self.spaces.meta,
                view_key(&account, version),
                encode_view(&spec)?,
            );
            tx.insert(
                &self.spaces.refs,
                key(repo, view_reference(version).as_bytes()),
                encode(&StoredReference {
                    generation: 1,
                    commit: Some(descriptor.into()),
                })?,
            );
        }
        tx.insert(
            &self.spaces.meta,
            account_key(&account),
            encode_account(&record)?,
        );
        Ok(())
    }

    pub(crate) fn forge_check_view_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
    ) -> Result<()> {
        let current =
            self.forge_resolve_view_in(tx, &view.account, view.actor.as_ref(), view.version)?;
        if current.epoch != view.epoch {
            return Err(Error::StaleForgePolicy);
        }
        Ok(())
    }
    pub(crate) fn forge_authorize_view_export_in(
        &self,
        tx: &impl Readable,
        view: &ResolvedView,
    ) -> Result<()> {
        self.forge_check_view_in(tx, view)?;
        if !view.spec.export_allowed || !view.policy_spec.export_allowed {
            return Err(Error::ForgeAccessDenied);
        }
        Ok(())
    }
    fn forge_account_in(&self, tx: &impl Readable, account: &AccountId) -> Result<AccountRecord> {
        account.validate()?;
        let bytes = tx
            .get(&self.spaces.meta, account_key(account))?
            .ok_or(Error::ForgeAccessDenied)?;
        let record: AccountRecord = decode_account(&bytes)?;
        if record.id != *account || record.epoch == 0 {
            return Err(Error::Corrupt("forge account record"));
        }
        Ok(record)
    }
    fn forge_view_in(
        &self,
        tx: &impl Readable,
        account: &AccountId,
        version: ViewVersion,
    ) -> Result<ViewSpec> {
        let bytes = tx
            .get(&self.spaces.meta, view_key(account, version))?
            .ok_or(Error::ForgeAccessDenied)?;
        let spec: ViewSpec = decode_view(&bytes)?;
        spec.validate()?;
        if view_version(account, &spec)? != version {
            return Err(Error::Corrupt("forge view identity"));
        }
        Ok(spec)
    }
    pub(crate) fn forge_account_for_repository_in(
        &self,
        tx: &impl Readable,
        repo: &str,
    ) -> Result<Option<AccountId>> {
        let Some(id) = repo.strip_prefix("forge-") else {
            return Ok(None);
        };
        let account = AccountId(id.into());
        account.validate()?;
        if tx.get(&self.spaces.meta, account_key(&account))?.is_none() {
            return Ok(None);
        }
        self.forge_account_in(tx, &account)?;
        Ok(Some(account))
    }
    pub(crate) fn forge_authorize_in(
        &self,
        tx: &impl Readable,
        account: &AccountId,
        actor: &ForgeActor,
        action: ForgeAction,
        expected: Option<PolicyEpoch>,
    ) -> Result<ForgeAccess> {
        valid_principal(&actor.principal)?;
        let record = self.forge_account_in(tx, account)?;
        let owner = record.owner == actor.principal;
        let member = record.members.get(&actor.principal);
        if !owner && member.is_none() {
            return Err(Error::ForgeAccessDenied);
        }
        let admin = owner || member.is_some_and(|m| m.is_admin);
        let write = admin || member.is_some_and(|m| m.can_write);
        let review = admin || member.is_some_and(|m| m.can_review);
        let allowed = match action {
            ForgeAction::Read => true,
            ForgeAction::Write => write,
            ForgeAction::Review => review,
            ForgeAction::Admin | ForgeAction::Export => admin,
        };
        if !allowed {
            return Err(Error::ForgeAccessDenied);
        }
        if expected.is_some_and(|epoch| epoch.0 != record.epoch) {
            return Err(Error::StaleForgePolicy);
        }
        Ok(ForgeAccess {
            account: account.clone(),
            repository: account.repository_id(),
            epoch: PolicyEpoch(record.epoch),
            can_write: write,
            can_review: review,
            is_admin: admin,
        })
    }
    pub(crate) fn forge_resolve_view_in(
        &self,
        tx: &impl Readable,
        account: &AccountId,
        actor: Option<&ForgeActor>,
        version: ViewVersion,
    ) -> Result<ResolvedView> {
        let record = self.forge_account_in(tx, account)?;
        if tx
            .get(&self.spaces.meta, retired_key(account, version))?
            .is_some()
        {
            return Err(Error::ForgeAccessDenied);
        }
        let spec = self.forge_view_in(tx, account, version)?;
        let current = *record
            .views
            .get(&spec.name)
            .ok_or(Error::ForgeAccessDenied)?;
        let policy = self.forge_view_in(tx, account, current)?;
        let member = actor.and_then(|a| record.members.get(&a.principal));
        let admin = actor.is_some_and(|a| a.principal == record.owner)
            || member.is_some_and(|m| m.is_admin);
        let allowed = admin
            || match &policy.visibility {
                ForgeViewVisibility::Public => true,
                ForgeViewVisibility::Members => member.is_some(),
                ForgeViewVisibility::Roles(roles) => {
                    member.is_some_and(|m| roles.iter().any(|r| m.roles.contains(r)))
                }
            };
        if !allowed {
            return Err(Error::ForgeAccessDenied);
        }
        Ok(ResolvedView {
            account: account.clone(),
            repository: account.repository_id(),
            version,
            epoch: PolicyEpoch(record.epoch),
            actor: actor.cloned(),
            spec,
            policy_spec: policy,
        })
    }
    pub(crate) fn validate_forge_accounts_on_reopen(&self) -> Result<()> {
        let tx = self.db.read_tx();
        let mut count = 0;
        for row in tx.prefix(&self.spaces.meta, ACCOUNT_PREFIX) {
            let (account_physical, value) = row.into_inner()?;
            count += 1;
            if count > MAX_ACCOUNTS {
                return Err(Error::Limit("forge reopen accounts"));
            }
            let record: AccountRecord = decode_account(&value)?;
            if account_physical.as_ref() != account_key(&record.id)
                || record.members.len() > MAX_MEMBERS
                || record.views.len() > MAX_VIEWS
                || record.versions > MAX_VERSIONS
            {
                return Err(Error::Corrupt("forge account limits"));
            }
            self.repository(&record.id.repository_id())?;
            record.id.validate()?;
            valid_principal(&record.owner)?;
            if record.epoch == 0 {
                return Err(Error::Corrupt("forge policy epoch"));
            }
            for (principal, member) in &record.members {
                if principal != &member.principal || member.roles.len() > 16 {
                    return Err(Error::Corrupt("forge membership"));
                }
                valid_principal(principal)?;
                for role in &member.roles {
                    valid_name(role)?;
                }
            }
            for version in record.views.values() {
                self.forge_view_in(&tx, &record.id, *version)?;
            }
            if let Some(version) = record.public_view {
                if self.forge_view_in(&tx, &record.id, version)?.visibility
                    != ForgeViewVisibility::Public
                {
                    return Err(Error::Corrupt("forge public view"));
                }
            }
            let mut versions = 0u64;
            let prefix = [VIEW_PREFIX, record.id.0.as_bytes(), b"/"].concat();
            for item in tx.prefix(&self.spaces.meta, &prefix) {
                let (physical, bytes) = item.into_inner()?;
                versions += 1;
                if versions > MAX_VERSIONS {
                    return Err(Error::Limit("forge reopen views"));
                }
                let spec: ViewSpec = decode_view(&bytes)?;
                spec.validate()?;
                let version = view_version(&record.id, &spec)?;
                if physical.as_ref() != view_key(&record.id, version) {
                    return Err(Error::Corrupt("forge view key"));
                }
                let root = tx.get(
                    &self.spaces.refs,
                    key(
                        &record.id.repository_id(),
                        view_reference(version).as_bytes(),
                    ),
                )?;
                let retired = tx.get(&self.spaces.meta, retired_key(&record.id, version))?;
                if let Some(retired) = retired {
                    if retired.as_ref() != [1]
                        || root.is_some()
                        || record.views.values().any(|v| *v == version)
                    {
                        return Err(Error::Corrupt("forge retired view"));
                    }
                } else {
                    let root: StoredReference =
                        decode(&root.ok_or(Error::Corrupt("forge view root missing"))?)?;
                    if root.generation != 1
                        || root
                            .commit
                            .as_ref()
                            .is_none_or(|d| d.id != spec.revision || d.kind != Kind::Commit as u8)
                    {
                        return Err(Error::Corrupt("forge view root identity"));
                    }
                    self.require_descriptor(
                        &record.id.repository_id(),
                        root.commit
                            .ok_or(Error::Corrupt("forge view root"))?
                            .descriptor()?,
                    )?;
                }
            }
            if record.follow_main.len() > MAX_VIEWS
                || record
                    .follow_main
                    .iter()
                    .any(|name| !record.views.contains_key(name))
            {
                return Err(Error::Corrupt("forge following view policy"));
            }
            if versions != record.versions {
                return Err(Error::Corrupt("forge view count"));
            }
        }
        let actual = tx
            .get(&self.spaces.meta, b"forge/account-count/v1")?
            .map(|b| decode::<u64>(&b))
            .transpose()?
            .unwrap_or(0);
        if count != actual {
            return Err(Error::Corrupt("forge account count"));
        }
        let mut replay_counts = BTreeMap::<AccountId, u64>::new();
        for entry in tx.prefix(&self.spaces.meta, b"forge/account-replays/v1/") {
            let (physical, bytes) = entry.into_inner()?;
            if bytes.len() > MAX_REPLAY_BYTES {
                return Err(Error::Corrupt("forge account replay size"));
            }
            let replay: AccountReplay = decode_replay(&bytes)?;
            replay.account.validate()?;
            valid_principal(&replay.principal)?;
            self.forge_account_in(&tx, &replay.account)?;
            if replay.request_id.is_empty()
                || replay.request_id.len() > 128
                || physical.as_ref()
                    != replay_key(
                        &replay.account,
                        &ForgeActor {
                            principal: replay.principal.clone(),
                        },
                        &replay.request_id,
                    )
            {
                return Err(Error::Corrupt("forge account replay binding"));
            }
            let count = replay_counts.entry(replay.account).or_default();
            *count += 1;
            if *count > 4096 {
                return Err(Error::Corrupt("forge account replay count"));
            }
        }
        for (account, count) in replay_counts {
            if tx
                .get(
                    &self.spaces.meta,
                    format!("forge/account-replay-count/v1/{}", account.0),
                )?
                .map(|b| decode::<u64>(&b))
                .transpose()?
                != Some(count)
            {
                return Err(Error::Corrupt("forge account replay count"));
            }
        }
        Ok(())
    }
}
