//! Read-only provider enrollment census for the replay-default transition.
//! Recomputed private material never leaves memory or grants new authority.
use crate::{
    config::{Config, NativeRepository},
    native_commands::NativeCommandHost,
};
use anyhow::{Context, Result, ensure};
use native_workspace::Scope;
use private_fs::PrivateDir;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    sync::Arc,
};

const MAX_ENTRIES: usize = 32768;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_LOCKS: usize = 257;
pub(crate) fn snapshot_bounds(config: &Config) -> (usize, usize) {
    (
        MAX_ENTRIES,
        config.native.workspaces.max_metadata_bytes.min(MAX_BYTES),
    )
}

struct Candidates {
    current: [u8; 32],
    legacy: Option<[u8; 32]>,
    current_scope: String,
    legacy_scope: Option<String>,
    family: Option<bool>,
}
impl Candidates {
    fn observe(&mut self, current: bool, legacy: bool) -> Result<()> {
        ensure!(
            current != legacy,
            "native provider enrollment does not match the effective configuration"
        );
        let family = legacy;
        ensure!(
            self.family.is_none_or(|old| old == family),
            "native provider enrollment families conflict"
        );
        self.family = Some(family);
        Ok(())
    }
    fn selected(&self) -> [u8; 32] {
        if self.family == Some(true) {
            self.legacy.expect("checked legacy family")
        } else {
            self.current
        }
    }
}
/// Startup-only bounded private observations; deliberately no Debug/Serialize.
pub(crate) struct Census {
    candidates: BTreeMap<String, Candidates>,
    entries: usize,
    bytes: usize,
    max_bytes: usize,
}
impl Census {
    pub(crate) fn entry(&mut self) -> Result<()> {
        self.entries = self
            .entries
            .checked_add(1)
            .context("provider census count overflow")?;
        ensure!(self.entries <= MAX_ENTRIES, "provider census entry limit");
        Ok(())
    }
    pub(crate) fn charge(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .context("provider census byte overflow")?;
        ensure!(self.bytes <= self.max_bytes, "provider census byte limit");
        Ok(())
    }
    pub(crate) fn read(&mut self, dir: &PrivateDir, name: &str, cap: usize) -> Result<Vec<u8>> {
        let cap = cap.min(self.max_bytes.saturating_sub(self.bytes));
        ensure!(cap > 0, "provider census byte limit");
        let bytes = dir.read(name, cap)?;
        self.charge(bytes.len())?;
        Ok(bytes)
    }
    pub(crate) fn identity(&mut self, alias: &str, id: [u8; 32]) -> Result<()> {
        let c = self
            .candidates
            .get_mut(alias)
            .context("saved native provider alias is not configured")?;
        c.observe(id == c.current, c.legacy == Some(id))
    }
    pub(crate) fn scope(&mut self, alias: &str, scope: &str) -> Result<()> {
        let c = self
            .candidates
            .get_mut(alias)
            .context("saved native provider alias is not configured")?;
        c.observe(
            scope == c.current_scope,
            c.legacy_scope.as_deref() == Some(scope),
        )
    }
}
fn fingerprint(
    settings: &NativeRepository,
    path: &Path,
    reapi: &reapi::policy::Policy,
    credential: Option<&Option<String>>,
    ca: Option<&Arc<[u8]>>,
) -> Result<[u8; 32]> {
    let mut fingerprint = blake3::Hasher::new();
    fingerprint.update(b"native-workspace-provider-scope-v1\0");
    for bytes in [
        serde_json::to_vec(settings)?,
        serde_json::to_vec(path)?,
        serde_json::to_vec(reapi)?,
        serde_json::to_vec(&credential)?,
        ca.map_or_else(Vec::new, |v| v.to_vec()),
    ] {
        fingerprint.update(&(bytes.len() as u64).to_le_bytes());
        fingerprint.update(&bytes);
    }
    Ok(*fingerprint.finalize().as_bytes())
}
#[cfg(test)]
pub(crate) fn resolve(
    config: &Config,
    ca: &BTreeMap<String, Arc<[u8]>>,
    credentials: &BTreeMap<String, Option<String>>,
    commands: Option<&NativeCommandHost>,
) -> Result<BTreeMap<String, [u8; 32]>> {
    resolve_with_pending(config, ca, credentials, commands, &[])
}
pub(crate) fn resolve_with_pending(
    config: &Config,
    ca: &BTreeMap<String, Arc<[u8]>>,
    credentials: &BTreeMap<String, Option<String>>,
    commands: Option<&NativeCommandHost>,
    pending: &[native_retention::Registration],
) -> Result<BTreeMap<String, [u8; 32]>> {
    let mut census = Census {
        candidates: BTreeMap::new(),
        entries: 0,
        bytes: 0,
        max_bytes: config.native.workspaces.max_metadata_bytes.min(MAX_BYTES),
    };
    for _ in pending {
        census.entry()?;
        census.charge(native_retention::CaptureRegistrations::BYTES_PER_ENTRY)?;
    }
    for (alias, settings) in &config.native.repositories {
        let path = config.state_dir.join("native").join(alias).canonicalize()?;
        let current = fingerprint(
            settings,
            &path,
            &config.reapi,
            credentials.get(alias),
            ca.get(alias),
        )?;
        let legacy = if settings
            .remote
            .as_ref()
            .is_some_and(|r| r.query.max_replays == 4096)
        {
            let mut projected = settings.clone();
            projected.remote.as_mut().unwrap().query.max_replays = 256;
            let value = fingerprint(
                &projected,
                &path,
                &config.reapi,
                credentials.get(alias),
                ca.get(alias),
            )?;
            ensure!(
                value != current,
                "ambiguous native provider identity candidates"
            );
            Some(value)
        } else {
            None
        };
        census.candidates.insert(
            alias.clone(),
            Candidates {
                current,
                legacy,
                current_scope: Scope::from_configured_identity(alias, &current)?
                    .backend()
                    .into(),
                legacy_scope: legacy
                    .map(|id| {
                        Scope::from_configured_identity(alias, &id).map(|s| s.backend().to_owned())
                    })
                    .transpose()?,
                family: None,
            },
        );
    }
    let known = crate::native_workspaces::inspect_provider_enrollments(config, &mut census)?;
    let by_id = known
        .iter()
        .map(|e| (e.workspace_id, e))
        .collect::<BTreeMap<_, _>>();
    ensure!(
        by_id.len() == known.len(),
        "duplicate provider workspace census identity"
    );
    let mappings = if let Some(commands) = commands {
        commands.inspect_provider_mappings(&mut census, pending)?
    } else {
        ensure!(
            pending.is_empty(),
            "pending provider census host unavailable"
        );
        BTreeMap::new()
    };
    for (mount, (alias, _)) in &mappings {
        let enrolled = known
            .iter()
            .find(|entry| hex(&entry.workspace_id) == *mount)
            .context("orphan native command enrollment; preserve state")?;
        ensure!(
            &enrolled.store == alias,
            "provider command census alias differs"
        );
    }
    let mut writer_locks = Vec::new();
    let registry_path = config
        .native
        .workspaces
        .registry_path
        .clone()
        .map(Ok)
        .unwrap_or_else(native_workspace_admin::Registry::default_path)?;
    if registry_path.try_exists()? {
        let registry = native_workspace_admin::Registry::open_existing(&registry_path)
            .map_err(|_| anyhow::anyhow!("invalid native provider census registry"))?;
        let pending = known
            .iter()
            .filter(|e| e.pending)
            .map(|e| e.workspace_id)
            .collect::<BTreeSet<_>>();
        let snapshot = registry.inspect_enrollments(
            &native_workspace_admin::Endpoint::UnixSocket {
                path: config.state_dir.join("rbe.sock"),
            },
            &pending,
            MAX_ENTRIES.saturating_sub(census.entries),
            census.max_bytes.saturating_sub(census.bytes),
            MAX_LOCKS,
        )?;
        census.entries += snapshot.entries;
        census.charge(snapshot.read_bytes)?;
        let enrolled_ids = snapshot
            .bindings
            .iter()
            .map(|binding| binding.workspace_id)
            .collect::<BTreeSet<_>>();
        ensure!(
            known.iter().all(|entry| entry.pending
                || entry.retired
                || enrolled_ids.contains(&entry.workspace_id)),
            "initialized provider enrollment has no administration"
        );
        for binding in &snapshot.bindings {
            census.charge(4096)?;
            let enrolled = by_id
                .get(&binding.workspace_id)
                .context("orphan native administration; preserve state")?;
            ensure!(
                !enrolled.retired,
                "retired provider enrollment still has administration"
            );
            ensure!(
                binding.repository == enrolled.store
                    && binding.root == enrolled.root
                    && binding.rpc_mount_id == hex(&binding.workspace_id),
                "provider census binding differs"
            );
            if let Some((alias, _)) = mappings.get(&binding.rpc_mount_id) {
                ensure!(
                    alias == &binding.repository,
                    "provider command census alias differs"
                );
                ensure!(
                    writer_locks.len() + snapshot.lock_count() < MAX_LOCKS,
                    "provider census lock limit"
                );
                writer_locks.push(NativeCommandHost::inspect_writer_lease(binding)?);
            }
            let index = binding.repo.join("index");
            if index.try_exists()? {
                ensure!(
                    mappings.contains_key(&binding.rpc_mount_id),
                    "native index has no command enrollment"
                );
                let parent = PrivateDir::open_existing(&binding.repo)?;
                let kind = parent.read_descendant("index/type", 128)?;
                ensure!(
                    kind == jj_native_index::NAME.as_bytes() || kind == b"default",
                    "unknown provider index type"
                );
                if kind == jj_native_index::NAME.as_bytes() {
                    census.charge(16384)?;
                    let index_config = jj_native_index::NativeIndexStore::inspect_config(&index)
                        .map_err(|_| anyhow::anyhow!("invalid provider index census manifest"))?;
                    ensure!(
                        index_config.store_alias == binding.repository
                            && index_config.endpoint
                                == jj_native_index::Endpoint::UnixSocket(
                                    config.state_dir.join("rbe.sock")
                                ),
                        "provider index census binding differs"
                    );
                    census.identity(&binding.repository, index_config.provider_fingerprint)?;
                }
            } else {
                ensure!(
                    enrolled.pending,
                    "initialized native enrollment has no index"
                );
            }
        }
        // Keep registry initializer guards alive through all index observations.
        drop(snapshot);
    } else {
        ensure!(
            known.iter().all(|entry| entry.retired || entry.intent_only) && mappings.is_empty(),
            "native enrollment registry missing; preserve state"
        );
    }
    let mut tickets = BTreeMap::new();
    for journal in crate::native_capture_upload::inspect_provider_journals(config, &mut census)? {
        if let Some(previous) =
            tickets.insert(journal.ticket, (journal.workspace, journal.provider))
        {
            ensure!(
                previous == (journal.workspace, journal.provider),
                "provider upload ticket witnesses conflict"
            );
        }
        let enrolled = by_id
            .get(&journal.workspace)
            .context("provider upload has no workspace enrollment")?;
        let mount = hex(&journal.workspace);
        let (alias, identity) = mappings
            .get(&mount)
            .context("provider upload has no command enrollment")?;
        ensure!(
            alias == &enrolled.store && identity == &journal.provider,
            "provider upload enrollment differs"
        );
        census.identity(alias, journal.provider)?;
    }
    Ok(census
        .candidates
        .into_iter()
        .map(|(alias, c)| (alias, c.selected()))
        .collect())
}
pub(crate) fn is_atomic_temporary(name: &str) -> bool {
    let Some(body) = name
        .strip_prefix(".private-")
        .and_then(|s| s.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((pid, sequence)) = body.split_once('-') else {
        return false;
    };
    !pid.is_empty()
        && !sequence.is_empty()
        && pid.bytes().all(|b| b.is_ascii_digit())
        && sequence.bytes().all(|b| b.is_ascii_digit())
        && pid.parse::<u32>().is_ok()
        && sequence.parse::<u64>().is_ok()
}
fn hex(id: &[u8; 32]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs2::FileExt;
    use jj_lib::index::{Index as _, MutableIndex as _, ReadonlyIndex as _};
    use native_broker::query_protocol as q;
    use native_broker::{
        BeginQuery, ForkQueryScope, NativeQueryIo, OpenQueryScope, QueryHandle, QueryOwner,
        QueryPage, QueryReply, QueryResources, QueryScope,
    };
    use native_workspace_admin::{Binding, Registry};
    use tinytree_model::CommitId;
    use tonic::Status;
    /// Explicit compatibility fixture: root is known, every new commit is local.
    /// Any graph/prefix/content query beyond that boundary is a fixture failure.
    #[derive(Debug)]
    struct RootOnlyQuery;
    fn ensure_root_scope(input: &OpenQueryScope) -> std::result::Result<(), Status> {
        if input.roots.iter().any(|id| *id != CommitId::ROOT) {
            return Err(Status::unimplemented(
                "root-only compatibility fixture scope",
            ));
        }
        Ok(())
    }
    #[allow(unused_variables)]
    #[async_trait::async_trait]
    impl NativeQueryIo for RootOnlyQuery {
        fn resources(&self) -> QueryResources {
            QueryResources {
                memory_bytes: 4096,
                temporary_bytes: 0,
                persistent_memory_bytes: 0,
            }
        }
        async fn open_scope(
            &self,
            input: OpenQueryScope,
            owner: QueryOwner,
        ) -> Result<QueryReply<QueryScope>, Status> {
            ensure_root_scope(&input)?;
            Ok(QueryReply {
                value: QueryScope {
                    handle: [1; 32],
                    expires_at_unix_seconds: i64::MAX,
                    query_identity_version: input.query_identity_version,
                    cloud_source_digest: None,
                },
                owner,
            })
        }
        async fn fork_scope(
            &self,
            input: ForkQueryScope,
            owner: QueryOwner,
        ) -> Result<QueryReply<QueryScope>, Status> {
            Err(Status::unimplemented(
                "root-only compatibility fixture query",
            ))
        }
        async fn facts(
            &self,
            scope: [u8; 32],
            facts: Vec<q::GraphFact>,
            owner: QueryOwner,
        ) -> Result<QueryReply<q::GraphFacts>, Status> {
            let mut answers = Vec::new();
            for fact in facts {
                if !matches!(fact.fact, Some(q::graph_fact::Fact::HasId(_))) {
                    return Err(Status::unimplemented(
                        "root-only compatibility fixture fact",
                    ));
                }
                answers.push(q::GraphFactAnswer {
                    answer: Some(q::graph_fact_answer::Answer::HasId(false)),
                });
            }
            Ok(QueryReply {
                value: q::GraphFacts {
                    answers,
                    scope_digest: vec![1; 32],
                    index_epoch: vec![1; 16],
                },
                owner,
            })
        }
        async fn resolve_commit_prefix(
            &self,
            scope: [u8; 32],
            prefix: String,
            owner: QueryOwner,
        ) -> Result<QueryReply<q::CommitPrefixResult>, Status> {
            Err(Status::unimplemented(
                "root-only compatibility fixture query",
            ))
        }
        async fn resolve_change_prefix(
            &self,
            scope: [u8; 32],
            prefix: String,
            visible_heads: Vec<CommitId>,
            owner: QueryOwner,
        ) -> Result<QueryReply<q::ChangePrefixResult>, Status> {
            Err(Status::unimplemented(
                "root-only compatibility fixture query",
            ))
        }
        async fn shortest_prefix(
            &self,
            scope: [u8; 32],
            target: q::shortest_prefix_request::Target,
            visible_heads: Vec<CommitId>,
            owner: QueryOwner,
        ) -> Result<QueryReply<q::ShortestPrefixResult>, Status> {
            Err(Status::unimplemented(
                "root-only compatibility fixture query",
            ))
        }
        async fn begin(
            &self,
            input: BeginQuery,
            owner: QueryOwner,
        ) -> Result<QueryReply<QueryHandle>, Status> {
            Err(Status::unimplemented(
                "root-only compatibility fixture query",
            ))
        }
        async fn page(
            &self,
            query: [u8; 32],
            expected_sequence: u64,
            owner: QueryOwner,
        ) -> Result<QueryReply<QueryPage>, Status> {
            Err(Status::unimplemented(
                "root-only compatibility fixture query",
            ))
        }
        async fn read_commit(
            &self,
            scope: [u8; 32],
            commit: CommitId,
            owner: QueryOwner,
        ) -> Result<QueryReply<jj_native_backend::VerifiedMetadata>, Status> {
            Err(Status::unimplemented(
                "root-only compatibility fixture query",
            ))
        }
        async fn close_query(&self, query: [u8; 32], owner: QueryOwner) -> Result<(), Status> {
            Ok(())
        }
        async fn close_scope(&self, scope: [u8; 32], owner: QueryOwner) -> Result<(), Status> {
            Ok(())
        }
    }
    struct NoFetch;
    #[async_trait::async_trait]
    impl local_cas::BlobSource for NoFetch {
        async fn fetch(&self, _: &local_cas::Artifact, _: &mut fs::File) -> Result<()> {
            anyhow::bail!("compatibility fixture must not fetch remote content")
        }
        async fn missing(&self, _: &[local_cas::Artifact]) -> Result<Vec<local_cas::Artifact>> {
            anyhow::bail!("compatibility fixture must not query remote content")
        }
    }
    struct Fixture {
        _directory: tempfile::TempDir,
        config: Config,
        credentials: BTreeMap<String, Option<String>>,
        commands: Arc<NativeCommandHost>,
        binding: Binding,
        old: [u8; 32],
        cache: Arc<local_cas::LocalCas>,
        remote: Option<jj_native_remote::RemoteRepository>,
        index: Option<jj_native_index::NativeIndexStore>,
        private_commit: jj_lib::backend::CommitId,
    }
    fn old_code_identity(
        config: &Config,
        native_credentials: &BTreeMap<String, Option<String>>,
        alias: &str,
    ) -> Result<[u8; 32]> {
        let root = config.state_dir.join("native");
        let settings = &config.native.repositories[alias];
        let native_ca = BTreeMap::<String, Arc<[u8]>>::new();
        let store_path = root.join(alias).canonicalize()?;
        let mut fingerprint = blake3::Hasher::new();
        fingerprint.update(b"native-workspace-provider-scope-v1\0");
        // Length-delimited private material is hashed in memory only. Neither
        // credential values nor this material are recorded in public output.
        for bytes in [
            serde_json::to_vec(settings)?,
            serde_json::to_vec(&store_path)?,
            serde_json::to_vec(&config.reapi)?,
            serde_json::to_vec(&native_credentials.get(alias))?,
            native_ca
                .get(alias)
                .map_or_else(Vec::new, |bytes| bytes.to_vec()),
        ] {
            fingerprint.update(&(bytes.len() as u64).to_le_bytes());
            fingerprint.update(&bytes);
        }
        let identity = *fingerprint.finalize().as_bytes();
        Ok(identity)
    }
    impl Fixture {
        async fn old(replays: usize) -> Result<Self> {
            let directory = tempfile::tempdir()?;
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700))?;
            let state = PrivateDir::create(directory.path().join("state"))?;
            let mut config = Config::default();
            config.state_dir = state.path().to_owned();
            config.native.workspaces.registry_path = Some(directory.path().join("registry"));
            let mut repository = NativeRepository::default();
            let mut remote = jj_native_remote::Config::default();
            remote.query.max_replays = replays;
            repository.remote = Some(remote);
            config
                .native
                .repositories
                .insert("example".into(), repository);
            let native = PrivateDir::create(state.path().join("native"))?;
            let objects = PrivateDir::create(native.path().join("example"))?;
            let credentials: BTreeMap<String, Option<String>> = [(
                "example".into(),
                Some("synthetic-provider-credential".into()),
            )]
            .into();
            let old = old_code_identity(&config, &credentials, "example")?;
            let cache = local_cas::LocalCas::open(
                directory.path().join("cache"),
                Arc::new(NoFetch),
                Default::default(),
            )?;
            let local = Arc::new(jj_native_backend::DirectObjectIo::init(
                objects.path(),
                Default::default(),
            )?);
            let remote = jj_native_remote::RemoteRepository::open(
                local,
                cache.clone(),
                &directory.path().join("remote-ledger"),
                &directory.path().join("remote-staging"),
                config.native.repositories["example"]
                    .remote
                    .clone()
                    .unwrap(),
            )?;
            let root = PrivateDir::create(directory.path().join("workspace"))?;
            let jj = PrivateDir::create(root.path().join(".jj"))?;
            jj.replace("native-workspace", hex(&[7; 32]).as_bytes())?;
            let registry =
                Registry::create(config.native.workspaces.registry_path.as_ref().unwrap())?;
            let binding = registry.binding(
                [7; 32],
                root.path(),
                "example".into(),
                native_broker::LocalEndpoint::UnixSocket(config.state_dir.join("rbe.sock")),
                hex(&[7; 32]),
            )?;
            registry.provision(&binding)?;
            let workspaces = PrivateDir::create(state.path().join("native-workspaces"))?;
            let entry = PrivateDir::create(workspaces.path().join(hex(&[7; 32])))?;
            // Explicit compatibility fixture: the committed pre-default-change
            // reservation, not a materialized projection or synthetic fallback.
            entry.replace("workspace.json",&serde_json::to_vec(&serde_json::json!({
                "version":2,"workspace_id":vec![7u8;32],"store":"example","root":root.path(),
                "origin":{"kind":"clone","bootstrap_commit":vec![0u8;32]},
                "scope":Scope::from_configured_identity("example",&old)?.backend(),"state":"pending",
                "native_mount":null
            }))?)?;
            let commands =
                Arc::new(NativeCommandHost::open(&state.path().join("native-commands")).await?);
            commands.register(&binding, old).await?;
            let admin = PrivateDir::open_existing(&binding.admin)?;
            admin.replace("initialization.json",&serde_json::to_vec(&serde_json::json!({
                "version":1,"binding":binding,"initial_commit":vec![0u8;32],"workspace":"default","ready":false
            }))?)?;
            let repo = PrivateDir::create(&binding.repo)?;
            let index_config = jj_native_index::Config {
                endpoint: jj_native_index::Endpoint::UnixSocket(config.state_dir.join("rbe.sock")),
                store_alias: "example".into(),
                provider_fingerprint: old,
            };
            let index = jj_native_index::NativeIndexStore::init(
                &repo.path().join("index"),
                index_config,
                Default::default(),
                Arc::new(RootOnlyQuery),
                remote.io.clone(),
                vec![],
            )
            .context("compatibility fixture index initialization")?;
            PrivateDir::open_existing(repo.path().join("index"))?
                .replace("type", jj_native_index::NAME.as_bytes())
                .context("compatibility fixture type record")?;
            let private_commit = {
                let backend = jj_native_backend::NativeBackend::from_io(
                    remote.io.clone(),
                    Default::default(),
                )
                .await?;
                let store = jj_lib::store::Store::new(
                    Box::new(backend),
                    jj_lib::signing::Signer::new(None, vec![]),
                    jj_lib::tree_merge::MergeOptions {
                        hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
                        same_change: jj_lib::merge::SameChange::Accept,
                    },
                );
                let root = store.root_commit();
                let mut value = root.store_commit().as_ref().clone();
                value.parents = vec![root.id().clone()];
                value.change_id = jj_lib::backend::ChangeId::from_bytes(&[9; 16]);
                value.description = "retained private compatibility draft".into();
                let draft = store
                    .write_commit(value, None)
                    .await
                    .context("compatibility fixture private commit")?;
                let mut pending = index.seed_index()?.start_modification();
                pending
                    .add_commit(&draft)
                    .await
                    .context("compatibility fixture private index")?;
                let saved = index
                    .persist_index(pending)
                    .context("compatibility fixture persist index")?;
                index
                    .associate_operation(&[8; 64], &saved)
                    .context("compatibility fixture operation")?;
                draft.id().clone()
            };
            Ok(Self {
                _directory: directory,
                config,
                credentials,
                commands,
                binding,
                old,
                cache,
                remote: Some(remote),
                index: Some(index),
                private_commit,
            })
        }
        fn selected(&self) -> Result<[u8; 32]> {
            Ok(resolve(
                &self.config,
                &BTreeMap::new(),
                &self.credentials,
                Some(&self.commands),
            )?["example"])
        }
    }
    #[tokio::test]
    async fn legacy_replay_default_preserves_existing_command_and_capture_enrollment() -> Result<()>
    {
        let mut fixture = Fixture::old(256).await?;
        let mapping = fixture
            .config
            .state_dir
            .join("native-commands/records")
            .join(format!("{}.json", fixture.binding.rpc_mount_id));
        let record = fixture
            .config
            .state_dir
            .join("native-workspaces")
            .join(fixture.binding.id())
            .join("workspace.json");
        let manifest = fixture.binding.repo.join("index/manifest");
        let command_records = PrivateDir::open_existing(mapping.parent().unwrap())?;
        command_records.replace(".private-12-34.tmp", b"{interrupted")?;
        let row: serde_json::Value = serde_json::from_slice(&fs::read(&mapping)?)?;
        let registration = native_retention::Registration {
            workspace: serde_json::from_value(row["workspace"].clone())?,
            op_store: serde_json::from_value(row["op_store"].clone())?,
            generation: 1,
        };
        assert_eq!(
            resolve_with_pending(
                &fixture.config,
                &BTreeMap::new(),
                &fixture.credentials,
                Some(&fixture.commands),
                &[registration]
            )?["example"],
            fixture.old
        );
        let unknown = native_retention::Registration {
            workspace: native_retention::WorkspaceId([3; 16]),
            ..registration
        };
        assert!(
            resolve_with_pending(
                &fixture.config,
                &BTreeMap::new(),
                &fixture.credentials,
                Some(&fixture.commands),
                &[unknown]
            )
            .is_err()
        );
        let mut duplicate = row;
        duplicate["mount"] = hex(&[6; 32]).into();
        let duplicate_name = format!("{}.json", hex(&[6; 32]));
        command_records.replace(&duplicate_name, &serde_json::to_vec(&duplicate)?)?;
        let error = resolve_with_pending(
            &fixture.config,
            &BTreeMap::new(),
            &fixture.credentials,
            Some(&fixture.commands),
            &[registration],
        )
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("ambiguous pending provider census mapping")
        );
        fs::remove_file(command_records.path().join(&duplicate_name))?;
        let admin_records = PrivateDir::open_existing(
            fixture
                .config
                .native
                .workspaces
                .registry_path
                .as_ref()
                .unwrap()
                .join("records"),
        )?;
        admin_records.replace(".private-12-35.tmp", b"{interrupted")?;
        let before = (
            fs::read(&mapping)?,
            fs::read(&record)?,
            fs::read(&manifest)?,
        );
        fixture
            .config
            .native
            .repositories
            .get_mut("example")
            .unwrap()
            .remote
            .as_mut()
            .unwrap()
            .query
            .max_replays = 4096;
        assert_eq!(fixture.selected()?, fixture.old);
        assert_eq!(
            fixture.config.native.repositories["example"]
                .remote
                .as_ref()
                .unwrap()
                .query
                .max_replays,
            4096
        );
        fixture
            .commands
            .register(&fixture.binding, fixture.selected()?)
            .await?;
        assert_eq!(
            crate::native_workspaces::capture_upload_workspace(
                &fixture.config.state_dir,
                &fixture.binding.rpc_mount_id,
                "example",
                &fixture.selected()?
            )?,
            [7; 32]
        );
        fixture.index.take();
        fixture.remote.as_ref().unwrap().wait_for_idle().await;
        fixture.remote.take();
        let local = Arc::new(jj_native_backend::DirectObjectIo::load(
            &fixture.config.state_dir.join("native/example"),
            Default::default(),
        )?);
        let remote = jj_native_remote::RemoteRepository::open(
            local,
            fixture.cache.clone(),
            &fixture._directory.path().join("remote-ledger"),
            &fixture._directory.path().join("remote-staging"),
            fixture.config.native.repositories["example"]
                .remote
                .clone()
                .unwrap(),
        )?;
        let expected = jj_native_index::Config {
            endpoint: jj_native_index::Endpoint::UnixSocket(
                fixture.config.state_dir.join("rbe.sock"),
            ),
            store_alias: "example".into(),
            provider_fingerprint: fixture.selected()?,
        };
        let reopened = jj_native_index::NativeIndexStore::open(
            &fixture.binding.repo.join("index"),
            &expected,
            Default::default(),
            remote.query.clone(),
            remote.io.clone(),
        )?;
        assert!(
            reopened
                .index_at_operation(&[8; 64])?
                .has_id(&fixture.private_commit)
                .await?
        );
        assert_eq!(
            (
                fs::read(&mapping)?,
                fs::read(&record)?,
                fs::read(&manifest)?
            ),
            before,
            "selection must not rewrite private enrollment/index state"
        );
        Ok(())
    }
    #[tokio::test]
    async fn explicit_current_or_legacy_limits_keep_their_existing_identity() -> Result<()> {
        for replays in [256, 4096] {
            let fixture = Fixture::old(replays).await?;
            assert_eq!(fixture.selected()?, fixture.old);
        }
        Ok(())
    }
    #[tokio::test]
    async fn compatibility_never_accepts_other_security_or_custom_policy_changes() -> Result<()> {
        let mut fixture = Fixture::old(256).await?;
        fixture
            .config
            .native
            .repositories
            .get_mut("example")
            .unwrap()
            .remote
            .as_mut()
            .unwrap()
            .query
            .max_replays = 128;
        assert!(fixture.selected().is_err());
        fixture
            .config
            .native
            .repositories
            .get_mut("example")
            .unwrap()
            .remote
            .as_mut()
            .unwrap()
            .query
            .max_replays = 4096;
        let base = fixture.config.clone();
        for mutate in [
            (|c: &mut Config| {
                c.native
                    .repositories
                    .get_mut("example")
                    .unwrap()
                    .max_staging_uploads += 1
            }) as fn(&mut Config),
            |c| {
                c.native
                    .repositories
                    .get_mut("example")
                    .unwrap()
                    .remote
                    .as_mut()
                    .unwrap()
                    .authorization_profile = "different".into()
            },
            |c| {
                c.native
                    .repositories
                    .get_mut("example")
                    .unwrap()
                    .remote
                    .as_mut()
                    .unwrap()
                    .remote
                    .repository_id = "different".into()
            },
            |c| {
                c.native
                    .repositories
                    .get_mut("example")
                    .unwrap()
                    .remote
                    .as_mut()
                    .unwrap()
                    .remote
                    .endpoint = "http://127.0.0.1:9".into()
            },
            |c| c.reapi.rpc_timeout_ms += 1,
            |c| {
                c.native
                    .repositories
                    .get_mut("example")
                    .unwrap()
                    .remote
                    .as_mut()
                    .unwrap()
                    .query
                    .max_scopes += 1
            },
        ] {
            fixture.config = base.clone();
            mutate(&mut fixture.config);
            assert!(fixture.selected().is_err());
        }
        fixture.config = base.clone();
        assert!(
            resolve(
                &fixture.config,
                &BTreeMap::from([(
                    "example".into(),
                    Arc::<[u8]>::from(b"synthetic-ca".as_slice())
                )]),
                &fixture.credentials,
                Some(&fixture.commands)
            )
            .is_err()
        );
        let other = PrivateDir::create(fixture._directory.path().join("other-state"))?;
        let native = PrivateDir::create(other.path().join("native"))?;
        PrivateDir::create(native.path().join("example"))?;
        let original = fingerprint(
            &base.native.repositories["example"],
            &base.state_dir.join("native/example").canonicalize()?,
            &base.reapi,
            fixture.credentials.get("example"),
            None,
        )?;
        let relocated = fingerprint(
            &base.native.repositories["example"],
            &native.path().join("example"),
            &base.reapi,
            fixture.credentials.get("example"),
            None,
        )?;
        assert_ne!(
            original, relocated,
            "store path remains part of the identity"
        );
        fixture.credentials.insert(
            "example".into(),
            Some("synthetic-rotated-credential".into()),
        );
        assert!(fixture.selected().is_err());
        Ok(())
    }
    #[tokio::test]
    async fn unreserved_index_or_upload_headers_fail_without_changing_metadata() -> Result<()> {
        let mut fixture = Fixture::old(256).await?;
        fixture
            .config
            .native
            .repositories
            .get_mut("example")
            .unwrap()
            .remote
            .as_mut()
            .unwrap()
            .query
            .max_replays = 4096;
        let upload = PrivateDir::create(fixture.config.state_dir.join("native-commands/upload"))?;
        let private_header = |name: &str, bytes: &[u8]| -> Result<()> {
            use std::os::unix::fs::PermissionsExt;
            upload.replace(name, bytes)?;
            fs::set_permissions(upload.path().join(name), fs::Permissions::from_mode(0o600))?;
            Ok(())
        };
        let ticket = [6u8; 32];
        let name = format!("{}.published", hex(&ticket));
        let header = serde_json::json!({"version":1,"ticket":ticket.to_vec(),"ticket_binding":vec![5u8;32],
            "provider":fixture.old.to_vec(),"workspace":vec![7u8;32],"expected_generation":0,
            "key":format!("capture_{}",hex(&ticket)),"manifest_id":vec![4u8;32],"manifest_size":10,"capture_size":10});
        let bytes = serde_json::to_vec(&header)?;
        private_header(&name, &bytes)?;
        assert_eq!(fixture.selected()?, fixture.old);
        // Successor envelopes remain identity witnesses even though their
        // payload is retained only in the original immutable job.
        let attempt_name = format!("{}.attempt-1", hex(&ticket));
        let mut attempt = header.clone();
        attempt["version"] = serde_json::json!(2);
        attempt["key"] = serde_json::json!(format!("capture_{}_attempt_1", hex(&ticket)));
        attempt["successor"] = serde_json::json!({
            "original":{"blake3":vec![1u8;32],"size_bytes":100},"predecessors":[]
        });
        let attempt_bytes = serde_json::to_vec(&attempt)?;
        private_header(&attempt_name, &attempt_bytes)?;
        assert_eq!(fixture.selected()?, fixture.old);
        attempt["provider"] = serde_json::to_value(vec![3u8;32])?;
        let conflicting = serde_json::to_vec(&attempt)?;
        private_header(&attempt_name, &conflicting)?;
        assert!(fixture.selected().is_err());
        assert_eq!(upload.read(&attempt_name, 4096)?, conflicting);
        fs::remove_file(upload.path().join(&attempt_name))?;
        let mut changed = header.clone();
        changed["workspace"] = serde_json::to_value(vec![3u8; 32])?;
        let bad = serde_json::to_vec(&changed)?;
        private_header(&name, &bad)?;
        assert!(fixture.selected().is_err());
        assert_eq!(upload.read(&name, 4096)?, bad);
        private_header(&name, b"{malformed")?;
        assert!(fixture.selected().is_err());
        private_header(&name, &bytes)?;
        let record = fixture
            .config
            .state_dir
            .join("native-workspaces")
            .join(fixture.binding.id())
            .join("workspace.json");
        fs::remove_file(record)?;
        assert!(
            fixture.selected().is_err(),
            "index-only enrollment must never become a fresh alias"
        );
        Ok(())
    }
    #[tokio::test]
    async fn completed_retirement_retains_command_lineage_but_never_administration() -> Result<()> {
        let mut fixture = Fixture::old(256).await?;
        fixture
            .config
            .native
            .repositories
            .get_mut("example")
            .unwrap()
            .remote
            .as_mut()
            .unwrap()
            .query
            .max_replays = 4096;
        let workspace = fixture
            .config
            .state_dir
            .join("native-workspaces")
            .join(fixture.binding.id());
        let mut record: serde_json::Value =
            serde_json::from_slice(&fs::read(workspace.join("workspace.json"))?)?;
        record["version"] = 3.into();
        record["state"] = "stopped".into();
        record["origin"] = serde_json::json!({"kind":"candidate","bootstrap_commit":vec![2u8;32]});
        PrivateDir::open_existing(&workspace)?
            .replace("workspace.json", &serde_json::to_vec(&record)?)?;
        let mut receipt = serde_json::json!({"version":1,"record":record,"binding":fixture.binding,
            "root_tree":vec![3u8;32],"complete":false,"root_removed":false});
        let receipts = PrivateDir::create(
            fixture
                .config
                .state_dir
                .join("native-candidate-retirements"),
        )?;
        receipts.replace(
            format!("{}.json", fixture.binding.id()),
            &serde_json::to_vec(&receipt)?,
        )?;
        assert_eq!(fixture.selected()?, fixture.old);
        fixture.index.take();
        fs::remove_dir_all(&fixture.binding.admin)?;
        assert_eq!(
            fixture.selected()?,
            fixture.old,
            "incomplete retirement may lose administration before workspace cleanup"
        );
        receipt["complete"] = true.into();
        receipts.replace(
            format!("{}.json", fixture.binding.id()),
            &serde_json::to_vec(&receipt)?,
        )?;
        assert!(
            fixture.selected().is_err(),
            "completed retirement cannot retain workspace state"
        );
        fs::remove_dir_all(workspace)?;
        assert_eq!(
            fixture.selected()?,
            fixture.old,
            "receipt accounts for retained command metadata"
        );
        Ok(())
    }
    #[test]
    fn fresh_alias_uses_current_identity_and_no_persisted_compatibility_choice() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let state = PrivateDir::create(directory.path().join("state"))?;
        let native = PrivateDir::create(state.path().join("native"))?;
        let objects = PrivateDir::create(native.path().join("example"))?;
        let mut config = Config::default();
        config.state_dir = state.path().to_owned();
        config.native.workspaces.registry_path = Some(directory.path().join("absent-registry"));
        let mut settings = NativeRepository::default();
        settings.remote = Some(Default::default());
        settings.remote.as_mut().unwrap().query.max_replays = 4096;
        config
            .native
            .repositories
            .insert("example".into(), settings);
        let identities = resolve(&config, &BTreeMap::new(), &BTreeMap::new(), None)?;
        assert_eq!(
            identities["example"],
            fingerprint(
                &config.native.repositories["example"],
                objects.path(),
                &config.reapi,
                None,
                None
            )?
        );
        assert_eq!(
            fs::read_dir(state.path())?.count(),
            1,
            "selection is read-only"
        );
        config.native.workspaces.max_metadata_bytes = 4096;
        let workspaces = PrivateDir::create(state.path().join("native-workspaces"))?;
        PrivateDir::create(workspaces.path().join(hex(&[2; 32])))?;
        PrivateDir::create(workspaces.path().join(hex(&[3; 32])))?;
        let error = resolve(&config, &BTreeMap::new(), &BTreeMap::new(), None).unwrap_err();
        assert!(
            error.to_string().contains("provider census byte limit"),
            "missing-record keys must be charged before collection"
        );
        Ok(())
    }
    #[tokio::test]
    async fn mixed_family_or_busy_initializer_is_refused_without_repair() -> Result<()> {
        let mut fixture = Fixture::old(256).await?;
        fixture
            .config
            .native
            .repositories
            .get_mut("example")
            .unwrap()
            .remote
            .as_mut()
            .unwrap()
            .query
            .max_replays = 4096;
        let registry = PrivateDir::open_existing(
            fixture
                .config
                .native
                .workspaces
                .registry_path
                .as_ref()
                .unwrap(),
        )?;
        let lock = registry
            .open_existing_file(format!("initialize-{}.lock", fixture.binding.id()), 4096)?;
        lock.try_lock_exclusive()?;
        assert!(fixture.selected().is_err());
        drop(lock);
        assert_eq!(fixture.selected()?, fixture.old);
        let writer = NativeCommandHost::inspect_writer_lease(&fixture.binding)?;
        assert!(
            fixture.selected().is_err(),
            "active command writer must fence the census"
        );
        drop(writer);
        assert_eq!(fixture.selected()?, fixture.old);
        let objects = fixture
            .config
            .state_dir
            .join("native/example")
            .canonicalize()?;
        let current = fingerprint(
            &fixture.config.native.repositories["example"],
            &objects,
            &fixture.config.reapi,
            fixture.credentials.get("example"),
            None,
        )?;
        let records =
            PrivateDir::open_existing(fixture.config.state_dir.join("native-commands/records"))?;
        let name = format!("{}.json", fixture.binding.id());
        let before = records.read(&name, 16384)?;
        let mut changed: serde_json::Value = serde_json::from_slice(&before)?;
        changed["identity"] = serde_json::to_value(current)?;
        records.replace(&name, &serde_json::to_vec(&changed)?)?;
        let failed = records.read(&name, 16384)?;
        assert!(fixture.selected().is_err());
        assert_eq!(records.read(&name, 16384)?, failed);
        records.replace(&name, &before)?;
        let header_before = registry.read("registry.json", 4096)?;
        let marker = "synthetic-private-registry-marker";
        let mut header: serde_json::Value = serde_json::from_slice(&header_before)?;
        header[marker] = marker.into();
        registry.replace("registry.json", &serde_json::to_vec(&header)?)?;
        let error = fixture.selected().unwrap_err();
        assert!(!format!("{error}").contains(marker));
        assert!(!format!("{error:?}").contains(marker));
        assert_eq!(
            registry.read("registry.json", 4096)?,
            serde_json::to_vec(&header)?
        );
        Ok(())
    }
}
