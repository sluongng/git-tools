use async_trait::async_trait;
use futures::{AsyncReadExt, StreamExt};
use jj_native_backend::{DirectObjectIo, NativeObjectIo};
use jj_native_remote::{Config, PublishRequest, RemoteRepository};
use local_cas::{Artifact, BlobSource, LocalCas};
use p::repository_server::{Repository, RepositoryServer};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tinytree_client::protocol as p;
use tinytree_model as m;
use tinytree_server::{Principal, RepositoryService, ServiceConfig};
use tinytree_store::Store;
use tonic::{Request, Response, Status, Streaming, codec::CompressionEncoding};
const TOKEN: &str = "native-remote-loopback-test-token";
fn private_dir() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    dir
}
fn make_private(path: &Path) {
    std::fs::create_dir(path).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
}
struct NoUpstream;
#[async_trait]
impl BlobSource for NoUpstream {
    async fn fetch(&self, _: &Artifact, _: &mut std::fs::File) -> anyhow::Result<()> {
        anyhow::bail!("shared cache default source must not serve native scope")
    }
    async fn missing(&self, a: &[Artifact]) -> anyhow::Result<Vec<Artifact>> {
        Ok(a.to_vec())
    }
}
#[derive(Clone)]
struct Counters {
    files: Arc<AtomicUsize>,
    holds: Arc<AtomicUsize>,
    metadata_rpcs: Arc<AtomicUsize>,
    metadata_keys: Arc<AtomicUsize>,
    metadata_requests: Arc<std::sync::Mutex<Vec<p::ObjectKey>>>,
    metadata_bytes: Arc<AtomicUsize>,
    largest_metadata_batch: Arc<AtomicUsize>,
    paused: Arc<AtomicBool>,
    metadata_started: Arc<tokio::sync::Notify>,
    public_paused: Arc<AtomicBool>,
    public_started: Arc<tokio::sync::Notify>,
}
impl Counters {
    fn new() -> Self {
        Self {
            files: Arc::new(AtomicUsize::new(0)),
            holds: Arc::new(AtomicUsize::new(0)),
            metadata_rpcs: Arc::new(AtomicUsize::new(0)),
            metadata_keys: Arc::new(AtomicUsize::new(0)),
            metadata_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            metadata_bytes: Arc::new(AtomicUsize::new(0)),
            largest_metadata_batch: Arc::new(AtomicUsize::new(0)),
            paused: Arc::new(AtomicBool::new(false)),
            metadata_started: Arc::new(tokio::sync::Notify::new()),
            public_paused: Arc::new(AtomicBool::new(false)),
            public_started: Arc::new(tokio::sync::Notify::new()),
        }
    }
}
#[derive(Clone)]
struct Counting {
    service: RepositoryService,
    counts: Counters,
}
#[tonic::async_trait]
impl Repository for Counting {
    async fn open_repository(
        &self,
        r: Request<p::OpenRepositoryRequest>,
    ) -> Result<Response<p::RepositoryView>, Status> {
        self.service.open_repository(r).await
    }
    async fn get_commit_graph(
        &self,
        r: Request<p::GetCommitGraphRequest>,
    ) -> Result<Response<p::CommitGraphPage>, Status> {
        self.service.get_commit_graph(r).await
    }
    type BatchReadObjectsStream = <RepositoryService as Repository>::BatchReadObjectsStream;
    async fn batch_read_objects(
        &self,
        r: Request<p::BatchReadObjectsRequest>,
    ) -> Result<Response<Self::BatchReadObjectsStream>, Status> {
        self.counts.metadata_rpcs.fetch_add(1, Ordering::SeqCst);
        self.counts
            .metadata_requests
            .lock()
            .unwrap()
            .extend(r.get_ref().keys.clone());
        self.counts
            .metadata_keys
            .fetch_add(r.get_ref().keys.len(), Ordering::SeqCst);
        self.counts
            .largest_metadata_batch
            .fetch_max(r.get_ref().keys.len(), Ordering::SeqCst);
        self.counts.metadata_started.notify_one();
        while self.counts.paused.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let response = self.service.batch_read_objects(r).await?;
        let counts = self.counts.clone();
        let stream = response.into_inner().map(move |result| {
            if let Ok(p::ObjectResult {
                result: Some(p::object_result::Result::Object(object)),
                ..
            }) = &result
            {
                counts
                    .metadata_bytes
                    .fetch_add(object.canonical_envelope.len(), Ordering::SeqCst);
            }
            result
        });
        Ok(Response::new(Box::pin(stream)))
    }
    async fn find_missing_objects(
        &self,
        r: Request<p::FindMissingObjectsRequest>,
    ) -> Result<Response<p::MissingObjects>, Status> {
        self.service.find_missing_objects(r).await
    }
    type ReadFileStream = <RepositoryService as Repository>::ReadFileStream;
    async fn read_file(
        &self,
        r: Request<p::ReadFileRequest>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        self.counts.files.fetch_add(1, Ordering::SeqCst);
        self.service.read_file(r).await
    }
    async fn write_object(
        &self,
        r: Request<Streaming<p::WriteFrame>>,
    ) -> Result<Response<p::WriteReceipt>, Status> {
        self.service.write_object(r).await
    }
    async fn publish_reference(
        &self,
        r: Request<p::PublishReferenceRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        let hold = r.get_ref().reference_name.starts_with("_tinytree/hold/v1/");
        if hold {
            assert!(
                r.get_ref().staging_leases.is_empty(),
                "private holds must not receive owner-opaque staging tokens"
            );
            self.counts.holds.fetch_add(1, Ordering::SeqCst);
        }
        let response = self.service.publish_reference(r).await?;
        if !hold && self.counts.public_paused.load(Ordering::SeqCst) {
            self.counts.public_started.notify_one();
            while self.counts.public_paused.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        Ok(response)
    }
    async fn get_publication(
        &self,
        r: Request<p::GetPublicationRequest>,
    ) -> Result<Response<p::Publication>, Status> {
        self.service.get_publication(r).await
    }
}
struct Server {
    address: std::net::SocketAddr,
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn start(store: Store, counts: Counters) -> Self {
        Self::start_with_tls(store, counts, None).await
    }
    async fn start_with_tls(
        store: Store,
        counts: Counters,
        tls: Option<tonic::transport::Identity>,
    ) -> Self {
        let service = RepositoryService::new(
            store,
            ServiceConfig::default(),
            vec!["example".into()],
            vec![
                Principal::new("test", TOKEN, vec!["example".into()], true)
                    .unwrap()
                    .with_retention(true),
                Principal::new(
                    "other",
                    "native-remote-other-loopback-test-token",
                    vec!["example".into()],
                    true,
                )
                .unwrap()
                .with_retention(true),
            ],
        )
        .unwrap();
        Self::start_service(service, counts, tls).await
    }
    async fn start_service(
        service: RepositoryService,
        counts: Counters,
        tls: Option<tonic::transport::Identity>,
    ) -> Self {
        let query = service.query_grpc_service();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut server = tonic::transport::Server::builder();
            if let Some(identity) = tls {
                server = server
                    .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))
                    .unwrap();
            }
            server
                .add_service(
                    RepositoryServer::new(Counting { service, counts })
                        .accept_compressed(CompressionEncoding::Zstd)
                        .send_compressed(CompressionEncoding::Zstd)
                        .max_decoding_message_size(20 * 1024 * 1024)
                        .max_encoding_message_size(20 * 1024 * 1024),
                )
                .add_service(query)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener).map(|accepted| {
                        accepted.and_then(|stream| {
                            stream.set_nodelay(true)?;
                            Ok(stream)
                        })
                    }),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });
        Self {
            address,
            stop,
            task,
        }
    }
    async fn stop(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
fn seed() -> (
    tempfile::TempDir,
    Store,
    tinytree_fixture::Fixture,
    m::CommitId,
) {
    let dir = private_dir();
    let store = Store::open(dir.path(), Default::default()).unwrap();
    store.create_repository("example").unwrap();
    let fixture = tinytree_fixture::build_fixture(tinytree_fixture::FixtureConfig {
        unused_files: 20,
        bytes_per_file: 3000,
    })
    .unwrap();
    let mut head = None;
    for object in fixture.objects() {
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
        if object.descriptor.key.kind == m::Kind::Commit {
            head = Some(m::CommitId(object.descriptor.key.id.0));
        }
    }
    let head = head.unwrap();
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "fixture-head".into(),
                reference_name: "main".into(),
                expected_generation: 0,
                new_commit: Some(head),
                staging_leases: vec![],
            },
            1,
        )
        .unwrap();
    (dir, store, fixture, head)
}
/// A wide metadata frontier with repeated shared subtrees and one promised file.
/// marker_bytes can make individual metadata fit while a batch exceeds its limit.
fn seed_wide(width: usize, marker_bytes: usize) -> (tempfile::TempDir, Store, m::CommitId, usize) {
    let (directory, store, fixture, old) = seed();
    let file = first_file(&fixture).descriptor;
    let file_value = || m::TreeValue::File {
        id: m::FileId(file.key.id.0),
        executable: false,
        size_bytes: file.size_bytes,
        copy_id: None,
    };
    let shared = m::encode(
        &m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "source.cc".into(),
                value: file_value(),
            }],
        }),
        m::Limits::default(),
    )
    .unwrap();
    store
        .put("example", shared.descriptor, &shared.bytes, 0)
        .unwrap();
    let mut entries = Vec::new();
    for index in 0..width {
        let target = format!("target-{index:04}-{}", "x".repeat(marker_bytes));
        let marker = m::encode(&m::Object::Symlink(target.clone()), m::Limits::default()).unwrap();
        store
            .put("example", marker.descriptor, &marker.bytes, 0)
            .unwrap();
        let mut children: Vec<_> = (0..8)
            .map(|number| m::TreeEntry {
                name: format!("file-{number:02}.cc"),
                value: file_value(),
            })
            .collect();
        children.push(m::TreeEntry {
            name: "marker".into(),
            value: m::TreeValue::Symlink {
                id: m::SymlinkId(marker.descriptor.key.id.0),
                target_bytes: target.len() as u64,
            },
        });
        children.push(m::TreeEntry {
            name: "shared".into(),
            value: m::TreeValue::Tree(m::TreeId(shared.descriptor.key.id.0)),
        });
        let leaf = m::encode(
            &m::Object::Tree(m::Tree { entries: children }),
            m::Limits::default(),
        )
        .unwrap();
        store
            .put("example", leaf.descriptor, &leaf.bytes, 0)
            .unwrap();
        entries.push(m::TreeEntry {
            name: format!("dir-{index:04}"),
            value: m::TreeValue::Tree(m::TreeId(leaf.descriptor.key.id.0)),
        });
    }
    let root = m::encode(&m::Object::Tree(m::Tree { entries }), m::Limits::default()).unwrap();
    store
        .put("example", root.descriptor, &root.bytes, 0)
        .unwrap();
    let object = fixture
        .objects()
        .iter()
        .find(|object| object.descriptor.key.id.0 == old.0)
        .unwrap();
    let m::Object::Commit(mut commit) = m::decode(&object.bytes, m::Limits::default()).unwrap()
    else {
        unreachable!()
    };
    commit.parents = vec![m::CommitId::ROOT];
    commit.predecessors.clear();
    commit.root_tree = m::TreeMerge {
        terms: vec![m::TreeId(root.descriptor.key.id.0)],
        labels: vec![String::new()],
    };
    commit.description = "wide shared metadata frontier".into();
    let commit = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
    store
        .put("example", commit.descriptor, &commit.bytes, 0)
        .unwrap();
    let head = m::CommitId(commit.descriptor.key.id.0);
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "wide-fixture-head".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: Some(head),
                staging_leases: vec![],
            },
            1,
        )
        .unwrap();
    (directory, store, head, width * 2 + 3)
}

fn seed_history(
    depth: usize,
) -> (
    tempfile::TempDir,
    Store,
    tinytree_fixture::Fixture,
    m::CommitId,
    Vec<m::EncodedObject>,
) {
    let (directory, store, fixture, old) = seed();
    let original = fixture
        .objects()
        .iter()
        .find(|object| object.descriptor.key.id.0 == old.0)
        .unwrap();
    let m::Object::Commit(base) = m::decode(&original.bytes, m::Limits::default()).unwrap() else {
        unreachable!()
    };
    let file = first_file(&fixture).descriptor;
    let mut parent = m::CommitId::ROOT;
    let mut trees = Vec::new();
    for index in 0..depth {
        let tree = m::encode(
            &m::Object::Tree(m::Tree {
                entries: vec![m::TreeEntry {
                    name: format!("historical-{index:04}.txt"),
                    value: m::TreeValue::File {
                        id: m::FileId(file.key.id.0),
                        executable: false,
                        size_bytes: file.size_bytes,
                        copy_id: None,
                    },
                }],
            }),
            m::Limits::default(),
        )
        .unwrap();
        store
            .put("example", tree.descriptor, &tree.bytes, 0)
            .unwrap();
        let mut commit = base.clone();
        commit.parents = vec![parent];
        commit.predecessors.clear();
        commit.root_tree = m::TreeMerge {
            terms: vec![m::TreeId(tree.descriptor.key.id.0)],
            labels: vec![String::new()],
        };
        commit.description = format!("historical metadata {index}");
        let commit = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
        store
            .put("example", commit.descriptor, &commit.bytes, 0)
            .unwrap();
        parent = m::CommitId(commit.descriptor.key.id.0);
        trees.push(tree);
    }
    let mut current = base;
    current.parents = vec![parent];
    current.predecessors.clear();
    current.description = "current sparse root".into();
    let current = m::encode(&m::Object::Commit(current), m::Limits::default()).unwrap();
    store
        .put("example", current.descriptor, &current.bytes, 0)
        .unwrap();
    let head = m::CommitId(current.descriptor.key.id.0);
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "history-fixture-head".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: Some(head),
                staging_leases: vec![],
            },
            1,
        )
        .unwrap();
    (directory, store, fixture, head, trees)
}

fn config(server: &Server) -> Config {
    let mut config = Config::default();
    config.remote.endpoint = format!("http://{}", server.address);
    config.remote.repository_id = "example".into();
    config.remote.token_env = Some("TINYTREE_REMOTE_TEST_TOKEN".into());
    config.remote.rpc.retry.initial_backoff_ms = 1;
    config.remote.rpc.retry.max_backoff_ms = 5;
    config.authorization_profile = "loopback-test-reader".into();
    config
}
fn local(
    root: &Path,
    cache: Arc<LocalCas>,
    config: Config,
    fresh: bool,
) -> (Arc<DirectObjectIo>, RemoteRepository) {
    let path = root.join("local");
    let direct = if fresh {
        make_private(&path);
        DirectObjectIo::init(&path, Default::default())
    } else {
        DirectObjectIo::load(&path, Default::default())
    }
    .unwrap();
    let direct = Arc::new(direct);
    let repository = RemoteRepository::open(
        direct.clone(),
        cache,
        &root.join("ledger"),
        &root.join("staging"),
        config,
    )
    .unwrap();
    (direct, repository)
}
fn cache(root: &Path) -> Arc<LocalCas> {
    LocalCas::open(root.join("cache"), Arc::new(NoUpstream), Default::default()).unwrap()
}
async fn bytes(io: &dyn NativeObjectIo, id: m::FileId) -> Vec<u8> {
    let read = io.read_file(id).await.unwrap();
    let (_, mut reader) = read.into_parts();
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    bytes
}
fn first_file(fixture: &tinytree_fixture::Fixture) -> &m::EncodedObject {
    fixture
        .objects()
        .iter()
        .find(|object| object.descriptor.key.kind == m::Kind::File)
        .unwrap()
}

async fn read_current_metadata(repo: &RemoteRepository, head: m::CommitId) {
    let root = repo
        .io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(head.0),
        })
        .await
        .unwrap();
    let m::Object::Commit(commit) = root.into_object() else {
        unreachable!()
    };
    let mut pending: Vec<_> = commit
        .root_tree
        .terms
        .into_iter()
        .map(|id| m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(id.0),
        })
        .collect();
    let mut seen = std::collections::HashSet::new();
    while let Some(key) = pending.pop() {
        if !seen.insert(key) {
            continue;
        }
        let object = repo.io.read_metadata(key).await.unwrap().into_object();
        pending.extend(
            object
                .references()
                .into_iter()
                .filter(|key| key.kind != m::Kind::File && key.kind != m::Kind::Commit),
        );
    }
}
async fn complete_copy_metadata(
    repo: &RemoteRepository,
) -> Result<jj_native_backend::CopyCatalogSnapshot, jj_lib::backend::BackendError> {
    repo.io
        .copy_catalog_snapshot(jj_native_backend::CopyCatalogLimits {
            max_objects: 10_000,
            max_metadata_bytes: 16 * 1024 * 1024,
            max_edges: 50_000,
        })
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pinned_admission_loads_commits_without_historical_trees_and_reopens_lazily() {
    let (_serverdir, store, fixture, head, historical) = seed_history(8);
    let counts = Counters::new();
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let configuration = config(&server);
    let (direct, repo) = local(directory.path(), cache.clone(), configuration.clone(), true);
    let admitted = repo.control.admit_reference("main").await.unwrap();
    assert_eq!(admitted.metadata_objects, 1);
    assert_eq!(counts.metadata_keys.load(Ordering::SeqCst), 1);
    assert_eq!(repo.control.rebuild_file_index().await.unwrap(), 0);
    let unknown = m::ObjectKey {
        kind: m::Kind::Tree,
        id: m::ObjectId([7; 32]),
    };
    assert!(repo.io.read_metadata(unknown).await.is_err());
    assert_eq!(
        counts.metadata_keys.load(Ordering::SeqCst),
        1,
        "an unrelated ID is not a remote-read grant"
    );
    let mut current = head;
    while current != m::CommitId::ROOT {
        let object = repo
            .io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(current.0),
            })
            .await
            .unwrap()
            .into_object();
        let m::Object::Commit(commit) = object else {
            unreachable!()
        };
        current = commit.parents[0];
    }
    assert!(
        counts
            .metadata_requests
            .lock()
            .unwrap()
            .iter()
            .all(|key| key.kind == m::Kind::Commit as i32)
    );
    for tree in &historical {
        assert!(
            direct
                .descriptor(tree.descriptor.key)
                .await
                .unwrap()
                .is_none()
        );
    }
    read_current_metadata(&repo, head).await;
    let file = first_file(&fixture);
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 1);
    cache.wait_for_idle().await.unwrap();
    repo.wait_for_idle().await;
    drop(repo);
    drop(direct);
    let (direct, repo) = local(directory.path(), cache, configuration, false);
    let oldest = &historical[0];
    assert!(
        direct
            .descriptor(oldest.descriptor.key)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        repo.io
            .read_metadata(oldest.descriptor.key)
            .await
            .unwrap()
            .bytes(),
        oldest.bytes
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 1);
    assert_eq!(
        repo.control.rebuild_file_index().await.unwrap(),
        fixture.manifest.files.len()
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_indexed_metadata_is_not_replaced_by_remote_fallback() {
    let (_serverdir, store, _fixture, head) = seed();
    let counts = Counters::new();
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let (_direct, repo) = local(directory.path(), cache, config(&server), true);
    repo.control.admit_reference("main").await.unwrap();
    let before = counts.metadata_rpcs.load(Ordering::SeqCst);
    let hash: String = head.0.iter().map(|byte| format!("{byte:02x}")).collect();
    let path = directory
        .path()
        .join("local/native-objects/repos/local/3")
        .join(hash);
    std::fs::write(path, b"corrupt indexed bytes").unwrap();
    assert!(
        repo.io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(head.0)
            })
            .await
            .is_err()
    );
    assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), before);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_owned_metadata_operations_retain_external_guard_until_physical_completion() {
    #[derive(Clone, Copy, Debug)]
    enum Operation {
        Read,
        Descriptor,
        Catalog,
    }
    for operation in [Operation::Read, Operation::Descriptor, Operation::Catalog] {
        let (_serverdir, store, _fixture, head) = seed();
        let counts = Counters::new();
        let server = Server::start(store, counts.clone()).await;
        let directory = private_dir();
        let cache = cache(directory.path());
        let (direct, repo) = local(directory.path(), cache, config(&server), true);
        let declared = repo.io.metadata_work_budget().unwrap();
        let expected = config(&server).control_work_budget();
        assert_eq!(declared.memory_bytes, expected.memory_bytes);
        assert_eq!(declared.temporary_bytes, expected.temporary_bytes);
        repo.control.admit_reference("main").await.unwrap();
        // Consume the pin's already-observed notification before pausing the next RPC.
        counts.metadata_started.notified().await;
        let object = repo
            .io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(head.0),
            })
            .await
            .unwrap()
            .into_object();
        let m::Object::Commit(commit) = object else {
            unreachable!()
        };
        let key = m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(commit.root_tree.terms[0].0),
        };
        counts.paused.store(true, Ordering::SeqCst);
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let io = repo.io.clone();
        let task = tokio::spawn(async move {
            match operation {
                Operation::Read => io.read_metadata_with_owner(key, owner).await.map(|_| ()),
                Operation::Descriptor => io.descriptor_with_owner(key, owner).await.map(|_| ()),
                Operation::Catalog => io
                    .copy_catalog_snapshot_with_owner(
                        jj_native_backend::CopyCatalogLimits {
                            max_objects: 10_000,
                            max_metadata_bytes: 16 * 1024 * 1024,
                            max_edges: 50_000,
                        },
                        owner,
                    )
                    .await
                    .map(|_| ()),
            }
        });
        tokio::time::timeout(Duration::from_secs(5), counts.metadata_started.notified())
            .await
            .unwrap();
        task.abort();
        let _ = task.await;
        assert!(
            weak.upgrade().is_some(),
            "caller cancellation must not release accepted metadata ownership"
        );
        assert!(direct.descriptor(key).await.unwrap().is_none());
        counts.paused.store(false, Ordering::SeqCst);
        repo.wait_for_idle().await;
        assert!(direct.descriptor(key).await.unwrap().is_some());
        assert!(
            weak.upgrade().is_none(),
            "idle must follow worker and owner destruction"
        );
        server.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_complete_ledger_upgrades_without_losing_resident_provenance() {
    use fjall::Readable;
    let (_serverdir, store, fixture, head) = seed();
    let counts = Counters::new();
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let configuration = config(&server);
    let (direct, repo) = local(directory.path(), cache.clone(), configuration.clone(), true);
    repo.control.admit_reference("main").await.unwrap();
    complete_copy_metadata(&repo).await.unwrap();
    repo.wait_for_idle().await;
    drop(repo);
    drop(direct);
    // Reconstruct the actual v1 serialized root/member representation. V2-only
    // keyspaces are empty; old readers ignored unknown empty keyspaces.
    {
        let db = fjall::SingleWriterTxDatabase::builder(directory.path().join("ledger/data"))
            .open()
            .unwrap();
        let meta = db
            .keyspace("meta", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let roots = db
            .keyspace("roots", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let known = db
            .keyspace("known-references-v2", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let progress = db
            .keyspace(
                "verified-progress-v2",
                fjall::KeyspaceCreateOptions::default,
            )
            .unwrap();
        let view = db.read_tx();
        let mut bytes: u64 =
            serde_json::from_slice(&view.get(&meta, b"bytes").unwrap().unwrap()).unwrap();
        let mut tx = db.write_tx().durability(Some(fjall::PersistMode::SyncAll));
        for item in view.iter(&roots) {
            let (key, previous) = item.into_inner().unwrap();
            let mut root: serde_json::Value = serde_json::from_slice(&previous).unwrap();
            assert_eq!(root["complete"], true);
            root.as_object_mut().unwrap().remove("pinned");
            root.as_object_mut().unwrap().remove("ready");
            let value = serde_json::to_vec(&root).unwrap();
            bytes = bytes - previous.len() as u64 + value.len() as u64;
            tx.insert(&roots, key, value);
        }
        for space in [&known, &progress] {
            for item in view.iter(space) {
                let (key, value) = item.into_inner().unwrap();
                bytes -= (key.len() + value.len()) as u64;
                tx.remove(space, key);
            }
        }
        tx.insert(&meta, b"format", b"tinytree-native-admission-1\n");
        tx.insert(&meta, b"bytes", serde_json::to_vec(&bytes).unwrap());
        tx.commit().unwrap();
    }
    std::fs::write(
        directory.path().join("ledger/admission.format"),
        b"tinytree-native-admission-1\n",
    )
    .unwrap();
    let before = counts.metadata_rpcs.load(Ordering::SeqCst);
    let (_direct, repo) = local(directory.path(), cache, configuration, false);
    let root = repo
        .io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(head.0),
        })
        .await
        .unwrap();
    assert_eq!(root.descriptor().key.id.0, head.0);
    let file = first_file(&fixture);
    assert_eq!(
        repo.io.descriptor(file.descriptor.key).await.unwrap(),
        Some(file.descriptor)
    );
    assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), before);
    assert_eq!(
        std::fs::read(directory.path().join("ledger/admission.format")).unwrap(),
        b"tinytree-native-admission-2\n"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual history-shaped sparse admission benchmark"]
async fn historical_metadata_admission_benchmark() {
    for depth in [8, 64, 256] {
        let (_serverdir, store, _fixture, head, historical) = seed_history(depth);
        let counts = Counters::new();
        let server = Server::start(store, counts.clone()).await;
        let directory = private_dir();
        let cache = cache(directory.path());
        let (direct, repo) = local(directory.path(), cache, config(&server), true);
        let started = std::time::Instant::now();
        let admitted = repo.control.admit_reference("main").await.unwrap();
        assert_eq!(admitted.head, head);
        let cold_ns = started.elapsed().as_nanos();
        let metadata_rpcs = counts.metadata_rpcs.load(Ordering::SeqCst);
        let canonical_bytes = counts.metadata_bytes.load(Ordering::SeqCst);
        for tree in &historical {
            let _ = direct.descriptor(tree.descriptor.key).await.unwrap();
        }
        let tree_rpcs = counts
            .metadata_requests
            .lock()
            .unwrap()
            .iter()
            .filter(|key| key.kind == m::Kind::Tree as i32)
            .count();
        assert_eq!(counts.files.load(Ordering::SeqCst), 0);
        println!(
            "{{\"schema\":1,\"benchmark\":\"historical_metadata_admission\",\"history_depth\":{depth},\"cold_ns\":{cold_ns},\"metadata_rpcs\":{metadata_rpcs},\"canonical_bytes\":{canonical_bytes},\"tree_requests\":{tree_rpcs},\"file_rpcs\":0}}"
        );
        server.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_batches_missing_siblings_and_reuses_warm_shared_metadata() {
    let (_serverdir, store, head, expected_objects) = seed_wide(32, 0);
    let counts = Counters::new();
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let (_direct, repo) = local(directory.path(), cache, config(&server), true);
    let admitted = repo.control.admit_reference("main").await.unwrap();
    assert_eq!(admitted.head, head);
    assert_eq!(admitted.metadata_objects, 1);
    assert_eq!(counts.metadata_keys.load(Ordering::SeqCst), 1);
    complete_copy_metadata(&repo).await.unwrap();
    assert_eq!(
        counts.metadata_keys.load(Ordering::SeqCst),
        expected_objects
    );
    assert!(counts.largest_metadata_batch.load(Ordering::SeqCst) > 1);
    let cold_rpcs = counts.metadata_rpcs.load(Ordering::SeqCst);
    assert!(
        cold_rpcs < expected_objects,
        "wide metadata must use fewer RPCs than objects"
    );
    repo.control.admit_reference("main").await.unwrap();
    assert_eq!(
        counts.metadata_rpcs.load(Ordering::SeqCst),
        cold_rpcs,
        "warm admission must use local metadata"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_splits_aggregate_metadata_bytes_without_rejecting_valid_objects() {
    let (_serverdir, store, head, expected_objects) = seed_wide(8, 800);
    let counts = Counters::new();
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let mut configuration = config(&server);
    configuration.remote.limits.max_metadata_bytes = 1024;
    configuration.remote.limits.max_batch_bytes = 1024;
    let (_direct, repo) = local(directory.path(), cache, configuration, true);
    let admitted = repo.control.admit_reference("main").await.unwrap();
    assert_eq!(admitted.head, head);
    assert_eq!(admitted.metadata_objects, 1);
    complete_copy_metadata(&repo).await.unwrap();
    assert!(counts.largest_metadata_batch.load(Ordering::SeqCst) > 1);
    assert!(
        counts.metadata_keys.load(Ordering::SeqCst) > expected_objects,
        "oversized aggregate retries smaller batches"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_respects_tight_request_and_closure_budgets() {
    struct Case {
        name: &'static str,
        missing_object_slots: usize,
        max_metadata_bytes: usize,
        succeeds: bool,
    }
    for case in [
        Case {
            name: "exact object limit",
            missing_object_slots: 0,
            max_metadata_bytes: 16 * 1024 * 1024,
            succeeds: true,
        },
        Case {
            name: "prefetched objects count toward limit",
            missing_object_slots: 1,
            max_metadata_bytes: 16 * 1024 * 1024,
            succeeds: false,
        },
        Case {
            name: "prefetched bytes count toward limit",
            missing_object_slots: 0,
            max_metadata_bytes: 4096,
            succeeds: false,
        },
    ] {
        let (_serverdir, store, head, expected_objects) = seed_wide(32, 0);
        let counts = Counters::new();
        let server = Server::start(store, counts.clone()).await;
        let directory = private_dir();
        let cache = cache(directory.path());
        let mut configuration = config(&server);
        configuration.remote.limits.max_request_bytes = 1024;
        configuration.closure.max_objects = expected_objects - case.missing_object_slots;
        configuration.closure.max_metadata_bytes = case.max_metadata_bytes;
        let (direct, repo) = local(directory.path(), cache, configuration, true);
        repo.control.admit_reference("main").await.unwrap();
        let head_object = repo
            .io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(head.0),
            })
            .await
            .unwrap()
            .into_object();
        let m::Object::Commit(head_object) = head_object else {
            unreachable!()
        };
        let current_tree = m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(head_object.root_tree.terms[0].0),
        };
        let result = complete_copy_metadata(&repo).await;
        assert_eq!(result.is_ok(), case.succeeds, "{}", case.name);
        assert!(
            counts.largest_metadata_batch.load(Ordering::SeqCst) <= 25,
            "{}: typed key framing must fit a 1KiB request",
            case.name
        );
        assert_eq!(counts.files.load(Ordering::SeqCst), 0, "{}", case.name);
        if !case.succeeds {
            assert!(
                direct.descriptor(current_tree).await.unwrap().is_none(),
                "{}: failed collection must not install prefetched metadata",
                case.name
            );
        }
        server.stop().await;
    }
}

/// Matched parent-run baseline/candidate benchmark: real compressed repository
/// admission only, not native mount or end-to-end Bazel startup acceptance.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual cold/warm metadata admission benchmark"]
async fn metadata_admission_benchmark() {
    for width in [16, 128] {
        let (_serverdir, store, _head, expected_objects) = seed_wide(width, 0);
        let counts = Counters::new();
        let server = Server::start(store, counts.clone()).await;
        let directory = private_dir();
        let cache = cache(directory.path());
        let (_direct, repo) = local(directory.path(), cache, config(&server), true);
        let started = std::time::Instant::now();
        let admitted = repo.control.admit_reference("main").await.unwrap();
        let cold_ns = started.elapsed().as_nanos();
        assert!(admitted.metadata_objects <= expected_objects);
        let cold_rpcs = counts.metadata_rpcs.load(Ordering::SeqCst);
        let cold_keys = counts.metadata_keys.load(Ordering::SeqCst);
        let cold_bytes = counts.metadata_bytes.load(Ordering::SeqCst);
        let started = std::time::Instant::now();
        repo.control.admit_reference("main").await.unwrap();
        let warm_ns = started.elapsed().as_nanos();
        assert_eq!(counts.files.load(Ordering::SeqCst), 0);
        assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), cold_rpcs);
        println!(
            "{{\"schema\":1,\"benchmark\":\"metadata_admission\",\"width\":{width},\"metadata_objects\":{expected_objects},\"cold_ns\":{cold_ns},\"warm_ns\":{warm_ns},\"cold_metadata_rpcs\":{cold_rpcs},\"cold_requested_keys\":{cold_keys},\"cold_canonical_bytes\":{cold_bytes},\"warm_metadata_rpcs\":0,\"file_rpcs\":0}}"
        );
        server.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_only_admission_and_public_ref_gc_keep_lazy_old_reads_available() {
    let (_serverdir, store, fixture, head) = seed();
    let counts = Counters::new();
    let server = Server::start(store.clone(), counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let (_direct, repo) = local(directory.path(), cache.clone(), config(&server), true);
    let admitted = repo.control.admit_reference("main").await.unwrap();
    assert_eq!(admitted.head, head);
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        0,
        "admission must never fetch payloads, including unused source files"
    );
    assert_eq!(counts.holds.load(Ordering::SeqCst), 1);
    assert_eq!(repo.control.rebuild_file_index().await.unwrap(), 0);
    read_current_metadata(&repo, head).await;
    assert!(repo.control.rebuild_file_index().await.unwrap() > 20);
    let file = first_file(&fixture);
    let descriptor = repo
        .io
        .descriptor(file.descriptor.key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(descriptor, file.descriptor);
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "remove-public-head".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: None,
                staging_leases: vec![],
            },
            2,
        )
        .unwrap();
    store.gc("example", i64::MAX / 2).unwrap();
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 1);
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        1,
        "second read must be a verified local CAS hit"
    );
    cache.wait_for_idle().await.unwrap();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cached_offline_reads_and_new_writes_survive_reopen_without_credentials() {
    let (_serverdir, store, fixture, _head) = seed();
    let counts = Counters::new();
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let original = config(&server);
    let (direct, repo) = local(directory.path(), cache.clone(), original.clone(), true);
    let admitted = repo.control.admit_reference("main").await.unwrap();
    read_current_metadata(&repo, admitted.head).await;
    let file = first_file(&fixture);
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    let client_id = repo.control.client_id();
    cache.wait_for_idle().await.unwrap();
    drop(repo);
    drop(direct);
    server.stop().await;
    let mut offline = original;
    offline.remote.token_env = Some(format!(
        "NATIVE_REMOTE_ABSENT_{}",
        uuid::Uuid::new_v4().simple()
    ));
    let (_direct, repo) = local(directory.path(), cache.clone(), offline, false);
    assert_eq!(repo.control.client_id(), client_id);
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    let data = b"offline local authoritative write";
    let id = repo
        .io
        .write_file(&mut futures::io::Cursor::new(data), 4096)
        .await
        .unwrap();
    assert_eq!(bytes(repo.io.as_ref(), id).await, data);
    let descriptor = repo
        .io
        .write_metadata(m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "new-local".into(),
                value: m::TreeValue::File {
                    id,
                    executable: false,
                    size_bytes: data.len() as u64,
                    copy_id: None,
                },
            }],
        }))
        .await
        .unwrap();
    assert_eq!(descriptor.key.kind, m::Kind::Tree);
    assert!(
        repo.control.admit_reference("main").await.is_err(),
        "missing credentials must not fall back to anonymous network access"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_admission_reconciles_current_durable_hold_after_restart() {
    let (_serverdir, store, _fixture, head) = seed();
    let counts = Counters::new();
    counts.paused.store(true, Ordering::SeqCst);
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let config = config(&server);
    let (direct, repo) = local(directory.path(), cache.clone(), config.clone(), true);
    let client_id = repo.control.client_id();
    let control = repo.control.clone();
    let task = tokio::spawn(async move { control.admit_commit(head).await });
    tokio::time::timeout(Duration::from_secs(5), counts.metadata_started.notified())
        .await
        .unwrap();
    assert_eq!(counts.holds.load(Ordering::SeqCst), 1);
    task.abort();
    let _ = task.await;
    counts.paused.store(false, Ordering::SeqCst);
    repo.wait_for_idle().await;
    drop(repo);
    drop(direct);
    let (_direct, repo) = local(directory.path(), cache, config, false);
    assert_eq!(repo.control.client_id(), client_id);
    assert_eq!(repo.control.reconcile_pending().await.unwrap(), 1);
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        1,
        "restart must use current hold proof rather than make a second promise"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrelated_tree_write_and_publication_upload_only_new_local_content() {
    let (_serverdir, store, fixture, old) = seed();
    let counts = Counters::new();
    let server = Server::start(store.clone(), counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let (direct, repo) = local(directory.path(), cache, config(&server), true);
    repo.control.admit_commit(old).await.unwrap();
    let root = repo
        .io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(old.0),
        })
        .await
        .unwrap();
    let m::Object::Commit(mut commit) = root.into_object() else {
        unreachable!()
    };
    let tree = repo
        .io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(commit.root_tree.terms[0].0),
        })
        .await
        .unwrap();
    let m::Object::Tree(mut tree) = tree.into_object() else {
        unreachable!()
    };
    let new = b"new local authoritative bytes";
    let file = repo
        .io
        .write_file(&mut futures::io::Cursor::new(new), 4096)
        .await
        .unwrap();
    tree.entries.push(m::TreeEntry {
        name: "new-local.txt".into(),
        value: m::TreeValue::File {
            id: file,
            executable: false,
            size_bytes: new.len() as u64,
            copy_id: None,
        },
    });
    tree.entries.sort_by(|a, b| a.name.cmp(&b.name));
    let tree = repo.io.write_metadata(m::Object::Tree(tree)).await.unwrap();
    commit.parents = vec![old];
    commit.description = "local iteration without hydrating unchanged files".into();
    commit.root_tree = m::TreeMerge {
        terms: vec![m::TreeId(tree.key.id.0)],
        labels: vec![String::new()],
    };
    let commit = repo
        .io
        .write_metadata(m::Object::Commit(commit))
        .await
        .unwrap();
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        0,
        "tree edits must not fetch unchanged source payloads"
    );
    let head = m::CommitId(commit.key.id.0);
    let published = repo
        .control
        .publish(PublishRequest {
            head,
            reference_name: "main".into(),
            expected_generation: 1,
            idempotency_key: "new-local-publication".into(),
        })
        .await
        .unwrap();
    assert_eq!(published.generation, 2);
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        0,
        "already-present remote files must not be downloaded merely to publish"
    );
    assert_eq!(
        store
            .descriptor(
                "example",
                m::ObjectKey {
                    kind: m::Kind::File,
                    id: m::ObjectId(file.0)
                }
            )
            .unwrap()
            .unwrap()
            .size_bytes,
        new.len() as u64
    );
    assert!(
        direct
            .descriptor(m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(file.0)
            })
            .await
            .unwrap()
            .is_some(),
        "publication ACK must not delete a sole local authoritative copy"
    );
    let error = repo
        .control
        .publish(PublishRequest {
            head,
            reference_name: "main".into(),
            expected_generation: 1,
            idempotency_key: "stale-local-publication".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::Aborted
    );
    assert_eq!(
        repo.control.reconcile_pending().await.unwrap(),
        0,
        "definite stale CAS is not an uncertain queued publication"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    assert!(!fixture.objects().is_empty());
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_traverses_negative_terms_legacy_edges_copy_parents_and_symlinks() {
    let (_serverdir, store, fixture, old) = seed();
    let original = fixture
        .objects()
        .iter()
        .find(|object| {
            object.descriptor.key
                == m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(old.0),
                }
        })
        .unwrap();
    let m::Object::Commit(mut commit) = m::decode(&original.bytes, m::Limits::default()).unwrap()
    else {
        unreachable!()
    };
    let tree_object = fixture
        .objects()
        .iter()
        .find(|object| {
            object.descriptor.key
                == m::ObjectKey {
                    kind: m::Kind::Tree,
                    id: m::ObjectId(commit.root_tree.terms[0].0),
                }
        })
        .unwrap();
    let m::Object::Tree(mut positive) =
        m::decode(&tree_object.bytes, m::Limits::default()).unwrap()
    else {
        unreachable!()
    };
    let parent = m::encode(
        &m::Object::CopyHistory(m::CopyHistory {
            current_path: "prior-name.cc".into(),
            parents: vec![],
            salt: b"parent".to_vec(),
        }),
        m::Limits::default(),
    )
    .unwrap();
    let child = m::encode(
        &m::Object::CopyHistory(m::CopyHistory {
            current_path: "hello.cc".into(),
            parents: vec![m::CopyId(parent.descriptor.key.id.0)],
            salt: b"child".to_vec(),
        }),
        m::Limits::default(),
    )
    .unwrap();
    for entry in &mut positive.entries {
        if entry.name == "hello.cc" {
            let m::TreeValue::File { copy_id, .. } = &mut entry.value else {
                unreachable!()
            };
            *copy_id = Some(m::CopyId(child.descriptor.key.id.0));
        }
    }
    let symlink = m::encode(
        &m::Object::Symlink("negative-only-target".into()),
        m::Limits::default(),
    )
    .unwrap();
    let negative = m::encode(
        &m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "negative-link".into(),
                value: m::TreeValue::Symlink {
                    id: m::SymlinkId(symlink.descriptor.key.id.0),
                    target_bytes: "negative-only-target".len() as u64,
                },
            }],
        }),
        m::Limits::default(),
    )
    .unwrap();
    let positive = m::encode(&m::Object::Tree(positive), m::Limits::default()).unwrap();
    let mut legacy = commit.clone();
    legacy.description = "legacy predecessor only".into();
    legacy.root_tree = m::TreeMerge {
        terms: vec![m::TreeId(negative.descriptor.key.id.0)],
        labels: vec![String::new()],
    };
    let legacy = m::encode(&m::Object::Commit(legacy), m::Limits::default()).unwrap();
    commit.parents = vec![old];
    commit.predecessors = vec![m::CommitId(legacy.descriptor.key.id.0)];
    commit.root_tree = m::TreeMerge {
        terms: vec![
            m::TreeId(positive.descriptor.key.id.0),
            m::TreeId(negative.descriptor.key.id.0),
            m::TreeId(positive.descriptor.key.id.0),
        ],
        labels: vec!["left".into(), "base".into(), "right".into()],
    };
    commit.description = "full conflict and copy closure".into();
    let head = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
    for object in [
        &parent, &child, &symlink, &negative, &positive, &legacy, &head,
    ] {
        store
            .put("example", object.descriptor, &object.bytes, 2)
            .unwrap();
    }
    let counts = Counters::new();
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let (_direct, repo) = local(directory.path(), cache, config(&server), true);
    let admitted = repo
        .control
        .admit_commit(m::CommitId(head.descriptor.key.id.0))
        .await
        .unwrap();
    assert_eq!(admitted.metadata_objects, 1);
    complete_copy_metadata(&repo).await.unwrap();
    for object in [&parent, &child, &symlink, &negative, &legacy] {
        assert_eq!(
            repo.io
                .read_metadata(object.descriptor.key)
                .await
                .unwrap()
                .bytes(),
            object.bytes
        );
    }
    let snapshot = repo
        .io
        .copy_catalog_snapshot(jj_native_backend::CopyCatalogLimits {
            max_objects: 100,
            max_metadata_bytes: 1024 * 1024,
            max_edges: 100,
        })
        .await
        .unwrap();
    assert_eq!(
        snapshot.into_entries().len(),
        2,
        "copy ancestry must be installed parent first"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_publication_ack_is_reconciled_after_restart_without_advancing_generation_again() {
    let (_serverdir, store, _fixture, head) = seed();
    let counts = Counters::new();
    let server = Server::start(store.clone(), counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let config = config(&server);
    let (direct, repo) = local(directory.path(), cache.clone(), config.clone(), true);
    repo.control.admit_commit(head).await.unwrap();
    counts.public_paused.store(true, Ordering::SeqCst);
    let control = repo.control.clone();
    let task = tokio::spawn(async move {
        control
            .publish(PublishRequest {
                head,
                reference_name: "main".into(),
                expected_generation: 1,
                idempotency_key: "restart-publication".into(),
            })
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), counts.public_started.notified())
        .await
        .unwrap();
    assert_eq!(
        store.references("example", &["main".into()]).unwrap()[0].generation,
        2
    );
    task.abort();
    let _ = task.await;
    drop(repo);
    drop(direct);
    counts.public_paused.store(false, Ordering::SeqCst);
    let (_direct, repo) = local(directory.path(), cache, config, false);
    let confirmed = repo
        .control
        .publication("restart-publication")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(confirmed.head, head);
    assert_eq!(confirmed.reference_name, "main");
    assert_eq!(confirmed.generation, 2);
    assert!(
        repo.control
            .publication("unknown-publication-key")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.references("example", &["main".into()]).unwrap()[0].generation,
        2,
        "replay must use the durable idempotency request, never force or advance CAS again"
    );
    assert_eq!(repo.control.reconcile_pending().await.unwrap(), 0);
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_admission_returns_initial_snapshot_generation_despite_concurrent_movement() {
    let (_serverdir, store, _fixture, head) = seed();
    let counts = Counters::new();
    counts.paused.store(true, Ordering::SeqCst);
    let server = Server::start(store.clone(), counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let (_direct, repo) = local(directory.path(), cache, config(&server), true);
    let control = repo.control.clone();
    let task = tokio::spawn(async move { control.admit_reference("main").await });
    tokio::time::timeout(Duration::from_secs(5), counts.metadata_started.notified())
        .await
        .unwrap();
    assert_eq!(counts.holds.load(Ordering::SeqCst), 1);
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "concurrent-main-movement".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: None,
                staging_leases: vec![],
            },
            2,
        )
        .unwrap();
    store.gc("example", i64::MAX / 2).unwrap();
    counts.paused.store(false, Ordering::SeqCst);
    let admitted = task.await.unwrap().unwrap();
    assert_eq!(admitted.head, head);
    let reference = admitted.public_reference.unwrap();
    assert_eq!(reference.name, "main");
    assert_eq!(
        reference.generation, 1,
        "fetch state must use its original public snapshot, never the newer shared-ledger generation"
    );
    let error = repo
        .control
        .publish(PublishRequest {
            head,
            reference_name: "main".into(),
            expected_generation: reference.generation,
            idempotency_key: "stale-selected-generation".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::Aborted
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credential_change_requires_own_hold_and_never_reuses_other_credential_cache_scope() {
    let (_serverdir, store, fixture, _head) = seed();
    let counts = Counters::new();
    let server = Server::start(store, counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let original = config(&server);
    let (direct, repo) = local(directory.path(), cache.clone(), original.clone(), true);
    let admitted = repo.control.admit_reference("main").await.unwrap();
    read_current_metadata(&repo, admitted.head).await;
    let client_id = repo.control.client_id();
    drop(repo);
    drop(direct);
    let mut other = original.clone();
    other.remote.token_env = Some("TINYTREE_REMOTE_OTHER_TOKEN".into());
    let (direct, repo) = local(directory.path(), cache.clone(), other, false);
    assert_eq!(repo.control.client_id(), client_id);
    let file = first_file(&fixture);
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        2,
        "new credential principal must establish its own current hold before an uncached network fetch"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 1);
    cache.wait_for_idle().await.unwrap();
    drop(repo);
    drop(direct);
    let (_direct, repo) = local(directory.path(), cache.clone(), original, false);
    assert_eq!(
        repo.control.reconcile_pending().await.unwrap(),
        1,
        "completed old-scope roots still need current-scope proof reconciliation"
    );
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        2,
        "existing own hold is reconciled through Open, not a redundant publication"
    );
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        2,
        "another credential's cached bytes must not become a cross-scope cache hit"
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ledger_rejects_unrelated_directory_and_missing_stable_identity_without_repair() {
    use fjall::Readable;
    let (_serverdir, store, _fixture, _head) = seed();
    let server = Server::start(store, Counters::new()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let local_path = directory.path().join("local");
    make_private(&local_path);
    let direct = Arc::new(DirectObjectIo::init(&local_path, Default::default()).unwrap());
    let unrelated = directory.path().join("unrelated");
    make_private(&unrelated);
    std::fs::write(unrelated.join("keep.txt"), b"caller-owned unrelated data").unwrap();
    let staging = directory.path().join("must-not-create");
    assert!(
        RemoteRepository::open(
            direct.clone(),
            cache.clone(),
            &unrelated,
            &staging,
            config(&server)
        )
        .is_err()
    );
    assert_eq!(
        std::fs::read(unrelated.join("keep.txt")).unwrap(),
        b"caller-owned unrelated data"
    );
    assert!(!unrelated.join("owner").exists());
    assert!(!unrelated.join("data").exists());
    assert!(!staging.exists());
    let ledger = directory.path().join("ledger");
    let staging = directory.path().join("staging");
    let repo = RemoteRepository::open(
        direct.clone(),
        cache.clone(),
        &ledger,
        &staging,
        config(&server),
    )
    .unwrap();
    let client_id = repo.control.client_id();
    drop(repo);
    {
        let db = fjall::SingleWriterTxDatabase::builder(ledger.join("data"))
            .open()
            .unwrap();
        let meta = db
            .keyspace("meta", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        assert_eq!(
            db.read_tx()
                .get(&meta, b"client")
                .unwrap()
                .unwrap()
                .as_ref(),
            client_id
        );
        let mut tx = db.write_tx().durability(Some(fjall::PersistMode::SyncAll));
        tx.remove(&meta, b"client");
        tx.commit().unwrap();
    }
    assert!(
        RemoteRepository::open(direct, cache, &ledger, &staging, config(&server)).is_err(),
        "corrupt stable UUID must never be silently regenerated"
    );
    {
        let db = fjall::SingleWriterTxDatabase::builder(ledger.join("data"))
            .open()
            .unwrap();
        let meta = db
            .keyspace("meta", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        assert!(db.read_tx().get(&meta, b"client").unwrap().is_none());
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_staging_reclaims_crash_orphans_and_refuses_unrecognized_files() {
    let (_serverdir, store, _fixture, _head) = seed();
    let server = Server::start(store, Counters::new()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let original = config(&server);
    let (direct, repo) = local(directory.path(), cache.clone(), original.clone(), true);
    drop(repo);
    drop(direct);
    let staging = directory.path().join("staging");
    std::fs::write(staging.join("tinytree-crashed-download"), vec![42; 8192]).unwrap();
    std::fs::write(
        staging.join("native-publish-crashed-snapshot"),
        vec![43; 8192],
    )
    .unwrap();
    let (direct, repo) = local(directory.path(), cache.clone(), original.clone(), false);
    assert!(!staging.join("tinytree-crashed-download").exists());
    assert!(!staging.join("native-publish-crashed-snapshot").exists());
    drop(repo);
    drop(direct);
    std::fs::write(
        staging.join("unrecognized-user-file"),
        b"must remain untouched",
    )
    .unwrap();
    let direct = Arc::new(
        DirectObjectIo::load(&directory.path().join("local"), Default::default()).unwrap(),
    );
    assert!(
        RemoteRepository::open(
            direct,
            cache,
            &directory.path().join("ledger"),
            &staging,
            original
        )
        .is_err()
    );
    assert_eq!(
        std::fs::read(staging.join("unrecognized-user-file")).unwrap(),
        b"must remain untouched"
    );
    server.stop().await;
}

#[test]
fn upload_spool_configuration_accounts_for_both_simultaneous_snapshots() {
    let mut config = Config::default();
    config.remote.limits.max_file_bytes = 4096;
    config.max_upload_spool_bytes = 4096;
    assert!(
        config.validate().is_err(),
        "one-copy budget must not admit a two-copy preparation"
    );
    config.max_upload_spool_bytes = 8192;
    assert!(config.validate().is_ok());
}

#[test]
fn conservative_control_resources_fit_default_pool_and_toml_inherits_tight_limits() {
    let config = Config::default();
    let required = config.control_work_budget();
    let broker = native_broker::Config::default();
    assert!(
        required.memory_bytes <= broker.max_memory_bytes as u64,
        "declared control memory {} exceeds {}",
        required.memory_bytes,
        broker.max_memory_bytes
    );
    assert!(required.temporary_bytes <= broker.max_temporary_bytes);
    assert_eq!(
        required.temporary_bytes,
        2 * config.remote.limits.max_file_bytes
    );
    let dir = private_dir();
    let path = dir.path().join("remote.toml");
    std::fs::write(&path,"authorization_profile = 'example'\n[remote]\nendpoint = 'http://127.0.0.1:8093'\nrepository_id = 'example'\n").unwrap();
    let loaded = Config::load(&path).unwrap();
    assert_eq!(
        loaded.remote.limits.max_buffered_bytes,
        config.remote.limits.max_buffered_bytes
    );
    assert!(loaded.control_work_budget().memory_bytes <= broker.max_memory_bytes as u64);
    std::fs::write(&path, "[remote.limits]\nunknown_limit = 1\n").unwrap();
    assert!(Config::load(&path).is_err());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_broker_control_preserves_absent_tombstones_and_exact_historical_publication() {
    use native_broker::{BrokerControlClient, BrokerService, NativeRepositoryControl};
    let (_serverdir, store, _fixture, head) = seed();
    let counts = Counters::new();
    let server = Server::start(store.clone(), counts.clone()).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let (_direct, repo) = local(directory.path(), cache, config(&server), true);
    let stores = std::collections::HashMap::from([(
        "workspace".to_owned(),
        repo.io.clone() as Arc<dyn NativeObjectIo>,
    )]);
    let controls = std::collections::HashMap::from([(
        "workspace".to_owned(),
        Arc::new(repo.control.clone()) as Arc<dyn NativeRepositoryControl>,
    )]);
    let required = repo.control.resources();
    let mut small = native_broker::Config::default();
    small.max_memory_bytes = required.memory_bytes as usize - 1;
    assert!(
        BrokerService::new(stores.clone(), small)
            .and_then(|broker| broker.with_controls(controls.clone()))
            .is_err(),
        "registration must reject a provider larger than the shared memory pool"
    );
    let service = BrokerService::new(stores, native_broker::Config::default())
        .unwrap()
        .with_controls(controls)
        .unwrap();
    let socket = directory.path().canonicalize().unwrap().join("broker.sock");
    let path = socket.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        native_broker::serve_unix(&path, service, async {
            let _ = stopped.await;
        })
        .await
    });
    let client = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if task.is_finished() {
                panic!("broker exited before readiness");
            }
            if let Ok(client) =
                BrokerControlClient::connect(&socket, "workspace", native_broker::Config::default())
                    .await
            {
                break client;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let absent = client.observe_reference("new-reference").await.unwrap();
    assert_eq!(absent.generation, 0);
    assert!(absent.head.is_none());
    assert_eq!(counts.holds.load(Ordering::SeqCst), 0);
    let selected = client.observe_reference("main").await.unwrap();
    assert_eq!(selected.generation, 1);
    assert_eq!(selected.head.unwrap().head, head);
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "create-tombstone".into(),
                reference_name: "removed-reference".into(),
                expected_generation: 0,
                new_commit: Some(head),
                staging_leases: vec![],
            },
            2,
        )
        .unwrap();
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "delete-tombstone".into(),
                reference_name: "removed-reference".into(),
                expected_generation: 1,
                new_commit: None,
                staging_leases: vec![],
            },
            3,
        )
        .unwrap();
    let absent = client.observe_reference("removed-reference").await.unwrap();
    assert_eq!(absent.generation, 2);
    assert!(absent.head.is_none());
    let request = native_broker::PublishRequest {
        name: "main".into(),
        head,
        expected_generation: 1,
        idempotency_key: "broker-exact-publication".into(),
    };
    let receipt = client.publish(request.clone()).await.unwrap();
    assert_eq!(receipt.generation, 2);
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "move-after-publish".into(),
                reference_name: "main".into(),
                expected_generation: 2,
                new_commit: None,
                staging_leases: vec![],
            },
            4,
        )
        .unwrap();
    assert_eq!(
        client
            .publication(&request.idempotency_key)
            .await
            .unwrap()
            .unwrap(),
        receipt,
        "lookup must return original intent receipt, never the latest host reference generation"
    );
    assert!(
        client
            .publication("unknown-intent")
            .await
            .unwrap()
            .is_none()
    );
    drop(client);
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lazy_connection_uses_captured_ca_and_new_trust_never_reuses_old_connector() {
    let (_serverdir, store, _fixture, _head) = seed();
    let counts = Counters::new();
    let identity = tonic::transport::Identity::from_pem(
        std::fs::read(env!("TLS_SERVER_CERT")).unwrap(),
        std::fs::read(env!("TLS_SERVER_KEY")).unwrap(),
    );
    let server = Server::start_with_tls(store, counts.clone(), Some(identity)).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let ca = std::fs::read(env!("TLS_CA_CERT")).unwrap();
    let ca_path = directory.path().join("ca.pem");
    std::fs::write(&ca_path, &ca).unwrap();
    let mut original = config(&server);
    original.remote.endpoint = format!("https://{}", server.address);
    original.remote.tls = Some(tinytree_client::Tls {
        ca_certificate: ca_path.clone(),
        domain_name: Some("localhost".into()),
    });
    let (direct, repo) = local(directory.path(), cache.clone(), original.clone(), true);
    // Nothing connected at open. Replacing the configured path must not replace
    // the already captured trust snapshot used by the first lazy connection.
    std::fs::write(&ca_path, std::fs::read(env!("TLS_WRONG_CA_CERT")).unwrap()).unwrap();
    repo.control.admit_reference("main").await.unwrap();
    assert_eq!(counts.holds.load(Ordering::SeqCst), 1);
    drop(repo);
    drop(direct);
    let (direct, repo) = local(directory.path(), cache.clone(), original.clone(), false);
    assert!(
        repo.control.admit_reference("main").await.is_err(),
        "reopen with a changed CA must not reuse the old trusted connector/proof"
    );
    drop(repo);
    drop(direct);
    let direct = Arc::new(
        DirectObjectIo::load(&directory.path().join("local"), Default::default()).unwrap(),
    );
    assert!(
        RemoteRepository::open_with_ca_certificate(
            direct.clone(),
            cache.clone(),
            &directory.path().join("must-not-create-ledger"),
            &directory.path().join("must-not-create-stage"),
            original.clone(),
            None
        )
        .is_err()
    );
    assert!(!directory.path().join("must-not-create-ledger").exists());
    assert!(!directory.path().join("must-not-create-stage").exists());
    let repo = RemoteRepository::open_with_ca_certificate(
        direct.clone(),
        cache.clone(),
        &directory.path().join("ledger"),
        &directory.path().join("staging"),
        original.clone(),
        Some(Arc::from(ca.clone())),
    )
    .unwrap();
    repo.control.admit_reference("main").await.unwrap();
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        1,
        "restored captured trust reconciles the existing current own hold"
    );
    drop(repo);
    // These two actual TLS configurations collided under raw CA || domain.
    // PEM readers permit trailing non-PEM text, so trust must be framed, not
    // concatenated. No network connection is necessary for namespace testing.
    let mut first = original.clone();
    first.remote.tls.as_mut().unwrap().domain_name = Some("foo".into());
    let repo = RemoteRepository::open_with_ca_certificate(
        direct.clone(),
        cache.clone(),
        &directory.path().join("ledger"),
        &directory.path().join("staging"),
        first,
        Some(Arc::from(ca.clone())),
    )
    .unwrap();
    drop(repo);
    let scope = || {
        use fjall::Readable;
        let db = fjall::SingleWriterTxDatabase::builder(directory.path().join("ledger/data"))
            .open()
            .unwrap();
        let meta = db
            .keyspace("meta", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let value = db.read_tx().get(&meta, b"binding").unwrap().unwrap();
        serde_json::from_slice::<serde_json::Value>(&value).unwrap()["scope"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let first_scope = scope();
    let mut extended = ca.clone();
    extended.push(b'f');
    assert_eq!(
        [ca.as_slice(), b"foo"].concat(),
        [extended.as_slice(), b"oo"].concat()
    );
    let mut second = original;
    second.remote.tls.as_mut().unwrap().domain_name = Some("oo".into());
    let repo = RemoteRepository::open_with_ca_certificate(
        direct,
        cache,
        &directory.path().join("ledger"),
        &directory.path().join("staging"),
        second,
        Some(Arc::from(extended)),
    )
    .unwrap();
    drop(repo);
    assert_ne!(
        first_scope,
        scope(),
        "TLS namespaces must frame CA identity and optional server name independently"
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_workspace_delivery_does_not_reenter_its_shared_cache() {
    struct Case {
        name: &'static str,
        fetch_slots: usize,
        inflight_bytes: u64,
    }
    for case in [
        Case {
            name: "single_fetch_slot",
            fetch_slots: 1,
            inflight_bytes: 64 * 1024,
        },
        Case {
            name: "single_byte_unit_for_3000_byte_file",
            fetch_slots: 8,
            inflight_bytes: 4096,
        },
    ] {
        let (_serverdir, store, fixture, head) = seed();
        let counts = Counters::new();
        let server = Server::start(store, counts.clone()).await;
        let directory = private_dir();
        let cache = LocalCas::open(
            directory.path().join("cache"),
            Arc::new(NoUpstream),
            local_cas::CacheConfig {
                max_concurrent_fetches: case.fetch_slots,
                max_inflight_bytes: case.inflight_bytes,
                operation_timeout: Duration::from_secs(3),
                fetch_timeout: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap();
        let remote_config = config(&server);
        // Trusted fixture configuration selects the security scope, not a file
        // request. Do not alias workspace and repository cache artifact keys.
        let configured_identity = blake3::hash(
            format!(
                "{}\0{}\0{}",
                remote_config.remote.endpoint, remote_config.authorization_profile, TOKEN
            )
            .as_bytes(),
        );
        let scope = native_workspace::Scope::from_configured_identity(
            &remote_config.remote.repository_id,
            configured_identity.as_bytes(),
        )
        .unwrap();
        let (direct, repo) = local(directory.path(), cache.clone(), remote_config, true);
        assert_eq!(
            repo.control.admit_reference("main").await.unwrap().head,
            head
        );
        let compiler =
            native_workspace::Compiler::new(repo.io.clone(), scope.clone(), Default::default())
                .unwrap();
        let compiled = compiler.compile_commit(head).await.unwrap();
        let tree = output_core::Tree::open_with_cache_and_source(
            directory.path().join("namespace"),
            scope.backend(),
            cache.clone(),
            compiled.source(),
        )
        .unwrap();
        tree.install_source(
            compiled.manifest(tree.identity()),
            None,
            "workspace-base",
            &Default::default(),
        )
        .unwrap();
        assert_eq!(
            counts.files.load(Ordering::SeqCst),
            0,
            "{} startup hydrates no files",
            case.name
        );
        let selected = fixture
            .manifest
            .files
            .iter()
            .find(|file| file.size_bytes == 3000)
            .unwrap();
        let object = fixture
            .objects()
            .iter()
            .find(|object| {
                object.descriptor.key.kind == m::Kind::File
                    && blake3::hash(&object.bytes).to_hex().as_str() == selected.id
            })
            .unwrap();
        assert_eq!(object.bytes.len(), 3000);
        assert_eq!(
            direct.descriptor(object.descriptor.key).await.unwrap(),
            None
        );
        let node = tree.resolve(&selected.path).unwrap();
        let bytes = tokio::time::timeout(Duration::from_secs(2), tree.read(node.id, 0, 3000))
            .await
            .unwrap_or_else(|_| {
                panic!("{} recursively waited on shared cache admission", case.name)
            })
            .unwrap();
        assert_eq!(bytes, object.bytes, "{} cold read", case.name);
        assert_eq!(
            counts.files.load(Ordering::SeqCst),
            1,
            "{} cold read uses exactly one native file RPC",
            case.name
        );
        assert_eq!(
            tree.read(node.id, 0, 3000).await.unwrap(),
            object.bytes,
            "{} warm read",
            case.name
        );
        assert_eq!(
            counts.files.load(Ordering::SeqCst),
            1,
            "{} warm read makes no file RPC",
            case.name
        );
        assert_eq!(
            direct.descriptor(object.descriptor.key).await.unwrap(),
            None,
            "{} clean cache bytes must not create authoritative residency",
            case.name
        );
        tokio::time::timeout(Duration::from_secs(2), cache.wait_for_idle())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(cache.metrics().reserved_bytes, 0);
        server.stop().await;
    }
}

// Reference-only fixture: this materialized empty workspace exists solely to
// invoke the actual pinned jj DefaultIndex graph iterator, not as acceptance of
// a native lazy-mount user workflow. It has no ambient identity configuration.
struct PinnedGraphReference {
    repo: Arc<jj_lib::repo::ReadonlyRepo>,
    ids: Vec<jj_lib::backend::CommitId>,
    canonical: Vec<(m::Descriptor, Vec<u8>)>,
    _io: Arc<DirectObjectIo>,
    _directory: tempfile::TempDir,
}
async fn pinned_graph_reference(parents: &[&[usize]], divergent: bool) -> PinnedGraphReference {
    use jj_lib::{
        backend::{Backend, BackendInitError, ChangeId},
        config::{ConfigLayer, ConfigSource, StackedConfig},
        object_id::ObjectId as _,
        repo::Repo,
        settings::UserSettings,
        signing::Signer,
        workspace::Workspace,
    };
    let directory = private_dir();
    let mut layers = StackedConfig::with_defaults();
    layers.add_layer(ConfigLayer::parse(ConfigSource::User,
        "[user]\nname='Graph Reference'\nemail='graph@example.invalid'\n[operation]\nhostname='fixture'\nusername='fixture'\n").unwrap());
    let settings = UserSettings::from_config_and_home_dir(layers, None).unwrap();
    let slot = Arc::new(std::sync::Mutex::new(None::<Arc<DirectObjectIo>>));
    let initializer = {
        let slot = slot.clone();
        move |_: &UserSettings, path: &Path| -> Result<Box<dyn Backend>, BackendInitError> {
            let io = Arc::new(
                DirectObjectIo::init(path, Default::default())
                    .map_err(|error| BackendInitError(Box::new(error)))?,
            );
            let backend = futures::executor::block_on(jj_native_backend::NativeBackend::from_io(
                io.clone(),
                Default::default(),
            ))
            .map_err(|error| BackendInitError(Box::new(error)))?;
            *slot.lock().unwrap() = Some(io);
            Ok(Box::new(backend))
        }
    };
    let (_workspace, repo) = Workspace::init_with_backend(
        &settings,
        directory.path(),
        &initializer,
        Signer::new(None, vec![]),
    )
    .await
    .unwrap();
    let io = slot.lock().unwrap().clone().unwrap();
    let tree = repo.store().root_commit().tree();
    let mut ids = vec![repo.store().root_commit_id().clone()];
    assert_eq!(ids[0].as_bytes(), &[0; 32]);
    let mut transaction = repo.start_transaction();
    for (position, parents) in parents.iter().enumerate() {
        let mut change = [0u8; 16];
        let logical_change = if divergent && matches!(position, 1 | 2) {
            2
        } else {
            position + 1
        };
        change[..8].copy_from_slice(&(logical_change as u64).to_be_bytes());
        let commit = transaction
            .repo_mut()
            .new_commit(
                parents.iter().map(|&parent| ids[parent].clone()).collect(),
                tree.clone(),
            )
            .set_change_id(ChangeId::from_bytes(&change))
            .set_description(format!("pinned graph reference commit {}", position + 1))
            .write()
            .await
            .unwrap();
        ids.push(commit.id().clone());
    }
    let repo = transaction.commit("graph reference fixture").await.unwrap();
    let mut canonical = Vec::with_capacity(parents.len());
    for (position, id) in ids.iter().enumerate().skip(1) {
        let verified = io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId::from_slice(id.as_bytes()).unwrap(),
            })
            .await
            .unwrap();
        let m::Object::Commit(commit) = m::decode(verified.bytes(), Default::default()).unwrap()
        else {
            panic!("reference backend returned a non-commit");
        };
        assert_eq!(
            commit.parents,
            parents[position - 1]
                .iter()
                .map(|&parent| m::CommitId::from_slice(ids[parent].as_bytes()).unwrap())
                .collect::<Vec<_>>(),
            "fixture writer must preserve canonical parent ordering/redundancy"
        );
        canonical.push((verified.descriptor(), verified.bytes().to_vec()));
    }
    PinnedGraphReference {
        repo,
        ids,
        canonical,
        _io: io,
        _directory: directory,
    }
}
fn pinned_graph_expected(
    reference: &PinnedGraphReference,
    selected: &[usize],
) -> Vec<(m::CommitId, Vec<tinytree_client::GraphEdge>)> {
    pinned_expression_expected(
        reference,
        &jj_lib::revset::ResolvedExpression::Commits(
            selected
                .iter()
                .map(|&index| reference.ids[index].clone())
                .collect(),
        ),
    )
}
fn pinned_expression_expected(
    reference: &PinnedGraphReference,
    expression: &jj_lib::revset::ResolvedExpression,
) -> Vec<(m::CommitId, Vec<tinytree_client::GraphEdge>)> {
    use jj_lib::{default_index::DefaultReadonlyIndex, object_id::ObjectId as _, repo::Repo};
    use tinytree_client::GraphEdge;
    let index: &DefaultReadonlyIndex = reference.repo.readonly_index().downcast_ref().unwrap();
    index
        .evaluate_revset_impl(expression, reference.repo.store())
        .unwrap()
        .iter_graph_impl(true)
        .map(|result| {
            let (id, edges) = result.unwrap();
            let id = m::CommitId::from_slice(id.as_bytes()).unwrap();
            let edges = edges
                .into_iter()
                .map(|edge| {
                    let target = m::CommitId::from_slice(edge.target.as_bytes()).unwrap();
                    if edge.is_missing() {
                        GraphEdge::Missing(target)
                    } else if edge.is_indirect() {
                        GraphEdge::Indirect(target)
                    } else {
                        GraphEdge::Direct(target)
                    }
                })
                .collect();
            (id, edges)
        })
        .collect()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checked_selected_graph_rpc_matches_actual_pinned_jj_default_index() {
    use jj_lib::object_id::ObjectId as _;
    use tinytree_client::{NativeClient, query_protocol as q};
    struct Case {
        name: &'static str,
        parents: &'static [&'static [usize]],
        selected: &'static [usize],
        public: usize,
        divergent: bool,
    }
    let cases = [
        Case {
            name: "linearized diamond",
            parents: &[&[0], &[1], &[1], &[2, 3]],
            selected: &[1, 4],
            public: 4,
            divergent: false,
        },
        Case {
            name: "virtual octopus",
            parents: &[&[0], &[0], &[0], &[1, 2], &[2, 3], &[4, 5]],
            selected: &[1, 2, 3, 6],
            public: 6,
            divergent: false,
        },
        Case {
            name: "multiple missing boundaries",
            parents: &[&[0], &[0], &[0], &[1, 2], &[2, 3], &[4, 5]],
            selected: &[2, 6],
            public: 6,
            divergent: false,
        },
        Case {
            name: "ancestor reduction",
            parents: &[&[0], &[0], &[1], &[2, 3], &[3], &[4, 5]],
            selected: &[3, 4, 6],
            public: 6,
            divergent: false,
        },
        Case {
            name: "deep lookahead escape",
            parents: &[
                &[0],
                &[1],
                &[1],
                &[2],
                &[4],
                &[4, 3],
                &[2],
                &[6],
                &[5, 8],
                &[7, 9],
            ],
            selected: &[1, 4, 7, 8, 10],
            public: 10,
            divergent: false,
        },
        Case {
            name: "all direct redundancy",
            parents: &[&[0], &[1], &[2, 1]],
            selected: &[1, 2, 3],
            public: 3,
            divergent: false,
        },
        Case {
            name: "mixed direct and indirect duplicates",
            parents: &[&[0], &[1], &[2], &[2, 1, 3]],
            selected: &[1, 2, 4],
            public: 4,
            divergent: false,
        },
        Case {
            name: "remote private boundary",
            parents: &[&[0], &[1], &[2], &[3]],
            selected: &[1, 4],
            public: 2,
            divergent: false,
        },
        Case {
            name: "private hidden divergent sibling",
            parents: &[&[0], &[1], &[1], &[2]],
            selected: &[1, 2, 3, 4],
            public: 1,
            divergent: true,
        },
        Case {
            name: "intrinsic root",
            parents: &[],
            selected: &[0],
            public: 0,
            divergent: false,
        },
    ];
    for case in cases {
        let reference = pinned_graph_reference(case.parents, case.divergent).await;
        let expected = pinned_graph_expected(&reference, case.selected);
        let store_directory = private_dir();
        let store = Store::open(store_directory.path(), Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let empty = m::empty_tree();
        store
            .put("example", empty.descriptor, &empty.bytes, 0)
            .unwrap();
        let index_directory = private_dir();
        let index =
            tinytree_index::Index::open(index_directory.path(), &Default::default()).unwrap();
        // Reference writes, public derived positions and private ordinals all
        // preserve the SAME parent-first order, including independent siblings.
        for (descriptor, bytes) in &reference.canonical[..case.public] {
            store.put("example", *descriptor, bytes, 0).unwrap();
            index.ingest("example", *descriptor, bytes).unwrap();
        }
        let mut service_config = ServiceConfig::default();
        service_config.query.max_eval_quantum_steps = 1;
        service_config.query.max_eval_step_work = 1;
        service_config.query.max_graph_query_step_work = 1;
        let service = RepositoryService::new(
            store.clone(),
            service_config,
            vec!["example".into()],
            vec![Principal::new("test", TOKEN, vec!["example".into()], true).unwrap()],
        )
        .unwrap()
        .with_query_index(index.clone())
        .unwrap();
        let counts = Counters::new();
        let server = Server::start_service(service, counts.clone(), None).await;
        let mut client_config = config(&server).remote;
        client_config.limits.max_graph_pages = 10_000;
        let client = NativeClient::connect(client_config).await.unwrap();
        let roots = if case.public == 0 {
            vec![m::CommitId::ROOT]
        } else {
            reference.ids[1..=case.public]
                .iter()
                .map(|id| m::CommitId::from_slice(id.as_bytes()).unwrap())
                .collect()
        };
        let mut overlay = Vec::new();
        for (descriptor, bytes) in &reference.canonical[case.public..] {
            overlay.push(
                client
                    .verify_metadata(*descriptor, bytes.clone())
                    .await
                    .unwrap(),
            );
        }
        let scope = client
            .open_query_scope_with_overlay(roots, overlay)
            .await
            .unwrap();
        let expression = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: case
                    .selected
                    .iter()
                    .map(|&index| q::CommitId {
                        value: reference.ids[index].as_bytes().to_vec(),
                    })
                    .collect(),
            })),
        };
        if case.divergent {
            let m::Object::Commit(shared) =
                m::decode(&reference.canonical[1].1, Default::default()).unwrap()
            else {
                unreachable!()
            };
            let prefix: String = shared
                .change_id
                .0
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            let visible_head = m::CommitId::from_slice(reference.ids[4].as_bytes()).unwrap();
            let tinytree_client::ChangePrefix::Single(resolution) = client
                .resolve_change_prefix(&scope, &prefix, vec![visible_head])
                .await
                .unwrap()
            else {
                panic!("divergent private ChangeId must remain one change with multiple targets");
            };
            let visibility: std::collections::BTreeMap<_, _> = resolution
                .targets
                .into_iter()
                .map(|target| (target.commit, target.visible))
                .collect();
            assert_eq!(
                visibility,
                std::collections::BTreeMap::from([
                    (
                        m::CommitId::from_slice(reference.ids[2].as_bytes()).unwrap(),
                        true
                    ),
                    (
                        m::CommitId::from_slice(reference.ids[3].as_bytes()).unwrap(),
                        false
                    ),
                ])
            );
        }
        let mut closed = false;
        for canonical in [false, true] {
            let mut query = client
                .begin_graph_query(&scope, &expression, 1, canonical)
                .await
                .unwrap();
            let mut actual = Vec::new();
            let mut sequence = 0;
            let mut progress = false;
            while !query.is_exhausted() {
                let page = client.evaluate_graph_page(&mut query).await.unwrap();
                assert_eq!(page.sequence, sequence, "{}", case.name);
                sequence += 1;
                progress |= page.is_progress();
                for row in page.rows() {
                    actual.push((row.commit, row.edges().to_vec()));
                    if row.commit == m::CommitId::ROOT {
                        assert_eq!(row.change, m::ChangeId::ROOT);
                        assert!(row.canonical.is_none());
                    } else {
                        let (descriptor, bytes) = reference
                            .canonical
                            .iter()
                            .find(|(descriptor, _)| descriptor.key.id.0 == row.commit.0)
                            .unwrap();
                        let m::Object::Commit(commit) =
                            m::decode(bytes, Default::default()).unwrap()
                        else {
                            unreachable!()
                        };
                        assert_eq!(row.change, commit.change_id);
                        if canonical {
                            let checked = row.canonical.as_ref().expect(
                                "canonical non-root projection must include verified bytes",
                            );
                            assert_eq!(checked.descriptor(), *descriptor);
                            assert_eq!(checked.canonical_bytes(), bytes.as_slice());
                        } else {
                            assert!(row.canonical.is_none());
                        }
                    }
                }
                // A captured private graph remains valid after its handle is
                // closed. Replay/GC/quota matrices live in server RPC coverage.
                if canonical && case.public < case.parents.len() && !closed {
                    client.close_query_scope(&scope).await.unwrap();
                    closed = true;
                }
                // Do not retain page budget ownership while requesting another.
                drop(page);
            }
            assert!(
                progress,
                "tiny graph quanta should yield bounded progress: {}",
                case.name
            );
            assert_eq!(
                actual, expected,
                "actual jj0.46 ordered graph mismatch: {}, canonical={canonical}",
                case.name
            );
        }
        if !closed {
            client.close_query_scope(&scope).await.unwrap();
        }
        assert_eq!(
            counts.files.load(Ordering::SeqCst),
            0,
            "graph projections must not fetch file blobs"
        );
        assert_eq!(
            counts.metadata_rpcs.load(Ordering::SeqCst),
            0,
            "canonical projection must not silently rehydrate full client history"
        );
        for (descriptor, _) in &reference.canonical[case.public..] {
            assert!(
                store
                    .descriptor("example", descriptor.key)
                    .unwrap()
                    .is_none()
            );
            assert!(
                index
                    .node("example", m::CommitId(descriptor.key.id.0))
                    .unwrap()
                    .is_none()
            );
        }
        server.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roots_and_dag_range_graph_rpc_match_actual_pinned_jj() {
    use jj_lib::{object_id::ObjectId as _, revset::ResolvedExpression as Jj};
    use tinytree_client::{NativeClient, query_protocol as q};
    enum Operator {
        Roots {
            candidates: &'static [usize],
        },
        DagRange {
            roots: &'static [usize],
            heads: &'static [usize],
            generation: Option<(u64, Option<u64>)>,
        },
    }
    struct Case {
        name: &'static str,
        parents: &'static [&'static [usize]],
        public: usize,
        operator: Operator,
    }
    use Operator::{DagRange, Roots};
    let linear: &'static [&'static [usize]] = &[&[0], &[1], &[2]];
    // A,b,c,D,E. A->D has lengths1/3; A->E has lengths2/4.
    // A min/max envelope would falsely admit E at generation3.
    let unequal: &'static [&'static [usize]] = &[&[0], &[1], &[2], &[1, 3], &[4]];
    let cases = [
        Case {
            name: "roots through excluded intermediate",
            parents: linear,
            public: 3,
            operator: Roots {
                candidates: &[1, 3],
            },
        },
        Case {
            name: "roots through excluded merge",
            parents: &[&[0], &[1], &[1], &[2, 3], &[4]],
            public: 5,
            operator: Roots {
                candidates: &[1, 5],
            },
        },
        Case {
            name: "roots disconnected candidates",
            parents: &[&[0], &[1], &[0], &[3], &[2, 4]],
            public: 5,
            operator: Roots {
                candidates: &[2, 4],
            },
        },
        Case {
            name: "roots intrinsic root dominates",
            parents: linear,
            public: 3,
            operator: Roots {
                candidates: &[0, 3],
            },
        },
        Case {
            name: "roots empty candidates",
            parents: linear,
            public: 3,
            operator: Roots { candidates: &[] },
        },
        Case {
            name: "roots private excluded bridge",
            parents: &[&[0], &[1], &[2], &[3]],
            public: 1,
            operator: Roots {
                candidates: &[2, 4],
            },
        },
        Case {
            name: "dag range full inclusive endpoints",
            parents: linear,
            public: 3,
            operator: DagRange {
                roots: &[1],
                heads: &[3],
                generation: None,
            },
        },
        Case {
            name: "dag range generation zero",
            parents: linear,
            public: 3,
            operator: DagRange {
                roots: &[1],
                heads: &[3],
                generation: Some((0, Some(1))),
            },
        },
        Case {
            name: "dag range middle generation",
            parents: linear,
            public: 3,
            operator: DagRange {
                roots: &[1],
                heads: &[3],
                generation: Some((1, Some(2))),
            },
        },
        Case {
            name: "dag range empty generation interval",
            parents: linear,
            public: 3,
            operator: DagRange {
                roots: &[1],
                heads: &[3],
                generation: Some((1, Some(1))),
            },
        },
        Case {
            name: "dag range explicit empty zero interval",
            parents: linear,
            public: 3,
            operator: DagRange {
                roots: &[1],
                heads: &[3],
                generation: Some((0, Some(0))),
            },
        },
        Case {
            name: "dag range intrinsic root generation zero",
            parents: linear,
            public: 3,
            operator: DagRange {
                roots: &[0],
                heads: &[3],
                generation: Some((0, Some(1))),
            },
        },
        Case {
            name: "dag range root only",
            parents: &[],
            public: 0,
            operator: DagRange {
                roots: &[0],
                heads: &[0],
                generation: None,
            },
        },
        Case {
            name: "dag range disconnected endpoints",
            parents: &[&[0], &[1], &[0], &[3]],
            public: 4,
            operator: DagRange {
                roots: &[1],
                heads: &[4],
                generation: None,
            },
        },
        Case {
            name: "dag range empty root set",
            parents: linear,
            public: 3,
            operator: DagRange {
                roots: &[],
                heads: &[3],
                generation: None,
            },
        },
        Case {
            name: "dag range unequal alternate path",
            parents: unequal,
            public: 5,
            operator: DagRange {
                roots: &[1],
                heads: &[5],
                generation: Some((3, Some(4))),
            },
        },
        Case {
            name: "dag range unequal long path endpoint",
            parents: unequal,
            public: 5,
            operator: DagRange {
                roots: &[1],
                heads: &[5],
                generation: Some((4, Some(5))),
            },
        },
        Case {
            name: "dag range multiple roots",
            parents: unequal,
            public: 5,
            operator: DagRange {
                roots: &[1, 2],
                heads: &[5],
                generation: Some((0, Some(1))),
            },
        },
        Case {
            name: "dag range private alternate path",
            parents: unequal,
            public: 2,
            operator: DagRange {
                roots: &[1],
                heads: &[5],
                generation: Some((3, Some(4))),
            },
        },
    ];
    for case in cases {
        let reference = pinned_graph_reference(case.parents, false).await;
        let wire_commits = |selected: &[usize]| q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: selected
                    .iter()
                    .map(|&index| q::CommitId {
                        value: reference.ids[index].as_bytes().to_vec(),
                    })
                    .collect(),
            })),
        };
        let jj_commits = |selected: &[usize]| {
            Jj::Commits(
                selected
                    .iter()
                    .map(|&index| reference.ids[index].clone())
                    .collect(),
            )
        };
        let (wire, expression) = match case.operator {
            Roots { candidates } => (
                q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Roots(Box::new(q::UnaryExpr {
                        input: Some(Box::new(wire_commits(candidates))),
                    }))),
                },
                Jj::Roots(Box::new(jj_commits(candidates))),
            ),
            DagRange {
                roots,
                heads,
                generation,
            } => {
                let interval = generation.map(|(start, end_exclusive)| q::GenerationRange {
                    start,
                    end_exclusive,
                });
                let (start, end) = generation.unwrap_or((0, None));
                (
                    q::GraphExpr {
                        version: 1,
                        expression: Some(q::graph_expr::Expression::DagRange(Box::new(
                            q::DagRangeExpr {
                                roots: Some(Box::new(wire_commits(roots))),
                                heads: Some(Box::new(wire_commits(heads))),
                                generations_from_roots: interval,
                            },
                        ))),
                    },
                    Jj::DagRange {
                        roots: Box::new(jj_commits(roots)),
                        heads: Box::new(jj_commits(heads)),
                        generation_from_roots: start..end.unwrap_or(u64::MAX),
                    },
                )
            }
        };
        let expected = pinned_expression_expected(&reference, &expression);
        let store_directory = private_dir();
        let store = Store::open(store_directory.path(), Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let empty = m::empty_tree();
        store
            .put("example", empty.descriptor, &empty.bytes, 0)
            .unwrap();
        let index_directory = private_dir();
        let index =
            tinytree_index::Index::open(index_directory.path(), &Default::default()).unwrap();
        for (descriptor, bytes) in &reference.canonical[..case.public] {
            store.put("example", *descriptor, bytes, 0).unwrap();
            index.ingest("example", *descriptor, bytes).unwrap();
        }
        let mut service_config = ServiceConfig::default();
        service_config.query.max_eval_quantum_steps = 1;
        service_config.query.max_eval_step_work = 1;
        service_config.query.max_graph_query_step_work = 1;
        let service = RepositoryService::new(
            store.clone(),
            service_config,
            vec!["example".into()],
            vec![Principal::new("test", TOKEN, vec!["example".into()], true).unwrap()],
        )
        .unwrap()
        .with_query_index(index.clone())
        .unwrap();
        let counts = Counters::new();
        let server = Server::start_service(service, counts.clone(), None).await;
        let mut client_config = config(&server).remote;
        client_config.limits.max_graph_pages = 10_000;
        let client = NativeClient::connect(client_config).await.unwrap();
        let roots = if case.public == 0 {
            vec![m::CommitId::ROOT]
        } else {
            reference.ids[1..=case.public]
                .iter()
                .map(|id| m::CommitId::from_slice(id.as_bytes()).unwrap())
                .collect()
        };
        let mut overlay = Vec::new();
        for (descriptor, bytes) in &reference.canonical[case.public..] {
            overlay.push(
                client
                    .verify_metadata(*descriptor, bytes.clone())
                    .await
                    .unwrap(),
            );
        }
        let scope = client
            .open_query_scope_with_overlay(roots, overlay)
            .await
            .unwrap();
        // One canonical selected-graph projection checks both the operator's
        // exact selected IDs and its ordered direct/indirect/missing edges.
        let mut query = client
            .begin_graph_query(&scope, &wire, 1, true)
            .await
            .unwrap();
        let mut actual = Vec::new();
        let mut sequence = 0;
        while !query.is_exhausted() {
            let page = client.evaluate_graph_page(&mut query).await.unwrap();
            assert_eq!(page.sequence, sequence, "{}", case.name);
            sequence += 1;
            for row in page.rows() {
                actual.push((row.commit, row.edges().to_vec()));
                if row.commit == m::CommitId::ROOT {
                    assert!(row.canonical.is_none());
                    assert_eq!(row.change, m::ChangeId::ROOT);
                } else {
                    let (descriptor, bytes) = reference
                        .canonical
                        .iter()
                        .find(|(descriptor, _)| descriptor.key.id.0 == row.commit.0)
                        .unwrap();
                    let canonical = row
                        .canonical
                        .as_ref()
                        .expect("canonical non-root row missing");
                    assert_eq!(canonical.descriptor(), *descriptor);
                    assert_eq!(canonical.canonical_bytes(), bytes.as_slice());
                }
            }
            drop(page);
        }
        assert_eq!(
            actual, expected,
            "actual jj Roots/DagRange mismatch: {}",
            case.name
        );
        client.close_graph_query(&mut query).await.unwrap();
        client.close_query_scope(&scope).await.unwrap();
        assert_eq!(counts.files.load(Ordering::SeqCst), 0, "{}", case.name);
        assert_eq!(
            counts.metadata_rpcs.load(Ordering::SeqCst),
            0,
            "{}",
            case.name
        );
        for (descriptor, _) in &reference.canonical[case.public..] {
            assert!(
                store
                    .descriptor("example", descriptor.key)
                    .unwrap()
                    .is_none()
            );
            assert!(
                index
                    .node("example", m::CommitId(descriptor.key.id.0))
                    .unwrap()
                    .is_none()
            );
        }
        server.stop().await;
    }
}
