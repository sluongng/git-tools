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
    capture_timeouts: Arc<AtomicBool>,
    metadata_timeouts: Arc<std::sync::Mutex<Vec<String>>>,
    query_timeouts: Arc<std::sync::Mutex<Vec<String>>>,
    graph_rpcs: Arc<AtomicUsize>,
    query_rpcs: Arc<AtomicUsize>,
    cloud_rpcs: Arc<AtomicUsize>,
    metadata_keys: Arc<AtomicUsize>,
    metadata_requests: Arc<std::sync::Mutex<Vec<p::ObjectKey>>>,
    metadata_bytes: Arc<AtomicUsize>,
    largest_metadata_batch: Arc<AtomicUsize>,
    paused: Arc<AtomicBool>,
    metadata_started: Arc<tokio::sync::Notify>,
    public_paused: Arc<AtomicBool>,
    public_started: Arc<tokio::sync::Notify>,
    missing_paused: Arc<AtomicBool>,
    missing_started: Arc<tokio::sync::Notify>,
}
impl Counters {
    fn new() -> Self {
        Self {
            files: Arc::new(AtomicUsize::new(0)),
            holds: Arc::new(AtomicUsize::new(0)),
            metadata_rpcs: Arc::new(AtomicUsize::new(0)),
            capture_timeouts: Arc::new(AtomicBool::new(false)),
            metadata_timeouts: Arc::new(std::sync::Mutex::new(Vec::new())),
            query_timeouts: Arc::new(std::sync::Mutex::new(Vec::new())),
            graph_rpcs: Arc::new(AtomicUsize::new(0)),
            query_rpcs: Arc::new(AtomicUsize::new(0)),
            cloud_rpcs: Arc::new(AtomicUsize::new(0)),
            metadata_keys: Arc::new(AtomicUsize::new(0)),
            metadata_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            metadata_bytes: Arc::new(AtomicUsize::new(0)),
            largest_metadata_batch: Arc::new(AtomicUsize::new(0)),
            paused: Arc::new(AtomicBool::new(false)),
            metadata_started: Arc::new(tokio::sync::Notify::new()),
            public_paused: Arc::new(AtomicBool::new(false)),
            public_started: Arc::new(tokio::sync::Notify::new()),
            missing_paused: Arc::new(AtomicBool::new(false)),
            missing_started: Arc::new(tokio::sync::Notify::new()),
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
        self.counts.graph_rpcs.fetch_add(1, Ordering::SeqCst);
        self.service.get_commit_graph(r).await
    }
    type BatchReadObjectsStream = <RepositoryService as Repository>::BatchReadObjectsStream;
    async fn batch_read_objects(
        &self,
        r: Request<p::BatchReadObjectsRequest>,
    ) -> Result<Response<Self::BatchReadObjectsStream>, Status> {
        if self.counts.capture_timeouts.load(Ordering::SeqCst) {
            self.counts.metadata_timeouts.lock().unwrap().push(
                r.metadata()
                    .get("grpc-timeout")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("missing")
                    .to_owned(),
            );
        }
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
        let response = self.service.find_missing_objects(r).await?;
        self.counts.missing_started.notify_one();
        while self.counts.missing_paused.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(response)
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
        Self::start_service_at(service, counts, tls, "127.0.0.1:0".parse().unwrap()).await
    }
    async fn start_service_at(
        service: RepositoryService,
        counts: Counters,
        tls: Option<tonic::transport::Identity>,
        bind: std::net::SocketAddr,
    ) -> Self {
        let query_requests = counts.query_rpcs.clone();
        let capture_timeouts = counts.capture_timeouts.clone();
        let query_timeouts = counts.query_timeouts.clone();
        let query = tonic::service::interceptor::InterceptedService::new(
            service.query_grpc_service(),
            move |request: Request<()>| {
                query_requests.fetch_add(1, Ordering::SeqCst);
                if capture_timeouts.load(Ordering::SeqCst) {
                    query_timeouts.lock().unwrap().push(
                        request
                            .metadata()
                            .get("grpc-timeout")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("missing")
                            .to_owned(),
                    );
                }
                Ok(request)
            },
        );
        let cloud_requests = counts.cloud_rpcs.clone();
        let cloud = tonic::service::interceptor::InterceptedService::new(
            service.cloud_grpc_service(),
            move |request: Request<()>| {
                cloud_requests.fetch_add(1, Ordering::SeqCst);
                Ok(request)
            },
        );
        let listener = tokio::net::TcpListener::bind(bind).await.unwrap();
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
                .add_service(cloud)
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
    // The caller's physical owner survives EOF while the reader is still held.
    let permit = Arc::new(tokio::sync::Semaphore::new(1));
    let owner = Arc::new(permit.clone().acquire_owned().await.unwrap());
    let owned = repo
        .io
        .read_file_with_owner(m::FileId(file.descriptor.key.id.0), owner.clone())
        .await
        .unwrap();
    drop(owner);
    let (_, mut held_reader) = owned.into_parts();
    let mut observed = Vec::new();
    held_reader.read_to_end(&mut observed).await.unwrap();
    assert_eq!(observed, file.bytes);
    assert!(permit.clone().try_acquire_owned().is_err());
    drop(held_reader);
    assert!(permit.clone().try_acquire_owned().is_ok());
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
        b"tinytree-native-admission-4\n"
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

#[derive(Debug)]
struct TransferSource {
    inner: Arc<dyn NativeObjectIo>,
    files: AtomicUsize,
}
#[async_trait]
impl NativeObjectIo for TransferSource {
    fn metadata_work_budget(&self) -> Option<jj_native_backend::ObjectIoResources> {
        self.inner.metadata_work_budget()
    }
    async fn descriptor(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.inner.descriptor(key).await
    }
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.inner.descriptor_with_owner(key, owner).await
    }
    async fn read_metadata(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.inner.read_metadata(key).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.inner.read_metadata_with_owner(key, owner).await
    }
    async fn read_file(
        &self,
        _: m::FileId,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        panic!("transfer must use physically owned source reads")
    }
    async fn read_file_with_owner(
        &self,
        id: m::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        self.files.fetch_add(1, Ordering::SeqCst);
        self.inner.read_file_with_owner(id, owner).await
    }
    async fn write_metadata(&self, _: m::Object) -> jj_lib::backend::BackendResult<m::Descriptor> {
        panic!("transfer cannot write its source")
    }
    async fn write_file(
        &self,
        _: &mut (dyn futures::io::AsyncRead + Send + Unpin),
        _: u64,
    ) -> jj_lib::backend::BackendResult<m::FileId> {
        panic!("transfer cannot write its source")
    }
    async fn copy_catalog_snapshot(
        &self,
        _: jj_native_backend::CopyCatalogLimits,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        panic!("transfer cannot query unrelated source catalog")
    }
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
    // Backup staging must not reuse publication: it accepts a TREE without a
    // COMMIT and never creates a hold, moves a reference or acquires read grants.
    let source = Arc::new(TransferSource {
        inner: repo.io.clone(),
        files: AtomicUsize::new(0),
    });
    let fence: Arc<dyn std::any::Any + Send + Sync> = Arc::new(());
    let transfer_limits = jj_native_remote::NativeTransferLimits::default();
    let work = repo
        .control
        .native_transfer_resources(source.as_ref(), &transfer_limits)
        .unwrap();
    assert!(
        work.memory_bytes > config(&server).control_work_budget().memory_bytes,
        "lazy source hydration is additional to retained transfer/target work"
    );
    let holds_before = counts.holds.load(Ordering::SeqCst);
    let references_before = store.references("example", &["main".into()]).unwrap();
    let staged = repo
        .control
        .transfer_native_closure(
            source.clone(),
            vec![tree.key],
            fence.clone(),
            transfer_limits.clone(),
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(
        source.files.load(Ordering::SeqCst),
        1,
        "only missing authored FILE is read"
    );
    assert!(
        staged
            .stages()
            .iter()
            .any(|stage| stage.descriptor.key.kind == m::Kind::File
                && stage.descriptor.key.id.0 == file.0)
    );
    assert!(
        staged
            .stages()
            .iter()
            .all(|stage| !stage.staging_lease.is_empty() && stage.expires_at_unix_seconds > 0)
    );
    let symlink = repo
        .io
        .write_metadata(m::Object::Symlink("new-local.txt".into()))
        .await
        .unwrap();
    let warm = repo
        .control
        .transfer_native_closure(
            source.clone(),
            vec![tree.key, symlink.key],
            fence.clone(),
            transfer_limits.clone(),
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap();
    assert_eq!(warm.stages().len(), 1);
    assert_eq!(warm.stages()[0].descriptor, symlink);
    assert!(warm.present_objects() > 0);
    assert_eq!(
        source.files.load(Ordering::SeqCst),
        1,
        "present FILEs must not be read even locally"
    );
    assert_eq!(counts.holds.load(Ordering::SeqCst), holds_before);
    assert_eq!(
        store.references("example", &["main".into()]).unwrap(),
        references_before
    );
    let mut tiny = transfer_limits.clone();
    tiny.max_memory_bytes = 1;
    let error = repo
        .control
        .transfer_native_closure(
            source.clone(),
            vec![tree.key],
            fence.clone(),
            tiny,
            tokio::time::Instant::now() + Duration::from_secs(10),
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<Status>().unwrap().code(),
        tonic::Code::ResourceExhausted
    );
    let missing_tree = repo
        .io
        .write_metadata(m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "absent.txt".into(),
                value: m::TreeValue::File {
                    id: m::FileId([217; 32]),
                    executable: false,
                    size_bytes: 7,
                    copy_id: None,
                },
            }],
        }))
        .await
        .unwrap();
    assert!(
        repo.control
            .transfer_native_closure(
                source.clone(),
                vec![missing_tree.key],
                fence,
                transfer_limits,
                tokio::time::Instant::now() + Duration::from_secs(10),
            )
            .await
            .is_err(),
        "FILE missing both upstream and at actual source cannot succeed"
    );
    assert_eq!(counts.holds.load(Ordering::SeqCst), holds_before);
    assert_eq!(
        store.references("example", &["main".into()]).unwrap(),
        references_before
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
    pinned_graph_reference_with_conflicts(parents, divergent, &[]).await
}
async fn pinned_graph_reference_with_conflicts(
    parents: &[&[usize]],
    divergent: bool,
    conflicts: &[usize],
) -> PinnedGraphReference {
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
        // Deliberately unavailable tree terms make this a metadata-only
        // has_conflict oracle: neither jj nor the query may load tree bodies.
        let selected_tree = if conflicts.contains(&(position + 1)) {
            jj_lib::merged_tree::MergedTree::new(
                repo.store().clone(),
                jj_lib::merge::Merge::from_vec(vec![
                    jj_lib::backend::TreeId::from_bytes(&[21; 32]),
                    jj_lib::backend::TreeId::from_bytes(&[22; 32]),
                    jj_lib::backend::TreeId::from_bytes(&[23; 32]),
                ]),
                jj_lib::conflict_labels::ConflictLabels::from_vec(vec![
                    "left".into(),
                    "base".into(),
                    "right".into(),
                ]),
            )
        } else {
            tree.clone()
        };
        let commit = transaction
            .repo_mut()
            .new_commit(
                parents.iter().map(|&parent| ids[parent].clone()).collect(),
                selected_tree,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn captured_multi_root_queries_admit_deep_metadata_without_intervening_history_or_new_holds()
{
    use native_broker::{NativeQueryIo, OpenQueryScope};
    let (_serverdir, store, fixture, head, historical) = seed_history(8);
    let independent = fixture
        .objects()
        .iter()
        .find(|o| o.descriptor.key.kind == m::Kind::Commit)
        .unwrap();
    let independent_head = m::CommitId(independent.descriptor.key.id.0);
    let mut selected = head;
    let mut ancestry = Vec::new();
    loop {
        ancestry.push(selected);
        let (_, bytes) = store
            .read_metadata(
                "example",
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(selected.0),
                },
            )
            .unwrap();
        let m::Object::Commit(commit) = m::decode(&bytes, m::Limits::default()).unwrap() else {
            unreachable!()
        };
        if commit.parents == vec![m::CommitId::ROOT] {
            break;
        }
        selected = commit.parents[0];
    }
    let counts = Counters::new();
    let indexdir = private_dir();
    let index = tinytree_index::Index::open(indexdir.path(), &Default::default()).unwrap();
    let service = RepositoryService::new(
        store.clone(),
        ServiceConfig::default(),
        vec!["example".into()],
        vec![
            Principal::new("test", TOKEN, vec!["example".into()], true)
                .unwrap()
                .with_retention(true),
        ],
    )
    .unwrap()
    .with_query_index(index)
    .unwrap();
    let server = Server::start_service(service, counts.clone(), None).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let mut configuration = config(&server);
    configuration.remote.limits.max_inflight = 1;
    configuration.remote.limits.max_graph_objects = 65_536;
    configuration.query.max_scopes = 2;
    configuration.query.max_queries = 1;
    configuration.query.max_replays = 8;
    configuration.query.max_registry_bytes = 128 * 1024;
    let (direct, repo) = local(directory.path(), cache.clone(), configuration.clone(), true);
    repo.control.admit_reference("main").await.unwrap();
    repo.control.admit_commit(independent_head).await.unwrap();
    assert_eq!(counts.holds.load(Ordering::SeqCst), 2);
    let before = counts.metadata_rpcs.load(Ordering::SeqCst);
    let key = m::ObjectKey {
        kind: m::Kind::Commit,
        id: m::ObjectId(selected.0),
    };
    assert!(repo.io.read_metadata(key).await.is_err());
    assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), before);
    let opened = repo
        .query
        .open_scope(
            OpenQueryScope {
                query_identity_version: 1,
                roots: vec![head, independent_head],
                overlay: Vec::new(),
                replay_key: [31; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let scope = opened.value.handle;
    drop(opened);
    let facts = vec![
        native_broker::query_protocol::GraphFact {
            fact: Some(native_broker::query_protocol::graph_fact::Fact::HasId(
                native_broker::query_protocol::HasId {
                    commit: Some(native_broker::query_protocol::CommitId {
                        value: head.0.to_vec()
                    })
                }
            ))
        };
        64
    ];
    let first_facts = repo
        .query
        .facts(scope, facts.clone(), Arc::new(()))
        .await
        .unwrap();
    let second_facts = repo
        .query
        .facts(scope, facts.clone(), Arc::new(()))
        .await
        .unwrap();
    let exhausted = repo
        .query
        .facts(scope, facts.clone(), Arc::new(()))
        .await
        .err()
        .unwrap();
    assert_eq!(
        exhausted.code(),
        tonic::Code::ResourceExhausted,
        "retained facts must consume independent registry bytes, not an inflight slot"
    );
    drop(first_facts);
    let recovered = repo.query.facts(scope, facts, Arc::new(())).await.unwrap();
    assert_eq!(recovered.value.answers.len(), 64);
    drop(recovered);
    drop(second_facts);
    // A retained reply never retains the sole transport slot. Exact replay is
    // stable and requesting a later page can progress while an old reply lives.
    let query = repo
        .query
        .begin(
            native_broker::BeginQuery {
                scope,
                expression: native_broker::query_protocol::GraphExpr {
                    version: 1,
                    expression: Some(
                        native_broker::query_protocol::graph_expr::Expression::Commits(
                            native_broker::query_protocol::CommitSet {
                                commits: vec![
                                    native_broker::query_protocol::CommitId {
                                        value: selected.0.to_vec(),
                                    },
                                    native_broker::query_protocol::CommitId {
                                        value: independent_head.0.to_vec(),
                                    },
                                ],
                            },
                        ),
                    ),
                },
                projection: native_broker::query_protocol::Projection::Canonical,
                page_size: 1,
                replay_key: [32; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let handle = query.value.handle;
    drop(query);
    let first = repo.query.page(handle, 0, Arc::new(())).await.unwrap();
    let replay = repo.query.page(handle, 0, Arc::new(())).await.unwrap();
    assert_eq!(first.value.rows, replay.value.rows);
    drop(replay);
    let second = tokio::time::timeout(
        Duration::from_secs(5),
        repo.query.page(handle, 1, Arc::new(())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(second.value.sequence, 1);
    drop(first);
    drop(second);
    repo.query.close_query(handle, Arc::new(())).await.unwrap();
    let selected_reply = repo
        .query
        .read_commit(scope, selected, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(selected_reply.value.descriptor().key, key);
    drop(selected_reply);
    let independent_reply = repo
        .query
        .read_commit(scope, independent_head, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(independent_reply.value.descriptor(), independent.descriptor);
    drop(independent_reply);
    let next_oldest = ancestry[ancestry.len() - 2];
    let next_reply = repo
        .query
        .read_commit(scope, next_oldest, Arc::new(()))
        .await
        .unwrap();
    drop(next_reply);
    assert_eq!(
        counts.metadata_rpcs.load(Ordering::SeqCst),
        before,
        "selected canonical query must not fall back to ancestor metadata RPCs"
    );
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        2,
        "queries must reuse only original durable holds"
    );
    assert!(
        direct
            .descriptor(historical[0].descriptor.key)
            .await
            .unwrap()
            .is_none()
    );
    // A private overlay can participate in canonical graph queries but must not
    // gain remote fetch provenance, residency or authoritative publication.
    let (_, head_bytes) = store
        .read_metadata(
            "example",
            m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(head.0),
            },
        )
        .unwrap();
    let m::Object::Commit(mut draft) = m::decode(&head_bytes, m::Limits::default()).unwrap() else {
        unreachable!()
    };
    draft.parents = vec![head];
    draft.predecessors.clear();
    draft.change_id = m::ChangeId([77; 16]);
    draft.description = "private query overlay".into();
    let draft = m::encode(&m::Object::Commit(draft), m::Limits::default()).unwrap();
    let private_scope = repo
        .query
        .open_scope(
            OpenQueryScope {
                query_identity_version: 1,
                roots: vec![head, independent_head],
                overlay: vec![draft.bytes.clone()],
                replay_key: [33; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let private = repo
        .query
        .read_commit(
            private_scope.value.handle,
            m::CommitId(draft.descriptor.key.id.0),
            Arc::new(()),
        )
        .await
        .unwrap();
    assert_eq!(private.value.bytes(), draft.bytes);
    drop(private);
    let private_query = repo
        .query
        .begin(
            native_broker::BeginQuery {
                scope: private_scope.value.handle,
                expression: native_broker::query_protocol::GraphExpr {
                    version: 1,
                    expression: Some(
                        native_broker::query_protocol::graph_expr::Expression::Commits(
                            native_broker::query_protocol::CommitSet {
                                commits: vec![native_broker::query_protocol::CommitId {
                                    value: draft.descriptor.key.id.0.to_vec(),
                                }],
                            },
                        ),
                    ),
                },
                projection: native_broker::query_protocol::Projection::Canonical,
                page_size: 1,
                replay_key: [34; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let mut seq = 0;
    loop {
        let page = repo
            .query
            .page(private_query.value.handle, seq, Arc::new(()))
            .await
            .unwrap();
        let done = page.value.exhausted;
        drop(page);
        if done {
            break;
        }
        seq += 1;
    }
    repo.query
        .close_query(private_query.value.handle, Arc::new(()))
        .await
        .unwrap();
    drop(private_query);
    repo.query
        .close_scope(private_scope.value.handle, Arc::new(()))
        .await
        .unwrap();
    drop(private_scope);
    assert!(
        direct
            .descriptor(draft.descriptor.key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(repo.io.read_metadata(draft.descriptor.key).await.is_err());
    assert!(
        store
            .descriptor("example", draft.descriptor.key)
            .unwrap()
            .is_none()
    );
    repo.query.close_scope(scope, Arc::new(())).await.unwrap();
    repo.wait_for_idle().await;
    drop(repo);
    drop(direct);
    let (_direct, repo) = local(directory.path(), cache, configuration.clone(), false);
    // The query cursor is gone, but its exact durable anchor-set witness still
    // authorizes selected commit -> actual tree -> FILE provenance after restart.
    let tree = repo
        .io
        .read_metadata(historical[0].descriptor.key)
        .await
        .unwrap();
    assert_eq!(tree.descriptor(), historical[0].descriptor);
    assert!(
        repo.control.rebuild_file_index().await.unwrap() > 0,
        "rebuild must preserve FILE claims from valid query-set trees"
    );
    let file = first_file(&fixture);
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 1);
    assert_eq!(counts.holds.load(Ordering::SeqCst), 2);
    // Replacing a confirmed hold generation invalidates the old exact union
    // witness even though the immutable head is unchanged. Refresh must requery,
    // not silently relabel the old grant under a replacement anchor.
    let hold_name = format!(
        "_tinytree/hold/v1/{}/{}",
        repo.control
            .client_id()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>(),
        blake3::Hash::from(head.0).to_hex()
    );
    let publisher = tinytree_client::NativeClient::connect(configuration.remote.clone())
        .await
        .unwrap();
    let released = publisher
        .publish(p::PublishReferenceRequest {
            repository_id: "example".into(),
            idempotency_key: "release-query-anchor".into(),
            reference_name: hold_name.clone(),
            expected_generation: 1,
            new_commit: None,
            staging_leases: vec![],
        })
        .await
        .unwrap();
    let released = released.reference.unwrap();
    assert_eq!(released.generation, 2);
    assert!(released.commit.is_none());
    let reacquired = publisher
        .publish(p::PublishReferenceRequest {
            repository_id: "example".into(),
            idempotency_key: "reacquire-query-anchor".into(),
            reference_name: hold_name,
            expected_generation: released.generation,
            new_commit: Some(p::ObjectKey {
                kind: m::Kind::Commit as i32,
                native_id: head.0.to_vec(),
            }),
            staging_leases: vec![],
        })
        .await
        .unwrap();
    assert_eq!(reacquired.reference.unwrap().generation, 3);
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        4,
        "two original holds plus the two intentional retention mutations"
    );
    repo.control.admit_reference("main").await.unwrap();
    repo.control.rebuild_file_index().await.unwrap();
    let before = counts.metadata_rpcs.load(Ordering::SeqCst);
    assert!(
        repo.io
            .read_metadata(historical[1].descriptor.key)
            .await
            .is_err()
    );
    assert_eq!(
        counts.metadata_rpcs.load(Ordering::SeqCst),
        before,
        "stale union witness must not authorize a network fetch"
    );
    let reopened = repo
        .query
        .open_scope(
            OpenQueryScope {
                query_identity_version: 1,
                roots: vec![head, independent_head],
                overlay: Vec::new(),
                replay_key: [35; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let fresh = repo
        .query
        .read_commit(reopened.value.handle, next_oldest, Arc::new(()))
        .await
        .unwrap();
    drop(fresh);
    repo.query
        .close_scope(reopened.value.handle, Arc::new(()))
        .await
        .unwrap();
    drop(reopened);
    assert_eq!(
        repo.io
            .read_metadata(historical[1].descriptor.key)
            .await
            .unwrap()
            .descriptor(),
        historical[1].descriptor
    );
    let unrelated = m::ObjectKey {
        kind: m::Kind::Commit,
        id: m::ObjectId([99; 32]),
    };
    let before = counts.metadata_rpcs.load(Ordering::SeqCst);
    assert!(repo.io.read_metadata(unrelated).await.is_err());
    assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), before);
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        4,
        "refresh and replacement-scope queries must not acquire extra holds"
    );
    server.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_broker_query_then_lazy_object_reads_share_the_captured_remote_adapter() {
    use native_broker::{NativeQueryIo, OpenQueryScope};
    use std::collections::HashMap;
    let (_serverdir, store, fixture, head, historical) = seed_history(4);
    let mut selected = head;
    loop {
        let (_, bytes) = store
            .read_metadata(
                "example",
                m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(selected.0),
                },
            )
            .unwrap();
        let m::Object::Commit(commit) = m::decode(&bytes, m::Limits::default()).unwrap() else {
            unreachable!()
        };
        if commit.parents == vec![m::CommitId::ROOT] {
            break;
        }
        selected = commit.parents[0];
    }
    let indexdir = private_dir();
    let index = tinytree_index::Index::open(indexdir.path(), &Default::default()).unwrap();
    let service = RepositoryService::new(
        store,
        ServiceConfig::default(),
        vec!["example".into()],
        vec![
            Principal::new("test", TOKEN, vec!["example".into()], true)
                .unwrap()
                .with_retention(true),
        ],
    )
    .unwrap()
    .with_query_index(index)
    .unwrap();
    let counts = Counters::new();
    let server = Server::start_service(service, counts.clone(), None).await;
    let directory = private_dir();
    let cache = cache(directory.path());
    let (direct, repo) = local(directory.path(), cache, config(&server), true);
    repo.control.admit_reference("main").await.unwrap();
    let before = counts.metadata_rpcs.load(Ordering::SeqCst);
    let mut broker_config = native_broker::Config::default();
    broker_config.max_inflight = 1;
    let broker = native_broker::BrokerService::new(
        HashMap::from([("example".into(), repo.io.clone() as Arc<dyn NativeObjectIo>)]),
        broker_config.clone(),
    )
    .unwrap()
    .with_queries(HashMap::from([(
        "example".into(),
        repo.query.clone() as Arc<dyn NativeQueryIo>,
    )]))
    .unwrap();
    let socket = directory.path().join("broker.sock");
    let serving = socket.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        native_broker::serve_unix(&serving, broker, async {
            let _ = stopped.await;
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !socket.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let query =
        native_broker::BrokerQueryClient::connect(&socket, "example", broker_config.clone())
            .await
            .unwrap();
    let spool = directory.path().join("broker-spool");
    make_private(&spool);
    let objects = native_broker::BrokerObjectIo::connect(&socket, "example", &spool, broker_config)
        .await
        .unwrap();
    let selected_key = m::ObjectKey {
        kind: m::Kind::Commit,
        id: m::ObjectId(selected.0),
    };
    assert!(objects.read_metadata(selected_key).await.is_err());
    let opened = query
        .open_scope(
            OpenQueryScope {
                query_identity_version: 1,
                roots: vec![head],
                overlay: vec![],
                replay_key: [41; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let scope = opened.value.handle;
    drop(opened);
    let selected_reply = query
        .read_commit(scope, selected, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(selected_reply.value.descriptor().key, selected_key);
    // Holding this decoded query reply must not pin the sole broker call slot.
    let ordinary =
        tokio::time::timeout(Duration::from_secs(5), objects.read_metadata(selected_key))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(ordinary.bytes(), selected_reply.value.bytes());
    drop(ordinary);
    drop(selected_reply);
    assert_eq!(
        counts.metadata_rpcs.load(Ordering::SeqCst),
        before,
        "broker selected metadata must not hydrate intervening public commits"
    );
    assert_eq!(counts.holds.load(Ordering::SeqCst), 1, "no per-result hold");
    assert!(
        direct
            .descriptor(historical[0].descriptor.key)
            .await
            .unwrap()
            .is_none()
    );
    let tree = objects
        .read_metadata(historical[0].descriptor.key)
        .await
        .unwrap();
    assert_eq!(tree.descriptor(), historical[0].descriptor);
    repo.control.rebuild_file_index().await.unwrap();
    let file = first_file(&fixture);
    assert_eq!(
        bytes(&objects, m::FileId(file.descriptor.key.id.0)).await,
        file.bytes
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 1);
    query.close_scope(scope, Arc::new(())).await.unwrap();
    drop(objects);
    drop(query);
    let _ = stop.send(());
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    repo.wait_for_idle().await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_conflict_queries_match_pinned_jj_without_loading_history_or_tree_file_bodies() {
    use jj_lib::{
        object_id::ObjectId as _,
        revset::{
            ResolvedExpression as Jj, ResolvedPredicateExpression as Predicate,
            RevsetFilterPredicate,
        },
    };
    use tinytree_client::{NativeClient, query_protocol as q};
    struct Case {
        name: &'static str,
        parents: &'static [&'static [usize]],
        conflicts: &'static [usize],
        public: usize,
        candidates: &'static [usize],
    }
    let linear: &'static [&'static [usize]] = &[&[0], &[1], &[2], &[3]];
    let cases = [
        Case {
            name: "clean history",
            parents: linear,
            conflicts: &[],
            public: 4,
            candidates: &[4, 3, 2, 1, 0],
        },
        Case {
            name: "single parent conflict",
            parents: linear,
            conflicts: &[2],
            public: 4,
            candidates: &[4, 3, 2, 1, 0],
        },
        Case {
            name: "resolved multiparent merge",
            parents: &[&[0], &[1], &[1], &[2, 3]],
            conflicts: &[2],
            public: 4,
            candidates: &[4, 3, 2, 1, 0],
        },
        Case {
            name: "private conflicted draft",
            parents: linear,
            conflicts: &[3],
            public: 2,
            candidates: &[4, 3, 2, 1, 0],
        },
        Case {
            name: "private clean draft with public conflict",
            parents: linear,
            conflicts: &[2],
            public: 2,
            candidates: &[4, 3, 2, 1, 0],
        },
        Case {
            name: "empty selected set",
            parents: linear,
            conflicts: &[2],
            public: 4,
            candidates: &[],
        },
        Case {
            name: "intrinsic root is clean",
            parents: linear,
            conflicts: &[2],
            public: 4,
            candidates: &[0],
        },
        Case {
            name: "ordered separated conflicts",
            parents: linear,
            conflicts: &[1, 3],
            public: 4,
            candidates: &[4, 3, 2, 1, 0],
        },
    ];
    for case in cases {
        let reference =
            pinned_graph_reference_with_conflicts(case.parents, false, case.conflicts).await;
        let wire = q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::HasConflict(Box::new(
                q::UnaryExpr {
                    input: Some(Box::new(q::GraphExpr {
                        version: 1,
                        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                            commits: case
                                .candidates
                                .iter()
                                .map(|&i| q::CommitId {
                                    value: reference.ids[i].as_bytes().to_vec(),
                                })
                                .collect(),
                        })),
                    })),
                },
            ))),
        };
        let expected = pinned_expression_expected(
            &reference,
            &Jj::FilterWithin {
                candidates: Box::new(Jj::Commits(
                    case.candidates
                        .iter()
                        .map(|&i| reference.ids[i].clone())
                        .collect(),
                )),
                predicate: Predicate::Filter(RevsetFilterPredicate::HasConflict),
            },
        );
        let directory = private_dir();
        let store = Store::open(directory.path(), Default::default()).unwrap();
        store.create_repository("example").unwrap();
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
        let mut cfg = config(&server).remote;
        cfg.limits.max_graph_pages = 10_000;
        let client = NativeClient::connect(cfg).await.unwrap();
        let mut overlay = Vec::new();
        for (descriptor, bytes) in &reference.canonical[case.public..] {
            overlay.push(
                client
                    .verify_metadata(*descriptor, bytes.clone())
                    .await
                    .unwrap(),
            );
        }
        let root = m::CommitId::from_slice(reference.ids[case.public].as_bytes()).unwrap();
        let scope = client
            .open_query_scope_with_overlay(vec![root], overlay)
            .await
            .unwrap();
        let mut query = client.begin_query(&scope, &wire, 1).await.unwrap();
        let mut actual = Vec::new();
        while !query.is_exhausted() {
            let page = client.evaluate_page(&mut query).await.unwrap();
            actual.extend(page.rows.into_iter().map(|r| r.commit));
        }
        assert_eq!(
            actual,
            expected.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            "{}",
            case.name
        );
        client.close_ids_query(&mut query).await.unwrap();
        let mut query = client
            .begin_graph_query(&scope, &wire, 1, true)
            .await
            .unwrap();
        let mut actual = Vec::new();
        while !query.is_exhausted() {
            let page = client.evaluate_graph_page(&mut query).await.unwrap();
            for row in page.rows() {
                assert!(row.canonical.as_ref().is_some_and(
                    |m| matches!(m.object(),m::Object::Commit(c) if c.root_tree.terms.len()>1)
                ));
                actual.push((row.commit, row.edges().to_vec()));
            }
        }
        assert_eq!(actual, expected, "{}", case.name);
        client.close_graph_query(&mut query).await.unwrap();
        client.close_query_scope(&scope).await.unwrap();
        assert_eq!(counts.files.load(Ordering::SeqCst), 0, "{}", case.name);
        assert_eq!(
            counts.metadata_rpcs.load(Ordering::SeqCst),
            0,
            "{}",
            case.name
        );
        assert_eq!(counts.graph_rpcs.load(Ordering::SeqCst), 0, "{}", case.name);
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

/// Observation only: every operation still runs through the actual remote
/// query adapter. In particular, an observed graph ID is not an object grant.
#[derive(Debug)]
struct SelectedQueryObserver {
    inner: Arc<dyn native_broker::NativeQueryIo>,
    reads: std::sync::Mutex<Vec<m::CommitId>>,
}
#[async_trait]
impl native_broker::NativeQueryIo for SelectedQueryObserver {
    fn resources(&self) -> native_broker::QueryResources {
        self.inner.resources()
    }
    async fn open_scope(
        &self,
        input: native_broker::OpenQueryScope,
        owner: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryScope>, Status> {
        self.inner.open_scope(input, owner).await
    }
    async fn fork_scope(
        &self,
        input: native_broker::ForkQueryScope,
        owner: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryScope>, Status> {
        self.inner.fork_scope(input, owner).await
    }
    async fn facts(
        &self,
        scope: [u8; 32],
        facts: Vec<tinytree_client::query_protocol::GraphFact>,
        owner: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<tinytree_client::query_protocol::GraphFacts>, Status>
    {
        self.inner.facts(scope, facts, owner).await
    }
    async fn resolve_commit_prefix(
        &self,
        scope: [u8; 32],
        prefix: String,
        owner: native_broker::QueryOwner,
    ) -> Result<
        native_broker::QueryReply<tinytree_client::query_protocol::CommitPrefixResult>,
        Status,
    > {
        self.inner.resolve_commit_prefix(scope, prefix, owner).await
    }
    async fn resolve_change_prefix(
        &self,
        scope: [u8; 32],
        prefix: String,
        heads: Vec<m::CommitId>,
        owner: native_broker::QueryOwner,
    ) -> Result<
        native_broker::QueryReply<tinytree_client::query_protocol::ChangePrefixResult>,
        Status,
    > {
        self.inner
            .resolve_change_prefix(scope, prefix, heads, owner)
            .await
    }
    async fn shortest_prefix(
        &self,
        scope: [u8; 32],
        target: tinytree_client::query_protocol::shortest_prefix_request::Target,
        heads: Vec<m::CommitId>,
        owner: native_broker::QueryOwner,
    ) -> Result<
        native_broker::QueryReply<tinytree_client::query_protocol::ShortestPrefixResult>,
        Status,
    > {
        self.inner
            .shortest_prefix(scope, target, heads, owner)
            .await
    }
    async fn begin(
        &self,
        input: native_broker::BeginQuery,
        owner: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryHandle>, Status> {
        self.inner.begin(input, owner).await
    }
    async fn page(
        &self,
        query: [u8; 32],
        sequence: u64,
        owner: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryPage>, Status> {
        self.inner.page(query, sequence, owner).await
    }
    async fn read_commit(
        &self,
        scope: [u8; 32],
        commit: m::CommitId,
        owner: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<jj_native_backend::VerifiedMetadata>, Status> {
        let reply = self.inner.read_commit(scope, commit, owner).await?;
        self.reads.lock().unwrap().push(commit);
        Ok(reply)
    }
    async fn close_query(
        &self,
        query: [u8; 32],
        owner: native_broker::QueryOwner,
    ) -> Result<(), Status> {
        self.inner.close_query(query, owner).await
    }
    async fn close_scope(
        &self,
        scope: [u8; 32],
        owner: native_broker::QueryOwner,
    ) -> Result<(), Status> {
        self.inner.close_scope(scope, owner).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selected_file_conflicts_admit_only_needed_deep_bases_through_real_remote_query_scope() {
    use futures::TryStreamExt as _;
    use jj_lib::{
        backend::{ChangeId, CopyId, Tree, TreeValue},
        conflict_labels::ConflictLabels,
        fileset::FilesetExpression,
        index::Index as _,
        merge::Merge,
        merged_tree::MergedTree,
        object_id::ObjectId as _,
        repo::Repo as _,
        repo_path::{RepoPath, RepoPathComponentBuf},
        revset::{
            ResolvedExpression as R, ResolvedPredicateExpression as P, RevsetFilterPredicate as F,
        },
    };
    // Materialized reference is an oracle only. The tested index has a separate
    // empty local object store and actual scoped query/object remote adapters.
    let mut reference = pinned_graph_reference(&[], false).await;
    let mut trees = Vec::new();
    let mut files = Vec::new();
    for text in [
        "one\nmiddle\nthree\n",
        "ONE\nmiddle\nthree\n",
        "one\nmiddle\nTHREE\n",
        "ONE\nmiddle\nTHREE\n",
        "OTHER\nmiddle\nTHREE\n",
    ] {
        let id = reference
            .repo
            .store()
            .backend()
            .write_file(
                RepoPath::root(),
                &mut futures::io::Cursor::new(text.as_bytes()),
            )
            .await
            .unwrap();
        let tree = reference
            .repo
            .store()
            .backend()
            .write_tree(
                RepoPath::root(),
                &Tree::from_sorted_entries(vec![(
                    RepoPathComponentBuf::new("file").unwrap(),
                    TreeValue::File {
                        id: id.clone(),
                        executable: false,
                        copy_id: CopyId::placeholder(),
                    },
                )]),
            )
            .await
            .unwrap();
        files.push((id, text.as_bytes().to_vec()));
        trees.push(tree);
    }
    // 1 is older history that must remain cold; 2 is the deep merge base.
    // Intervening commits 3/4 must stay cold: the selected parents 5/6
    // therefore do not grant the common base just by exposing its ID.
    // 7 has an unresolved tree representation but no diff from merged parents;
    // 8 has the same parents and a real diff; 9 is a clean public head.
    let shapes: &[(usize, &[usize], bool)] = &[
        (0, &[0], false),
        (0, &[1], false),
        (0, &[2], false),
        (0, &[2], false),
        (1, &[3], false),
        (2, &[4], false),
        (3, &[5, 6], true),
        (4, &[5, 6], true),
        (3, &[7, 8], false),
    ];
    let mut transaction = reference.repo.start_transaction();
    for (position, (tree, parents, conflict)) in shapes.iter().enumerate() {
        let terms = if *conflict {
            Merge::from_vec(vec![
                trees[*tree].clone(),
                reference.repo.store().empty_tree_id().clone(),
                reference.repo.store().empty_tree_id().clone(),
            ])
        } else {
            Merge::resolved(trees[*tree].clone())
        };
        let selected = MergedTree::new(
            reference.repo.store().clone(),
            terms,
            if *conflict {
                ConflictLabels::from_vec(vec!["left".into(), "base".into(), "right".into()])
            } else {
                ConflictLabels::unlabeled()
            },
        );
        let commit = transaction
            .repo_mut()
            .new_commit(
                parents.iter().map(|n| reference.ids[*n].clone()).collect(),
                selected,
            )
            .set_change_id(ChangeId::from_bytes(&[80 + position as u8; 16]))
            .set_description(format!("selected merge fixture {}", position + 1))
            .write()
            .await
            .unwrap();
        reference.ids.push(commit.id().clone());
    }
    reference.repo = transaction.commit("selected merge fixture").await.unwrap();
    let commit_key = |position: usize| m::ObjectKey {
        kind: m::Kind::Commit,
        id: m::ObjectId::from_slice(reference.ids[position].as_bytes()).unwrap(),
    };
    let remote_directory = private_dir();
    let remote_store = Store::open(remote_directory.path(), Default::default()).unwrap();
    remote_store.create_repository("example").unwrap();
    let index_directory = private_dir();
    let index = tinytree_index::Index::open(index_directory.path(), &Default::default()).unwrap();
    for position in 1..reference.ids.len() {
        let metadata = reference
            ._io
            .read_metadata(commit_key(position))
            .await
            .unwrap();
        remote_store
            .put("example", metadata.descriptor(), metadata.bytes(), 0)
            .unwrap();
        index
            .ingest("example", metadata.descriptor(), metadata.bytes())
            .unwrap();
    }
    for tree in &trees {
        let value = reference
            ._io
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId::from_slice(tree.as_bytes()).unwrap(),
            })
            .await
            .unwrap();
        remote_store
            .put("example", value.descriptor(), value.bytes(), 0)
            .unwrap();
    }
    for (id, bytes) in &files {
        let descriptor = m::describe_file(bytes);
        assert_eq!(descriptor.key.id.0.as_slice(), id.as_bytes());
        remote_store.put("example", descriptor, bytes, 0).unwrap();
    }
    let head = m::CommitId(commit_key(9).id.0);
    remote_store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "selected-head".into(),
                reference_name: "main".into(),
                expected_generation: 0,
                new_commit: Some(head),
                staging_leases: vec![],
            },
            1,
        )
        .unwrap();
    let counts = Counters::new();
    let service = RepositoryService::new(
        remote_store,
        ServiceConfig::default(),
        vec!["example".into()],
        vec![
            Principal::new("test", TOKEN, vec!["example".into()], true)
                .unwrap()
                .with_retention(true),
        ],
    )
    .unwrap()
    .with_query_index(index)
    .unwrap();
    let server = Server::start_service(service, counts.clone(), None).await;
    let local_directory = private_dir();
    let (direct, remote) = local(
        local_directory.path(),
        cache(local_directory.path()),
        config(&server),
        true,
    );
    remote.control.admit_reference("main").await.unwrap();
    assert!(direct.descriptor(commit_key(2)).await.unwrap().is_none());
    assert!(direct.descriptor(commit_key(1)).await.unwrap().is_none());
    assert!(
        remote.io.read_metadata(commit_key(2)).await.is_err(),
        "an ID alone cannot fetch the deep base"
    );
    let initial_metadata = counts.metadata_keys.load(Ordering::SeqCst);
    let initial_holds = counts.holds.load(Ordering::SeqCst);
    let observed = Arc::new(SelectedQueryObserver {
        inner: remote.query.clone(),
        reads: std::sync::Mutex::new(vec![]),
    });
    let native_index = jj_native_index::NativeIndexStore::init(
        &local_directory.path().join("index"),
        jj_native_index::Config {
            endpoint: jj_native_index::Endpoint::UnixSocket(
                local_directory.path().join("unused-fixture.sock"),
            ),
            store_alias: "example".into(),
            provider_fingerprint: [8; 32],
        },
        Default::default(),
        observed.clone(),
        remote.io.clone(),
        vec![head],
    )
    .unwrap();
    let readonly = native_index.seed_index().unwrap();
    let backend = jj_native_backend::NativeBackend::from_io(remote.io.clone(), Default::default())
        .await
        .unwrap();
    let jj_store = jj_lib::store::Store::new(
        Box::new(backend),
        jj_lib::signing::Signer::new(None, vec![]),
        jj_lib::tree_merge::MergeOptions {
            hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
            same_change: jj_lib::merge::SameChange::Accept,
        },
    );
    for (name, candidates, expected_positions) in [
        (
            "mixed ancestor history",
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
            vec![8],
        ),
        ("auto-merged empty conflict", vec![7], vec![]),
    ] {
        let expression = R::FilterWithin {
            candidates: Box::new(R::Commits(
                candidates
                    .iter()
                    .map(|n| reference.ids[*n].clone())
                    .collect(),
            )),
            predicate: P::Intersection(
                Box::new(P::Filter(F::HasConflict)),
                Box::new(P::Filter(F::File(FilesetExpression::all()))),
            ),
        };
        let expected = reference
            .repo
            .index()
            .evaluate_revset(&expression, reference.repo.store())
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            expected,
            expected_positions
                .iter()
                .map(|n| reference.ids[*n].clone())
                .collect::<Vec<_>>(),
            "{name}"
        );
        let actual = readonly
            .evaluate_revset(&expression, &jj_store)
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(actual, expected, "{name}");
    }
    assert!(
        observed
            .reads
            .lock()
            .unwrap()
            .contains(&m::CommitId(commit_key(2).id.0)),
        "selected deep bases must be canonically admitted under the original scope"
    );
    assert!(direct.descriptor(commit_key(2)).await.unwrap().is_some());
    assert!(
        direct.descriptor(commit_key(1)).await.unwrap().is_none(),
        "unneeded older history must stay cold"
    );
    for position in [3, 4] {
        assert!(
            direct
                .descriptor(commit_key(position))
                .await
                .unwrap()
                .is_none(),
            "intervening parent history must stay cold"
        );
        assert!(
            !observed
                .reads
                .lock()
                .unwrap()
                .contains(&m::CommitId(commit_key(position).id.0))
        );
    }
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        initial_holds,
        "selected bases reuse the captured rootset, not per-base holds"
    );
    assert!(
        counts.metadata_requests.lock().unwrap()[initial_metadata..]
            .iter()
            .all(|key| key.kind != m::Kind::Commit as i32),
        "selected canonical admission must not hydrate intervening history through BatchReadObjects"
    );
    assert_eq!(counts.graph_rpcs.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_v2_retires_only_acknowledged_queries_after_scope_close_and_keeps_active_replays() {
    use native_broker::{
        BeginQuery, ForkQueryScope, NativeQueryIo, OpenQueryScope, query_protocol as q,
    };
    let (_serverdir, store, _fixture, head, _historical) = seed_history(4);
    let indexdir = private_dir();
    let index = tinytree_index::Index::open(indexdir.path(), &Default::default()).unwrap();
    let service = RepositoryService::new(
        store,
        ServiceConfig::default(),
        vec!["example".into()],
        vec![
            Principal::new("test", TOKEN, vec!["example".into()], true)
                .unwrap()
                .with_retention(true),
        ],
    )
    .unwrap()
    .with_query_index(index)
    .unwrap();
    let counts = Counters::new();
    let server = Server::start_service(service, counts.clone(), None).await;
    let directory = private_dir();
    let mut configuration = config(&server);
    configuration.query.max_queries = 4;
    configuration.query.max_replays = 8;
    let (_direct, repo) = local(
        directory.path(),
        cache(directory.path()),
        configuration,
        true,
    );
    repo.control.admit_reference("main").await.unwrap();
    let opened = repo
        .query
        .open_scope(
            OpenQueryScope {
                roots: vec![head],
                overlay: vec![],
                replay_key: [91; 32],
                query_identity_version: 2,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    assert_eq!(opened.value.query_identity_version, 2);
    let scope = opened.value.clone();
    let fork = ForkQueryScope {
        expected_cloud_source_digest: None,
        parent: scope.handle,
        overlay: vec![],
        replay_key: [92; 32],
    };
    let query_calls = counts.query_rpcs.load(Ordering::SeqCst);
    let mut unsupported = fork.clone();
    unsupported.expected_cloud_source_digest = Some([77; 32]);
    assert_eq!(
        repo.query
            .fork_scope(unsupported, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented
    );
    assert_eq!(
        counts.query_rpcs.load(Ordering::SeqCst),
        query_calls,
        "unsupported cloud fork must reject before any upstream RPC"
    );
    // The same replay key must still accept its ordinary intent; no reservation
    // or failed cloud replay may be inserted before the explicit support gate.
    let child = repo
        .query
        .fork_scope(fork.clone(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(child.value.query_identity_version, 2);
    let expression = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
            commits: vec![
                q::CommitId {
                    value: head.0.to_vec(),
                },
                q::CommitId { value: vec![0; 32] },
            ],
        })),
    };
    let request = |nonce: u8| BeginQuery {
        scope: scope.handle,
        expression: expression.clone(),
        projection: q::Projection::Ids,
        page_size: 1,
        replay_key: scope.bound_query_id([nonce; 16]).unwrap(),
    };
    let closed_request = request(1);
    let closed = repo
        .query
        .begin(closed_request.clone(), Arc::new(()))
        .await
        .unwrap()
        .value;
    let live_request = request(2);
    let live = repo
        .query
        .begin(live_request.clone(), Arc::new(()))
        .await
        .unwrap()
        .value;
    // Establish both upstream evaluators, then retain the exact delivered page.
    let _ = repo
        .query
        .page(closed.handle, 0, Arc::new(()))
        .await
        .unwrap();
    let delivered = repo.query.page(live.handle, 0, Arc::new(())).await.unwrap();
    let eof_request = BeginQuery {
        scope: scope.handle,
        expression: q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: vec![q::CommitId {
                    value: head.0.to_vec(),
                }],
            })),
        },
        projection: q::Projection::Ids,
        page_size: 1,
        replay_key: scope.bound_query_id([3; 16]).unwrap(),
    };
    let eof = repo
        .query
        .begin(eof_request.clone(), Arc::new(()))
        .await
        .unwrap()
        .value;
    let mut sequence = 0;
    let last = loop {
        let page = repo
            .query
            .page(eof.handle, sequence, Arc::new(()))
            .await
            .unwrap();
        if page.value.exhausted {
            break page;
        }
        sequence += 1;
    };
    // Either serialization order is valid: only an acknowledged close is retired.
    let (query_close, scope_close) = tokio::join!(
        repo.query.close_query(closed.handle, Arc::new(())),
        repo.query.close_scope(scope.handle, Arc::new(()))
    );
    query_close.unwrap();
    scope_close.unwrap();
    assert_eq!(
        repo.query
            .begin(closed_request.clone(), Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let repeated = repo
        .query
        .begin(live_request.clone(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(repeated.value.handle, live.handle);
    let replay = repo.query.page(live.handle, 0, Arc::new(())).await.unwrap();
    assert_eq!(replay.value.rows, delivered.value.rows);
    assert_eq!(replay.value.exhausted, delivered.value.exhausted);
    let replay_eof = repo
        .query
        .page(eof.handle, sequence, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(replay_eof.value.rows, last.value.rows);
    assert!(replay_eof.value.exhausted);
    // Continue the established live evaluator after public scope closure.
    let mut next = 1;
    if !delivered.value.exhausted {
        loop {
            let page = repo
                .query
                .page(live.handle, next, Arc::new(()))
                .await
                .unwrap();
            if page.value.exhausted {
                break;
            }
            next += 1;
        }
    }
    repo.query
        .close_query(live.handle, Arc::new(()))
        .await
        .unwrap();
    repo.query
        .close_query(eof.handle, Arc::new(()))
        .await
        .unwrap();
    repo.query
        .close_query(live.handle, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(
        repo.query
            .begin(live_request, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let fork_replay = repo.query.fork_scope(fork, Arc::new(())).await.unwrap();
    assert_eq!(
        fork_replay.value, child.value,
        "lost Fork ACK remains replayable after parent close"
    );
    let new_scope = repo
        .query
        .open_scope(
            OpenQueryScope {
                roots: vec![head],
                overlay: vec![],
                replay_key: [93; 32],
                query_identity_version: 2,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let rebound = BeginQuery {
        scope: new_scope.value.handle,
        ..closed_request
    };
    assert_eq!(
        repo.query
            .begin(rebound, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument,
        "retired identity cannot rebind under a different exact scope"
    );
    let mut malformed = BeginQuery {
        scope: new_scope.value.handle,
        expression,
        projection: q::Projection::Ids,
        page_size: 1,
        replay_key: new_scope.value.bound_query_id([4; 16]).unwrap(),
    };
    malformed.replay_key[31] ^= 1;
    assert_eq!(
        repo.query
            .begin(malformed, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    // Keeping closed-query tombstones would exhaust the unchanged eight-entry
    // local replay budget here. Creation/scope replays remain retained.
    async fn complete_literal(
        query: &dyn NativeQueryIo,
        scope: &native_broker::QueryScope,
        head: m::CommitId,
        nonce: u8,
    ) {
        let query_handle = query
            .begin(
                BeginQuery {
                    scope: scope.handle,
                    expression: q::GraphExpr {
                        version: 1,
                        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                            commits: vec![q::CommitId {
                                value: head.0.to_vec(),
                            }],
                        })),
                    },
                    projection: q::Projection::Ids,
                    page_size: 1,
                    replay_key: scope.bound_query_id([nonce; 16]).unwrap(),
                },
                Arc::new(()),
            )
            .await
            .unwrap()
            .value;
        let mut sequence = 0;
        loop {
            let page = query
                .page(query_handle.handle, sequence, Arc::new(()))
                .await
                .unwrap();
            if page.value.exhausted {
                break;
            }
            sequence += 1;
        }
        query
            .close_query(query_handle.handle, Arc::new(()))
            .await
            .unwrap();
    }
    for nonce in [5, 6, 7] {
        complete_literal(repo.query.as_ref(), &new_scope.value, head, nonce).await;
    }
    let unstarted = repo
        .query
        .begin(
            BeginQuery {
                scope: new_scope.value.handle,
                expression: q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                        commits: vec![q::CommitId {
                            value: head.0.to_vec(),
                        }],
                    })),
                },
                projection: q::Projection::Ids,
                page_size: 1,
                replay_key: new_scope.value.bound_query_id([8; 16]).unwrap(),
            },
            Arc::new(()),
        )
        .await
        .unwrap()
        .value;
    repo.query
        .close_scope(new_scope.value.handle, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(
        repo.query
            .page(unstarted.handle, 0, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition,
        "a locally prepared but unpaged query has no upstream evaluator to preserve"
    );
    repo.query
        .close_query(unstarted.handle, Arc::new(()))
        .await
        .unwrap();
    for scope_key in [94, 95] {
        let current = repo
            .query
            .open_scope(
                OpenQueryScope {
                    roots: vec![head],
                    overlay: vec![],
                    replay_key: [scope_key; 32],
                    query_identity_version: 2,
                },
                Arc::new(()),
            )
            .await
            .unwrap();
        for nonce in [5, 6, 7] {
            complete_literal(repo.query.as_ref(), &current.value, head, nonce).await;
        }
        repo.query
            .close_scope(current.value.handle, Arc::new(()))
            .await
            .unwrap();
    }
    repo.query
        .close_scope(child.value.handle, Arc::new(()))
        .await
        .unwrap();
    repo.query
        .close_scope(new_scope.value.handle, Arc::new(()))
        .await
        .unwrap();
    // V1 keeps its original tombstone/rebinding rules; no early retirement.
    let legacy_directory = private_dir();
    let (_legacy_direct, legacy) = local(
        legacy_directory.path(),
        cache(legacy_directory.path()),
        config(&server),
        true,
    );
    legacy.control.admit_reference("main").await.unwrap();
    let first = legacy
        .query
        .open_scope(
            OpenQueryScope {
                roots: vec![head],
                overlay: vec![],
                replay_key: [101; 32],
                query_identity_version: 1,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    assert_eq!(first.value.query_identity_version, 1);
    let legacy_request = BeginQuery {
        scope: first.value.handle,
        expression: q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: vec![q::CommitId {
                    value: head.0.to_vec(),
                }],
            })),
        },
        projection: q::Projection::Ids,
        page_size: 1,
        replay_key: [102; 32],
    };
    let legacy_query = legacy
        .query
        .begin(legacy_request.clone(), Arc::new(()))
        .await
        .unwrap();
    let _ = legacy
        .query
        .page(legacy_query.value.handle, 0, Arc::new(()))
        .await
        .unwrap();
    legacy
        .query
        .close_query(legacy_query.value.handle, Arc::new(()))
        .await
        .unwrap();
    legacy
        .query
        .close_scope(first.value.handle, Arc::new(()))
        .await
        .unwrap();
    let second = legacy
        .query
        .open_scope(
            OpenQueryScope {
                roots: vec![head],
                overlay: vec![],
                replay_key: [103; 32],
                query_identity_version: 1,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let rebound = BeginQuery {
        scope: second.value.handle,
        ..legacy_request
    };
    assert_eq!(
        legacy
            .query
            .begin(rebound, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists,
        "legacy creation identity remains bound to its original intent"
    );
    legacy
        .query
        .close_scope(second.value.handle, Arc::new(()))
        .await
        .unwrap();
    server.stop().await;
}

// Use the public Repo surface so IdPrefixContext performs its actual policy,
// rather than testing the optional provider in isolation.
struct PrefixContextRepo {
    base: Arc<jj_lib::repo::ReadonlyRepo>,
    native: Option<jj_native_index::NativeReadonlyIndex>,
    view: jj_lib::view::View,
}
#[async_trait(?Send)]
impl jj_lib::repo::Repo for PrefixContextRepo {
    fn base_repo(&self) -> &jj_lib::repo::ReadonlyRepo {
        &self.base
    }
    fn store(&self) -> &Arc<jj_lib::store::Store> {
        self.base.store()
    }
    fn op_store(&self) -> &Arc<dyn jj_lib::op_store::OpStore> {
        self.base.op_store()
    }
    fn index(&self) -> &dyn jj_lib::index::Index {
        use jj_lib::index::ReadonlyIndex;
        match &self.native {
            Some(i) => i.as_index(),
            None => self.base.index(),
        }
    }
    fn view(&self) -> &jj_lib::view::View {
        &self.view
    }
    fn submodule_store(&self) -> &Arc<dyn jj_lib::submodule_store::SubmoduleStore> {
        self.base.submodule_store()
    }
    async fn resolve_change_id_prefix(
        &self,
        prefix: &jj_lib::object_id::HexPrefix,
    ) -> jj_lib::index::IndexResult<
        jj_lib::object_id::PrefixResolution<jj_lib::index::ResolvedChangeTargets>,
    > {
        use jj_lib::index::ReadonlyIndex;
        match &self.native {
            Some(i) => {
                i.change_id_index(&mut self.view.heads().iter())
                    .unwrap()
                    .resolve_prefix(prefix)
                    .await
            }
            None => self.base.resolve_change_id_prefix(prefix).await,
        }
    }
    async fn shortest_unique_change_id_prefix_len(
        &self,
        id: &jj_lib::backend::ChangeId,
    ) -> jj_lib::index::IndexResult<usize> {
        use jj_lib::index::ReadonlyIndex;
        match &self.native {
            Some(i) => {
                i.change_id_index(&mut self.view.heads().iter())
                    .unwrap()
                    .shortest_unique_prefix_len(id)
                    .await
            }
            None => self.base.shortest_unique_change_id_prefix_len(id).await,
        }
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_prefix_context_matches_pinned_default_through_broker_on_current_thread() {
    use jj_lib::{
        object_id::{HexPrefix, ObjectId as _},
        repo::Repo,
    };

    // ResolvedChangeTargets explicitly leaves ordering to the index. Compare
    // sorted vectors (not sets) so multiplicity and visibility remain checked;
    // separately check native stability because offsets use its own ordering.
    fn comparable_changes(
        value: jj_lib::object_id::PrefixResolution<jj_lib::index::ResolvedChangeTargets>,
    ) -> jj_lib::object_id::PrefixResolution<Vec<(jj_lib::backend::CommitId, bool)>> {
        value.map(|targets| {
            let mut values = targets
                .targets
                .into_iter()
                .map(|(id, state)| (id, state == jj_lib::index::ResolvedChangeState::Visible))
                .collect::<Vec<_>>();
            values.sort();
            values
        })
    }
    let mut reference = pinned_graph_reference(&[&[0], &[1], &[1], &[2, 3]], true).await;
    // Workspace initialization also creates an initial working-copy commit.
    // Include every DefaultIndex head so global fallback sees identical logical
    // universes, rather than accidentally comparing only our authored fixture.
    let roots = reference
        .repo
        .index()
        .all_heads_for_gc()
        .unwrap()
        .collect::<Vec<_>>();
    for root in &roots {
        if !reference.ids.contains(root) {
            let metadata = reference
                ._io
                .read_metadata(m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId::from_slice(root.as_bytes()).unwrap(),
                })
                .await
                .unwrap();
            let m::Object::Commit(commit) =
                m::decode(metadata.bytes(), m::Limits::default()).unwrap()
            else {
                panic!("fixture head is not a commit")
            };
            assert_eq!(
                commit.parents,
                vec![m::CommitId::ROOT],
                "unexpected initial-workspace fixture ancestry"
            );
            reference.ids.push(root.clone());
            reference
                .canonical
                .push((metadata.descriptor(), metadata.bytes().to_vec()));
        }
    }
    let server_dir = private_dir();
    let store = Store::open(server_dir.path(), Default::default()).unwrap();
    store.create_repository("example").unwrap();
    let empty = m::empty_tree();
    store
        .put("example", empty.descriptor, &empty.bytes, 0)
        .unwrap();
    let index_dir = private_dir();
    let index = tinytree_index::Index::open(index_dir.path(), &Default::default()).unwrap();
    for (descriptor, bytes) in &reference.canonical {
        store.put("example", *descriptor, bytes, 0).unwrap();
        index.ingest("example", *descriptor, bytes).unwrap();
    }
    let head = m::CommitId::from_slice(reference.ids[4].as_bytes()).unwrap();
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "prefix-head".into(),
                reference_name: "main".into(),
                expected_generation: 0,
                new_commit: Some(head),
                staging_leases: vec![],
            },
            1,
        )
        .unwrap();
    for (position, root) in roots
        .iter()
        .filter(|id| id.as_bytes() != head.0.as_slice())
        .enumerate()
    {
        store
            .publish(
                "example",
                &tinytree_store::PublishRequest {
                    idempotency_key: format!("prefix-extra-{position}"),
                    reference_name: format!("extra-{position}"),
                    expected_generation: 0,
                    new_commit: Some(m::CommitId::from_slice(root.as_bytes()).unwrap()),
                    staging_leases: vec![],
                },
                1,
            )
            .unwrap();
    }
    let service = RepositoryService::new(
        store,
        ServiceConfig::default(),
        vec!["example".into()],
        vec![
            Principal::new("test", TOKEN, vec!["example".into()], true)
                .unwrap()
                .with_retention(true),
        ],
    )
    .unwrap()
    .with_query_index(index)
    .unwrap();
    let counts = Counters::new();
    let server = Server::start_service(service, counts.clone(), None).await;
    let directory = private_dir();
    let (_direct, remote) = local(
        directory.path(),
        cache(directory.path()),
        config(&server),
        true,
    );
    remote.control.admit_reference("main").await.unwrap();
    for (position, _) in roots
        .iter()
        .filter(|id| id.as_bytes() != head.0.as_slice())
        .enumerate()
    {
        remote
            .control
            .admit_reference(&format!("extra-{position}"))
            .await
            .unwrap();
    }
    let mut broker_config = native_broker::Config::default();
    broker_config.max_inflight = 1;
    let broker = native_broker::BrokerService::new(
        std::collections::HashMap::from([(
            "example".into(),
            remote.io.clone() as Arc<dyn NativeObjectIo>,
        )]),
        broker_config.clone(),
    )
    .unwrap()
    .with_queries(std::collections::HashMap::from([(
        "example".into(),
        remote.query.clone() as Arc<dyn native_broker::NativeQueryIo>,
    )]))
    .unwrap();
    let socket = directory.path().join("prefix.sock");
    let serving = socket.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        native_broker::serve_unix(&serving, broker, async {
            let _ = stopped.await;
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !socket.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let query = Arc::new(
        native_broker::BrokerQueryClient::connect(&socket, "example", broker_config)
            .await
            .unwrap(),
    );
    let native_store = jj_native_index::NativeIndexStore::init(
        &directory.path().join("index"),
        jj_native_index::Config {
            endpoint: jj_native_index::Endpoint::UnixSocket(socket),
            store_alias: "example".into(),
            provider_fingerprint: [8; 32],
        },
        Default::default(),
        query.clone(),
        remote.io.clone(),
        roots
            .iter()
            .map(|id| m::CommitId::from_slice(id.as_bytes()).unwrap())
            .collect(),
    )
    .unwrap();
    let readonly = native_store.seed_index().unwrap();
    let empty_store = jj_native_index::NativeIndexStore::init(
        &directory.path().join("empty-index"),
        jj_native_index::Config {
            endpoint: jj_native_index::Endpoint::UnixSocket(directory.path().join("prefix.sock")),
            store_alias: "example".into(),
            provider_fingerprint: [8; 32],
        },
        Default::default(),
        query.clone(),
        remote.io.clone(),
        vec![],
    )
    .unwrap();
    let empty_index = empty_store.seed_index().unwrap();
    let baseline_metadata = counts.metadata_rpcs.load(Ordering::SeqCst);
    let ids = reference.ids.clone();
    let base = reference.repo.clone();
    // Synchronous IdPrefixContext methods run inside an actual current-thread
    // runtime. The broker transport, not that blocked caller, drives its I/O.
    tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async move {
                let mut view = base.view().clone();
                view.set_local_bookmark_target(
                    ids[2].to_string()[..1].as_ref(),
                    jj_lib::op_store::RefTarget::normal(ids[1].clone()),
                );
                let default = PrefixContextRepo {
                    base: base.clone(),
                    native: None,
                    view: view.clone(),
                };
                let native = PrefixContextRepo {
                    base: base.clone(),
                    native: Some(readonly),
                    view,
                };
                use jj_lib::{
                    fileset::FilesetExpression,
                    revset::{
                        ResolvedExpression as R, ResolvedPredicateExpression as P,
                        RevsetFilterPredicate as F,
                    },
                };
                let selected_files = R::FilterWithin {
                    candidates: Box::new(R::Commits(ids.clone())),
                    predicate: P::Intersection(
                        Box::new(P::Filter(F::HasConflict)),
                        Box::new(P::Filter(F::File(FilesetExpression::all()))),
                    ),
                };
                let selected_revset = native
                    .index()
                    .evaluate_revset(&selected_files, native.store())
                    .unwrap();
                assert!(
                    selected_revset
                        .prefix_disambiguation_index()
                        .unwrap()
                        .is_none(),
                    "selected file work cannot be delegated as a graph-only set"
                );
                drop(selected_revset);
                for (selection_name, selected) in [
                    ("singleton", vec![ids[2].clone()]),
                    ("divergent change", vec![ids[2].clone(), ids[3].clone()]),
                    (
                        "root and ancestry",
                        vec![ids[0].clone(), ids[2].clone(), ids[4].clone()],
                    ),
                ] {
                    let expression = jj_lib::revset::UserRevsetExpression::commits(selected);
                    let expected_context = jj_lib::id_prefix::IdPrefixContext::new(Arc::new(
                        jj_lib::revset::RevsetExtensions::default(),
                    ))
                    .disambiguate_within(expression.clone());
                    let actual_context = jj_lib::id_prefix::IdPrefixContext::new(Arc::new(
                        jj_lib::revset::RevsetExtensions::default(),
                    ))
                    .disambiguate_within(expression);
                    let expected = expected_context.populate(&default).unwrap();
                    let actual = actual_context.populate(&native).unwrap();
                    for id in &ids {
                        for prefix in [
                            HexPrefix::try_from_hex("").unwrap(),
                            HexPrefix::from_id(id),
                            HexPrefix::try_from_hex(id.to_string()[..1].to_owned()).unwrap(),
                        ] {
                            assert_eq!(
                                actual.resolve_commit_prefix(&native, &prefix).unwrap(),
                                expected.resolve_commit_prefix(&default, &prefix).unwrap(),
                                "selection={selection_name} commit prefix={prefix:?} target={id}"
                            );
                        }
                        assert_eq!(
                            actual.shortest_commit_prefix_len(&native, id).unwrap(),
                            expected.shortest_commit_prefix_len(&default, id).unwrap(),
                            "selection={selection_name} shortest commit target={id}"
                        );
                        let change = base
                            .store()
                            .get_commit_async(id)
                            .await
                            .unwrap()
                            .change_id()
                            .clone();
                        assert_eq!(
                            actual
                                .resolve_change_prefix(
                                    &native,
                                    &HexPrefix::try_from_hex("").unwrap()
                                )
                                .await
                                .unwrap(),
                            expected
                                .resolve_change_prefix(
                                    &default,
                                    &HexPrefix::try_from_hex("").unwrap()
                                )
                                .await
                                .unwrap()
                        );
                        let actual_changes = actual
                            .resolve_change_prefix(&native, &HexPrefix::from_id(&change))
                            .await
                            .unwrap();
                        let expected_changes = expected
                            .resolve_change_prefix(&default, &HexPrefix::from_id(&change))
                            .await
                            .unwrap();
                        assert_eq!(
                            actual
                                .resolve_change_prefix(&native, &HexPrefix::from_id(&change))
                                .await
                                .unwrap(),
                            actual_changes,
                            "native target offsets must remain stable within an immutable context"
                        );
                        assert_eq!(
                            comparable_changes(actual_changes),
                            comparable_changes(expected_changes),
                            "selection={selection_name} change target={change}"
                        );
                        assert_eq!(
                            actual
                                .shortest_change_prefix_len(&native, &change)
                                .await
                                .unwrap(),
                            expected
                                .shortest_change_prefix_len(&default, &change)
                                .await
                                .unwrap(),
                            "selection={selection_name} shortest change target={change}"
                        );
                    }
                    let mut current_view = base.view().clone();
                    for head in base.view().heads() {
                        current_view.remove_head(head);
                    }
                    current_view.add_head(&ids[0]);
                    let current = PrefixContextRepo {
                        base: base.clone(),
                        native: Some(empty_index.clone()),
                        view: current_view,
                    };
                    assert_eq!(
                        actual
                            .resolve_commit_prefix(&current, &HexPrefix::from_id(&ids[2]))
                            .unwrap(),
                        expected
                            .resolve_commit_prefix(&current, &HexPrefix::from_id(&ids[2]))
                            .unwrap(),
                        "captured narrow result must respect current-index membership"
                    );
                    let change = base
                        .store()
                        .get_commit_async(&ids[2])
                        .await
                        .unwrap()
                        .change_id()
                        .clone();
                    assert_eq!(
                        actual
                            .resolve_change_prefix(&current, &HexPrefix::from_id(&change))
                            .await
                            .unwrap(),
                        expected
                            .resolve_change_prefix(&current, &HexPrefix::from_id(&change))
                            .await
                            .unwrap(),
                        "current view supplies change targets"
                    );
                }
            });
    })
    .await
    .unwrap();
    assert_eq!(
        counts.metadata_rpcs.load(Ordering::SeqCst),
        baseline_metadata,
        "prefix contexts must not hydrate canonical public history"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    let _ = stop.send(());
    task.await.unwrap().unwrap();
    server.stop().await;
}

#[test]
fn owned_remote_publication_keeps_command_guard_after_deadline_and_cancelled_sdk_verification() {
    use native_broker::NativeRepositoryControl as _;
    use std::sync::{Condvar, Mutex};
    struct ReleaseOnDrop(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            let (lock, changed) = &*self.0;
            *lock.lock().unwrap_or_else(|error| error.into_inner()) = true;
            changed.notify_all();
        }
    }
    // Queue the real SDK verification worker, not a mock trait operation. The
    // upstream missing-object job has finished before occupying this one slot.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let server_directory = private_dir();
        let store = Store::open(server_directory.path(), Default::default()).unwrap();
        store.create_repository("example").unwrap();
        let counts = Counters::new();
        counts.missing_paused.store(true, Ordering::SeqCst);
        let server = Server::start(store, counts.clone()).await;
        let directory = private_dir();
        let mut cfg = config(&server);
        cfg.remote.rpc.operation_timeout_ms = 1000;
        cfg.remote.rpc.rpc_timeout_ms = 1000;
        cfg.remote.rpc.retry.max_attempts = 1;
        let (_direct, repo) = local(directory.path(), cache(directory.path()), cfg, true);
        let mut commit = m::synthetic_root();
        commit.parents = vec![m::CommitId::ROOT];
        commit.change_id = m::ChangeId([77; 16]);
        commit.description = "queued SDK canonical verification".into();
        let descriptor = repo
            .io
            .write_metadata(m::Object::Commit(commit))
            .await
            .unwrap();
        let head = m::CommitId(descriptor.key.id.0);
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let owner = Arc::new(permits.clone().acquire_owned().await.unwrap());
        let weak = Arc::downgrade(&owner);
        let control = repo.control.clone();
        let started = tokio::time::Instant::now();
        let task = tokio::spawn(async move {
            control
                .publish_with_owner(
                    native_broker::PublishRequest {
                        name: "main".into(),
                        head,
                        expected_generation: 0,
                        idempotency_key: "owned-publication".into(),
                    },
                    owner,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), counts.missing_started.notified())
            .await
            .unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let release = ReleaseOnDrop(gate.clone());
        let worker_gate = gate.clone();
        let (entered, accepted) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = entered.send(());
            let (lock, changed) = &*worker_gate;
            let mut done = lock.lock().unwrap();
            while !*done {
                done = changed.wait(done).unwrap();
            }
        });
        accepted.await.unwrap();
        counts.missing_paused.store(false, Ordering::SeqCst);
        // The operation deadline has now passed while verification remains
        // physically queued. Draining must not return or release its owner.
        tokio::time::sleep_until(started + Duration::from_millis(1100)).await;
        assert!(
            !task.is_finished(),
            "owned control must drain the accepted SDK worker after timeout"
        );
        assert!(weak.upgrade().is_some());
        assert!(permits.clone().try_acquire_owned().is_err());
        task.abort();
        let _ = task.await;
        assert!(
            weak.upgrade().is_some(),
            "cancelling the drain waiter cannot drop the queued worker's guard"
        );
        drop(release);
        blocker.await.unwrap();
        // Weak::upgrade(None) only observes the strong count reaching zero,
        // not completion of the final owner's destructor. The actual next
        // control operation is the public physical-drain completion barrier.
        let next = repo
            .control
            .observe_reference_with_owner("absent", Arc::new(()))
            .await
            .unwrap();
        assert!(
            next.head.is_none(),
            "actual codec/worker teardown must release control admission for the next command"
        );
        assert!(weak.upgrade().is_none());
        assert!(permits.clone().try_acquire_owned().is_ok());
        server.stop().await;
    });
}

// Retained-version authority and exact-index acceptance, not workspace restore.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_version_roots_are_lazy_durable_and_never_fall_back_to_generic_repository_reads() {
    use jj_lib::{
        backend::{MillisSinceEpoch, Timestamp},
        object_id::ObjectId as _,
        op_store::{
            OpStore as _, Operation, OperationMetadata, RootOperationData, TimestampRange, View,
        },
        simple_op_store::SimpleOpStore,
    };
    use tinytree_client::{
        CloudManifestId, CloudObjectId, CloudPublishIntent, CloudRawDigest, RetainedVersionSelector,
    };
    let profile: tinytree_server::Config = toml::from_str(
        r#"
[service]
max_metadata_bytes=4194304
[service.cloud]
enabled=true
indexed_versions=true
captured_versions=true
max_capture_bytes=131072
max_manifest_bytes=524288
max_staging_leases=256
max_inflight=2
max_inflight_per_repository=1
max_buffered_bytes=1342177280
[store]
max_retention_validation_records=1024
max_retention_validation_bytes=2097152
max_closure_objects=4096
max_closure_edges=16384
max_path_contexts=4096
max_path_context_bytes=4194304
[store.cloud]
max_object_bytes=4194304
max_decode_bytes=16777216
max_version_decode_bytes=134217728
max_manifest_bytes=524288
max_manifest_decode_bytes=16777216
max_manifest_members=1024
"#,
    )
    .unwrap();
    profile.validate().unwrap();
    let store_configuration = profile
        .store
        .materialize(profile.service.max_metadata_bytes);
    let server_dir = private_dir();
    let store = Store::open(server_dir.path(), store_configuration.clone()).unwrap();
    store.create_repository("example").unwrap();
    let data = tinytree_fixture::build_fixture(tinytree_fixture::FixtureConfig {
        unused_files: 20,
        bytes_per_file: 3000,
    })
    .unwrap();
    for object in data.objects() {
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let standalone_bytes = b"standalone retained file".to_vec();
    let standalone = m::describe_file(&standalone_bytes);
    store
        .put("example", standalone, &standalone_bytes, 0)
        .unwrap();
    let copy_parent = m::encode(
        &m::Object::CopyHistory(m::CopyHistory {
            current_path: "prior-copy".into(),
            parents: vec![],
            salt: b"retained parent".to_vec(),
        }),
        Default::default(),
    )
    .unwrap();
    let copy_head = m::encode(
        &m::Object::CopyHistory(m::CopyHistory {
            current_path: "current-copy".into(),
            parents: vec![m::CopyId(copy_parent.descriptor.key.id.0)],
            salt: b"retained head".to_vec(),
        }),
        Default::default(),
    )
    .unwrap();
    for object in [&copy_parent, &copy_head] {
        store
            .put("example", object.descriptor, &object.bytes, 0)
            .unwrap();
    }
    let head_object = data
        .objects()
        .iter()
        .rev()
        .find(|o| o.descriptor.key.kind == m::Kind::Commit)
        .unwrap();
    let head = head_object.descriptor.key;
    let m::Object::Commit(commit) = m::decode(&head_object.bytes, Default::default()).unwrap()
    else {
        panic!()
    };
    let tree_key = m::ObjectKey {
        kind: m::Kind::Tree,
        id: m::ObjectId(commit.root_tree.terms[0].0),
    };
    let tree_object = data
        .objects()
        .iter()
        .find(|o| o.descriptor.key == tree_key)
        .unwrap();
    let m::Object::Tree(tree) = m::decode(&tree_object.bytes, Default::default()).unwrap() else {
        panic!()
    };
    let files = tree
        .entries
        .iter()
        .filter_map(|entry| match entry.value {
            m::TreeValue::File { id, .. } => Some(id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(files.len() >= 2);
    let source_dir = private_dir();
    let source = SimpleOpStore::init(
        source_dir.path(),
        RootOperationData {
            root_commit_id: jj_lib::backend::CommitId::from_bytes(&[0; 32]),
        },
    )
    .unwrap();
    let mut view = View::make_root(jj_lib::backend::CommitId::from_bytes(&head.id.0));
    view.wc_commit_ids.insert(
        "task".into(),
        jj_lib::backend::CommitId::from_bytes(&head.id.0),
    );
    let view_id = source.write_view(&view).await.unwrap();
    let time = Timestamp {
        timestamp: MillisSinceEpoch(0),
        tz_offset: 0,
    };
    let operation = Operation {
        view_id: view_id.clone(),
        parents: vec![jj_lib::op_store::OperationId::from_bytes(&[0; 64])],
        metadata: OperationMetadata {
            time: TimestampRange {
                start: time.clone(),
                end: time,
            },
            description: "retained native fixture".into(),
            hostname: "fixture".into(),
            username: "fixture".into(),
            is_snapshot: true,
            workspace_name: Some("task".into()),
            attributes: Default::default(),
        },
        commit_predecessors: None,
    };
    let op_id = source.write_operation(&operation).await.unwrap();
    let objects = vec![
        tinytree_cloud::export_view(&source, &view_id, Default::default())
            .await
            .unwrap(),
        tinytree_cloud::export_operation(&source, &op_id, Default::default())
            .await
            .unwrap(),
    ];
    let op: [u8; 64] = op_id.as_bytes().try_into().unwrap();
    let mut members = objects
        .iter()
        .map(|o| tinytree_cloud::Member {
            id: o.id(),
            selected: o.digest().unwrap(),
        })
        .collect::<Vec<_>>();
    members.sort_by_key(|m| m.id);
    let tree_terminal = m::ObjectKey {
        kind: m::Kind::File,
        id: m::ObjectId(files[0].0),
    };
    let mut roots = vec![
        head,
        standalone.key,
        tree_terminal,
        copy_head.descriptor.key,
    ];
    roots.extend(commit.root_tree.terms.iter().map(|id| m::ObjectKey {
        kind: m::Kind::Tree,
        id: m::ObjectId(id.0),
    }));
    roots.sort_by_key(|key| (key.kind as u8, key.id.0));
    roots.dedup();
    let manifest = tinytree_cloud::Manifest {
        workspace: [71; 32],
        exporter: [72; 16],
        sequence: 1,
        operation_heads: vec![op],
        undo_operations: vec![],
        intent_operations: vec![],
        working_copy: tinytree_cloud::WorkingCopy {
            workspace_name: "task".into(),
            wc_operation: op,
            snapshot_operation: op,
            snapshot_commit: head.id.0,
            tree: commit.root_tree,
        },
        parents: vec![],
        members,
        intent_roots: vec![tinytree_cloud::IntentRoots {
            kind: tinytree_cloud::IntentKind::Recovery,
            id: [73; 32],
            roots: {
                let mut roots = vec![standalone.key, tree_terminal, copy_head.descriptor.key];
                roots.sort_by_key(|key| (key.kind as u8, key.id.0));
                roots
            },
        }],
        native_roots: roots.clone(),
        coverage: tinytree_cloud::Coverage::FullRequired,
        detached: vec![],
    };
    let manifest_bytes = tinytree_cloud::encode_manifest(&manifest, Default::default()).unwrap();
    let selected = CloudRawDigest::of(&manifest_bytes);
    let selector = RetainedVersionSelector {
        workspace_id: manifest.workspace,
        generation: 1,
        manifest_id: CloudManifestId::of(&manifest_bytes),
    };
    let principals = || {
        vec![
            Principal::new("test", TOKEN, vec!["example".into()], true)
                .unwrap()
                .with_cloud(true),
            Principal::new(
                "other",
                "native-remote-other-loopback-test-token",
                vec!["example".into()],
                true,
            )
            .unwrap()
            .with_cloud(true),
        ]
    };
    let query_index_directory = private_dir();
    let query_index =
        tinytree_index::Index::open(query_index_directory.path(), &Default::default()).unwrap();
    let service = RepositoryService::new(
        store.clone(),
        profile.service.clone(),
        vec!["example".into()],
        principals(),
    )
    .unwrap()
    .with_query_index(query_index)
    .unwrap();
    let counts = Counters::new();
    let server = Server::start_service(service, counts.clone(), None).await;
    let directory = private_dir();
    let configuration = config(&server);
    let client = tinytree_client::NativeClient::connect(configuration.remote.clone())
        .await
        .unwrap();
    let mut leases = Vec::new();
    for object in &objects {
        let id = match object.id() {
            tinytree_cloud::ObjectId::Operation(id) => CloudObjectId::Operation(id),
            tinytree_cloud::ObjectId::View(id) => CloudObjectId::View(id),
        };
        leases.push(
            client
                .cloud_put_object(id, object.payload().unwrap())
                .await
                .unwrap()
                .staging_lease,
        );
    }
    client
        .cloud_publish_version(&CloudPublishIntent {
            workspace_id: manifest.workspace,
            expected_generation: 0,
            idempotency_key: "native-retained-version".into(),
            manifest: manifest_bytes,
            staging_leases: leases,
        })
        .await
        .unwrap();
    let query_rotation = exercise_retained_query_facade(
        &store,
        &client,
        &manifest,
        &objects,
        head_object,
        &data,
        configuration.clone(),
        &counts,
    )
    .await;
    let shared = cache(directory.path());
    let (direct, repo) = local(
        directory.path(),
        shared.clone(),
        configuration.clone(),
        true,
    );
    let admitted = repo
        .control
        .admit_retained_version(selector, selected, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(admitted.native_root_count(), roots.len());
    async fn assert_unproven_catalog(repo: &RemoteRepository) {
        assert!(matches!(
            complete_copy_metadata(repo).await,
            Err(jj_lib::backend::BackendError::Unsupported(_))
        ));
    }
    async fn related_copy_error(
        repo: &RemoteRepository,
        copy: m::CopyId,
    ) -> jj_lib::backend::BackendError {
        use jj_lib::backend::Backend as _;
        let backend =
            jj_native_backend::NativeBackend::from_io(repo.io.clone(), Default::default())
                .await
                .unwrap();
        backend
            .get_related_copies(&jj_lib::backend::CopyId::from_bytes(&copy.0))
            .await
            .err()
            .expect("unproven retained-version copy history must not return a partial result")
    }
    assert_unproven_catalog(&repo).await;
    assert!(
        direct.descriptor(head).await.unwrap().is_none(),
        "admitting a canonical manifest must not hydrate native commit bodies"
    );
    assert_eq!(
        repo.io.read_metadata(head).await.unwrap().descriptor(),
        head_object.descriptor
    );
    repo.io.read_metadata(tree_key).await.unwrap();
    let nested_tree = tree
        .entries
        .iter()
        .find_map(|entry| match entry.value {
            m::TreeValue::Tree(id) => Some(m::ObjectKey {
                kind: m::Kind::Tree,
                id: m::ObjectId(id.0),
            }),
            _ => None,
        })
        .unwrap();
    assert!(direct.descriptor(nested_tree).await.unwrap().is_none());
    repo.io.read_metadata(nested_tree).await.unwrap();
    assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), 0);
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    assert_eq!(
        counts.holds.load(Ordering::SeqCst),
        0,
        "cloud authority must not manufacture reference holds"
    );
    let expected_file = data
        .objects()
        .iter()
        .find(|o| o.descriptor.key.kind == m::Kind::File && o.descriptor.key.id.0 == files[0].0)
        .unwrap()
        .bytes
        .clone();
    assert_eq!(bytes(repo.io.as_ref(), files[0]).await, expected_file);
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        0,
        "version-only FILE delivery cannot use generic ReadFile"
    );
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(standalone.key.id.0)).await,
        standalone_bytes,
        "standalone FILE terminal has no invented TREE size claim"
    );
    assert_eq!(
        repo.io.descriptor(standalone.key).await.unwrap(),
        Some(standalone)
    );
    assert_eq!(
        bytes(repo.io.as_ref(), m::FileId(standalone.key.id.0)).await,
        standalone_bytes,
        "warm standalone FILE reuses its verified terminal descriptor and CAS bytes"
    );
    // Force a real parent-publication failure AFTER the CopyHistory dependency
    // can be installed. Native descriptor lookup is index-backed, so an
    // unindexed destination directory does not masquerade as a resident object.
    let copy_hash = copy_head
        .descriptor
        .key
        .id
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let copy_blocker = directory
        .path()
        .join("local/native-objects/repos/local/5")
        .join(copy_hash);
    std::fs::create_dir(&copy_blocker).unwrap();
    assert!(
        repo.io
            .read_metadata(copy_head.descriptor.key)
            .await
            .is_err()
    );
    assert_eq!(
        direct.descriptor(copy_parent.descriptor.key).await.unwrap(),
        Some(copy_parent.descriptor),
        "dependency bytes were physically published before the forced parent error"
    );
    assert!(
        direct
            .descriptor(copy_head.descriptor.key)
            .await
            .unwrap()
            .is_none()
    );
    std::fs::remove_dir(&copy_blocker).unwrap();
    drop(repo);
    drop(direct);
    let mut foreign = configuration.clone();
    foreign.remote.token_env = Some("TINYTREE_REMOTE_OTHER_TOKEN".into());
    let (_direct, other) = local(directory.path(), shared.clone(), foreign, false);
    assert_unproven_catalog(&other).await;
    // Foreign metadata is denied before the catalog is reached. Neither this
    // request nor the direct catalog API may expose a partial result.
    let _ = related_copy_error(&other, m::CopyId(copy_parent.descriptor.key.id.0)).await;
    assert!(
        other
            .io
            .read_metadata(copy_parent.descriptor.key)
            .await
            .is_err(),
        "partially published fetched dependency must retain cloud provenance, never authored fallback"
    );
    assert!(
        other.io.read_metadata(head).await.is_err(),
        "old fetched metadata is not authored content under new credentials"
    );
    assert!(
        other
            .io
            .descriptor(m::ObjectKey {
                kind: m::Kind::File,
                id: m::ObjectId(files[0].0)
            })
            .await
            .is_err(),
        "old canonical TREE claims cannot be relabeled across credential bindings"
    );
    assert!(
        other
            .io
            .read_file(m::FileId(standalone.key.id.0))
            .await
            .is_err(),
        "old FILE-terminal descriptors do not authorize a different credential binding"
    );
    assert!(
        other
            .control
            .admit_retained_version(selector, selected, Arc::new(()))
            .await
            .is_err(),
        "a raw selector is not a transferable private-owner grant"
    );
    drop(other);
    drop(_direct);
    let mut wrong_repo = configuration.clone();
    wrong_repo.remote.repository_id = "different".into();
    let foreign_direct = Arc::new(
        DirectObjectIo::load(&directory.path().join("local"), Default::default()).unwrap(),
    );
    assert!(
        RemoteRepository::open(
            foreign_direct,
            shared.clone(),
            &directory.path().join("ledger"),
            &directory.path().join("staging"),
            wrong_repo
        )
        .is_err(),
        "a portable selector must not rebind an existing provider to another repository"
    );
    let address = server.address;
    drop(client);
    server.stop().await;
    // Rotate credentials while preserving the authenticated owner. Existing
    // cached bytes were denied above; fresh admission may authorize a refill,
    // but cannot reuse the stale binding's terminal descriptor.
    let rotated_service = RepositoryService::new(
        store.clone(),
        profile.service.clone(),
        vec!["example".into()],
        vec![
            Principal::new(
                "test",
                "native-remote-other-loopback-test-token",
                vec!["example".into()],
                true,
            )
            .unwrap()
            .with_cloud(true),
        ],
    )
    .unwrap();
    let rotated_server =
        Server::start_service_at(rotated_service, Counters::new(), None, address).await;
    let mut rotated_config = configuration.clone();
    rotated_config.remote.token_env = Some("TINYTREE_REMOTE_OTHER_TOKEN".into());
    let (rotated_direct, rotated) = local(directory.path(), shared.clone(), rotated_config, false);
    assert!(
        rotated
            .io
            .read_file(m::FileId(standalone.key.id.0))
            .await
            .is_err(),
        "credential rotation alone cannot reuse stale cached authority"
    );
    rotated
        .control
        .admit_retained_version(selector, selected, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(
        bytes(rotated.io.as_ref(), m::FileId(standalone.key.id.0)).await,
        standalone_bytes,
        "fresh same-owner admission authorizes a verified FILE refill"
    );
    assert_eq!(
        bytes(rotated.io.as_ref(), files[0]).await,
        expected_file,
        "current exact FILE terminal supersedes an old TREE claim without borrowing its descriptor"
    );
    assert_eq!(query_rotation.file, files[0]);
    let mut query_configuration = query_rotation.configuration.clone();
    query_configuration.remote.token_env = Some("TINYTREE_REMOTE_OTHER_TOKEN".into());
    let (mixed_direct, mixed) = local(
        query_rotation.directory.path(),
        query_rotation.cache.clone(),
        query_configuration,
        false,
    );
    assert!(
        mixed.io.read_file(query_rotation.file).await.is_err(),
        "old CQ TREE claim alone stays denied after credential rotation"
    );
    mixed
        .control
        .admit_retained_version(selector, selected, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(
        bytes(mixed.io.as_ref(), query_rotation.file).await,
        query_rotation.bytes,
        "fresh current exact FILE terminal takes precedence over stale CQ TREE authority"
    );
    let read = mixed
        .io
        .read_file_for_cache(query_rotation.file, Arc::new(()))
        .await
        .unwrap();
    let (descriptor, mut reader) = read.into_parts();
    let mut delivered = Vec::new();
    reader.read_to_end(&mut delivered).await.unwrap();
    assert_eq!(delivered, query_rotation.bytes);
    assert_eq!(
        mixed.io.descriptor(descriptor.key).await.unwrap(),
        Some(descriptor),
        "descriptor/CAS paths use the same independently current authority"
    );
    drop(reader);
    mixed.wait_for_idle().await;
    drop(mixed);
    drop(mixed_direct);
    drop(rotated);
    drop(rotated_direct);
    rotated_server.stop().await;
    // Same endpoint/auth profile, but the retained version is now unavailable.
    // No deletion API or fake public reference is needed to exercise expiry/loss.
    let empty_dir = private_dir();
    let empty = Store::open(empty_dir.path(), store_configuration).unwrap();
    empty.create_repository("example").unwrap();
    let second_counts = Counters::new();
    let service = RepositoryService::new(
        empty,
        profile.service.clone(),
        vec!["example".into()],
        principals(),
    )
    .unwrap();
    let second = Server::start_service_at(service, second_counts.clone(), None, address).await;
    let (_direct, reopened) = local(directory.path(), shared, configuration, false);
    assert_eq!(
        reopened
            .io
            .read_metadata(copy_parent.descriptor.key)
            .await
            .unwrap()
            .descriptor(),
        copy_parent.descriptor,
        "same-binding resident dependency proves its durable canonical path even when parent payload is absent"
    );
    let catalog_error =
        related_copy_error(&reopened, m::CopyId(copy_parent.descriptor.key.id.0)).await;
    assert!(
        matches!(
            &catalog_error,
            jj_lib::backend::BackendError::Unsupported(_)
        ),
        "authorized resident copy must reach typed incomplete-catalog refusal: {catalog_error}"
    );

    assert_eq!(
        reopened.io.read_metadata(head).await.unwrap().descriptor(),
        head_object.descriptor,
        "same-binding verified resident metadata remains usable without reauthorizing a network read"
    );
    assert_eq!(
        bytes(reopened.io.as_ref(), files[0]).await,
        expected_file,
        "verified resident FILE survives unavailable source version"
    );
    assert_eq!(
        bytes(reopened.io.as_ref(), m::FileId(standalone.key.id.0)).await,
        standalone_bytes,
        "verified standalone FILE remains offline-readable under its captured binding"
    );
    assert!(
        reopened.io.read_file(files[1]).await.is_err(),
        "a missing cloud-only file must reauthorize the exact selected version"
    );
    assert_eq!(second_counts.files.load(Ordering::SeqCst), 0);
    assert_eq!(
        second_counts.metadata_rpcs.load(Ordering::SeqCst),
        0,
        "no generic fallback after version reauthorization fails"
    );
    drop(reopened);
    drop(_direct);
    second.stop().await;
}

struct QueryRotationFixture {
    directory: tempfile::TempDir,
    cache: Arc<LocalCas>,
    configuration: Config,
    file: m::FileId,
    bytes: Vec<u8>,
}
// Extend the retained-version fixture with an independent local provider. This
// keeps its V1 residency assertions intact while exercising V2 import and the
// V3 retained-provider/query chain. Neither asserts a physically sealed restore.
async fn exercise_retained_query_facade(
    store: &Store,
    publisher: &tinytree_client::NativeClient,
    recovery: &tinytree_cloud::Manifest,
    objects: &[tinytree_cloud::VerifiedObject],
    base: &m::EncodedObject,
    fixture: &tinytree_fixture::Fixture,
    configuration: Config,
    counts: &Counters,
) -> QueryRotationFixture {
    use jj_lib::object_id::ObjectId as _;
    use native_broker::{NativeQueryIo as _, query_protocol as q};
    let m::Object::Commit(mut model) = m::decode(&base.bytes, Default::default()).unwrap() else {
        panic!()
    };
    model.parents = vec![m::CommitId(base.descriptor.key.id.0)];
    model.change_id = m::ChangeId([114; 16]);
    model.description = "frozen selected baseline".into();
    let frozen = m::encode(&m::Object::Commit(model.clone()), Default::default()).unwrap();
    store
        .put("example", frozen.descriptor, &frozen.bytes, 0)
        .unwrap();
    let mut unselected_model = model.clone();
    unselected_model.change_id = m::ChangeId([123; 16]);
    unselected_model.description = "outside selected retained snapshot".into();
    let unselected = m::encode(&m::Object::Commit(unselected_model), Default::default()).unwrap();
    store
        .put("example", unselected.descriptor, &unselected.bytes, 0)
        .unwrap();
    let selected_index = tinytree_cloud::build_index_artifacts(
        &tinytree_cloud::PortableIndexSnapshot {
            roots: vec![m::CommitId(base.descriptor.key.id.0)],
            drafts: vec![tinytree_cloud::IndexDraft {
                descriptor: frozen.descriptor,
                change: model.change_id,
                parents: model.parents.clone(),
                order: 1,
            }],
        },
        Default::default(),
    )
    .unwrap();
    let intrinsic = tinytree_cloud::build_index_artifacts(
        &tinytree_cloud::PortableIndexSnapshot {
            roots: vec![m::CommitId::ROOT],
            drafts: vec![],
        },
        Default::default(),
    )
    .unwrap();
    let mut leases = Vec::new();
    for artifact in selected_index.artifacts.iter().chain(&intrinsic.artifacts) {
        let id = match artifact.descriptor.id {
            tinytree_cloud::IndexArtifactId::Root(id) => {
                tinytree_client::CloudIndexArtifactId::Root(id.0)
            }
            tinytree_cloud::IndexArtifactId::Node(id) => {
                tinytree_client::CloudIndexArtifactId::Node(id.0)
            }
        };
        leases.push(
            publisher
                .cloud_put_index_artifact(id, &artifact.bytes)
                .await
                .unwrap()
                .staging_lease,
        );
    }
    let mut next = recovery.clone();
    next.sequence = 2;
    next.parents = vec![tinytree_cloud::ManifestId::of(
        &tinytree_cloud::encode_manifest(recovery, Default::default()).unwrap(),
    )];
    next.native_roots.push(frozen.descriptor.key);
    next.native_roots
        .sort_by_key(|key| (key.kind as u8, key.id.0));
    next.native_roots.dedup();
    let mut index_roots = vec![intrinsic.root, selected_index.root];
    index_roots.sort_by_key(|root| root.id);
    index_roots.dedup_by_key(|root| root.id);
    let manifest = tinytree_cloud::ManifestV2 {
        recovery: next,
        index_roots,
        operation_indexes: vec![
            tinytree_cloud::OperationIndex {
                operation: [0; 64],
                root: intrinsic.root.id,
            },
            tinytree_cloud::OperationIndex {
                operation: recovery.working_copy.snapshot_operation,
                root: selected_index.root.id,
            },
        ],
    };
    let manifest_bytes = tinytree_cloud::encode_manifest_v2(&manifest, Default::default()).unwrap();
    publisher
        .cloud_publish_version(&tinytree_client::CloudPublishIntent {
            workspace_id: recovery.workspace,
            expected_generation: 1,
            idempotency_key: "selected-index-query-version".into(),
            manifest: manifest_bytes.clone(),
            staging_leases: leases,
        })
        .await
        .unwrap();
    // The V2 compatibility check uses a separate provider so its terminal
    // grants cannot mask any V3 snapshot/FILE authority in the full chain.
    {
        let compatibility_dir = private_dir();
        let compatibility_cache = cache(compatibility_dir.path());
        let (direct, compatibility) = local(
            compatibility_dir.path(),
            compatibility_cache,
            configuration.clone(),
            true,
        );
        let v2_selector = tinytree_client::RetainedVersionSelector {
            workspace_id: recovery.workspace,
            generation: 2,
            manifest_id: tinytree_client::CloudManifestId::of(&manifest_bytes),
        };
        assert_eq!(
            compatibility
                .control
                .admit_retained_version(
                    v2_selector,
                    tinytree_client::CloudRawDigest::of(&manifest_bytes),
                    Arc::new(())
                )
                .await
                .unwrap()
                .native_root_count(),
            manifest.recovery.native_roots.len()
        );
        compatibility.wait_for_idle().await;
        drop(compatibility);
        drop(direct);
    }
    // Owner-declared portable capture DATA; no physically sealed restore is
    // inferred. Every native root is independently derivable by the server.
    let mut history = manifest.clone();
    history.recovery.sequence = 3;
    history.recovery.parents = vec![tinytree_cloud::ManifestId::of(&manifest_bytes)];
    history.recovery.intent_roots.clear();
    let mut roots = std::collections::BTreeSet::new();
    for object in objects {
        for id in &object.dependencies().native_commits {
            if *id != [0; 32] {
                roots.insert((m::Kind::Commit as u8, *id));
            }
        }
    }
    roots.insert((
        m::Kind::Commit as u8,
        history.recovery.working_copy.snapshot_commit,
    ));
    for id in &history.recovery.working_copy.tree.terms {
        roots.insert((m::Kind::Tree as u8, id.0));
    }
    roots.insert((m::Kind::Commit as u8, base.descriptor.key.id.0));
    roots.insert((m::Kind::Commit as u8, frozen.descriptor.key.id.0));
    roots.remove(&(m::Kind::Commit as u8, [0; 32]));
    history.recovery.native_roots = roots
        .iter()
        .map(|(kind, id)| m::ObjectKey {
            kind: m::Kind::try_from(*kind).unwrap(),
            id: m::ObjectId(*id),
        })
        .collect();
    let text = |bytes: &[u8]| bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let zero = text(&[0; 64]);
    let mut operations = vec![tinytree_cloud::OperationRecord {
        operation: zero.clone(),
        view: zero,
        parents: vec![],
    }];
    let mut blobs = Vec::new();
    for object in objects {
        let (kind, id) = match object.id() {
            tinytree_cloud::ObjectId::Operation(id) => (1, id),
            tinytree_cloud::ObjectId::View(id) => (2, id),
        };
        let selected = object.digest().unwrap();
        blobs.push(tinytree_cloud::BlobRecord {
            kind,
            id: text(&id),
            blake3: selected.blake3,
            size: selected.size_bytes,
        });
        if let Some(op) = object.operation() {
            operations.push(tinytree_cloud::OperationRecord {
                operation: text(&id),
                view: text(op.view_id.as_bytes()),
                parents: op.parents.iter().map(|id| text(id.as_bytes())).collect(),
            });
        }
    }
    operations.sort_by(|a, b| a.operation.cmp(&b.operation));
    let mut selected_artifacts = std::collections::BTreeMap::new();
    for artifact in selected_index.artifacts.iter().chain(&intrinsic.artifacts) {
        selected_artifacts.insert(artifact.descriptor.id, artifact);
    }
    for artifact in selected_artifacts.values() {
        let (kind, id) = match artifact.descriptor.id {
            tinytree_cloud::IndexArtifactId::Root(id) => (3, id.0),
            tinytree_cloud::IndexArtifactId::Node(id) => (4, id.0),
        };
        blobs.push(tinytree_cloud::BlobRecord {
            kind,
            id: text(&id),
            blake3: artifact.descriptor.selected.blake3,
            size: artifact.descriptor.selected.size_bytes,
        });
    }
    blobs.sort_by(|a, b| (a.kind, &a.id).cmp(&(b.kind, &b.id)));
    let wc = &history.recovery.working_copy;
    let capture = tinytree_cloud::CaptureData {
        version: 1,
        policy: tinytree_cloud::CapturePolicy::HostQuiescentV1,
        codec: tinytree_cloud::CODEC.into(),
        operation_heads: history
            .recovery
            .operation_heads
            .iter()
            .map(|id| text(id))
            .collect(),
        working_copy: tinytree_cloud::PortableWorkingCopy {
            workspace: wc.workspace_name.clone(),
            wc_operation: text(&wc.wc_operation),
            snapshot_operation: text(&wc.snapshot_operation),
            snapshot_commit: wc.snapshot_commit,
            tree: tinytree_cloud::CaptureTree {
                terms: wc.tree.terms.iter().map(|id| id.0).collect(),
                labels: if wc.tree.labels.iter().all(String::is_empty) {
                    vec![]
                } else {
                    wc.tree.labels.clone()
                },
            },
        },
        operations,
        indexes: history
            .operation_indexes
            .iter()
            .map(|a| tinytree_cloud::IndexRecord {
                operation: text(&a.operation),
                root: a.root.0,
            })
            .collect(),
        blobs,
        roots: roots
            .into_iter()
            .map(|(kind, id)| tinytree_cloud::CapturedRoot { kind, id })
            .collect(),
        remote: None,
    };
    let capture_bytes = tinytree_cloud::encode_capture_data(&capture, Default::default()).unwrap();
    let captured = tinytree_cloud::ManifestV3 {
        history,
        capture: tinytree_cloud::CaptureDescriptor::of(&capture_bytes),
    };
    let captured_bytes = tinytree_cloud::encode_manifest_v3(&captured, Default::default()).unwrap();
    let intent = tinytree_client::CloudCapturedPublishIntent::new(
        tinytree_client::CloudPublishIntent {
            workspace_id: recovery.workspace,
            expected_generation: 2,
            idempotency_key: "selected-captured-query-version".into(),
            manifest: captured_bytes.clone(),
            staging_leases: vec![],
        },
        capture_bytes,
    )
    .unwrap();
    let publication = publisher
        .cloud_publish_captured_version(&intent)
        .await
        .unwrap();
    assert_eq!(publication.workspace.generation, 3);
    // Reconcile the immutable intent without selecting a different generation.
    assert_eq!(
        publisher
            .cloud_publish_captured_version(&intent)
            .await
            .unwrap(),
        publication
    );
    let selector = tinytree_client::RetainedVersionSelector {
        workspace_id: recovery.workspace,
        generation: 3,
        manifest_id: tinytree_client::CloudManifestId::of(&captured_bytes),
    };
    let selected = tinytree_client::CloudRawDigest::of(&captured_bytes);
    let manifest = captured.history;
    let manifest_bytes = captured_bytes;
    let source = native_broker::CloudQuerySource {
        provider_identity: [41; 32],
        workspace_id: recovery.workspace,
        generation: 3,
        manifest_id: tinytree_client::CloudManifestId::of(&manifest_bytes).0,
        manifest_blake3: *blake3::hash(&manifest_bytes).as_bytes(),
        manifest_size_bytes: manifest_bytes.len() as u64,
        index_root_ids: vec![selected_index.root.id.0],
        expected_anchors: vec![m::CommitId(base.descriptor.key.id.0)],
    };
    let directory = private_dir();
    let shared = cache(directory.path());
    let (direct, remote) = local(
        directory.path(),
        shared.clone(),
        configuration.clone(),
        true,
    );
    let mut wrong = selected;
    wrong.blake3[0] ^= 1;
    assert!(
        remote
            .control
            .admit_retained_version(selector, wrong, Arc::new(()))
            .await
            .is_err()
    );
    assert_eq!(
        remote
            .control
            .admit_retained_version(selector, selected, Arc::new(()))
            .await
            .unwrap()
            .native_root_count(),
        manifest.recovery.native_roots.len()
    );
    assert_eq!(
        std::fs::read(directory.path().join("ledger/admission.format")).unwrap(),
        b"tinytree-native-admission-4\n"
    );
    let mut broker_config = native_broker::Config::default();
    broker_config.max_inflight = 1;
    let broker = native_broker::BrokerService::new(
        std::collections::HashMap::from([(
            "example".into(),
            remote.io.clone() as Arc<dyn NativeObjectIo>,
        )]),
        broker_config.clone(),
    )
    .unwrap()
    .with_queries(std::collections::HashMap::from([(
        "example".into(),
        remote.query.clone() as Arc<dyn native_broker::NativeQueryIo>,
    )]))
    .unwrap()
    .with_provider_descriptions(std::collections::HashMap::from([(
        "example".into(),
        native_broker::ProviderDescription {
            identity: [41; 32],
            query_capable: true,
            index_mode: native_broker::ProviderIndexMode::Legacy,
        },
    )]))
    .unwrap();
    let socket = directory.path().join("snapshot.sock");
    let serving = socket.clone();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        native_broker::serve_unix(&serving, broker, async {
            let _ = stopped.await;
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !socket.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let query =
        native_broker::BrokerQueryClient::connect(&socket, "example", broker_config.clone())
            .await
            .unwrap();
    let input = native_broker::OpenCloudQueryScope {
        source: source.clone(),
        overlay: vec![],
        replay_key: [111; 32],
    };
    let mut oversized_selection = input.clone();
    oversized_selection.replay_key = [108; 32];
    oversized_selection.source.index_root_ids = (0..257u32)
        .map(|value| {
            let mut id = [0; 32];
            id[..4].copy_from_slice(&value.to_be_bytes());
            id
        })
        .collect();
    let before = counts.query_rpcs.load(Ordering::SeqCst);
    assert_eq!(
        remote
            .query
            .open_cloud_scope(oversized_selection, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(counts.query_rpcs.load(Ordering::SeqCst), before);
    let mut wrong_anchors = input.clone();
    wrong_anchors.replay_key = [109; 32];
    wrong_anchors.source.expected_anchors = vec![m::CommitId::ROOT];
    assert!(
        query
            .open_cloud_scope(wrong_anchors, Arc::new(()))
            .await
            .is_err()
    );
    assert_eq!(
        std::fs::read(directory.path().join("ledger/admission.format")).unwrap(),
        b"tinytree-native-admission-4\n",
        "failed source check cannot enable or install snapshot grants"
    );
    let opened = query
        .open_cloud_scope(input.clone(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(opened.value.cloud_source_digest, Some(source.digest()));
    assert_eq!(opened.value.query_identity_version, 2);
    assert_eq!(
        query
            .open_cloud_scope(input.clone(), Arc::new(()))
            .await
            .unwrap()
            .value,
        opened.value
    );
    let mut changed = input;
    changed.source.manifest_blake3[0] ^= 1;
    assert_eq!(
        query
            .open_cloud_scope(changed, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists
    );
    assert_eq!(
        std::fs::read(directory.path().join("ledger/admission.format")).unwrap(),
        b"tinytree-native-admission-5\n"
    );
    assert!(
        direct
            .descriptor(frozen.descriptor.key)
            .await
            .unwrap()
            .is_none(),
        "opening selected snapshot must not hydrate its frozen draft bodies"
    );
    let frozen_id = m::CommitId(frozen.descriptor.key.id.0);
    let facts = vec![q::GraphFact {
        fact: Some(q::graph_fact::Fact::HasId(q::HasId {
            commit: Some(q::CommitId {
                value: frozen_id.0.to_vec(),
            }),
        })),
    }];
    let answer = query
        .facts(opened.value.handle, facts.clone(), Arc::new(()))
        .await
        .unwrap();
    assert!(matches!(
        answer.value.answers[0].answer,
        Some(q::graph_fact_answer::Answer::HasId(true))
    ));
    drop(answer);
    let prefix = query
        .resolve_commit_prefix(
            opened.value.handle,
            blake3::Hash::from(frozen_id.0).to_hex().to_string(),
            Arc::new(()),
        )
        .await
        .unwrap();
    assert!(matches!(
        prefix.value.result,
        Some(q::commit_prefix_result::Result::Single(_))
    ));
    drop(prefix);
    // Actual index/provider acceptance, not OPVIEW restore or a Ready workspace.
    // The importer receives only compact artifacts and selected manifest bytes;
    // its query and object adapters both traverse this SAME broker/Remote pair.
    {
        use futures::TryStreamExt as _;
        use jj_lib::{
            index::{Index as _, ReadonlyIndex as _},
            object_id::{HexPrefix, ObjectId as _, PrefixResolution},
            revset::ResolvedExpression,
        };
        let description = query.describe_provider().await.unwrap();
        assert_eq!(description.identity, source.provider_identity);
        let verified_artifacts: Vec<_> = selected_index
            .artifacts
            .iter()
            .chain(&intrinsic.artifacts)
            .map(|artifact| {
                (
                    artifact.descriptor.id,
                    tinytree_cloud::decode_index_artifact(
                        &artifact.bytes,
                        artifact.descriptor,
                        Default::default(),
                    )
                    .unwrap(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>()
            .into_values()
            .collect();
        let graph = tinytree_cloud::verify_index_artifact_graph(
            &manifest.index_roots,
            &verified_artifacts,
            Default::default(),
        )
        .unwrap();
        let spool = directory.path().join("index-spool");
        make_private(&spool);
        let objects = Arc::new(
            native_broker::BrokerObjectIo::connect(
                &socket,
                "example",
                &spool,
                broker_config.clone(),
            )
            .await
            .unwrap(),
        );
        let index_path = directory.path().join("imported-index");
        assert!(!index_path.exists());
        let configuration = jj_native_index::Config {
            endpoint: jj_native_index::Endpoint::UnixSocket(socket.clone()),
            store_alias: "example".into(),
            provider_fingerprint: description.identity,
        };
        let query_calls_before_import = counts.query_rpcs.load(Ordering::SeqCst);
        let imported = jj_native_index::NativeIndexStore::import_fresh_operation_indexes(
            &index_path,
            configuration.clone(),
            Default::default(),
            Arc::new(query.clone()),
            objects.clone(),
            jj_native_index::PortableImportVersion {
                provider_identity: description.identity,
                workspace_id: source.workspace_id,
                generation: source.generation,
                manifest_id: source.manifest_id,
                manifest_blake3: source.manifest_blake3,
                manifest_size_bytes: source.manifest_size_bytes,
            },
            &manifest_bytes,
            &graph,
            Default::default(),
            Arc::new(()),
        )
        .await
        .unwrap();
        assert!(
            counts.query_rpcs.load(Ordering::SeqCst) > query_calls_before_import,
            "the importer must traverse the real remote query service"
        );
        assert_eq!(imported.operation_indexes, manifest.operation_indexes);
        assert_eq!(imported.unique_snapshots, manifest.index_roots.len());
        let imported = imported.store;
        let cleanup = imported.cleanup_handle();
        let root_index = imported.index_at_operation(&[0; 64]).unwrap();
        let selected = imported
            .index_at_operation(&recovery.working_copy.snapshot_operation)
            .unwrap();
        let frozen_jj = jj_lib::backend::CommitId::from_bytes(&frozen_id.0);
        assert!(selected.has_id(&frozen_jj).await.unwrap());
        assert!(
            !root_index.has_id(&frozen_jj).await.unwrap(),
            "exact op0 association must not acquire another snapshot's frozen membership"
        );
        let full_prefix =
            HexPrefix::try_from_hex(blake3::Hash::from(frozen_id.0).to_hex().to_string()).unwrap();
        assert_eq!(
            selected
                .resolve_commit_id_prefix(&full_prefix)
                .await
                .unwrap(),
            PrefixResolution::SingleMatch(frozen_jj.clone())
        );
        assert_eq!(
            root_index
                .resolve_commit_id_prefix(&full_prefix)
                .await
                .unwrap(),
            PrefixResolution::NoMatch
        );
        let backend =
            jj_native_backend::NativeBackend::from_io(objects.clone(), Default::default())
                .await
                .unwrap();
        let jj_store = jj_lib::store::Store::new(
            Box::new(backend),
            jj_lib::signing::Signer::new(None, vec![]),
            jj_lib::tree_merge::MergeOptions {
                hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
                same_change: jj_lib::merge::SameChange::Accept,
            },
        );
        assert!(
            direct
                .descriptor(frozen.descriptor.key)
                .await
                .unwrap()
                .is_none(),
            "import/facts/prefix must not hydrate frozen canonical bodies"
        );
        assert!(
            direct
                .descriptor(base.descriptor.key)
                .await
                .unwrap()
                .is_none(),
            "import must not hydrate anchor history"
        );
        assert_eq!(counts.holds.load(Ordering::SeqCst), 0);
        assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), 0);
        assert_eq!(counts.graph_rpcs.load(Ordering::SeqCst), 0);
        assert_eq!(counts.files.load(Ordering::SeqCst), 0);
        let query_calls_before_render = counts.query_rpcs.load(Ordering::SeqCst);
        // NativeRevset::stream deliberately requests selected canonical metadata
        // for jj rendering. This phase is NOT a zero-selected-body log claim.
        let history = selected
            .evaluate_revset(
                &ResolvedExpression::Ancestors {
                    heads: Box::new(ResolvedExpression::Commits(vec![frozen_jj.clone()])),
                    generation: 0..2,
                    parents_range: 0..u32::MAX,
                },
                &jj_store,
            )
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            history,
            vec![
                frozen_jj.clone(),
                jj_lib::backend::CommitId::from_bytes(&base.descriptor.key.id.0)
            ]
        );
        assert!(counts.query_rpcs.load(Ordering::SeqCst) > query_calls_before_render);
        assert_eq!(
            direct
                .read_metadata(frozen.descriptor.key)
                .await
                .unwrap()
                .bytes(),
            frozen.bytes
        );
        assert_eq!(
            direct
                .read_metadata(base.descriptor.key)
                .await
                .unwrap()
                .bytes(),
            base.bytes
        );
        assert!(
            direct
                .descriptor(unselected.descriptor.key)
                .await
                .unwrap()
                .is_none(),
            "rendering selected rows must not hydrate an unrelated server COMMIT"
        );
        for object in fixture.objects().iter().filter(|object| {
            matches!(
                object.descriptor.key.kind,
                m::Kind::Tree | m::Kind::File | m::Kind::Symlink | m::Kind::CopyHistory
            ) && object.descriptor.key != m::empty_tree().descriptor.key
        }) {
            assert!(
                direct
                    .descriptor(object.descriptor.key)
                    .await
                    .unwrap()
                    .is_none(),
                "selected canonical rendering must not hydrate TREE/FILE or unrelated bodies"
            );
        }
        assert_eq!(counts.holds.load(Ordering::SeqCst), 0);
        assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), 0);
        assert_eq!(counts.graph_rpcs.load(Ordering::SeqCst), 0);
        assert_eq!(counts.files.load(Ordering::SeqCst), 0);
        // A normal local authored COMMIT becomes a delta, while the frozen
        // baseline is not re-uploaded as an overlay body.
        let mut authored = model.clone();
        authored.parents = vec![frozen_id];
        authored.change_id = m::ChangeId([122; 16]);
        authored.description = "imported-index local delta".into();
        let authored = m::encode(&m::Object::Commit(authored), Default::default()).unwrap();
        let object = m::decode(&authored.bytes, Default::default()).unwrap();
        assert_eq!(
            objects.write_metadata(object).await.unwrap(),
            authored.descriptor
        );
        let authored_jj = jj_lib::backend::CommitId::from_bytes(&authored.descriptor.key.id.0);
        let authored_commit = jj_store.get_commit_async(&authored_jj).await.unwrap();
        let mut mutable = selected.start_modification();
        mutable.add_commit(&authored_commit).await.unwrap();
        let updated = imported.persist_index(mutable).unwrap();
        let history = updated
            .evaluate_revset(
                &ResolvedExpression::Commits(vec![frozen_jj.clone(), authored_jj.clone()]),
                &jj_store,
            )
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(history.len(), 2);
        assert!(history.contains(&frozen_jj) && history.contains(&authored_jj));
        assert_eq!(
            updated
                .resolve_commit_id_prefix(&HexPrefix::try_from_hex(authored_jj.hex()).unwrap())
                .await
                .unwrap(),
            PrefixResolution::SingleMatch(authored_jj)
        );
        assert_eq!(
            direct
                .read_metadata(frozen.descriptor.key)
                .await
                .unwrap()
                .descriptor(),
            frozen.descriptor,
            "selected frozen metadata is expected after intentional canonical rendering"
        );
        assert!(
            direct
                .descriptor(unselected.descriptor.key)
                .await
                .unwrap()
                .is_none()
        );
        for object in fixture.objects().iter().filter(|object| {
            matches!(
                object.descriptor.key.kind,
                m::Kind::Tree | m::Kind::File | m::Kind::Symlink | m::Kind::CopyHistory
            ) && object.descriptor.key != m::empty_tree().descriptor.key
        }) {
            assert!(
                direct
                    .descriptor(object.descriptor.key)
                    .await
                    .unwrap()
                    .is_none(),
                "delta canonical rendering must remain metadata-only"
            );
        }
        assert!(
            store
                .descriptor("example", authored.descriptor.key)
                .unwrap()
                .is_none(),
            "local delta is not a canonical server publication"
        );
        assert_eq!(counts.holds.load(Ordering::SeqCst), 0);
        assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), 0);
        assert_eq!(counts.graph_rpcs.load(Ordering::SeqCst), 0);
        assert_eq!(counts.files.load(Ordering::SeqCst), 0);
        drop((updated, selected, root_index, imported, jj_store, objects));
        cleanup.drain(Duration::from_secs(5)).await.unwrap();
    }
    let expression = q::GraphExpr {
        version: 1,
        expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
            commits: vec![q::CommitId {
                value: frozen_id.0.to_vec(),
            }],
        })),
    };
    let handle = query
        .begin(
            native_broker::BeginQuery {
                scope: opened.value.handle,
                expression,
                projection: q::Projection::Canonical,
                page_size: 1,
                replay_key: opened.value.bound_query_id([1; 16]).unwrap(),
            },
            Arc::new(()),
        )
        .await
        .unwrap()
        .value;
    let mut sequence = 0;
    let mut rows = 0;
    loop {
        let page = query
            .page(handle.handle, sequence, Arc::new(()))
            .await
            .unwrap();
        for row in &page.value.rows {
            assert_eq!(row.commit.as_ref().unwrap().value, frozen_id.0);
            assert!(row.canonical.is_some());
            rows += 1;
        }
        // A held canonical reply cannot retain the sole broker physical slot.
        query
            .facts(opened.value.handle, facts.clone(), Arc::new(()))
            .await
            .unwrap();
        if page.value.exhausted {
            break;
        }
        sequence += 1;
    }
    assert_eq!(rows, 1);
    query
        .close_query(handle.handle, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(
        query
            .read_commit(opened.value.handle, frozen_id, Arc::new(()))
            .await
            .unwrap()
            .value
            .descriptor(),
        frozen.descriptor
    );
    let mut private_model = model.clone();
    private_model.parents = vec![frozen_id];
    private_model.change_id = m::ChangeId([115; 16]);
    private_model.description = "post-restore private delta".into();
    let private = m::encode(&m::Object::Commit(private_model), Default::default()).unwrap();
    let mut fork = native_broker::ForkQueryScope {
        parent: opened.value.handle,
        expected_cloud_source_digest: Some([99; 32]),
        overlay: vec![private.bytes.clone()],
        replay_key: [112; 32],
    };
    let before = counts.query_rpcs.load(Ordering::SeqCst);
    assert_eq!(
        query
            .fork_scope(fork.clone(), Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        counts.query_rpcs.load(Ordering::SeqCst),
        before,
        "changed source rejects before replay/upstream fork"
    );
    fork.expected_cloud_source_digest = opened.value.cloud_source_digest;
    let child = query.fork_scope(fork.clone(), Arc::new(())).await.unwrap();
    assert_eq!(
        query.fork_scope(fork, Arc::new(())).await.unwrap().value,
        child.value
    );
    assert_eq!(
        query
            .read_commit(
                child.value.handle,
                m::CommitId(private.descriptor.key.id.0),
                Arc::new(())
            )
            .await
            .unwrap()
            .value
            .descriptor(),
        private.descriptor
    );
    assert!(
        direct
            .descriptor(private.descriptor.key)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .descriptor("example", private.descriptor.key)
            .unwrap()
            .is_none(),
        "private delta is not published"
    );
    let tree_key = m::ObjectKey {
        kind: m::Kind::Tree,
        id: m::ObjectId(model.root_tree.terms[0].0),
    };
    remote.io.read_metadata(tree_key).await.unwrap();
    let tree = m::decode(
        &fixture
            .objects()
            .iter()
            .find(|object| object.descriptor.key == tree_key)
            .unwrap()
            .bytes,
        Default::default(),
    )
    .unwrap();
    let m::Object::Tree(tree) = tree else {
        panic!()
    };
    let file = tree
        .entries
        .iter()
        .find_map(|entry| match entry.value {
            m::TreeValue::File { id, .. } => Some(id),
            _ => None,
        })
        .unwrap();
    let expected_file = fixture
        .objects()
        .iter()
        .find(|object| {
            object.descriptor.key.kind == m::Kind::File && object.descriptor.key.id.0 == file.0
        })
        .unwrap()
        .bytes
        .clone();
    assert_eq!(bytes(remote.io.as_ref(), file).await, expected_file);
    query
        .close_scope(child.value.handle, Arc::new(()))
        .await
        .unwrap();
    query
        .close_scope(opened.value.handle, Arc::new(()))
        .await
        .unwrap();
    drop((opened, child, query));
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    remote.wait_for_idle().await;
    drop(remote);
    drop(direct);
    // Move live references independently and collect expired staging leases.
    // Refetch must reauthorize through the exact V3 selector, not the current
    // ref. Older retained history also pins this shared closure, so this is an
    // authority/refetch check, not an exclusive V3 retention-availability oracle.
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "captured-ref-initial".into(),
                reference_name: "main".into(),
                expected_generation: 0,
                new_commit: Some(m::CommitId(base.descriptor.key.id.0)),
                staging_leases: vec![],
            },
            1,
        )
        .unwrap();
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "captured-ref-advanced".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: Some(m::CommitId(unselected.descriptor.key.id.0)),
                staging_leases: vec![],
            },
            2,
        )
        .unwrap();
    store.gc("example", i64::MAX / 4).unwrap();
    assert_eq!(
        store.references("example", &["main".into()]).unwrap()[0].commit,
        Some(unselected.descriptor)
    );
    assert_eq!(
        store.descriptor("example", frozen.descriptor.key).unwrap(),
        Some(frozen.descriptor)
    );
    // Use the supported local store collector to evict fetched payloads while
    // keeping the separate provenance ledger. No fabricated body/path edits.
    {
        let local_store = Store::open_existing(
            directory.path().join("local/native-objects"),
            Default::default(),
        )
        .unwrap();
        local_store.gc("local", i64::MAX / 4).unwrap();
        assert!(
            local_store
                .descriptor("local", frozen.descriptor.key)
                .unwrap()
                .is_none()
        );
    }
    // Simulate the safe feature-gate crash window: DB5 is already SyncAll while
    // marker replacement was interrupted. A legacy reader rejects the DB gate.
    std::fs::write(
        directory.path().join("ledger/admission.format"),
        b"tinytree-native-admission-4\n",
    )
    .unwrap();
    let cold_cache_root = directory.path().join("cold-cache");
    assert!(!cold_cache_root.exists());
    std::fs::create_dir(&cold_cache_root).unwrap();
    let cold_cache = cache(&cold_cache_root);
    let (direct, reopened) = local(
        directory.path(),
        cold_cache.clone(),
        configuration.clone(),
        false,
    );
    assert_eq!(
        std::fs::read(directory.path().join("ledger/admission.format")).unwrap(),
        b"tinytree-native-admission-5\n"
    );
    assert!(
        direct
            .descriptor(frozen.descriptor.key)
            .await
            .unwrap()
            .is_none()
    );
    let cloud_before_metadata = counts.cloud_rpcs.load(Ordering::SeqCst);
    assert_eq!(
        reopened
            .io
            .read_metadata(frozen.descriptor.key)
            .await
            .unwrap()
            .descriptor(),
        frozen.descriptor,
        "deep commit refetch uses retained snapshot membership after all query scopes close"
    );
    assert!(
        counts.cloud_rpcs.load(Ordering::SeqCst) > cloud_before_metadata,
        "evicted canonical metadata must reauthorize the retained V3 source"
    );
    assert_eq!(
        reopened
            .io
            .read_metadata(tree_key)
            .await
            .unwrap()
            .descriptor()
            .key,
        tree_key
    );
    let cloud_before_file = counts.cloud_rpcs.load(Ordering::SeqCst);
    assert_eq!(bytes(reopened.io.as_ref(), file).await, expected_file);
    assert!(
        counts.cloud_rpcs.load(Ordering::SeqCst) > cloud_before_file,
        "empty CAS must trigger fresh V3 selected-version FILE delivery"
    );
    assert_eq!(counts.holds.load(Ordering::SeqCst), 0);
    assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), 0);
    assert_eq!(counts.graph_rpcs.load(Ordering::SeqCst), 0);
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        0,
        "snapshot reads must never use generic repository reads"
    );
    reopened.wait_for_idle().await;
    drop(reopened);
    drop(direct);
    let mut foreign = configuration.clone();
    foreign.remote.token_env = Some("TINYTREE_REMOTE_OTHER_TOKEN".into());
    let (foreign_direct, other) = local(directory.path(), cold_cache.clone(), foreign, false);
    let before = counts.query_rpcs.load(Ordering::SeqCst);
    assert!(
        other.io.read_metadata(frozen.descriptor.key).await.is_err(),
        "fetched canonical snapshot bytes cannot become authored under another credential"
    );
    assert!(other.io.descriptor(tree_key).await.is_err());
    assert!(
        other.io.read_file(file).await.is_err(),
        "warm fetched snapshot FILE bytes remain bound to their current authority"
    );
    assert_eq!(
        counts.query_rpcs.load(Ordering::SeqCst),
        before,
        "stale local provenance must fail before upstream query attempts"
    );
    assert!(
        other
            .control
            .admit_retained_version(selector, selected, Arc::new(()))
            .await
            .is_err(),
        "V3 selectors do not transfer private owner authority"
    );
    assert!(
        other
            .query
            .open_cloud_scope(
                native_broker::OpenCloudQueryScope {
                    source,
                    overlay: vec![],
                    replay_key: [113; 32]
                },
                Arc::new(())
            )
            .await
            .is_err(),
        "private retained selectors cannot cross owners"
    );
    other.wait_for_idle().await;
    drop(other);
    drop(foreign_direct);
    QueryRotationFixture {
        directory,
        cache: cold_cache,
        configuration,
        file,
        bytes: expected_file,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_factory_bounds_independent_runtime_data_but_allows_release_only_cleanup() {
    use native_broker::{NativeQueryIo, OpenQueryScope, query_protocol as q};
    let (_serverdir, store, _fixture, head) = seed();
    let indexdir = private_dir();
    let index = tinytree_index::Index::open(indexdir.path(), &Default::default()).unwrap();
    for object in _fixture
        .objects()
        .iter()
        .filter(|object| object.descriptor.key.kind == m::Kind::Commit)
    {
        index
            .ingest("example", object.descriptor, &object.bytes)
            .unwrap();
    }
    let service = RepositoryService::new(
        store,
        ServiceConfig::default(),
        vec!["example".into()],
        vec![
            Principal::new("test", TOKEN, vec!["example".into()], true)
                .unwrap()
                .with_retention(true),
        ],
    )
    .unwrap()
    .with_query_index(index)
    .unwrap();
    let counts = Counters::new();
    let server = Server::start_service(service, counts.clone(), None).await;
    let directory = private_dir();
    let (direct, repo) = local(
        directory.path(),
        cache(directory.path()),
        config(&server),
        true,
    );
    let factory =
        jj_native_remote::RemoteRestoreIo::new(repo.io.clone(), repo.query.clone()).unwrap();
    let raw_objects: Arc<dyn NativeObjectIo> = repo.io.clone();
    let raw_query: Arc<dyn NativeQueryIo> = repo.query.clone();
    assert!(factory.matches(&raw_objects, &raw_query));
    let otherdir = private_dir();
    let (_other_direct, other) = local(
        otherdir.path(),
        cache(otherdir.path()),
        config(&server),
        true,
    );
    assert!(jj_native_remote::RemoteRestoreIo::new(repo.io.clone(), other.query.clone()).is_err());
    let other_query: Arc<dyn NativeQueryIo> = other.query.clone();
    assert!(!factory.matches(&raw_objects, &other_query));

    repo.control.admit_reference("main").await.unwrap();
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
    let tree = m::ObjectKey {
        kind: m::Kind::Tree,
        id: m::ObjectId(commit.root_tree.terms[0].0),
    };
    assert!(direct.descriptor(tree).await.unwrap().is_none());
    counts.paused.store(true, Ordering::SeqCst);
    counts.capture_timeouts.store(true, Ordering::SeqCst);
    let original = tokio::time::Instant::now() + Duration::from_millis(400);
    let (objects, _) = factory
        .bind(original, Duration::from_secs(5), Arc::new(()))
        .unwrap();
    let file = first_file(&_fixture).descriptor.key;
    let before_file = counts.files.load(Ordering::SeqCst);
    assert!(objects.descriptor(file).await.is_err());
    assert!(objects.read_metadata(file).await.is_err());
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        before_file,
        "metadata-only FILE requests must fail before raw delegation"
    );
    // This is the same boundary as the admin worker: a new runtime has no
    // inherited task-local. Only the explicitly bound provider carries time.
    let read = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(objects.read_metadata(tree))
    });
    let result = tokio::time::timeout(Duration::from_secs(2), read)
        .await
        .expect("metadata renewed the configured wire deadline")
        .unwrap();
    assert!(result.is_err());
    let timeouts = counts.metadata_timeouts.lock().unwrap().clone();
    assert_eq!(timeouts.len(), 1, "one bounded, non-retrying physical read");
    let encoded = &timeouts[0];
    let (digits, unit) = encoded.split_at(encoded.len() - 1);
    let amount: u64 = digits.parse().expect("valid grpc-timeout header required");
    let duration = match unit {
        "n" => Duration::from_nanos(amount),
        "u" => Duration::from_micros(amount),
        "m" => Duration::from_millis(amount),
        "S" => Duration::from_secs(amount),
        _ => panic!("wire deadline was not bounded to the original operation"),
    };
    assert!(
        duration <= Duration::from_millis(400),
        "wire must retain original allowance"
    );
    assert!(direct.descriptor(tree).await.unwrap().is_none());
    counts.paused.store(false, Ordering::SeqCst);
    repo.wait_for_idle().await;
    counts.capture_timeouts.store(false, Ordering::SeqCst);

    let original = tokio::time::Instant::now() + Duration::from_secs(2);
    let (objects, query) = factory
        .bind(original, Duration::from_millis(300), Arc::new(()))
        .unwrap();
    let opened = query
        .open_scope(
            OpenQueryScope {
                roots: vec![head],
                overlay: vec![],
                replay_key: [231; 32],
                query_identity_version: 2,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let scope = opened.value.handle;
    let query_replay = opened.value.bound_query_id([233; 16]).unwrap();
    drop(opened);
    // A real non-root canonical read performs its own internal CloseQuery.
    // Capture that final RPC separately from the later explicit cleanup pair.
    counts.query_timeouts.lock().unwrap().clear();
    counts.capture_timeouts.store(true, Ordering::SeqCst);
    let canonical = query.read_commit(scope, head, Arc::new(())).await.unwrap();
    assert_eq!(canonical.value.descriptor().key.id.0, head.0);
    drop(canonical);
    counts.capture_timeouts.store(false, Ordering::SeqCst);
    let timeouts = counts.query_timeouts.lock().unwrap().clone();
    assert!(
        timeouts.len() >= 2,
        "canonical read must evaluate then explicitly close"
    );
    let encoded = timeouts
        .last()
        .expect("canonical read must release its upstream query");
    let (digits, unit) = encoded.split_at(encoded.len() - 1);
    let amount: u64 = digits
        .parse()
        .expect("inline cleanup grpc-timeout required");
    let duration = match unit {
        "n" => Duration::from_nanos(amount),
        "u" => Duration::from_micros(amount),
        "m" => Duration::from_millis(amount),
        "S" => Duration::from_secs(amount),
        _ => panic!("inline cleanup renewed its wire allowance"),
    };
    assert!(
        duration <= Duration::from_millis(300),
        "inline close must obey original and facade cleanup bounds"
    );
    let begun = query
        .begin(
            native_broker::BeginQuery {
                scope,
                expression: q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                        commits: vec![q::CommitId {
                            value: m::CommitId::ROOT.0.to_vec(),
                        }],
                    })),
                },
                projection: q::Projection::Ids,
                page_size: 1,
                replay_key: query_replay,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let handle = begun.value.handle;
    drop(begun);
    let page = query.page(handle, 0, Arc::new(())).await.unwrap();
    assert_eq!(page.value.rows.len(), 1);
    drop(page);
    tokio::time::sleep_until(original + Duration::from_millis(1)).await;
    let before_query = counts.query_rpcs.load(Ordering::SeqCst);
    let before_metadata = counts.metadata_rpcs.load(Ordering::SeqCst);
    let error = query
        .facts(
            scope,
            vec![q::GraphFact {
                fact: Some(q::graph_fact::Fact::HasId(q::HasId {
                    commit: Some(q::CommitId { value: vec![0; 32] }),
                })),
            }],
            Arc::new(()),
        )
        .await;
    let error = match error {
        Err(error) => error,
        Ok(_) => panic!("expired data request succeeded"),
    };
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert!(objects.read_metadata(tree).await.is_err());
    assert_eq!(counts.query_rpcs.load(Ordering::SeqCst), before_query);
    assert_eq!(counts.metadata_rpcs.load(Ordering::SeqCst), before_metadata);
    // Both release RPCs are permitted after data expiry. Their physical SDK
    // wire budgets must use the cleanup bound, not configured fresh allowances.
    counts.query_timeouts.lock().unwrap().clear();
    counts.capture_timeouts.store(true, Ordering::SeqCst);
    query.close_query(handle, Arc::new(())).await.unwrap();
    query.close_scope(scope, Arc::new(())).await.unwrap();
    counts.capture_timeouts.store(false, Ordering::SeqCst);
    let cleanup_timeouts = counts.query_timeouts.lock().unwrap().clone();
    assert_eq!(cleanup_timeouts.len(), 2);
    for encoded in cleanup_timeouts {
        let (digits, unit) = encoded.split_at(encoded.len() - 1);
        let amount: u64 = digits.parse().expect("cleanup grpc-timeout required");
        let duration = match unit {
            "n" => Duration::from_nanos(amount),
            "u" => Duration::from_micros(amount),
            "m" => Duration::from_millis(amount),
            "S" => Duration::from_secs(amount),
            _ => panic!("cleanup renewed a configured wire allowance"),
        };
        assert!(duration <= Duration::from_millis(300));
    }
    assert!(counts.query_rpcs.load(Ordering::SeqCst) > before_query);
    let opened = repo
        .query
        .open_scope(
            OpenQueryScope {
                roots: vec![m::CommitId::ROOT],
                overlay: vec![],
                replay_key: [232; 32],
                query_identity_version: 2,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    repo.query
        .close_scope(opened.value.handle, Arc::new(()))
        .await
        .unwrap();
    drop(opened);
    assert!(
        repo.io.read_metadata(tree).await.is_ok(),
        "ordinary provider must not inherit restore expiry"
    );
    assert_eq!(counts.files.load(Ordering::SeqCst), 0);
    repo.wait_for_idle().await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indexed_search_local_cursors_bind_source_inputs_and_replay_without_pinning_slots() {
    use native_broker::{NativeQueryIo, OpenQueryScope, ScopedSearchQuery, query_protocol as q};
    let (_serverdir, store, fixture, old) = seed();
    let payload = b"needle first\nneedle second\n";
    let file = m::describe_file(payload);
    store.put("example", file, payload, 0).unwrap();
    let tree = m::encode(
        &m::Object::Tree(m::Tree {
            entries: vec![m::TreeEntry {
                name: "search.txt".into(),
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
    let base = fixture
        .objects()
        .iter()
        .find(|object| object.descriptor.key.id.0 == old.0)
        .unwrap();
    let m::Object::Commit(mut commit) = m::decode(&base.bytes, m::Limits::default()).unwrap()
    else {
        unreachable!()
    };
    commit.parents = vec![old];
    commit.predecessors.clear();
    commit.root_tree = m::TreeMerge {
        terms: vec![m::TreeId(tree.descriptor.key.id.0)],
        labels: vec![String::new()],
    };
    commit.description = "indexed search cursor fixture".into();
    let commit = m::encode(&m::Object::Commit(commit), m::Limits::default()).unwrap();
    store
        .put("example", commit.descriptor, &commit.bytes, 0)
        .unwrap();
    let head = m::CommitId(commit.descriptor.key.id.0);
    store
        .publish(
            "example",
            &tinytree_store::PublishRequest {
                idempotency_key: "indexed-search-head".into(),
                reference_name: "main".into(),
                expected_generation: 1,
                new_commit: Some(head),
                staging_leases: vec![],
            },
            1,
        )
        .unwrap();
    let indexdir = private_dir();
    let index = tinytree_index::Index::open(indexdir.path(), &Default::default()).unwrap();
    let service = RepositoryService::new(
        store,
        ServiceConfig::default(),
        vec!["example".into()],
        vec![
            Principal::new("test", TOKEN, vec!["example".into()], true)
                .unwrap()
                .with_retention(true),
        ],
    )
    .unwrap()
    .with_query_index(index)
    .unwrap();
    let counts = Counters::new();
    let server = Server::start_service(service, counts.clone(), None).await;
    let directory = private_dir();
    let mut cfg = config(&server);
    cfg.remote.limits.max_inflight = 1;
    let (_direct, repo) = local(directory.path(), cache(directory.path()), cfg, true);
    repo.control.admit_reference("main").await.unwrap();
    let scope = repo
        .query
        .open_scope(
            OpenQueryScope {
                roots: vec![head],
                overlay: vec![],
                replay_key: [141; 32],
                query_identity_version: 2,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let other = repo
        .query
        .open_scope(
            OpenQueryScope {
                roots: vec![head],
                overlay: vec![],
                replay_key: [142; 32],
                query_identity_version: 2,
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let input = ScopedSearchQuery {
        scope: scope.value.handle,
        revision: head,
        pattern: b"needle".to_vec(),
        path_prefix: "".into(),
        policy: q::SearchPolicy::LiteralBytesV1,
        page_size: 1,
        cursor: vec![],
    };
    let file_reads = counts.files.load(Ordering::SeqCst);
    let first = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = repo
                .query
                .search(input.clone(), Arc::new(()))
                .await
                .unwrap();
            if page.value.coverage != q::SearchCoverage::SearchWarming as i32 {
                break page;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(first.value.coverage, q::SearchCoverage::SearchMore as i32);
    assert_eq!(first.value.hits[0].path, "search.txt");
    assert_eq!(first.value.hits[0].byte_offset, 0);
    assert!(!first.value.cursor.is_empty());
    let replay = tokio::time::timeout(
        Duration::from_secs(2),
        repo.query.search(input.clone(), Arc::new(())),
    )
    .await
    .expect("retained search reply held the single upstream slot")
    .unwrap();
    assert_eq!(replay.value, first.value);
    let continuing = ScopedSearchQuery {
        cursor: first.value.cursor.clone(),
        ..input.clone()
    };
    let next = repo
        .query
        .clone()
        .search(continuing.clone(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(next.value.hits.len(), 1);
    assert_eq!(next.value.hits[0].byte_offset, 13);
    assert_eq!(next.value.hits[0].line, 2);
    assert_eq!(next.value.hits[0].byte_column, 0);
    let mut final_cursor = next.value.cursor.clone();
    let mut final_coverage = next.value.coverage;
    let mut pages = 0;
    while final_coverage == q::SearchCoverage::SearchMore as i32 {
        assert!(pages < 3, "bounded fixture failed to terminate its cursor");
        let final_page = repo
            .query
            .search(
                ScopedSearchQuery {
                    cursor: final_cursor,
                    ..input.clone()
                },
                Arc::new(()),
            )
            .await
            .unwrap();
        assert!(
            final_page.value.hits.is_empty(),
            "two-match file returned duplicate or invented matches"
        );
        final_cursor = final_page.value.cursor.clone();
        final_coverage = final_page.value.coverage;
        pages += 1;
    }
    assert_eq!(final_coverage, q::SearchCoverage::SearchComplete as i32);
    assert_eq!(
        counts.files.load(Ordering::SeqCst),
        file_reads,
        "search does not download native FILE bodies into the client"
    );
    let mut tampered = continuing.clone();
    *tampered.cursor.last_mut().unwrap() ^= 1;
    let invalid = [
        ("authentication", tampered),
        (
            "pattern",
            ScopedSearchQuery {
                pattern: b"first".to_vec(),
                ..continuing.clone()
            },
        ),
        (
            "prefix",
            ScopedSearchQuery {
                path_prefix: "search.txt".into(),
                ..continuing.clone()
            },
        ),
        (
            "revision",
            ScopedSearchQuery {
                revision: old,
                ..continuing.clone()
            },
        ),
        (
            "page size",
            ScopedSearchQuery {
                page_size: 2,
                ..continuing.clone()
            },
        ),
        (
            "local scope",
            ScopedSearchQuery {
                scope: other.value.handle,
                ..continuing.clone()
            },
        ),
    ];
    let before = counts.query_rpcs.load(Ordering::SeqCst);
    for (behavior, input) in invalid {
        assert_eq!(
            repo.query
                .search(input, Arc::new(()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument,
            "{behavior}"
        );
    }
    assert_eq!(
        counts.query_rpcs.load(Ordering::SeqCst),
        before,
        "invalid local cursors must fail before any upstream RPC"
    );
    repo.query
        .close_scope(scope.value.handle, Arc::new(()))
        .await
        .unwrap();
    assert!(
        repo.query.search(continuing, Arc::new(())).await.is_err(),
        "closing a captured scope invalidates its continuations"
    );
    repo.query
        .close_scope(other.value.handle, Arc::new(()))
        .await
        .unwrap();
    drop((first, replay, next, scope, other));
    repo.wait_for_idle().await;
    server.stop().await;
}
