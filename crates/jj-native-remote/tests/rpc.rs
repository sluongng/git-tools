use async_trait::async_trait;
use futures::AsyncReadExt;
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
        self.counts.metadata_started.notify_one();
        while self.counts.paused.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        self.service.batch_read_objects(r).await
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
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(
                    RepositoryServer::new(Counting { service, counts })
                        .accept_compressed(CompressionEncoding::Zstd)
                        .send_compressed(CompressionEncoding::Zstd)
                        .max_decoding_message_size(20 * 1024 * 1024)
                        .max_encoding_message_size(20 * 1024 * 1024),
                )
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
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
    repo.control.admit_reference("main").await.unwrap();
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
    drop(repo);
    drop(direct);
    counts.paused.store(false, Ordering::SeqCst);
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
    assert!(admitted.metadata_objects >= 7);
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
    assert_eq!(repo.control.reconcile_pending().await.unwrap(), 1);
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
    repo.control.admit_reference("main").await.unwrap();
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
