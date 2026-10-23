#![cfg(unix)]
use futures::io::Cursor;
use jj_native_backend::{CopyCatalogLimits, DirectObjectIo, NativeObjectIo};
use native_broker::{BrokerObjectIo, BrokerService, Config};
use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::oneshot;
fn private(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::create_dir_all(path).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
fn config() -> Config {
    Config {
        max_metadata_bytes: 1024 * 1024,
        max_file_bytes: 16 * 1024 * 1024,
        max_catalog_bytes: 4 * 1024 * 1024,
        max_catalog_objects: 1000,
        max_memory_bytes: 32 * 1024 * 1024,
        max_temporary_bytes: 32 * 1024 * 1024,
        ..Default::default()
    }
}
fn backend_config() -> jj_native_backend::Config {
    let mut c = jj_native_backend::Config::default();
    c.store.max_file_bytes = 16 * 1024 * 1024;
    c.store.max_staging_bytes = 32 * 1024 * 1024;
    c
}
struct Running {
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<(), tonic::Status>>>,
}
impl Running {
    async fn start(socket: &Path, provider: Arc<dyn NativeObjectIo>, config: Config) -> Self {
        let service =
            BrokerService::new(HashMap::from([("example".into(), provider)]), config).unwrap();
        let (stop, signal) = oneshot::channel();
        let path = socket.to_owned();
        let task = tokio::spawn(async move {
            native_broker::serve_unix(&path, service, async {
                let _ = signal.await;
            })
            .await
        });
        let mut out = Self {
            stop: Some(stop),
            task: Some(task),
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if out.task.as_ref().unwrap().is_finished() {
                    panic!(
                        "broker failed startup: {:?}",
                        out.task.take().unwrap().await
                    );
                }
                if socket.exists() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        out
    }
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
struct OwnedChild {
    child: Option<Child>,
}
impl OwnedChild {
    fn spawn(socket: &Path, spool: &Path, mode: &str, args: &[&str]) -> Self {
        private(spool);
        let executable = Path::new(env!("BROKER_CLIENT_DRIVER"))
            .canonicalize()
            .unwrap();
        let child = Command::new(executable)
            .arg(socket)
            .arg(spool)
            .arg(mode)
            .args(args)
            .env_clear()
            .env("PATH", "")
            .env("HOME", spool)
            .current_dir(spool)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        Self { child: Some(child) }
    }
    async fn finish(mut self) -> String {
        tokio::task::spawn_blocking(move || {
            let child = self.child.as_mut().unwrap();
            let deadline = Instant::now() + Duration::from_secs(20);
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                assert!(Instant::now() < deadline, "owned broker client timed out");
                std::thread::sleep(Duration::from_millis(5));
            };
            let mut output = String::new();
            child
                .stdout
                .take()
                .unwrap()
                .take(4097)
                .read_to_string(&mut output)
                .unwrap();
            assert!(output.len() <= 4096);
            assert!(status.success(), "broker client failed: {output}");
            self.child.take();
            output.trim().to_owned()
        })
        .await
        .unwrap()
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let deadline = Instant::now() + Duration::from_secs(5);
            while child.try_wait().ok().flatten().is_none() {
                if Instant::now() >= deadline {
                    let _ = std::thread::spawn(move || {
                        let _ = child.wait();
                    });
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_jj_processes_share_one_store_and_read_after_host_restart() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let backend = root.join("store");
    private(&backend);
    let socket = root.join("broker.sock");
    let provider = Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap());
    assert!(DirectObjectIo::load(&backend, backend_config()).is_err());
    let host = Running::start(&socket, provider.clone(), config()).await;
    let one = OwnedChild::spawn(&socket, &root.join("one-spool"), "one", &[]);
    let two = OwnedChild::spawn(&socket, &root.join("two-spool"), "two", &[]);
    let (one, two) = tokio::join!(one.finish(), two.finish());
    let one: Vec<_> = one.split_whitespace().collect();
    let two: Vec<_> = two.split_whitespace().collect();
    assert_eq!(one[0], "OK");
    assert_eq!(two[0], "OK");
    assert_ne!(one[2], two[2]);
    let catalog = provider
        .copy_catalog_snapshot(CopyCatalogLimits {
            max_objects: 1000,
            max_metadata_bytes: 4 * 1024 * 1024,
            max_edges: 10000,
        })
        .await
        .unwrap()
        .into_entries();
    assert_eq!(catalog.len(), 2);
    let unknown = OwnedChild::spawn(&socket, &root.join("unknown-spool"), "unknown", &[])
        .finish()
        .await;
    assert_eq!(unknown, "UNKNOWN_DENIED");
    host.stop().await;
    drop(provider);
    assert!(!socket.exists());
    let provider = Arc::new(DirectObjectIo::load(&backend, backend_config()).unwrap());
    let host = Running::start(&socket, provider, config()).await;
    assert_eq!(
        OwnedChild::spawn(&socket, &root.join("restart-one"), "read", &[one[1], "one"])
            .finish()
            .await,
        "READ_OK"
    );
    assert_eq!(
        OwnedChild::spawn(&socket, &root.join("restart-two"), "read", &[two[1], "two"])
            .finish()
            .await,
        "READ_OK"
    );
    host.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_external_executor_upload_releases_admission_for_later_work() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let backend = root.join("store");
    private(&backend);
    let provider = Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap());
    let socket = root.join("broker.sock");
    let mut c = config();
    c.max_inflight = 1;
    let host = Running::start(&socket, provider, c).await;
    let result = OwnedChild::spawn(&socket, &root.join("cancel-spool"), "cancel", &[])
        .finish()
        .await;
    assert!(result.starts_with("OK "));
    host.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_file_stream_expires_without_leaking_server_admission() {
    use tonic::{Request, codec::CompressionEncoding, transport::Endpoint};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let backend = root.join("store");
    private(&backend);
    let spool = root.join("spool");
    private(&spool);
    let provider = Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap());
    let mut state = 17u64;
    let bytes: Vec<_> = (0..8 * 1024 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let file = provider
        .write_file(&mut Cursor::new(bytes), 16 * 1024 * 1024)
        .await
        .unwrap();
    let socket = root.join("broker.sock");
    let mut c = config();
    c.max_inflight = 1;
    c.policy.operation_timeout_ms = 350;
    c.policy.stream_idle_timeout_ms = 200;
    let host = Running::start(&socket, provider, c.clone()).await;
    let path = socket.clone();
    let channel = Endpoint::from_static("http://[::]:50051")
        .connect_with_connector(tower::service_fn(move |_: http::Uri| {
            let path = path.clone();
            async move {
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                    tokio::net::UnixStream::connect(path).await?,
                ))
            }
        }))
        .await
        .unwrap();
    let mut raw = native_broker::proto::object_io_client::ObjectIoClient::new(channel)
        .send_compressed(CompressionEncoding::Zstd);
    let held = raw
        .read_file(Request::new(native_broker::proto::ObjectRequest {
            store_id: "example".into(),
            kind: 1,
            id: file.0.to_vec(),
        }))
        .await
        .unwrap()
        .into_inner();
    let client = BrokerObjectIo::connect(&socket, "example", &spool, config())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(40),
            client.descriptor(tinytree_model::empty_tree().descriptor.key)
        )
        .await
        .is_err()
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        client
            .descriptor(tinytree_model::empty_tree().descriptor.key)
            .await
            .unwrap()
            .is_some()
    );
    drop(held);
    drop(raw);
    drop(client);
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_metadata_limits_do_not_reject_valid_larger_file_frames() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let backend = root.join("store");
    private(&backend);
    let spool = root.join("spool");
    private(&spool);
    let provider = Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap());
    let socket = root.join("broker.sock");
    let mut c = config();
    c.max_metadata_bytes = 1024;
    c.max_frame_bytes = 128 * 1024;
    let host = Running::start(&socket, provider, c.clone()).await;
    let client = BrokerObjectIo::connect(&socket, "example", &spool, c)
        .await
        .unwrap();
    let bytes = vec![19; 320 * 1024];
    let id = client
        .write_file(&mut Cursor::new(bytes.clone()), 16 * 1024 * 1024)
        .await
        .unwrap();
    let (_, mut reader) = client.read_file(id).await.unwrap().into_parts();
    let mut actual = Vec::new();
    futures::AsyncReadExt::read_to_end(&mut reader, &mut actual)
        .await
        .unwrap();
    assert_eq!(actual, bytes);
    drop(reader);
    drop(client);
    host.stop().await;
}
struct Probe(std::sync::Arc<std::sync::atomic::AtomicBool>);
impl futures::AsyncRead for Probe {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
        _: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        std::task::Poll::Pending
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_upload_does_not_poll_or_buffer_input_before_client_admission() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let backend = root.join("store");
    private(&backend);
    let spool = root.join("spool");
    private(&spool);
    let provider = Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap());
    let socket = root.join("broker.sock");
    let host = Running::start(&socket, provider, config()).await;
    let mut c = config();
    c.max_inflight = 1;
    c.policy.operation_timeout_ms = 2000;
    let client = Arc::new(
        BrokerObjectIo::connect(&socket, "example", &spool, c)
            .await
            .unwrap(),
    );
    let first_polled = Arc::new(AtomicBool::new(false));
    let flag = first_polled.clone();
    let first_client = client.clone();
    let first = tokio::spawn(async move { first_client.write_file(&mut Probe(flag), 1024).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !first_polled.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let second_polled = Arc::new(AtomicBool::new(false));
    let mut second = Probe(second_polled.clone());
    assert!(
        tokio::time::timeout(
            Duration::from_millis(40),
            client.write_file(&mut second, 1024)
        )
        .await
        .is_err()
    );
    assert!(!second_polled.load(Ordering::SeqCst));
    first.abort();
    let _ = first.await;
    assert!(
        client
            .descriptor(tinytree_model::empty_tree().descriptor.key)
            .await
            .unwrap()
            .is_some()
    );
    drop(client);
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_tiny_readers_keep_their_buffer_memory_charged_until_drop() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let backend = root.join("store");
    private(&backend);
    let spool = root.join("spool");
    private(&spool);
    let provider = Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap());
    let file = provider
        .write_file(&mut Cursor::new(vec![1]), 16 * 1024 * 1024)
        .await
        .unwrap();
    let socket = root.join("broker.sock");
    let mut c = config();
    c.max_metadata_bytes = 1024;
    c.max_frame_bytes = 1024;
    c.max_catalog_bytes = 1024;
    c.max_catalog_objects = 1;
    c.max_memory_bytes = 1024 * 1024;
    let host = Running::start(&socket, provider, c.clone()).await;
    c.max_file_bytes = 1;
    c.max_temporary_bytes = 1024 * 1024;
    c.policy.operation_timeout_ms = 200;
    let client = BrokerObjectIo::connect(&socket, "example", &spool, c)
        .await
        .unwrap();
    let mut held = Vec::new();
    let mut exhausted = false;
    for _ in 0..600 {
        match client.read_file(file).await {
            Ok(source) => {
                let (_, mut reader) = source.into_parts();
                let mut out = [0];
                assert_eq!(
                    futures::AsyncReadExt::read(&mut reader, &mut out)
                        .await
                        .unwrap(),
                    1
                );
                held.push(reader);
            }
            Err(_) => {
                exhausted = true;
                break;
            }
        }
    }
    assert!(
        held.len() > 32,
        "tiny reads failed before meaningful memory pressure"
    );
    assert!(
        exhausted,
        "retained readers escaped aggregate memory admission"
    );
    drop(held);
    let (_, mut reader) = client.read_file(file).await.unwrap().into_parts();
    let mut bytes = Vec::new();
    futures::AsyncReadExt::read_to_end(&mut reader, &mut bytes)
        .await
        .unwrap();
    assert_eq!(bytes, vec![1]);
    drop(reader);
    drop(client);
    host.stop().await;
}
#[derive(Debug)]
struct PausedDescriptor {
    inner: Arc<dyn NativeObjectIo>,
    entered: Arc<std::sync::atomic::AtomicUsize>,
    release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}
#[async_trait::async_trait]
impl NativeObjectIo for PausedDescriptor {
    async fn descriptor(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<tinytree_model::Descriptor>> {
        if self
            .entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            let release = self.release.clone();
            tokio::task::spawn_blocking(move || {
                let (lock, wake) = &*release;
                let mut done = lock.lock().unwrap();
                while !*done {
                    done = wake.wait(done).unwrap();
                }
            })
            .await
            .unwrap();
        }
        self.inner.descriptor(key).await
    }
    async fn read_metadata(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.inner.read_metadata(key).await
    }
    async fn write_metadata(
        &self,
        value: tinytree_model::Object,
    ) -> jj_lib::backend::BackendResult<tinytree_model::Descriptor> {
        self.inner.write_metadata(value).await
    }
    async fn read_file(
        &self,
        id: tinytree_model::FileId,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        self.inner.read_file(id).await
    }
    async fn write_file(
        &self,
        input: &mut (dyn futures::AsyncRead + Send + Unpin),
        max: u64,
    ) -> jj_lib::backend::BackendResult<tinytree_model::FileId> {
        self.inner.write_file(input, max).await
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        self.inner.copy_catalog_snapshot(limits).await
    }
}
struct Release(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
impl Release {
    fn release(&self) {
        let (lock, wake) = &*self.0;
        *lock.lock().unwrap() = true;
        wake.notify_all();
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        self.release();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timed_out_provider_job_keeps_admission_until_its_blocking_work_finishes() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let backend = root.join("store");
    private(&backend);
    let spool = root.join("spool");
    private(&spool);
    let release = Release(Arc::new((
        std::sync::Mutex::new(false),
        std::sync::Condvar::new(),
    )));
    let entered = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(PausedDescriptor {
        inner: Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap()),
        entered: entered.clone(),
        release: release.0.clone(),
    });
    let socket = root.join("broker.sock");
    let mut server_config = config();
    server_config.max_inflight = 1;
    let host = Running::start(&socket, provider, server_config).await;
    let mut c = config();
    c.policy.operation_timeout_ms = 100;
    let client = BrokerObjectIo::connect(&socket, "example", &spool, c)
        .await
        .unwrap();
    assert!(
        client
            .descriptor(tinytree_model::empty_tree().descriptor.key)
            .await
            .is_err()
    );
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    assert!(
        client
            .descriptor(tinytree_model::empty_tree().descriptor.key)
            .await
            .is_err()
    );
    assert_eq!(
        entered.load(Ordering::SeqCst),
        1,
        "cancelled blocking job lost its admission guard"
    );
    release.release();
    drop(client);
    let client = BrokerObjectIo::connect(&socket, "example", &spool, config())
        .await
        .unwrap();
    assert!(
        client
            .descriptor(tinytree_model::empty_tree().descriptor.key)
            .await
            .unwrap()
            .is_some()
    );
    drop(client);
    host.stop().await;
}
#[derive(Clone)]
struct EarlyAck;
#[tonic::async_trait]
impl native_broker::proto::object_io_server::ObjectIo for EarlyAck {
    async fn descriptor(
        &self,
        _: tonic::Request<native_broker::proto::ObjectRequest>,
    ) -> Result<tonic::Response<native_broker::proto::DescriptorReply>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }
    async fn read_metadata(
        &self,
        _: tonic::Request<native_broker::proto::ObjectRequest>,
    ) -> Result<tonic::Response<native_broker::proto::Metadata>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }
    async fn write_metadata(
        &self,
        _: tonic::Request<native_broker::proto::MetadataWrite>,
    ) -> Result<tonic::Response<native_broker::proto::ObjectDescriptor>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }
    type ReadFileStream =
        futures::stream::Empty<Result<native_broker::proto::ReadFrame, tonic::Status>>;
    async fn read_file(
        &self,
        _: tonic::Request<native_broker::proto::ObjectRequest>,
    ) -> Result<tonic::Response<Self::ReadFileStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }
    async fn write_file(
        &self,
        _: tonic::Request<tonic::Streaming<native_broker::proto::WriteFrame>>,
    ) -> Result<tonic::Response<native_broker::proto::ObjectDescriptor>, tonic::Status> {
        let descriptor = tinytree_model::describe_file(&[]);
        Ok(tonic::Response::new(
            native_broker::proto::ObjectDescriptor {
                kind: 1,
                id: descriptor.key.id.0.to_vec(),
                size_bytes: 0,
                format_version: descriptor.format_version as u32,
            },
        ))
    }
    type CopyCatalogStream =
        futures::stream::Empty<Result<native_broker::proto::CatalogFrame, tonic::Status>>;
    async fn copy_catalog(
        &self,
        _: tonic::Request<native_broker::proto::CatalogRequest>,
    ) -> Result<tonic::Response<Self::CopyCatalogStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("test"))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compatibility_early_upload_ack_cannot_wait_forever_for_a_pending_input() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let spool = root.join("spool");
    private(&spool);
    let socket = root.join("broker.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600)).unwrap();
    let (stop, signal) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(
                native_broker::proto::object_io_server::ObjectIoServer::new(EarlyAck)
                    .accept_compressed(tonic::codec::CompressionEncoding::Zstd),
            )
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                async {
                    let _ = signal.await;
                },
            )
            .await
            .map_err(|_| tonic::Status::internal("test listener"))
    });
    let host = Running {
        stop: Some(stop),
        task: Some(task),
    };
    let client = BrokerObjectIo::connect(&socket, "example", &spool, config())
        .await
        .unwrap();
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut input = Probe(flag);
    let error = tokio::time::timeout(Duration::from_secs(1), client.write_file(&mut input, 1024))
        .await
        .expect("early ACK kept awaiting pending producer")
        .unwrap_err();
    let jj_lib::backend::BackendError::Other(error) = error else {
        panic!("unexpected broker error")
    };
    assert_eq!(
        error.downcast_ref::<tonic::Status>().unwrap().code(),
        tonic::Code::DataLoss
    );
    drop(client);
    host.stop().await;
}

#[derive(Debug)]
struct ObservedWrites {
    inner: Arc<dyn NativeObjectIo>,
    files: Arc<std::sync::atomic::AtomicUsize>,
    metadata: Arc<std::sync::atomic::AtomicUsize>,
    pause: Option<Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>>,
    blocked: Arc<std::sync::atomic::AtomicBool>,
    finished: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait::async_trait]
impl NativeObjectIo for ObservedWrites {
    async fn descriptor(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<tinytree_model::Descriptor>> {
        self.inner.descriptor(key).await
    }
    async fn read_metadata(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.inner.read_metadata(key).await
    }
    async fn write_metadata(
        &self,
        value: tinytree_model::Object,
    ) -> jj_lib::backend::BackendResult<tinytree_model::Descriptor> {
        self.metadata
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.write_metadata(value).await
    }
    async fn read_file(
        &self,
        id: tinytree_model::FileId,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        self.inner.read_file(id).await
    }
    async fn write_file(
        &self,
        input: &mut (dyn futures::AsyncRead + Send + Unpin),
        max: u64,
    ) -> jj_lib::backend::BackendResult<tinytree_model::FileId> {
        use std::sync::atomic::Ordering;
        self.files.fetch_add(1, Ordering::SeqCst);
        let mut bytes = Vec::new();
        futures::AsyncReadExt::read_to_end(
            &mut futures::AsyncReadExt::take(input, max.saturating_add(1)),
            &mut bytes,
        )
        .await
        .map_err(|error| jj_lib::backend::BackendError::Other(Box::new(error)))?;
        if let Some(pause) = &self.pause {
            self.blocked.store(true, Ordering::SeqCst);
            let pause = pause.clone();
            tokio::task::spawn_blocking(move || {
                let (lock, wake) = &*pause;
                let mut done = lock.lock().unwrap();
                while !*done {
                    done = wake.wait(done).unwrap();
                }
            })
            .await
            .unwrap();
        }
        let result = self.inner.write_file(&mut Cursor::new(bytes), max).await;
        self.finished.store(true, Ordering::SeqCst);
        result
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        self.inner.copy_catalog_snapshot(limits).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_staging_is_global_across_aliases_and_survives_client_cancellation() {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let alpha_path = root.join("alpha");
    private(&alpha_path);
    let beta_path = root.join("beta");
    private(&beta_path);
    let alpha_spool = root.join("alpha-spool");
    private(&alpha_spool);
    let beta_spool = root.join("beta-spool");
    private(&beta_spool);
    let release = Release(Arc::new((
        std::sync::Mutex::new(false),
        std::sync::Condvar::new(),
    )));
    let blocked = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let alpha = Arc::new(ObservedWrites {
        inner: Arc::new(DirectObjectIo::init(&alpha_path, backend_config()).unwrap()),
        files: Arc::new(AtomicUsize::new(0)),
        metadata: Arc::new(AtomicUsize::new(0)),
        pause: Some(release.0.clone()),
        blocked: blocked.clone(),
        finished: finished.clone(),
    });
    let beta_files = Arc::new(AtomicUsize::new(0));
    let beta_metadata = Arc::new(AtomicUsize::new(0));
    let beta = Arc::new(ObservedWrites {
        inner: Arc::new(DirectObjectIo::init(&beta_path, backend_config()).unwrap()),
        files: beta_files.clone(),
        metadata: beta_metadata.clone(),
        pause: None,
        blocked: Arc::new(AtomicBool::new(false)),
        finished: Arc::new(AtomicBool::new(false)),
    });
    // Independent provider-local staging budgets would both allow a write.
    // Broker memory/call capacity also admits all three requests concurrently.
    let mut server_config = config();
    server_config.max_memory_bytes = 128 * 1024 * 1024;
    let stores: HashMap<String, Arc<dyn NativeObjectIo>> = HashMap::from([
        ("alpha".into(), alpha as Arc<dyn NativeObjectIo>),
        ("beta".into(), beta as Arc<dyn NativeObjectIo>),
    ]);
    let service = BrokerService::new(stores, server_config).unwrap();
    let socket = root.join("broker.sock");
    let path = socket.clone();
    let (stop, signal) = oneshot::channel();
    let task = tokio::spawn(async move {
        native_broker::serve_unix(&path, service, async {
            let _ = signal.await;
        })
        .await
    });
    let host = Running {
        stop: Some(stop),
        task: Some(task),
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while !socket.exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let mut short = config();
    short.policy.operation_timeout_ms = 150;
    let alpha_client = BrokerObjectIo::connect(&socket, "alpha", &alpha_spool, short.clone())
        .await
        .unwrap();
    let first = tokio::spawn(async move {
        alpha_client
            .write_file(&mut Cursor::new(b"first"), 16 * 1024 * 1024)
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !blocked.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    assert!(first.await.unwrap().is_err());
    assert!(!finished.load(Ordering::SeqCst));
    let beta_client = Arc::new(
        BrokerObjectIo::connect(&socket, "beta", &beta_spool, short)
            .await
            .unwrap(),
    );
    let mut second_input = Cursor::new(b"second");
    let write_file = beta_client.write_file(&mut second_input, 16 * 1024 * 1024);
    let write_metadata =
        beta_client.write_metadata(tinytree_model::Object::Tree(tinytree_model::Tree {
            entries: vec![],
        }));
    let (file_result, metadata_result) = tokio::join!(write_file, write_metadata);
    assert!(file_result.is_err());
    assert!(metadata_result.is_err());
    assert_eq!(
        beta_files.load(Ordering::SeqCst),
        0,
        "second alias read its input before global staging admission"
    );
    assert_eq!(
        beta_metadata.load(Ordering::SeqCst),
        0,
        "metadata bypassed another alias's global staging reservation"
    );
    release.release();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !finished.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    drop(beta_client);
    let beta_client = BrokerObjectIo::connect(&socket, "beta", &beta_spool, config())
        .await
        .unwrap();
    beta_client
        .write_file(&mut Cursor::new(b"after release"), 16 * 1024 * 1024)
        .await
        .unwrap();
    beta_client
        .write_metadata(tinytree_model::Object::Tree(tinytree_model::Tree {
            entries: vec![],
        }))
        .await
        .unwrap();
    assert_eq!(beta_files.load(Ordering::SeqCst), 1);
    assert_eq!(beta_metadata.load(Ordering::SeqCst), 1);
    drop(beta_client);
    host.stop().await;
}
