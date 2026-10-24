#![cfg(unix)]
use futures::io::Cursor;
use jj_native_backend::{CopyCatalogLimits, DirectObjectIo, NativeObjectIo, ObjectIoResources};
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
fn socket_ready(path: &Path) -> bool {
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    // Binding publishes the inode before the server has applied its private
    // permissions. Wait for the supported client-facing startup state.
    std::fs::symlink_metadata(path).is_ok_and(|metadata| {
        metadata.file_type().is_socket() && metadata.permissions().mode() & 0o777 == 0o600
    })
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
        Self::start_service(socket, service).await
    }
    async fn start_service(socket: &Path, service: BrokerService) -> Self {
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
                if socket_ready(socket) {
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
    c.policy.operation_timeout_ms = 200;
    let access = Arc::new(TestHostAccess::default());
    let service = BrokerService::new(
        HashMap::from([("example".into(), provider as Arc<dyn NativeObjectIo>)]),
        c.clone(),
    )
    .unwrap()
    .with_host_access_resolver(access.clone())
    .unwrap();
    let local = service.source_io("example").unwrap();
    let host = Running::start_service(&socket, service).await;
    let exclusive = access.fence.clone().try_write_owned().unwrap();
    assert!(
        local
            .write_file(&mut Cursor::new(b"blocked host write"), 64)
            .await
            .is_err()
    );
    assert!(
        local
            .write_metadata(tinytree_model::Object::Tree(tinytree_model::Tree {
                entries: vec![]
            }))
            .await
            .is_err()
    );
    let waiting_local = local.clone();
    let before = access.calls.load(std::sync::atomic::Ordering::SeqCst);
    let waiting = tokio::spawn(async move { waiting_local.read_file(file).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while access.calls.load(std::sync::atomic::Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !waiting.is_finished(),
        "foreground file open waits for collection rather than returning EIO"
    );
    drop(exclusive);
    let local_reader = tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        access.fence.try_write().is_err(),
        "returned local reader owns host access"
    );

    c.max_file_bytes = 1;
    c.max_temporary_bytes = 1024 * 1024;
    c.policy.operation_timeout_ms = 200;
    let client = BrokerObjectIo::connect(&socket, "example", &spool, c)
        .await
        .unwrap();
    assert!(
        client.read_file(file).await.is_err(),
        "local reader must retain shared host memory until drop"
    );
    drop(local_reader);
    let exclusive =
        tokio::time::timeout(Duration::from_secs(2), access.fence.clone().write_owned())
            .await
            .unwrap();
    assert!(
        local.read_file(file).await.is_err(),
        "facade retries admission for each I/O"
    );
    drop(exclusive);
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
struct PausedObjectRead {
    resources: Option<ObjectIoResources>,
    inner: Arc<dyn NativeObjectIo>,
    entered: Arc<std::sync::atomic::AtomicUsize>,
    release: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}
#[async_trait::async_trait]
impl NativeObjectIo for PausedObjectRead {
    fn metadata_work_budget(&self) -> Option<ObjectIoResources> {
        self.resources
    }
    async fn descriptor_with_owner(
        &self,
        key: tinytree_model::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<Option<tinytree_model::Descriptor>> {
        if self
            .entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            == 0
        {
            let release = self.release.clone();
            let blocking_owner = owner.clone();
            tokio::task::spawn_blocking(move || {
                let _owner = blocking_owner;
                let (lock, wake) = &*release;
                let mut done = lock.lock().unwrap();
                while !*done {
                    done = wake.wait(done).unwrap();
                }
            })
            .await
            .unwrap();
        }
        self.inner.descriptor_with_owner(key, owner).await
    }
    async fn copy_catalog_snapshot_with_owner(
        &self,
        limits: CopyCatalogLimits,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        self.inner
            .copy_catalog_snapshot_with_owner(limits, owner)
            .await
    }
    async fn descriptor(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<tinytree_model::Descriptor>> {
        self.descriptor_with_owner(key, Arc::new(())).await
    }
    async fn read_metadata(
        &self,
        key: tinytree_model::ObjectKey,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.inner.read_metadata(key).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: tinytree_model::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        if self
            .entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            != 0
        {
            return self.inner.read_metadata_with_owner(key, owner).await;
        }
        let release = self.release.clone();
        let inner = self.inner.clone();
        // Deliberately return a bounded error while owned physical work drains.
        // The real provider must keep the broker's resource scope in that job.
        let task = tokio::spawn(async move {
            let owner_for_blocking = owner.clone();
            tokio::task::spawn_blocking(move || {
                let _owner = owner_for_blocking;
                let (lock, wake) = &*release;
                let mut done = lock.lock().unwrap();
                while !*done {
                    done = wake.wait(done).unwrap();
                }
            })
            .await
            .unwrap();
            inner.read_metadata_with_owner(key, owner).await
        });
        tokio::time::timeout(Duration::from_millis(200), task)
            .await
            .map_err(|_| {
                jj_lib::backend::BackendError::Other(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "metadata waiter timed out",
                )))
            })?
            .unwrap()
    }
    async fn write_metadata(
        &self,
        value: tinytree_model::Object,
    ) -> jj_lib::backend::BackendResult<tinytree_model::Descriptor> {
        self.inner.write_metadata(value).await
    }
    async fn read_file_for_cache(
        &self,
        id: tinytree_model::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        self.inner.read_file_for_cache(id, owner).await
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
    let provider = Arc::new(PausedObjectRead {
        resources: None,
        inner: Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap()),
        entered: entered.clone(),
        release: release.0.clone(),
    });
    let socket = root.join("broker.sock");
    let mut server_config = config();
    server_config.max_inflight = 1;
    server_config.policy.operation_timeout_ms = 100;
    let access = Arc::new(TestHostAccess::default());
    let service = BrokerService::new(
        HashMap::from([("example".into(), provider as Arc<dyn NativeObjectIo>)]),
        server_config,
    )
    .unwrap()
    .with_host_access_resolver(access.clone())
    .unwrap();
    let local = service.source_io("example").unwrap();
    let host = Running::start_service(&socket, service).await;
    let mut c = config();
    c.policy.operation_timeout_ms = 100;
    let client = BrokerObjectIo::connect(&socket, "example", &spool, c)
        .await
        .unwrap();
    assert!(
        local
            .descriptor(tinytree_model::empty_tree().descriptor.key)
            .await
            .is_err()
    );
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    assert!(
        access.fence.try_write().is_err(),
        "expired local waiter retains host fence through blocking work"
    );
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
    let _exclusive =
        tokio::time::timeout(Duration::from_secs(2), access.fence.clone().write_owned())
            .await
            .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_metadata_waiter_keeps_owners_until_local_and_provider_jobs_finish() {
    // Exercise each global pool independently: either missing reservation would
    // otherwise let the second alias enter while the first job is still live.
    for resources in [
        ObjectIoResources {
            memory_bytes: 128 * 1024 * 1024,
            temporary_bytes: 0,
        },
        ObjectIoResources {
            memory_bytes: 1,
            temporary_bytes: 64 * 1024 * 1024,
        },
    ] {
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
        let provider: Arc<dyn NativeObjectIo> = Arc::new(PausedObjectRead {
            resources: Some(resources),
            inner: Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap()),
            entered: entered.clone(),
            release: release.0.clone(),
        });
        let socket = root.join("broker.sock");
        let mut server_config = config();
        server_config.max_inflight = 2;
        server_config.max_memory_bytes = 128 * 1024 * 1024;
        server_config.max_temporary_bytes = 64 * 1024 * 1024;
        server_config.policy.operation_timeout_ms = 1000;
        let stores = HashMap::from([
            ("example".into(), provider.clone()),
            ("peer".into(), provider),
        ]);
        let mut insufficient = server_config.clone();
        if resources.temporary_bytes == 0 {
            insufficient.max_memory_bytes -= 1;
        } else {
            insufficient.max_temporary_bytes -= 1;
        }
        assert!(
            BrokerService::new(stores.clone(), insufficient).is_err(),
            "provider workset must fit pools before control registration"
        );
        let access = Arc::new(TestHostAccess::default());
        let service = BrokerService::new(stores, server_config)
            .unwrap()
            .with_host_access_resolver(access.clone())
            .unwrap();
        let host = Running::start_service(&socket, service.clone()).await;
        let mut client_config = config();
        client_config.policy.operation_timeout_ms = 1000;
        let client = BrokerObjectIo::connect(&socket, "example", &spool, client_config)
            .await
            .unwrap();
        let key = tinytree_model::empty_tree().descriptor.key;
        let owner = Arc::new(());
        let weak = Arc::downgrade(&owner);
        let task_client = client.clone();
        let task =
            tokio::spawn(async move { task_client.read_metadata_with_owner(key, owner).await });
        tokio::time::timeout(Duration::from_secs(3), async {
            while entered.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        task.abort();
        let _ = task.await;
        assert!(
            weak.upgrade().is_some(),
            "cancelled caller lost its local read owner"
        );
        // The provider returns its timeout while the blocking job remains live.
        // Once the bounded client job completes, only the remote Work guard remains.
        tokio::time::timeout(Duration::from_secs(3), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        let mut probe_config = config();
        probe_config.policy.operation_timeout_ms = 100;
        let probe = BrokerObjectIo::connect(&socket, "peer", &spool, probe_config)
            .await
            .unwrap();
        let local_probe = service.source_io("peer").unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), local_probe.read_metadata(key))
                .await
                .is_err(),
            "local mount must share RPC admission"
        );
        let exclusive = tokio::time::timeout(
            Duration::from_millis(500),
            access.fence.clone().write_owned(),
        )
        .await
        .unwrap();
        drop(exclusive); // cancelled pre-admission sources must not hold host fences

        assert!(probe.read_metadata(key).await.is_err());
        assert!(probe.descriptor(key).await.is_err());
        assert!(
            probe
                .copy_catalog_snapshot(CopyCatalogLimits {
                    max_objects: 10,
                    max_metadata_bytes: 1024,
                    max_edges: 10,
                })
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), service.drain())
                .await
                .is_err(),
            "drain must await detached physical work"
        );
        assert_eq!(
            entered.load(Ordering::SeqCst),
            1,
            "provider's detached metadata work lost the server admission owner"
        );
        release.release();
        let result = client.read_metadata(key).await.unwrap();
        assert_eq!(result.descriptor().key, key);
        drop((client, probe));
        host.stop().await;
    }
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
    async fn descriptor_with_owner(
        &self,
        key: tinytree_model::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<Option<tinytree_model::Descriptor>> {
        self.inner.descriptor_with_owner(key, owner).await
    }
    async fn copy_catalog_snapshot_with_owner(
        &self,
        limits: CopyCatalogLimits,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        self.inner
            .copy_catalog_snapshot_with_owner(limits, owner)
            .await
    }
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
    async fn read_metadata_with_owner(
        &self,
        key: tinytree_model::ObjectKey,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.inner.read_metadata_with_owner(key, owner).await
    }
    async fn write_metadata(
        &self,
        value: tinytree_model::Object,
    ) -> jj_lib::backend::BackendResult<tinytree_model::Descriptor> {
        self.metadata
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.write_metadata(value).await
    }
    async fn read_file_for_cache(
        &self,
        id: tinytree_model::FileId,
        owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        self.inner.read_file_for_cache(id, owner).await
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
        while !socket_ready(&socket) {
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

#[derive(Debug)]
struct FakeControl {
    supports_owned_calls: bool,
    state: std::sync::Mutex<FakeReferences>,
    observations: std::sync::atomic::AtomicUsize,
    resources: native_broker::ControlResources,
    admission_pause: Option<Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>>,
    admission_started: Arc<std::sync::atomic::AtomicBool>,
}
#[derive(Debug)]
struct FakeReferences {
    references: HashMap<String, (u64, Option<native_broker::CommitId>)>,
    publications: HashMap<
        String,
        (
            native_broker::PublishRequest,
            native_broker::ConfirmedPublication,
        ),
    >,
    requests: Vec<native_broker::PublishRequest>,
}
impl FakeControl {
    fn unowned() -> Self {
        Self {
            supports_owned_calls: false,
            ..Self::new()
        }
    }
    fn new() -> Self {
        Self {
            supports_owned_calls: true,
            state: std::sync::Mutex::new(FakeReferences {
                references: HashMap::from([
                    ("main".into(), (3, Some(native_broker::CommitId([1; 32])))),
                    ("absent".into(), (7, None)),
                ]),
                publications: HashMap::new(),
                requests: vec![],
            }),
            observations: std::sync::atomic::AtomicUsize::new(0),
            resources: native_broker::ControlResources {
                memory_bytes: 4096,
                temporary_bytes: 4096,
            },
            admission_pause: None,
            admission_started: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }
}
#[async_trait::async_trait]
impl native_broker::NativeRepositoryControl for FakeControl {
    async fn observe_reference_with_owner(
        &self,
        name: &str,
        owner: native_broker::CommandGuard,
    ) -> Result<native_broker::ReferenceSnapshot, tonic::Status> {
        if !self.supports_owned_calls {
            return Err(tonic::Status::unimplemented(
                "owned reference observation unsupported",
            ));
        }
        let _owner = owner;
        self.observe_reference(name).await
    }
    async fn admit_commit_with_owner(
        &self,
        head: native_broker::CommitId,
        owner: native_broker::CommandGuard,
    ) -> Result<native_broker::AdmittedCommit, tonic::Status> {
        if !self.supports_owned_calls {
            return Err(tonic::Status::unimplemented(
                "owned commit admission unsupported",
            ));
        }
        let _owner = owner;
        self.admit_commit(head).await
    }
    fn resources(&self) -> native_broker::ControlResources {
        self.resources
    }
    async fn observe_reference(
        &self,
        name: &str,
    ) -> Result<native_broker::ReferenceSnapshot, tonic::Status> {
        self.observations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if name == "offline" {
            let mut error = tonic::Status::unavailable("fixture diagnostic must stay server-side");
            error
                .metadata_mut()
                .insert("x-test-detail", "not-for-client".parse().unwrap());
            return Err(error);
        }
        let state = self.state.lock().unwrap();
        let (generation, head) = state.references.get(name).copied().unwrap_or((0, None));
        Ok(native_broker::ReferenceSnapshot {
            name: name.into(),
            generation,
            head: head.map(|head| native_broker::AdmittedCommit {
                head,
                metadata_objects: 2,
            }),
        })
    }
    async fn admit_commit(
        &self,
        head: native_broker::CommitId,
    ) -> Result<native_broker::AdmittedCommit, tonic::Status> {
        self.admission_started
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(pause) = &self.admission_pause {
            let pause = pause.clone();
            tokio::task::spawn_blocking(move || {
                let (lock, wake) = &*pause;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
            })
            .await
            .unwrap();
        }
        Ok(native_broker::AdmittedCommit {
            head,
            metadata_objects: 2,
        })
    }
    async fn publish(
        &self,
        request: native_broker::PublishRequest,
    ) -> Result<native_broker::ConfirmedPublication, tonic::Status> {
        let mut state = self.state.lock().unwrap();
        if let Some((original, receipt)) = state.publications.get(&request.idempotency_key) {
            return if original == &request {
                Ok(receipt.clone())
            } else {
                Err(tonic::Status::already_exists("another intent"))
            };
        }
        state.requests.push(request.clone());
        let (current, _) = state
            .references
            .get(&request.name)
            .copied()
            .unwrap_or((0, None));
        if current != request.expected_generation {
            return Err(tonic::Status::aborted("stale authoritative generation"));
        }
        let receipt = native_broker::ConfirmedPublication {
            idempotency_key: request.idempotency_key.clone(),
            name: request.name.clone(),
            generation: current + 1,
            head: request.head,
        };
        state
            .references
            .insert(request.name.clone(), (current + 1, Some(request.head)));
        state
            .publications
            .insert(request.idempotency_key.clone(), (request, receipt.clone()));
        Ok(receipt)
    }
    async fn publication(
        &self,
        key: &str,
    ) -> Result<Option<native_broker::ConfirmedPublication>, tonic::Status> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .publications
            .get(key)
            .map(|(_, receipt)| receipt.clone()))
    }
}
async fn control_fixture(
    root: &Path,
    control: Arc<FakeControl>,
) -> (Running, PathBuf, Arc<TestHostAccess>) {
    let backend = root.join("store");
    private(&backend);
    let provider: Arc<dyn NativeObjectIo> =
        Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap());
    let access = Arc::new(TestHostAccess::default());
    let service = BrokerService::new(
        HashMap::from([
            ("example".into(), provider.clone()),
            ("local-only".into(), provider),
        ]),
        config(),
    )
    .unwrap()
    .with_controls(HashMap::from([(
        "example".into(),
        control as Arc<dyn native_broker::NativeRepositoryControl>,
    )]))
    .unwrap()
    .with_host_access_resolver(access.clone())
    .unwrap();
    let socket = root.join("broker.sock");
    (
        Running::start_service(&socket, service).await,
        socket,
        access,
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_preserves_observation_generation_and_caller_cas_across_independent_processes() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let control = Arc::new(FakeControl::new());
    let (host, socket, _access) = control_fixture(&root, control.clone()).await;
    let a = OwnedChild::spawn(&socket, &root.join("a"), "control-observe", &["main"]);
    let b = OwnedChild::spawn(&socket, &root.join("b"), "control-observe", &["main"]);
    let (a, b) = tokio::join!(a.finish(), b.finish());
    assert_eq!(a, b);
    assert!(a.starts_with("OBSERVED 3 "));
    let head_a = "02".repeat(32);
    let head_b = "03".repeat(32);
    let a = OwnedChild::spawn(
        &socket,
        &root.join("a-push"),
        "control-publish",
        &["main", &head_a, "3", "intent-a"],
    );
    let b = OwnedChild::spawn(
        &socket,
        &root.join("b-push"),
        "control-publish",
        &["main", &head_b, "3", "intent-b"],
    );
    let (a, b) = tokio::join!(a.finish(), b.finish());
    assert_eq!(
        usize::from(a.starts_with("PUBLISHED 4 ")) + usize::from(b.starts_with("PUBLISHED 4 ")),
        1
    );
    assert_eq!(usize::from(a == "STALE") + usize::from(b == "STALE"), 1);
    {
        let state = control.state.lock().unwrap();
        assert_eq!(state.requests.len(), 2);
        assert!(
            state
                .requests
                .iter()
                .all(|request| request.expected_generation == 3)
        );
    }
    let client = native_broker::BrokerControlClient::connect(&socket, "example", config())
        .await
        .unwrap();
    let absent = client.observe_reference("absent").await.unwrap();
    assert_eq!(absent.generation, 7);
    assert!(absent.head.is_none());
    let admitted = client
        .admit_commit(native_broker::CommitId([4; 32]))
        .await
        .unwrap();
    assert_eq!(admitted.head, native_broker::CommitId([4; 32]));
    let initial = native_broker::PublishRequest {
        name: "new".into(),
        head: admitted.head,
        expected_generation: 0,
        idempotency_key: "explicit-zero".into(),
    };
    let receipt = client.publish(initial.clone()).await.unwrap();
    assert_eq!(receipt.generation, 1);
    assert_eq!(client.publish(initial).await.unwrap(), receipt);
    assert_eq!(
        client.publication("explicit-zero").await.unwrap(),
        Some(receipt)
    );
    assert!(client.publication("unconfirmed").await.unwrap().is_none());
    let error = client.observe_reference("offline").await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert!(!error.message().contains("fixture diagnostic"));
    assert!(error.metadata().get("x-test-detail").is_none());
    let local = native_broker::BrokerControlClient::connect(&socket, "local-only", config())
        .await
        .unwrap();
    assert_eq!(
        local.observe_reference("main").await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let unknown = native_broker::BrokerControlClient::connect(&socket, "unknown", config())
        .await
        .unwrap();
    assert_eq!(
        unknown.observe_reference("main").await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    drop(unknown);
    drop(local);
    drop(client);
    host.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_registration_and_wire_requests_fail_before_adapter_work() {
    use tonic::{codec::CompressionEncoding, transport::Endpoint};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let backend = root.join("preflight-store");
    private(&backend);
    let provider: Arc<dyn NativeObjectIo> =
        Arc::new(DirectObjectIo::init(&backend, backend_config()).unwrap());
    let mut too_large = FakeControl::new();
    too_large.resources.memory_bytes = config().max_memory_bytes as u64 + 1;
    assert!(
        BrokerService::new(
            HashMap::from([("example".into(), provider.clone())]),
            config()
        )
        .unwrap()
        .with_controls(HashMap::from([(
            "example".into(),
            Arc::new(too_large) as Arc<dyn native_broker::NativeRepositoryControl>
        )]))
        .is_err()
    );
    let mut too_much_disk = FakeControl::new();
    too_much_disk.resources.temporary_bytes = config().max_temporary_bytes + 1;
    assert!(
        BrokerService::new(HashMap::from([("example".into(), provider)]), config())
            .unwrap()
            .with_controls(HashMap::from([(
                "example".into(),
                Arc::new(too_much_disk) as Arc<dyn native_broker::NativeRepositoryControl>
            )]))
            .is_err()
    );
    let control = Arc::new(FakeControl::new());
    let (host, socket, _access) = control_fixture(&root, control.clone()).await;
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
    let mut raw =
        native_broker::proto::repository_control_client::RepositoryControlClient::new(channel)
            .send_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Zstd);
    let error = raw
        .publish(native_broker::proto::PublishRequest {
            store_id: "example".into(),
            name: "new".into(),
            head: vec![5; 32],
            expected_generation: None,
            idempotency_key: "missing-generation".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(control.state.lock().unwrap().requests.is_empty());
    drop(raw);
    host.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_control_drains_adapter_work_and_keeps_shared_object_pools_charged() {
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let spool = root.join("spool");
    private(&spool);
    let release = Release(Arc::new((
        std::sync::Mutex::new(false),
        std::sync::Condvar::new(),
    )));
    let mut fake = FakeControl::new();
    fake.admission_pause = Some(release.0.clone());
    let started = fake.admission_started.clone();
    let control = Arc::new(fake);
    let (host, socket, access) = control_fixture(&root, control).await;
    let mut short = config();
    short.policy.operation_timeout_ms = 100;
    let client = native_broker::BrokerControlClient::connect(&socket, "example", short.clone())
        .await
        .unwrap();
    assert!(
        client
            .admit_commit(native_broker::CommitId([6; 32]))
            .await
            .is_err()
    );
    assert!(started.load(Ordering::SeqCst));
    assert!(
        access.fence.try_write().is_err(),
        "timed-out bootstrap retains the host fence through physical adapter work"
    );
    let io = BrokerObjectIo::connect(&socket, "example", &spool, short)
        .await
        .unwrap();
    assert!(
        io.descriptor(tinytree_model::empty_tree().descriptor.key)
            .await
            .is_err()
    );
    drop(io);
    drop(client);
    let stopping = tokio::spawn(host.stop());
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        !stopping.is_finished(),
        "transport shutdown discarded draining control ownership"
    );
    release.release();
    stopping.await.unwrap();
    let _exclusive =
        tokio::time::timeout(Duration::from_secs(2), access.fence.clone().write_owned())
            .await
            .unwrap();
}

#[derive(Debug)]
struct QueryPageProbe(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for QueryPageProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CloudQueryMode {
    Valid,
    MissingEcho,
    WrongEcho,
    Legacy,
    Retry,
    WrongFork,
}
#[derive(Debug)]
struct HostFenceDropProbe {
    fence: Arc<tokio::sync::RwLock<()>>,
    observed: Option<oneshot::Sender<bool>>,
}
impl Drop for HostFenceDropProbe {
    fn drop(&mut self) {
        let _ = self
            .observed
            .take()
            .unwrap()
            .send(self.fence.try_write().is_err());
    }
}
#[derive(Debug)]
struct FakeQuery {
    marker: u8,
    persistent: u64,
    cloud_mode: Option<CloudQueryMode>,
    cloud_calls: std::sync::atomic::AtomicUsize,
    cloud_replays: std::sync::Mutex<HashMap<[u8; 32], ([u8; 32], native_broker::QueryScope)>>,
    closed: std::sync::atomic::AtomicBool,
    page: std::sync::Mutex<Option<Arc<QueryPageProbe>>>,
    response_drop_probe: std::sync::Mutex<Option<HostFenceDropProbe>>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
    scope_calls: std::sync::atomic::AtomicUsize,
    transient_pages: std::sync::atomic::AtomicUsize,
    page_calls: std::sync::Mutex<Vec<([u8; 32], u64)>>,
    fact_calls: std::sync::atomic::AtomicUsize,
    page_pause: Option<Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>>,
    page_entered: Arc<std::sync::atomic::AtomicBool>,
    page_finished: Arc<std::sync::atomic::AtomicBool>,
}
impl FakeQuery {
    fn new(marker: u8, persistent: u64) -> Self {
        Self {
            marker,
            persistent,
            cloud_mode: None,
            cloud_calls: Default::default(),
            cloud_replays: Default::default(),
            closed: Default::default(),
            page: Default::default(),
            response_drop_probe: Default::default(),
            dropped: Default::default(),
            scope_calls: Default::default(),
            transient_pages: Default::default(),
            page_calls: Default::default(),
            fact_calls: Default::default(),
            page_pause: None,
            page_entered: Default::default(),
            page_finished: Default::default(),
        }
    }
    fn reply<T>(&self, value: T) -> native_broker::QueryReply<T> {
        native_broker::QueryReply {
            value,
            owner: Arc::new(()),
        }
    }
    fn scope(&self, scope: [u8; 32]) -> Result<(), tonic::Status> {
        if scope != [self.marker; 32] {
            return Err(tonic::Status::permission_denied(
                "fixture private scope detail",
            ));
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl native_broker::NativeQueryIo for FakeQuery {
    fn resources(&self) -> native_broker::QueryResources {
        native_broker::QueryResources {
            memory_bytes: 1024,
            temporary_bytes: 0,
            persistent_memory_bytes: self.persistent,
        }
    }
    async fn open_cloud_scope(
        &self,
        input: native_broker::OpenCloudQueryScope,
        _: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryScope>, tonic::Status> {
        let Some(mode) = self.cloud_mode else {
            return Err(tonic::Status::unimplemented("fixture old query provider"));
        };
        let attempt = self
            .cloud_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let digest = input.source.digest();
        let mut hash = blake3::Hasher::new();
        hash.update(&digest);
        for bytes in &input.overlay {
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(bytes);
        }
        let fingerprint = *hash.finalize().as_bytes();
        let mut replays = self.cloud_replays.lock().unwrap();
        let scope = if let Some((old, scope)) = replays.get(&input.replay_key) {
            if old != &fingerprint {
                return Err(tonic::Status::already_exists(
                    "fixture changed cloud intent",
                ));
            }
            scope.clone()
        } else {
            let scope = native_broker::QueryScope {
                handle: [self.marker; 32],
                expires_at_unix_seconds: i64::MAX / 2,
                query_identity_version: if mode == CloudQueryMode::Legacy { 1 } else { 2 },
                cloud_source_digest: match mode {
                    CloudQueryMode::MissingEcho => None,
                    CloudQueryMode::WrongEcho => Some([88; 32]),
                    _ => Some(digest),
                },
            };
            replays.insert(input.replay_key, (fingerprint, scope.clone()));
            scope
        };
        if mode == CloudQueryMode::Retry && attempt == 0 {
            return Err(tonic::Status::unavailable("fixture withheld cloud ACK"));
        }
        Ok(self.reply(scope))
    }
    async fn open_scope(
        &self,
        _: native_broker::OpenQueryScope,
        _: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryScope>, tonic::Status> {
        self.scope_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(self.reply(native_broker::QueryScope {
            cloud_source_digest: None,
            handle: [self.marker; 32],
            expires_at_unix_seconds: i64::MAX / 2,
            query_identity_version: 1,
        }))
    }
    async fn fork_scope(
        &self,
        i: native_broker::ForkQueryScope,
        _: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryScope>, tonic::Status> {
        self.scope(i.parent)?;
        if self.cloud_mode.is_some() {
            let replays = self.cloud_replays.lock().unwrap();
            let scope = replays
                .values()
                .map(|(_, scope)| scope)
                .find(|scope| scope.handle == i.parent)
                .ok_or_else(|| tonic::Status::not_found("fixture cloud parent missing"))?;
            if scope.cloud_source_digest != i.expected_cloud_source_digest {
                return Err(tonic::Status::permission_denied(
                    "fixture cloud fork source mismatch",
                ));
            }
            let mut scope = scope.clone();
            scope.handle = [self.marker.wrapping_add(1); 32];
            if self.cloud_mode == Some(CloudQueryMode::WrongFork) {
                scope.cloud_source_digest = Some([91; 32]);
            }
            return Ok(self.reply(scope));
        }
        Ok(self.reply(native_broker::QueryScope {
            cloud_source_digest: None,
            handle: [self.marker; 32],
            expires_at_unix_seconds: i64::MAX / 2,
            query_identity_version: 1,
        }))
    }
    async fn facts(
        &self,
        s: [u8; 32],
        f: Vec<native_broker::query_protocol::GraphFact>,
        _: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::query_protocol::GraphFacts>, tonic::Status>
    {
        self.fact_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.scope(s)?;
        Ok(self.reply(native_broker::query_protocol::GraphFacts {
            answers: f
                .into_iter()
                .map(|_| native_broker::query_protocol::GraphFactAnswer {
                    answer: Some(
                        native_broker::query_protocol::graph_fact_answer::Answer::HasId(
                            self.marker == 1,
                        ),
                    ),
                })
                .collect(),
            scope_digest: vec![self.marker; 32],
            index_epoch: vec![self.marker; 16],
        }))
    }
    async fn resolve_commit_prefix(
        &self,
        s: [u8; 32],
        _: String,
        _: native_broker::QueryOwner,
    ) -> Result<
        native_broker::QueryReply<native_broker::query_protocol::CommitPrefixResult>,
        tonic::Status,
    > {
        self.scope(s)?;
        Ok(
            self.reply(native_broker::query_protocol::CommitPrefixResult {
                selection_digest: None,
                result: Some(
                    native_broker::query_protocol::commit_prefix_result::Result::Single(
                        native_broker::query_protocol::CommitMatch {
                            commit: Some(native_broker::query_protocol::CommitId {
                                value: vec![self.marker; 32],
                            }),
                        },
                    ),
                ),
            }),
        )
    }
    async fn resolve_change_prefix(
        &self,
        _: [u8; 32],
        _: String,
        _: Vec<native_broker::CommitId>,
        _: native_broker::QueryOwner,
    ) -> Result<
        native_broker::QueryReply<native_broker::query_protocol::ChangePrefixResult>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture private change detail",
        ))
    }
    async fn shortest_prefix(
        &self,
        _: [u8; 32],
        _: native_broker::query_protocol::shortest_prefix_request::Target,
        _: Vec<native_broker::CommitId>,
        _: native_broker::QueryOwner,
    ) -> Result<
        native_broker::QueryReply<native_broker::query_protocol::ShortestPrefixResult>,
        tonic::Status,
    > {
        Err(tonic::Status::unimplemented(
            "fixture private shortest detail",
        ))
    }
    async fn begin(
        &self,
        i: native_broker::BeginQuery,
        _: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryHandle>, tonic::Status> {
        self.scope(i.scope)?;
        Ok(self.reply(native_broker::QueryHandle {
            handle: [self.marker + 10; 32],
            expires_at_unix_seconds: i64::MAX / 2,
        }))
    }
    async fn page(
        &self,
        q: [u8; 32],
        s: u64,
        physical: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<native_broker::QueryPage>, tonic::Status> {
        let _physical = physical;
        self.page_calls.lock().unwrap().push((q, s));
        if let Some(pause) = &self.page_pause {
            self.page_entered
                .store(true, std::sync::atomic::Ordering::SeqCst);
            let pause = pause.clone();
            tokio::task::spawn_blocking(move || {
                let (lock, wake) = &*pause;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = wake.wait(released).unwrap();
                }
            })
            .await
            .unwrap();
            self.page_finished
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        if self
            .transient_pages
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| n.checked_sub(1),
            )
            .is_ok()
        {
            return Err(tonic::Status::unavailable(
                "fixture private transient detail",
            ));
        }
        if q != [self.marker + 10; 32] {
            return Err(tonic::Status::permission_denied(
                "fixture private query detail",
            ));
        }
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) || s > 1 {
            return Err(tonic::Status::failed_precondition(
                "fixture private closed detail",
            ));
        }
        let owner = self
            .page
            .lock()
            .unwrap()
            .get_or_insert_with(|| Arc::new(QueryPageProbe(self.dropped.clone())))
            .clone();
        let owner: native_broker::QueryOwner = match self.response_drop_probe.lock().unwrap().take()
        {
            Some(probe) => Arc::new((owner, probe)),
            None => owner,
        };
        Ok(native_broker::QueryReply {
            value: native_broker::QueryPage {
                sequence: s,
                rows: if s == 0 {
                    vec![native_broker::query_protocol::GraphRow {
                        commit: Some(native_broker::query_protocol::CommitId {
                            value: vec![self.marker; 32],
                        }),
                        change: Some(native_broker::query_protocol::ChangeId {
                            value: vec![self.marker; 16],
                        }),
                        edges: vec![],
                        canonical: None,
                    }]
                } else {
                    vec![]
                },
                exhausted: s == 1,
            },
            owner,
        })
    }
    async fn read_commit(
        &self,
        _: [u8; 32],
        _: native_broker::CommitId,
        _: native_broker::QueryOwner,
    ) -> Result<native_broker::QueryReply<jj_native_backend::VerifiedMetadata>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture private read detail"))
    }
    async fn close_query(
        &self,
        _: [u8; 32],
        _: native_broker::QueryOwner,
    ) -> Result<(), tonic::Status> {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
        self.page.lock().unwrap().take();
        Ok(())
    }
    async fn close_scope(
        &self,
        _: [u8; 32],
        _: native_broker::QueryOwner,
    ) -> Result<(), tonic::Status> {
        Ok(())
    }
}
fn query_config() -> Config {
    Config {
        max_query_bytes: 16 * 1024,
        max_inflight: 2,
        ..config()
    }
}
fn query_service(root: &Path, a: Arc<FakeQuery>, b: Arc<FakeQuery>, cfg: Config) -> BrokerService {
    private(&root.join("query-store"));
    let provider: Arc<dyn NativeObjectIo> =
        Arc::new(DirectObjectIo::init(&root.join("query-store"), backend_config()).unwrap());
    BrokerService::new(
        HashMap::from([
            ("first".into(), provider.clone()),
            ("second".into(), provider.clone()),
            ("local-only".into(), provider),
        ]),
        cfg,
    )
    .unwrap()
    .with_queries(HashMap::from([
        ("first".into(), a as Arc<dyn native_broker::NativeQueryIo>),
        ("second".into(), b as Arc<dyn native_broker::NativeQueryIo>),
    ]))
    .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_aliases_preserve_scopes_replay_pages_and_cancellation_without_holding_client_slots()
{
    use native_broker::{NativeQueryIo, query_protocol as q};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let cfg = Config {
        max_inflight: 1,
        ..query_config()
    };
    let a = Arc::new(FakeQuery::new(1, 1024));
    let b = Arc::new(FakeQuery::new(2, 1024));
    a.transient_pages
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let service = query_service(&root, a.clone(), b, cfg.clone());
    let socket = root.join("query.sock");
    let host = Running::start_service(&socket, service.clone()).await;
    let first = native_broker::BrokerQueryClient::connect(&socket, "first", cfg.clone())
        .await
        .unwrap();
    let second = native_broker::BrokerQueryClient::connect(&socket, "second", cfg.clone())
        .await
        .unwrap();
    let open = || native_broker::OpenQueryScope {
        query_identity_version: 1,
        roots: vec![native_broker::CommitId([1; 32])],
        overlay: vec![],
        replay_key: [3; 32],
    };
    let sa = first.open_scope(open(), Arc::new(())).await.unwrap();
    let sb = second.open_scope(open(), Arc::new(())).await.unwrap();
    assert_ne!(sa.value.handle, sb.value.handle);
    let oversized = first
        .open_scope(
            native_broker::OpenQueryScope {
                overlay: vec![vec![0; cfg.max_query_bytes]],
                ..open()
            },
            Arc::new(()),
        )
        .await
        .err()
        .unwrap();
    assert_eq!(oversized.code(), tonic::Code::ResourceExhausted);
    assert_eq!(
        a.scope_calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "oversized input must fail before adapter invocation"
    );

    let unsupported = first
        .open_scope(
            native_broker::OpenQueryScope {
                query_identity_version: 2,
                replay_key: [9; 32],
                ..open()
            },
            Arc::new(()),
        )
        .await
        .unwrap_err();
    assert_eq!(
        unsupported.code(),
        tonic::Code::Unimplemented,
        "explicit v2 request must not silently accept a legacy provider"
    );
    let fact = || q::GraphFact {
        fact: Some(q::graph_fact::Fact::HasId(q::HasId {
            commit: Some(q::CommitId { value: vec![1; 32] }),
        })),
    };
    assert_ne!(
        first
            .facts(sa.value.handle, vec![fact()], Arc::new(()))
            .await
            .unwrap()
            .value
            .answers,
        second
            .facts(sb.value.handle, vec![fact()], Arc::new(()))
            .await
            .unwrap()
            .value
            .answers
    );
    let foreign = second
        .facts(sa.value.handle, vec![fact()], Arc::new(()))
        .await
        .err()
        .unwrap();
    assert_eq!(foreign.code(), tonic::Code::PermissionDenied);
    assert!(!foreign.message().contains("fixture private"));
    let local = service.query_io("first").unwrap();
    let facts_before = a.fact_calls.load(std::sync::atomic::Ordering::SeqCst);
    let huge = vec![1u8; cfg.max_query_bytes * 256];
    let malformed = native_broker::BeginQuery {
        scope: sa.value.handle,
        expression: q::GraphExpr {
            version: 1,
            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                commits: vec![q::CommitId { value: huge }],
            })),
        },
        projection: q::Projection::Graph,
        page_size: 1,
        replay_key: [8; 32],
    };
    assert_eq!(
        first
            .begin(malformed, Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::InvalidArgument
    );
    let too_many = vec![native_broker::CommitId([1; 32]); cfg.max_query_bytes / 34 + 1];
    assert_eq!(
        first
            .resolve_change_prefix(sa.value.handle, "a".into(), too_many.clone(), Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        local
            .resolve_change_prefix(sa.value.handle, "a".into(), too_many.clone(), Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    let target = q::shortest_prefix_request::Target::Commit(q::CommitId { value: vec![1; 32] });
    assert_eq!(
        local
            .shortest_prefix(sa.value.handle, target, too_many, Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    let oversized_fact = q::GraphFact {
        fact: Some(q::graph_fact::Fact::Heads(q::Heads {
            commits: vec![q::CommitId { value: vec![1; 32] }; cfg.max_query_bytes / 34 + 1],
        })),
    };
    assert_eq!(
        first
            .facts(sa.value.handle, vec![oversized_fact.clone()], Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        local
            .facts(sa.value.handle, vec![oversized_fact], Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        a.fact_calls.load(std::sync::atomic::Ordering::SeqCst),
        facts_before,
        "invalid inputs must not enter the adapter"
    );
    assert_eq!(
        first
            .fork_scope(
                native_broker::ForkQueryScope {
                    expected_cloud_source_digest: None,
                    parent: sa.value.handle,
                    overlay: vec![
                        vec![0; cfg.max_query_bytes / 2],
                        vec![0; cfg.max_query_bytes / 2]
                    ],
                    replay_key: [9; 32]
                },
                Arc::new(())
            )
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    let query = first
        .begin(
            native_broker::BeginQuery {
                scope: sa.value.handle,
                expression: q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                        commits: vec![q::CommitId { value: vec![1; 32] }],
                    })),
                },
                projection: q::Projection::Graph,
                page_size: 1,
                replay_key: [4; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let page = first
        .page(query.value.handle, 0, Arc::new(()))
        .await
        .unwrap();
    assert!(!page.value.exhausted);
    assert_eq!(
        &a.page_calls.lock().unwrap()[..2],
        &[(query.value.handle, 0), (query.value.handle, 0)],
        "retry must reuse the exact query identity and page sequence"
    );

    let replay = first
        .page(query.value.handle, 0, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(page.value.rows, replay.value.rows);
    // Both decoded pages remain held while another RPC acquires the sole slot.
    first
        .close_query(query.value.handle, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(
        first
            .page(query.value.handle, 1, Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let local = native_broker::BrokerQueryClient::connect(&socket, "local-only", cfg.clone())
        .await
        .unwrap();
    assert_eq!(
        local
            .open_scope(open(), Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let unknown = native_broker::BrokerQueryClient::connect(&socket, "unknown", cfg)
        .await
        .unwrap();
    assert_eq!(
        unknown
            .open_scope(open(), Arc::new(()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::NotFound
    );
    drop((page, replay, query, sa, sb, first, second, local, unknown));
    host.stop().await;
}
#[test]
fn query_registration_aggregates_distinct_persistent_pools_not_alias_count() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    private(root);
    private(&root.join("budget-store"));
    let provider: Arc<dyn NativeObjectIo> =
        Arc::new(DirectObjectIo::init(&root.join("budget-store"), backend_config()).unwrap());
    let make = |cfg| {
        BrokerService::new(
            HashMap::from([
                ("first".into(), provider.clone()),
                ("second".into(), provider.clone()),
            ]),
            cfg,
        )
        .unwrap()
    };
    let first: Arc<dyn native_broker::NativeQueryIo> =
        Arc::new(FakeQuery::new(1, 320 * 1024 * 1024));
    let second: Arc<dyn native_broker::NativeQueryIo> =
        Arc::new(FakeQuery::new(2, 320 * 1024 * 1024));
    assert!(
        make(Config::default())
            .with_queries(HashMap::from([
                ("first".into(), first.clone()),
                ("second".into(), second.clone())
            ]))
            .is_err()
    );
    assert!(
        make(Config {
            max_query_persistent_bytes: 768 * 1024 * 1024,
            ..Default::default()
        })
        .with_queries(HashMap::from([
            ("first".into(), first.clone()),
            ("second".into(), second)
        ]))
        .is_ok()
    );
    assert!(
        make(Config::default())
            .with_queries(HashMap::from([
                ("first".into(), first.clone()),
                ("second".into(), first)
            ]))
            .is_ok()
    );
}

struct QueryRequestBody(Option<bytes::Bytes>);
impl http_body::Body for QueryRequestBody {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Ready(self.0.take().map(|b| Ok(http_body::Frame::data(b))))
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_returned_response_keeps_page_owner_after_registry_close_until_body_drop() {
    use native_broker::NativeQueryIo;
    use prost::Message;
    use tower::ServiceExt;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let first = Arc::new(FakeQuery::new(1, 1024));
    let second = Arc::new(FakeQuery::new(2, 1024));
    let service = query_service(&root, first.clone(), second, query_config());
    let body = native_broker::proto::QueryPageRequest {
        store_id: "first".into(),
        query: vec![11; 32],
        expected_sequence: 0,
    }
    .encode_to_vec();
    let mut framed = vec![0];
    framed.extend_from_slice(&(body.len() as u32).to_be_bytes());
    framed.extend_from_slice(&body);
    let request = http::Request::builder()
        .uri("/native.broker.v1.QueryIo/Page")
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::new(QueryRequestBody(Some(
            framed.into(),
        ))))
        .unwrap();
    let response = service.query_rpc_service().oneshot(request).await.unwrap();
    assert_eq!(response.status(), http::StatusCode::OK);
    assert!(first.page.lock().unwrap().is_some());
    let local = service.query_io("first").unwrap();
    local.close_query([11; 32], Arc::new(())).await.unwrap();
    assert!(first.page.lock().unwrap().is_none());
    assert_eq!(
        first.dropped.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the old physical response must retain its source independently of the cleared registry"
    );
    drop(response);
    assert_eq!(first.dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
    service.drain().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_query_replies_exhaust_only_retained_pool_and_release_restores_progress() {
    use native_broker::{NativeQueryIo, query_protocol as q};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let service = query_service(
        &root,
        Arc::new(FakeQuery::new(1, 1024)),
        Arc::new(FakeQuery::new(2, 1024)),
        query_config(),
    );
    let socket = root.join("reply-budget.sock");
    let host = Running::start_service(&socket, service).await;
    let tiny = Config {
        max_inflight: 1,
        max_metadata_bytes: 1024,
        max_catalog_bytes: 1024,
        max_catalog_objects: 1,
        max_memory_bytes: 16 * 1024 * 128 + 4096 + 64 * 1024,
        ..query_config()
    };
    let client = native_broker::BrokerQueryClient::connect(&socket, "first", tiny)
        .await
        .unwrap();
    let scope = client
        .open_scope(
            native_broker::OpenQueryScope {
                query_identity_version: 1,
                roots: vec![native_broker::CommitId([1; 32])],
                overlay: vec![],
                replay_key: [1; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let query = client
        .begin(
            native_broker::BeginQuery {
                scope: scope.value.handle,
                expression: q::GraphExpr {
                    version: 1,
                    expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                        commits: vec![q::CommitId { value: vec![1; 32] }],
                    })),
                },
                projection: q::Projection::Graph,
                page_size: 1,
                replay_key: [2; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let query_id = query.value.handle;
    drop(query);
    drop(scope);
    let mut held = Vec::new();
    let mut exhausted = false;
    for _ in 0..32 {
        match client.page(query_id, 0, Arc::new(())).await {
            Ok(page) => {
                assert!(!page.value.exhausted);
                held.push(page);
            }
            Err(status) => {
                assert_eq!(status.code(), tonic::Code::ResourceExhausted);
                exhausted = true;
                break;
            }
        }
    }
    assert!(
        exhausted && !held.is_empty(),
        "held replies must be bounded without consuming decode headroom or claiming EOF"
    );
    drop(held);
    let page = client.page(query_id, 0, Arc::new(())).await.unwrap();
    assert!(!page.value.exhausted);
    client.close_query(query_id, Arc::new(())).await.unwrap();
    drop(page);
    drop(client);
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn composed_query_brokers_release_physical_owner_without_pinning_completed_replies() {
    use native_broker::{NativeQueryIo, query_protocol as q};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let shared = Arc::new(FakeQuery::new(1, 1024));
    // Explicit registrations of the same adapter are trusted synonyms.
    let upstream = query_service(&root, shared.clone(), shared, query_config());
    let socket = root.join("upstream-query.sock");
    let host = Running::start_service(&socket, upstream).await;
    let cfg = Config {
        max_inflight: 1,
        ..query_config()
    };
    let upstream_client = Arc::new(
        native_broker::BrokerQueryClient::connect(&socket, "first", cfg.clone())
            .await
            .unwrap(),
    );
    private(&root.join("outer-store"));
    let provider: Arc<dyn NativeObjectIo> =
        Arc::new(DirectObjectIo::init(&root.join("outer-store"), backend_config()).unwrap());
    let outer = BrokerService::new(HashMap::from([("outer".into(), provider)]), cfg.clone())
        .unwrap()
        .with_queries(HashMap::from([(
            "outer".into(),
            upstream_client as Arc<dyn NativeQueryIo>,
        )]))
        .unwrap();
    let local = outer.query_io("outer").unwrap();
    let scope = local
        .open_scope(
            native_broker::OpenQueryScope {
                query_identity_version: 1,
                roots: vec![native_broker::CommitId([1; 32])],
                overlay: vec![],
                replay_key: [5; 32],
            },
            Arc::new(()),
        )
        .await
        .unwrap();
    let fact = || q::GraphFact {
        fact: Some(q::graph_fact::Fact::HasId(q::HasId {
            commit: Some(q::CommitId { value: vec![1; 32] }),
        })),
    };
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let physical: native_broker::QueryOwner = Arc::new(QueryPageProbe(dropped.clone()));
    let first = tokio::time::timeout(
        Duration::from_secs(2),
        local.facts(scope.value.handle, vec![fact()], physical),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while dropped.load(std::sync::atomic::Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let second = tokio::time::timeout(
        Duration::from_secs(2),
        local.facts(scope.value.handle, vec![fact()], Arc::new(())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(first.value.answers, second.value.answers);
    let synonym = native_broker::BrokerQueryClient::connect(&socket, "second", cfg)
        .await
        .unwrap();
    assert_eq!(
        synonym
            .facts(scope.value.handle, vec![fact()], Arc::new(()))
            .await
            .unwrap()
            .value
            .answers,
        first.value.answers
    );
    tokio::time::timeout(
        Duration::from_secs(2),
        local.close_scope(scope.value.handle, Arc::new(())),
    )
    .await
    .unwrap()
    .unwrap();
    drop((first, second, scope, synonym, local));
    outer.drain().await.unwrap();
    drop(outer);
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_query_deadline_returns_while_accepted_provider_job_and_drain_remain_owned() {
    use native_broker::NativeQueryIo;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = Release(gate.clone());
    let access = Arc::new(TestHostAccess::default());
    let (observed, payload_dropped) = oneshot::channel();
    let mut slow = FakeQuery::new(1, 1024);
    *slow.response_drop_probe.get_mut().unwrap() = Some(HostFenceDropProbe {
        fence: access.fence.clone(),
        observed: Some(observed),
    });
    slow.page_pause = Some(gate);
    let entered = slow.page_entered.clone();
    let finished = slow.page_finished.clone();
    let mut cfg = Config {
        max_inflight: 1,
        ..query_config()
    };
    cfg.policy.operation_timeout_ms = 80;
    let service = query_service(
        &root,
        Arc::new(slow),
        Arc::new(FakeQuery::new(2, 1024)),
        cfg,
    )
    .with_host_access_resolver(access.clone())
    .unwrap();
    let local = service.query_io("first").unwrap();
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let physical: native_broker::QueryOwner = Arc::new(QueryPageProbe(dropped.clone()));
    let call = tokio::spawn(async move { local.page([11; 32], 0, physical).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !entered.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let status = tokio::time::timeout(Duration::from_secs(2), call)
        .await
        .unwrap()
        .unwrap()
        .err()
        .unwrap();
    assert_eq!(status.code(), tonic::Code::DeadlineExceeded);
    assert!(
        access.fence.try_write().is_err(),
        "accepted local query owns the host fence"
    );
    assert!(!finished.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
    let drain = service.clone();
    let draining = tokio::spawn(async move { drain.drain().await });
    tokio::task::yield_now().await;
    assert!(
        !draining.is_finished(),
        "deadline must not release the physical provider slot"
    );
    release.release();
    tokio::time::timeout(Duration::from_secs(2), draining)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), payload_dropped)
            .await
            .unwrap()
            .unwrap(),
        "discarded detached query payload must drop before its host fence"
    );
    let _exclusive =
        tokio::time::timeout(Duration::from_secs(2), access.fence.clone().write_owned())
            .await
            .unwrap();
    assert!(finished.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn live_provider_description_binds_alias_identity_and_separate_creation_policy() {
    use native_broker::{BrokerQueryClient, ProviderDescription, ProviderIndexMode};
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    private(&root.join("store"));
    let provider: Arc<dyn NativeObjectIo> =
        Arc::new(DirectObjectIo::init(&root.join("store"), backend_config()).unwrap());
    let stores = HashMap::from([
        ("first".to_owned(), provider.clone()),
        ("second".to_owned(), provider),
    ]);
    let description = ProviderDescription {
        identity: [17; 32],
        query_capable: false,
        index_mode: ProviderIndexMode::Legacy,
    };
    assert!(
        BrokerService::new(stores.clone(), query_config())
            .unwrap()
            .with_provider_descriptions(HashMap::from([("first".into(), description)]))
            .is_err()
    );
    let mut wrong = description;
    wrong.query_capable = true;
    assert!(
        BrokerService::new(stores.clone(), query_config())
            .unwrap()
            .with_provider_descriptions(HashMap::from([
                ("first".into(), wrong),
                ("second".into(), description)
            ]))
            .is_err()
    );
    let query: Arc<dyn native_broker::NativeQueryIo> = Arc::new(FakeQuery::new(1, 1024));
    let descriptions = HashMap::from([
        ("first".into(), description),
        ("second".into(), description),
    ]);
    assert!(
        BrokerService::new(stores.clone(), query_config())
            .unwrap()
            .with_provider_descriptions(descriptions)
            .unwrap()
            .with_queries(HashMap::from([("first".into(), query.clone())]))
            .is_err()
    );
    assert!(
        BrokerService::new(stores.clone(), query_config())
            .unwrap()
            .with_queries(HashMap::from([("first".into(), query)]))
            .unwrap()
            .with_provider_descriptions(HashMap::from([
                ("first".into(), wrong),
                ("second".into(), description)
            ]))
            .unwrap()
            .with_queries(HashMap::new())
            .is_err()
    );
    let mut second = description;
    second.identity = [18; 32];
    second.index_mode = ProviderIndexMode::Federated;
    let service = BrokerService::new(stores, query_config())
        .unwrap()
        .with_provider_descriptions(HashMap::from([
            ("first".into(), description),
            ("second".into(), second),
        ]))
        .unwrap();
    assert_eq!(service.describe_provider("first").unwrap(), description);
    let socket = root.join("describe.sock");
    let host = Running::start_service(&socket, service).await;
    let first = BrokerQueryClient::connect(&socket, "first", query_config())
        .await
        .unwrap();
    let second_client = BrokerQueryClient::connect(&socket, "second", query_config())
        .await
        .unwrap();
    assert_eq!(
        first.describe_provider().await.unwrap(),
        description,
        "description is available even without a query adapter"
    );
    assert_eq!(second_client.describe_provider().await.unwrap(), second);
    let unknown = BrokerQueryClient::connect(&socket, "unknown", query_config())
        .await
        .unwrap();
    assert_eq!(
        unknown.describe_provider().await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    // Unknown fields vanish during Prost decoding: the route must enforce the
    // small handshake cap on the raw/decompressed message, before allocation.
    #[derive(Clone, prost::Message)]
    struct OversizedDescription {
        #[prost(string, tag = "1")]
        store_id: String,
        #[prost(bytes = "vec", tag = "99")]
        unknown: Vec<u8>,
    }
    for compressed in [false, true] {
        let socket = socket.clone();
        let channel = tonic::transport::Endpoint::from_static("http://[::]:50051")
            .connect_with_connector(tower::service_fn(move |_: http::Uri| {
                let path = socket.clone();
                async move {
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(
                        tokio::net::UnixStream::connect(path).await?,
                    ))
                }
            }))
            .await
            .unwrap();
        let mut raw = tonic::client::Grpc::new(channel);
        if compressed {
            raw = raw.send_compressed(tonic::codec::CompressionEncoding::Zstd);
        }
        raw.ready().await.unwrap();
        let result: Result<
            tonic::Response<native_broker::proto::ProviderDescription>,
            tonic::Status,
        > = raw
            .unary(
                tonic::Request::new(OversizedDescription {
                    store_id: "first".into(),
                    unknown: vec![0; 2048],
                }),
                http::uri::PathAndQuery::from_static("/native.broker.v1.QueryIo/DescribeProvider"),
                tonic_prost::ProstCodec::default(),
            )
            .await;
        assert_eq!(
            result.unwrap_err().code(),
            if compressed {
                tonic::Code::ResourceExhausted
            } else {
                tonic::Code::OutOfRange
            }
        );
    }
    drop((first, second_client, unknown));
    host.stop().await;
}

#[derive(Debug, Default)]
struct TestHostAccess {
    fence: Arc<tokio::sync::RwLock<()>>,
    calls: std::sync::atomic::AtomicUsize,
}
impl native_broker::HostAccessResolver for TestHostAccess {
    fn acquire_host_access(&self) -> Result<native_broker::CommandGuard, tonic::Status> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.fence
            .clone()
            .try_read_owned()
            .map(|guard| Arc::new(guard) as native_broker::CommandGuard)
            .map_err(|_| tonic::Status::aborted("host collection is active"))
    }
}
#[derive(Debug)]
struct TestCommands {
    tokens: HashMap<[u8; 32], String>,
    active: std::sync::atomic::AtomicBool,
    jobs: Arc<std::sync::atomic::AtomicUsize>,
}
struct CommandJob(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for CommandJob {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl native_broker::CommandResolver for TestCommands {
    fn resolve(
        &self,
        alias: &str,
        token: [u8; 32],
    ) -> Result<native_broker::CommandGuard, tonic::Status> {
        if !self.active.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(tonic::Status::failed_precondition("command is closing"));
        }
        if self
            .tokens
            .get(&token)
            .is_none_or(|expected| expected != alias)
        {
            return Err(tonic::Status::permission_denied("command alias mismatch"));
        }
        self.jobs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Arc::new(CommandJob(self.jobs.clone())))
    }
}
async fn command_jobs_idle(commands: &TestCommands) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while commands.jobs.load(std::sync::atomic::Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_commands_bind_all_client_routes_to_exact_registered_alias_and_fail_before_io() {
    use native_broker::NativeQueryIo as _;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let first = Arc::new(FakeQuery::new(11, 1024));
    let second = Arc::new(FakeQuery::new(12, 1024));
    let commands = Arc::new(TestCommands {
        tokens: HashMap::from([([1; 32], "first".into()), ([2; 32], "second".into())]),
        active: true.into(),
        jobs: Default::default(),
    });
    let control = Arc::new(FakeControl::new());
    let unowned_control = Arc::new(FakeControl::unowned());
    let access = Arc::new(TestHostAccess::default());
    let service = query_service(&root, first.clone(), second.clone(), query_config())
        .with_controls(HashMap::from([
            (
                "first".into(),
                control.clone() as Arc<dyn native_broker::NativeRepositoryControl>,
            ),
            (
                "second".into(),
                unowned_control.clone() as Arc<dyn native_broker::NativeRepositoryControl>,
            ),
        ]))
        .unwrap()
        .with_provider_descriptions(HashMap::from([
            (
                "first".into(),
                native_broker::ProviderDescription {
                    identity: [1; 32],
                    query_capable: true,
                    index_mode: native_broker::ProviderIndexMode::Legacy,
                },
            ),
            (
                "second".into(),
                native_broker::ProviderDescription {
                    identity: [2; 32],
                    query_capable: true,
                    index_mode: native_broker::ProviderIndexMode::Legacy,
                },
            ),
            (
                "local-only".into(),
                native_broker::ProviderDescription {
                    identity: [3; 32],
                    query_capable: false,
                    index_mode: native_broker::ProviderIndexMode::Legacy,
                },
            ),
        ]))
        .unwrap()
        .with_command_resolver(commands.clone())
        .unwrap()
        .with_host_access_resolver(access.clone())
        .unwrap();
    let socket = root.join("managed.sock");
    let host = Running::start_service(&socket, service).await;
    let query = native_broker::BrokerQueryClient::connect(&socket, "first", query_config())
        .await
        .unwrap();
    let exclusive = access.fence.clone().try_write_owned().unwrap();
    let mut short = query_config();
    short.policy.operation_timeout_ms = 200;
    let blocked_query = native_broker::BrokerQueryClient::connect(&socket, "first", short.clone())
        .await
        .unwrap();
    assert_eq!(
        blocked_query.describe_provider().await.unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    let blocked_control = native_broker::BrokerControlClient::connect(&socket, "first", short)
        .await
        .unwrap();
    assert_eq!(
        blocked_control
            .observe_reference("main")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(
        blocked_control
            .admit_commit(native_broker::CommitId([1; 32]))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(
        control
            .observations
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    let waiting_query = query.clone();
    let before = access.calls.load(std::sync::atomic::Ordering::SeqCst);
    let waiting = tokio::spawn(async move { waiting_query.describe_provider().await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while access.calls.load(std::sync::atomic::Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !waiting.is_finished(),
        "bootstrap waits without invoking the provider during collection"
    );
    drop(exclusive);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .identity,
        [1; 32],
        "bootstrap discovery resumes without a command after collection"
    );
    let open = || native_broker::OpenQueryScope {
        roots: vec![native_broker::CommitId::ROOT],
        overlay: vec![],
        replay_key: [7; 32],
        query_identity_version: 1,
    };
    assert_eq!(
        query
            .open_scope(open(), Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert!(
        query
            .clone()
            .with_command_token([9; 32])
            .describe_provider()
            .await
            .is_err(),
        "a present token is validated even on bootstrap routes"
    );
    let foreign = native_broker::BrokerQueryClient::connect(&socket, "second", query_config())
        .await
        .unwrap()
        .with_command_token([1; 32]);
    assert_eq!(
        foreign
            .open_scope(open(), Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        first.scope_calls.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        second.scope_calls.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    let first_scope = query
        .with_command_token([1; 32])
        .open_scope(open(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(first_scope.value.handle, [11; 32]);
    let second_client =
        native_broker::BrokerQueryClient::connect(&socket, "second", query_config())
            .await
            .unwrap()
            .with_command_token([2; 32]);
    let second_scope = second_client
        .open_scope(open(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(second_scope.value.handle, [12; 32]);
    let control_client =
        native_broker::BrokerControlClient::connect(&socket, "first", query_config())
            .await
            .unwrap();
    control_client.observe_reference("main").await.unwrap();
    let observations = control
        .observations
        .load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        control_client
            .publication("intent")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert!(
        control_client
            .clone()
            .with_command_token([9; 32])
            .observe_reference("main")
            .await
            .is_err()
    );
    assert_eq!(
        control
            .observations
            .load(std::sync::atomic::Ordering::SeqCst),
        observations
    );
    let unsupported_client =
        native_broker::BrokerControlClient::connect(&socket, "second", query_config())
            .await
            .unwrap()
            .with_command_token([2; 32]);
    assert_eq!(
        unsupported_client
            .observe_reference("main")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unimplemented,
        "managed control must not silently use a provider lacking owned opt-in"
    );
    assert_eq!(
        unowned_control
            .observations
            .load(std::sync::atomic::Ordering::SeqCst),
        0,
        "unsupported owned observation must never fall back to unowned I/O"
    );
    assert_eq!(
        control
            .observations
            .load(std::sync::atomic::Ordering::SeqCst),
        observations,
        "the unsupported alias must not be routed to the supported provider"
    );
    let spool = root.join("managed-spool");
    private(&spool);
    let object = BrokerObjectIo::connect(&socket, "first", &spool, query_config())
        .await
        .unwrap();
    let value = tinytree_model::Object::Symlink("managed snapshot".into());
    assert!(object.write_metadata(value.clone()).await.is_err());
    assert!(
        object
            .clone()
            .with_command_token([2; 32])
            .write_metadata(value.clone())
            .await
            .is_err()
    );
    let descriptor = object
        .clone()
        .with_command_token([1; 32])
        .write_metadata(value)
        .await
        .unwrap();
    assert!(object.read_metadata(descriptor.key).await.is_err());
    assert_eq!(
        object
            .with_command_token([1; 32])
            .read_metadata(descriptor.key)
            .await
            .unwrap()
            .descriptor(),
        descriptor
    );
    let channel = native_broker::connect_local_channel(
        native_broker::LocalEndpoint::UnixSocket(socket.clone()),
        &query_config(),
    )
    .await
    .unwrap();
    let mut raw = native_broker::proto::object_io_client::ObjectIoClient::new(channel);
    let mut request = tonic::Request::new(native_broker::proto::ObjectRequest {
        store_id: "first".into(),
        kind: descriptor.key.kind as u32,
        id: descriptor.key.id.0.to_vec(),
    });
    request.metadata_mut().insert_bin(
        "native-command-bin",
        tonic::metadata::MetadataValue::from_bytes(&[1; 31]),
    );
    assert_eq!(
        raw.descriptor(request).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    command_jobs_idle(&commands).await;
    drop(first_scope);
    drop(second_scope);
    host.stop().await;
}

#[derive(Debug)]
struct PendingCommandWrite {
    inner: Arc<DirectObjectIo>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}
#[async_trait::async_trait]
impl NativeObjectIo for PendingCommandWrite {
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
        object: tinytree_model::Object,
    ) -> jj_lib::backend::BackendResult<tinytree_model::Descriptor> {
        self.inner.write_metadata(object).await
    }
    async fn write_metadata_with_owner(
        &self,
        object: tinytree_model::Object,
        owner: native_broker::CommandGuard,
    ) -> jj_lib::backend::BackendResult<tinytree_model::Descriptor> {
        let local = self.inner.clone();
        let entered = self.entered.clone();
        let release = self.release.clone();
        tokio::spawn(async move {
            entered.notify_one();
            release.notified().await;
            local.write_metadata_with_owner(object, owner).await
        })
        .await
        .map_err(|error| jj_lib::backend::BackendError::Other(Box::new(error)))?
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
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_managed_metadata_write_keeps_command_job_until_actual_owned_put_finishes() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    private(&root.join("objects"));
    private(&root.join("spool"));
    let local = Arc::new(DirectObjectIo::init(&root.join("objects"), backend_config()).unwrap());
    let provider = Arc::new(PendingCommandWrite {
        inner: local.clone(),
        entered: Default::default(),
        release: Default::default(),
    });
    let commands = Arc::new(TestCommands {
        tokens: HashMap::from([([1; 32], "example".into())]),
        active: true.into(),
        jobs: Default::default(),
    });
    let service = BrokerService::new(
        HashMap::from([(
            "example".into(),
            provider.clone() as Arc<dyn NativeObjectIo>,
        )]),
        config(),
    )
    .unwrap()
    .with_command_resolver(commands.clone())
    .unwrap();
    let socket = root.join("managed-write.sock");
    let host = Running::start_service(&socket, service).await;
    let client = BrokerObjectIo::connect(&socket, "example", root.join("spool"), config())
        .await
        .unwrap()
        .with_command_token([1; 32]);
    let object = tinytree_model::Object::Symlink("accepted immutable snapshot".into());
    let expected = tinytree_model::encode(&object, Default::default())
        .unwrap()
        .descriptor;
    let task = tokio::spawn(async move { client.write_metadata(object).await });
    tokio::time::timeout(Duration::from_secs(2), provider.entered.notified())
        .await
        .unwrap();
    task.abort();
    let _ = task.await;
    commands
        .active
        .store(false, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        commands.jobs.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "caller cancellation cannot release an accepted physical command job"
    );
    provider.release.notify_one();
    command_jobs_idle(&commands).await;
    assert_eq!(
        local.descriptor(expected.key).await.unwrap(),
        Some(expected)
    );
    host.stop().await;
}

fn retained_query_source() -> native_broker::CloudQuerySource {
    native_broker::CloudQuerySource {
        provider_identity: [1; 32],
        workspace_id: [4; 32],
        generation: 1,
        manifest_id: [5; 32],
        manifest_blake3: [6; 32],
        manifest_size_bytes: 1024,
        index_root_ids: vec![[7; 32]],
        expected_anchors: vec![native_broker::CommitId::ROOT],
    }
}
fn cloud_query_service(root: &Path, query: Arc<FakeQuery>, cfg: Config) -> BrokerService {
    query_service(root, query, Arc::new(FakeQuery::new(2, 1024)), cfg)
        .with_provider_descriptions(HashMap::from([
            (
                "first".into(),
                native_broker::ProviderDescription {
                    identity: [1; 32],
                    query_capable: true,
                    index_mode: native_broker::ProviderIndexMode::Legacy,
                },
            ),
            (
                "second".into(),
                native_broker::ProviderDescription {
                    identity: [2; 32],
                    query_capable: true,
                    index_mode: native_broker::ProviderIndexMode::Legacy,
                },
            ),
            (
                "local-only".into(),
                native_broker::ProviderDescription {
                    identity: [3; 32],
                    query_capable: false,
                    index_mode: native_broker::ProviderIndexMode::Legacy,
                },
            ),
        ]))
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_query_facade_checks_exact_source_replay_and_fork_without_slot_retention() {
    use native_broker::NativeQueryIo as _;
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let cfg = Config {
        max_inflight: 1,
        ..query_config()
    };
    let mut fake = FakeQuery::new(1, 1024);
    fake.cloud_mode = Some(CloudQueryMode::Retry);
    let fake = Arc::new(fake);
    let service = cloud_query_service(&root, fake.clone(), cfg.clone());
    let local = service.query_io("first").unwrap();
    let socket = root.join("cloud-query.sock");
    let host = Running::start_service(&socket, service).await;
    let client = native_broker::BrokerQueryClient::connect(&socket, "first", cfg)
        .await
        .unwrap();
    let input = native_broker::OpenCloudQueryScope {
        source: retained_query_source(),
        overlay: vec![b"bounded opaque overlay fixture".to_vec()],
        replay_key: [9; 32],
    };
    let expected = input.source.digest();
    let held = client
        .open_cloud_scope(input.clone(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(
        fake.cloud_calls.load(Ordering::SeqCst),
        2,
        "lost ACK retains exact immutable cloud intent"
    );
    assert_eq!(held.value.cloud_source_digest, Some(expected));
    assert_eq!(held.value.query_identity_version, 2);
    let replay = local
        .open_cloud_scope(input.clone(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(held.value, replay.value);
    let fork = native_broker::ForkQueryScope {
        parent: held.value.handle,
        expected_cloud_source_digest: Some(expected),
        overlay: vec![],
        replay_key: [10; 32],
    };
    let first = client.fork_scope(fork.clone(), Arc::new(())).await.unwrap();
    let second = local.fork_scope(fork, Arc::new(())).await.unwrap();
    assert_eq!(first.value, second.value);
    assert_eq!(first.value.cloud_source_digest, Some(expected));
    let wrong = native_broker::ForkQueryScope {
        parent: held.value.handle,
        expected_cloud_source_digest: Some([77; 32]),
        overlay: vec![],
        replay_key: [11; 32],
    };
    assert_eq!(
        client
            .fork_scope(wrong, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let mut changed = input;
    changed.source.manifest_blake3 = [99; 32];
    assert_eq!(
        client
            .open_cloud_scope(changed, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::AlreadyExists
    );
    assert_eq!(
        fake.scope_calls.load(Ordering::SeqCst),
        0,
        "cloud requests never become ordinary roots"
    );
    drop((held, replay, first, second, client, local));
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_query_facade_rejects_unsupported_missing_wrong_and_substituted_source_echoes() {
    use native_broker::NativeQueryIo as _;
    use std::sync::atomic::Ordering;
    for (mode, expected) in [
        (None, tonic::Code::Unimplemented),
        (Some(CloudQueryMode::MissingEcho), tonic::Code::DataLoss),
        (Some(CloudQueryMode::WrongEcho), tonic::Code::DataLoss),
        (Some(CloudQueryMode::Legacy), tonic::Code::DataLoss),
        (Some(CloudQueryMode::WrongFork), tonic::Code::DataLoss),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        private(&root);
        let mut query = FakeQuery::new(1, 1024);
        query.cloud_mode = mode;
        let query = Arc::new(query);
        let service = cloud_query_service(&root, query.clone(), query_config());
        let local = service.query_io("first").unwrap();
        let socket = root.join("cloud-query.sock");
        let host = Running::start_service(&socket, service).await;
        let client = native_broker::BrokerQueryClient::connect(&socket, "first", query_config())
            .await
            .unwrap();
        let input = || native_broker::OpenCloudQueryScope {
            source: retained_query_source(),
            overlay: vec![],
            replay_key: [9; 32],
        };
        if mode == Some(CloudQueryMode::WrongFork) {
            let scope = client
                .open_cloud_scope(input(), Arc::new(()))
                .await
                .unwrap();
            let fork = native_broker::ForkQueryScope {
                parent: scope.value.handle,
                expected_cloud_source_digest: scope.value.cloud_source_digest,
                overlay: vec![],
                replay_key: [10; 32],
            };
            assert_eq!(
                client
                    .fork_scope(fork.clone(), Arc::new(()))
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
            assert_eq!(
                local
                    .fork_scope(fork, Arc::new(()))
                    .await
                    .unwrap_err()
                    .code(),
                expected
            );
        } else {
            assert_eq!(
                client
                    .open_cloud_scope(input(), Arc::new(()))
                    .await
                    .unwrap_err()
                    .code(),
                expected,
                "{mode:?}"
            );
            assert_eq!(
                local
                    .open_cloud_scope(input(), Arc::new(()))
                    .await
                    .unwrap_err()
                    .code(),
                expected,
                "{mode:?}"
            );
        }
        assert_eq!(query.scope_calls.load(Ordering::SeqCst), 0);
        drop((client, local));
        host.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_query_facade_enforces_managed_tokens_provider_identity_and_bounded_sources_before_io()
 {
    use native_broker::NativeQueryIo as _;
    use std::sync::atomic::Ordering;
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().canonicalize().unwrap();
    private(&root);
    let cfg = query_config();
    let mut fake = FakeQuery::new(1, 1024);
    fake.cloud_mode = Some(CloudQueryMode::Valid);
    let fake = Arc::new(fake);
    let commands = Arc::new(TestCommands {
        tokens: HashMap::from([([1; 32], "first".into()), ([2; 32], "second".into())]),
        active: true.into(),
        jobs: Default::default(),
    });
    let service = cloud_query_service(&root, fake.clone(), cfg.clone())
        .with_command_resolver(commands.clone())
        .unwrap();
    let local = service.query_io("first").unwrap();
    let socket = root.join("cloud-query.sock");
    let host = Running::start_service(&socket, service).await;
    let client = native_broker::BrokerQueryClient::connect(&socket, "first", cfg.clone())
        .await
        .unwrap();
    let input = || native_broker::OpenCloudQueryScope {
        source: retained_query_source(),
        overlay: vec![],
        replay_key: [9; 32],
    };
    assert_eq!(
        client
            .open_cloud_scope(input(), Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    assert_eq!(
        client
            .clone()
            .with_command_token([2; 32])
            .open_cloud_scope(input(), Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    let client = client.with_command_token([1; 32]);
    let mut changed = input();
    changed.source.provider_identity = [2; 32];
    assert_eq!(
        client
            .open_cloud_scope(changed.clone(), Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        local
            .open_cloud_scope(changed, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut duplicate = input();
    duplicate.source.index_root_ids.push([7; 32]);
    assert_eq!(
        client
            .open_cloud_scope(duplicate, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    let mut oversized = input();
    oversized.overlay.push(vec![0; cfg.max_query_bytes + 1]);
    assert_eq!(
        client
            .open_cloud_scope(oversized.clone(), Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        local
            .open_cloud_scope(oversized, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    let mut roots = input();
    roots.source.index_root_ids = (0..4097u32)
        .map(|i| {
            let mut id = [0; 32];
            id[..4].copy_from_slice(&i.to_be_bytes());
            id
        })
        .collect();
    assert_eq!(
        client
            .open_cloud_scope(roots, Arc::new(()))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        fake.cloud_calls.load(Ordering::SeqCst),
        0,
        "all invalid paths reject before provider execution"
    );
    // Raw generated-client malformed ID widths must also fail at the service,
    // independently of the typed client's preflight.
    let channel = native_broker::connect_local_channel(
        native_broker::LocalEndpoint::UnixSocket(socket),
        &cfg,
    )
    .await
    .unwrap();
    let mut raw = native_broker::proto::query_io_client::QueryIoClient::new(channel)
        .send_compressed(tonic::codec::CompressionEncoding::Zstd)
        .accept_compressed(tonic::codec::CompressionEncoding::Zstd);
    let source = retained_query_source();
    let mut request = tonic::Request::new(native_broker::proto::QueryOpenCloudScopeRequest {
        store_id: "first".into(),
        source: Some(native_broker::proto::NativeCloudQuerySource {
            provider_identity: source.provider_identity.to_vec(),
            workspace_id: source.workspace_id.to_vec(),
            generation: source.generation,
            manifest_id: source.manifest_id.to_vec(),
            manifest_blake3: source.manifest_blake3.to_vec(),
            manifest_size_bytes: source.manifest_size_bytes,
            index_root_ids: vec![vec![7; 31]],
            expected_anchors: vec![vec![0; 32]],
        }),
        overlay: vec![],
        replay_key: vec![9; 32],
    });
    request.metadata_mut().insert_bin(
        "native-command-bin",
        tonic::metadata::MetadataValue::from_bytes(&[1; 32]),
    );
    let mut oversized_wire = request.get_ref().clone();
    oversized_wire.source.as_mut().unwrap().index_root_ids = vec![vec![7; 32]];
    oversized_wire.overlay = vec![vec![0; cfg.max_query_bytes + 1]];
    assert_eq!(
        raw.open_cloud_scope(request).await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    let mut oversized_request = tonic::Request::new(oversized_wire);
    oversized_request.metadata_mut().insert_bin(
        "native-command-bin",
        tonic::metadata::MetadataValue::from_bytes(&[1; 32]),
    );
    assert_eq!(
        raw.open_cloud_scope(oversized_request)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted,
        "compressed wire bytes do not bypass decompressed request admission"
    );
    assert_eq!(fake.cloud_calls.load(Ordering::SeqCst), 0);
    let held = client
        .open_cloud_scope(input(), Arc::new(()))
        .await
        .unwrap();
    assert_eq!(
        held.value.cloud_source_digest,
        Some(retained_query_source().digest())
    );
    command_jobs_idle(&commands).await;
    drop((held, raw, client, local));
    host.stop().await;
}
