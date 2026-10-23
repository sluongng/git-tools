#![cfg(windows)]
use async_trait::async_trait;
use jj_native_backend::{DirectObjectIo, NativeObjectIo};
use native_broker::{
    AdmittedCommit, BrokerControlClient, BrokerService, CommitId, Config, ConfirmedPublication,
    ControlResources, LocalEndpoint, NativeRepositoryControl, PublishRequest, ReferenceSnapshot,
};
use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::net::windows::named_pipe::ServerOptions;
use tonic::Status;
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
fn fresh_endpoint() -> LocalEndpoint {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    LocalEndpoint::WindowsNamedPipe(format!(
        r"\\.\pipe\native-broker-test-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ))
}
fn name(endpoint: &LocalEndpoint) -> &str {
    let LocalEndpoint::WindowsNamedPipe(name) = endpoint else {
        panic!("expected Windows pipe")
    };
    name
}
#[derive(Debug)]
struct Control;
#[async_trait]
impl NativeRepositoryControl for Control {
    fn resources(&self) -> ControlResources {
        ControlResources {
            memory_bytes: 1024,
            temporary_bytes: 0,
        }
    }
    async fn observe_reference(&self, name: &str) -> Result<ReferenceSnapshot, Status> {
        Ok(ReferenceSnapshot {
            name: name.into(),
            generation: 7,
            head: None,
        })
    }
    async fn admit_commit(&self, _: CommitId) -> Result<AdmittedCommit, Status> {
        Err(Status::unimplemented("fixture"))
    }
    async fn publish(&self, _: PublishRequest) -> Result<ConfirmedPublication, Status> {
        Err(Status::aborted("fixture stale generation"))
    }
    async fn publication(&self, _: &str) -> Result<Option<ConfirmedPublication>, Status> {
        Ok(None)
    }
}
async fn service(path: &Path, existing: bool) -> BrokerService {
    let mut backend = jj_native_backend::Config::default();
    backend.store.max_file_bytes = 16 * 1024 * 1024;
    backend.store.max_staging_bytes = 32 * 1024 * 1024;
    let io: Arc<dyn NativeObjectIo> = Arc::new(if existing {
        DirectObjectIo::load(path, backend).unwrap()
    } else {
        DirectObjectIo::init(path, backend).unwrap()
    });
    BrokerService::new(HashMap::from([("example".into(), io)]), config())
        .unwrap()
        .with_controls(HashMap::from([(
            "example".into(),
            Arc::new(Control) as Arc<dyn NativeRepositoryControl>,
        )]))
        .unwrap()
}
struct OwnedChild(Option<Child>);
impl OwnedChild {
    fn spawn(endpoint: &LocalEndpoint, spool: &Path, mode: &str, args: &[&str]) -> Self {
        let _private = private_fs::PrivateDir::create(spool).unwrap();
        let executable = PathBuf::from(env!("BROKER_CLIENT_DRIVER"))
            .canonicalize()
            .unwrap();
        Self(Some(
            Command::new(executable)
                .arg(name(endpoint))
                .arg(spool)
                .arg(mode)
                .args(args)
                .env_clear()
                .env("PATH", "")
                .env("TEMP", spool)
                .env("TMP", spool)
                .env("SYSTEMROOT", std::env::var_os("SYSTEMROOT").unwrap())
                .current_dir(spool)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap(),
        ))
    }
    async fn finish(mut self) -> String {
        tokio::task::spawn_blocking(move || {
            let child = self.0.as_mut().unwrap();
            let deadline = Instant::now() + Duration::from_secs(30);
            let status = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    break status;
                }
                assert!(Instant::now() < deadline, "owned client deadline");
                std::thread::sleep(Duration::from_millis(10));
            };
            let mut out = String::new();
            child
                .stdout
                .take()
                .unwrap()
                .take(4097)
                .read_to_string(&mut out)
                .unwrap();
            assert!(out.len() <= 4096);
            assert!(status.success(), "client failed: {out}");
            self.0.take();
            out.trim().into()
        })
        .await
        .unwrap()
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let deadline = Instant::now() + Duration::from_secs(5);
            while child.try_wait().ok().flatten().is_none() {
                if Instant::now() >= deadline {
                    let _ = std::thread::spawn(move || {
                        let _ = child.wait();
                    });
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
async fn ready(endpoint: &LocalEndpoint) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if BrokerControlClient::connect_local(endpoint.clone(), "example", config())
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn independent_processes_share_native_pipe_object_and_control_services() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("private");
    let _root = private_fs::PrivateDir::create(&root).unwrap();
    let endpoint = fresh_endpoint();
    let broker_service = service(&root.join("store"), false).await;
    let (stop, signal) = tokio::sync::oneshot::channel();
    let address = endpoint.clone();
    let server = tokio::spawn(async move {
        native_broker::serve_local(&address, broker_service, async {
            let _ = signal.await;
        })
        .await
    });
    ready(&endpoint).await;
    let first = OwnedChild::spawn(&endpoint, &root.join("first"), "first", &[]);
    let second = OwnedChild::spawn(&endpoint, &root.join("second"), "second", &[]);
    let (first, second) = tokio::join!(first.finish(), second.finish());
    assert!(first.starts_with("OK "));
    assert!(second.starts_with("OK "));
    let id = first.split_whitespace().nth(1).unwrap();
    assert_eq!(
        OwnedChild::spawn(&endpoint, &root.join("reader"), "read", &[id, "first"])
            .finish()
            .await,
        "READ_OK"
    );
    assert_eq!(
        OwnedChild::spawn(
            &endpoint,
            &root.join("control"),
            "control-observe",
            &["main"]
        )
        .finish()
        .await,
        "OBSERVED 7 absent"
    );
    assert_eq!(
        OwnedChild::spawn(&endpoint, &root.join("unknown"), "unknown", &[])
            .finish()
            .await,
        "UNKNOWN_DENIED"
    );
    assert!(
        OwnedChild::spawn(&endpoint, &root.join("cancel"), "cancel", &[])
            .finish()
            .await
            .starts_with("OK ")
    );
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let reopened_endpoint = fresh_endpoint();
    let reopened_service = service(&root.join("store"), true).await;
    let (stop, signal) = tokio::sync::oneshot::channel();
    let address = reopened_endpoint.clone();
    let server = tokio::spawn(async move {
        native_broker::serve_local(&address, reopened_service, async {
            let _ = signal.await;
        })
        .await
    });
    ready(&reopened_endpoint).await;
    assert_eq!(
        OwnedChild::spawn(
            &reopened_endpoint,
            &root.join("reopened-reader"),
            "read",
            &[id, "first"]
        )
        .finish()
        .await,
        "READ_OK"
    );
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn existing_pipe_instance_is_not_reused_or_removed() {
    let temp = tempfile::tempdir().unwrap();
    let endpoint = fresh_endpoint();
    let occupied = ServerOptions::new()
        .first_pipe_instance(true)
        .create(name(&endpoint))
        .unwrap();
    let result = native_broker::serve_local(
        &endpoint,
        service(&temp.path().join("store"), false).await,
        std::future::pending(),
    )
    .await;
    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
    drop(occupied);
}
#[tokio::test]
async fn client_rejects_same_user_server_with_null_dacl() {
    use windows_sys::Win32::Security::{
        InitializeSecurityDescriptor, SECURITY_ATTRIBUTES, SECURITY_DESCRIPTOR,
        SetSecurityDescriptorDacl,
    };
    let endpoint = fresh_endpoint();
    // Explicitly insecure compatibility fixture, never used by production bind.
    let mut descriptor: SECURITY_DESCRIPTOR = unsafe { std::mem::zeroed() };
    unsafe {
        assert_ne!(
            InitializeSecurityDescriptor((&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(), 1),
            0
        );
        assert_ne!(
            SetSecurityDescriptorDacl(
                (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
                1,
                std::ptr::null_mut(),
                0
            ),
            0
        );
    }
    let mut attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: (&mut descriptor as *mut SECURITY_DESCRIPTOR).cast(),
        bInheritHandle: 0,
    };
    let pipe = unsafe {
        ServerOptions::new()
            .first_pipe_instance(true)
            .reject_remote_clients(true)
            .create_with_security_attributes_raw(
                name(&endpoint),
                (&mut attributes as *mut SECURITY_ATTRIBUTES).cast(),
            )
            .unwrap()
    };
    let client = BrokerControlClient::connect_local(endpoint, "example", config());
    let connect = tokio::spawn(async move {
        let _ = pipe.connect().await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    assert!(client.await.is_err());
    connect.abort();
}
