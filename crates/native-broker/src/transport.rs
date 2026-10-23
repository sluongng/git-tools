use std::{
    future::Future,
    path::{Path, PathBuf},
};
use tonic::Status;

#[cfg(windows)]
mod windows;

/// Explicit local transport; never inferred as a network endpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LocalEndpoint {
    UnixSocket(PathBuf),
    WindowsNamedPipe(String),
}
impl LocalEndpoint {
    pub fn validate(&self) -> Result<(), Status> {
        match self {
            Self::UnixSocket(path) if path.is_absolute() => Ok(()),
            Self::UnixSocket(_) => Err(Status::invalid_argument("broker socket must be absolute")),
            Self::WindowsNamedPipe(name) => {
                let Some(suffix) = name.strip_prefix(r"\\.\pipe\") else {
                    return Err(Status::invalid_argument(
                        "broker pipe must use the local pipe namespace",
                    ));
                };
                if suffix.is_empty()
                    || suffix.len() > 128
                    || suffix == "."
                    || suffix == ".."
                    || !suffix.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
                {
                    return Err(Status::invalid_argument("invalid broker pipe name"));
                }
                Ok(())
            }
        }
    }
}
/// Serve both private RPC surfaces over the explicitly selected native transport.
/// Unsupported transports fail; there is no loopback network fallback.
pub async fn serve_local(
    endpoint: &LocalEndpoint,
    service: crate::BrokerService,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Status> {
    endpoint.validate()?;
    match endpoint {
        #[cfg(unix)]
        LocalEndpoint::UnixSocket(path) => serve_unix(path, service, shutdown).await,
        #[cfg(windows)]
        LocalEndpoint::WindowsNamedPipe(name) => windows::serve(name, service, shutdown).await,
        _ => Err(Status::unimplemented(
            "selected native broker transport is unavailable",
        )),
    }
}

pub async fn connect_local_channel(
    endpoint: LocalEndpoint,
    config: &crate::Config,
) -> Result<tonic::transport::Channel, Status> {
    endpoint.validate()?;
    match endpoint {
        #[cfg(unix)]
        LocalEndpoint::UnixSocket(socket) => connect_unix_channel(socket, config).await,
        #[cfg(windows)]
        LocalEndpoint::WindowsNamedPipe(name) => windows::connect(name, config).await,
        _ => Err(Status::unimplemented(
            "selected native broker transport is unavailable",
        )),
    }
}

/// Serve a private router using exactly the broker's endpoint authentication.
/// Services must carry their own admission/body-lifetime/draining scopes.
pub async fn serve_local_router(
    endpoint: &LocalEndpoint,
    router: tonic::transport::server::Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Status> {
    endpoint.validate()?;
    match endpoint {
        #[cfg(unix)]
        LocalEndpoint::UnixSocket(path) => {
            serve_unix_router(path, router, shutdown, async { Ok(()) }).await
        }
        #[cfg(windows)]
        LocalEndpoint::WindowsNamedPipe(name) => {
            windows::serve_router(name, router, shutdown).await
        }
        _ => Err(Status::unimplemented(
            "selected native broker transport is unavailable",
        )),
    }
}

#[cfg(unix)]
pub(crate) fn private_directory(path: &Path) -> Result<(), Status> {
    use std::{os::unix::fs::MetadataExt, path::Component};
    if !path.is_absolute()
        || path
            .components()
            .any(|p| matches!(p, Component::ParentDir | Component::CurDir))
    {
        return Err(Status::invalid_argument(
            "broker directory must be absolute and normalized",
        ));
    }
    let uid = unsafe { libc::geteuid() };
    let mut cursor = PathBuf::new();
    for part in path.components() {
        cursor.push(part);
        let m = std::fs::symlink_metadata(&cursor)
            .map_err(|_| Status::failed_precondition("broker directory unavailable"))?;
        if !m.is_dir()
            || (m.uid() != uid && m.uid() != 0)
            || (m.mode() & 0o022 != 0 && !(m.uid() == 0 && m.mode() & 0o1000 != 0))
        {
            return Err(Status::permission_denied(
                "broker directory ancestors are not trusted",
            ));
        }
    }
    let m = std::fs::symlink_metadata(path)
        .map_err(|_| Status::failed_precondition("broker directory unavailable"))?;
    if m.uid() != uid || m.mode() & 0o077 != 0 {
        return Err(Status::permission_denied(
            "broker directory must be owner-only",
        ));
    }
    Ok(())
}
#[cfg(windows)]
pub(crate) fn private_directory(path: &Path) -> Result<(), Status> {
    private_fs::PrivateDir::open_existing(path)
        .map(drop)
        .map_err(|_| Status::permission_denied("broker directory is not private"))
}
#[cfg(not(any(unix, windows)))]
pub(crate) fn private_directory(_: &Path) -> Result<(), Status> {
    Err(Status::unimplemented(
        "private native broker transport unavailable",
    ))
}
#[cfg(unix)]
pub(crate) fn validate_socket(path: &Path) -> Result<(), Status> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    private_directory(
        path.parent()
            .ok_or_else(|| Status::invalid_argument("invalid broker socket path"))?,
    )?;
    let m = std::fs::symlink_metadata(path)
        .map_err(|_| Status::unavailable("native broker unavailable"))?;
    if !m.file_type().is_socket() || m.uid() != unsafe { libc::geteuid() } || m.mode() & 0o077 != 0
    {
        return Err(Status::permission_denied(
            "native broker socket is not private",
        ));
    }
    Ok(())
}
#[cfg(unix)]
struct SocketGuard {
    path: PathBuf,
    dev: u64,
    ino: u64,
}
#[cfg(unix)]
impl Drop for SocketGuard {
    fn drop(&mut self) {
        use std::os::unix::fs::MetadataExt;
        if let Ok(m) = std::fs::symlink_metadata(&self.path) {
            if m.dev() == self.dev && m.ino() == self.ino {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}
/// Bind only in an existing private trusted directory. Existing endpoint paths
/// are never removed; shutdown drains requests and removes only our own inode.
#[cfg(unix)]
pub async fn serve_unix(
    path: &Path,
    service: crate::BrokerService,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Status> {
    let router = tonic::transport::Server::builder()
        .add_service(service.rpc_service())
        .add_service(service.control_rpc_service());
    serve_unix_router(path, router, shutdown, service.drain()).await
}
#[cfg(unix)]
async fn serve_unix_router(
    path: &Path,
    router: tonic::transport::server::Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
    drained: impl Future<Output = Result<(), Status>>,
) -> Result<(), Status> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    private_directory(
        path.parent()
            .ok_or_else(|| Status::invalid_argument("invalid broker socket path"))?,
    )?;
    let listener = tokio::net::UnixListener::bind(path)
        .map_err(|_| Status::failed_precondition("cannot bind native broker socket"))?;
    let m = std::fs::symlink_metadata(path)
        .map_err(|_| Status::internal("cannot inspect native broker socket"))?;
    let guard = SocketGuard {
        path: path.into(),
        dev: m.dev(),
        ino: m.ino(),
    };
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .map_err(|_| Status::internal("cannot secure native broker socket"))?;
    let uid = unsafe { libc::geteuid() };
    let incoming: std::pin::Pin<
        Box<dyn futures::Stream<Item = std::io::Result<tokio::net::UnixStream>> + Send>,
    > = Box::pin(async_stream::try_stream! {
        loop {let (stream,_)=listener.accept().await?;if stream.peer_cred()?.uid()==uid {yield stream;}}
    });
    let result = router
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
        .map_err(|_| Status::internal("native broker listener failed"));
    drained.await?;
    drop(guard);
    result
}

#[cfg(unix)]
async fn connect_unix_channel(
    socket: PathBuf,
    config: &crate::Config,
) -> Result<tonic::transport::Channel, Status> {
    use tonic::transport::Endpoint;

    crate::transport::validate_socket(&socket)?;
    let uid = unsafe { libc::geteuid() };
    tokio::time::timeout(
        config.policy.connect_timeout(),
        Endpoint::from_static("http://[::]:50051")
            .connect_timeout(config.policy.connect_timeout())
            .connect_with_connector(tower::service_fn(move |_: http::Uri| {
                let socket = socket.clone();
                async move {
                    let stream = tokio::net::UnixStream::connect(socket).await?;
                    if stream.peer_cred()?.uid() != uid {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "broker peer owner mismatch",
                        ));
                    }
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                }
            })),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("native broker connect deadline exceeded"))?
    .map_err(|_| Status::unavailable("cannot connect native broker"))
}
