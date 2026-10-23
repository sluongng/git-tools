use std::{
    future::Future,
    path::{Path, PathBuf},
};
use tonic::Status;
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
#[cfg(not(unix))]
pub(crate) fn private_directory(_: &Path) -> Result<(), Status> {
    Err(Status::unimplemented(
        "private broker transport requires native Unix support",
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
    let result = tonic::transport::Server::builder()
        .add_service(service.rpc_service())
        .add_service(service.control_rpc_service())
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
        .map_err(|_| Status::internal("native broker listener failed"));
    service.drain().await?;
    drop(guard);
    result
}
