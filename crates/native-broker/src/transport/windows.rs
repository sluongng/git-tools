//! Owner-authenticated Windows local IPC. No network transport fallback.
use private_fs::windows::{CurrentUser, UserSecurity, verify_private_handle};
use std::{
    future::Future,
    io,
    os::windows::io::AsRawHandle,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions},
};
use tonic::{
    Status,
    transport::{Channel, Endpoint, server::Connected},
};
use windows_sys::Win32::{
    Foundation::{ERROR_PIPE_BUSY, HANDLE},
    System::Pipes::{GetNamedPipeClientProcessId, GetNamedPipeServerProcessId},
};
fn denied() -> io::Error {
    io::Error::new(
        io::ErrorKind::PermissionDenied,
        "native broker pipe is not owner authenticated",
    )
}
fn verify_peer(handle: HANDLE, server_peer: bool) -> io::Result<()> {
    // SAFETY: caller holds its pipe object alive for this synchronous check.
    unsafe {
        verify_private_handle(handle)?;
    }
    let mut process_id = 0;
    // SAFETY: live pipe handle and valid writable process-ID output.
    let ok = unsafe {
        if server_peer {
            GetNamedPipeServerProcessId(handle, &mut process_id)
        } else {
            GetNamedPipeClientProcessId(handle, &mut process_id)
        }
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    if !CurrentUser::get()?.same_process_user(process_id)? {
        return Err(denied());
    }
    Ok(())
}
fn create_pipe(name: &str, first: bool) -> io::Result<NamedPipeServer> {
    let security = UserSecurity::new()?;
    let mut attributes = security.attributes();
    // SAFETY: the descriptor and attributes remain alive through CreateNamedPipe.
    // Non-inheritable owner-only protected ACL; remote clients are prohibited.
    let pipe = unsafe {
        ServerOptions::new()
            .first_pipe_instance(first)
            .reject_remote_clients(true)
            .create_with_security_attributes_raw(
                name,
                (&mut attributes as *mut windows_sys::Win32::Security::SECURITY_ATTRIBUTES).cast(),
            )?
    };
    // SAFETY: newly created pipe object is alive throughout the verification.
    unsafe {
        verify_private_handle(pipe.as_raw_handle().cast())?;
    }
    Ok(pipe)
}
pub(super) async fn connect(name: String, config: &crate::Config) -> Result<Channel, Status> {
    tokio::time::timeout(
        config.policy.connect_timeout(),
        Endpoint::from_static("http://[::]:50051")
            .connect_timeout(config.policy.connect_timeout())
            .connect_with_connector(tower::service_fn(move |_: http::Uri| {
                let name = name.clone();
                async move {
                    loop {
                        // Tokio defaults to SECURITY_IDENTIFICATION: the peer
                        // can identify this client but cannot impersonate it.
                        match ClientOptions::new().open(&name) {
                            Ok(pipe) => {
                                verify_peer(pipe.as_raw_handle().cast(), true)?;
                                return Ok::<_, io::Error>(hyper_util::rt::TokioIo::new(pipe));
                            }
                            Err(error) if error.raw_os_error() == Some(ERROR_PIPE_BUSY as i32) => {
                                tokio::time::sleep(Duration::from_millis(10)).await;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
            })),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("native broker connect deadline exceeded"))?
    .map_err(|_| Status::unavailable("cannot connect authenticated native broker pipe"))
}
struct AcceptedPipe(NamedPipeServer);
impl Connected for AcceptedPipe {
    type ConnectInfo = ();
    fn connect_info(&self) {}
}
impl AsyncRead for AcceptedPipe {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buffer)
    }
}
impl AsyncWrite for AcceptedPipe {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}
pub(super) async fn serve(
    name: &str,
    service: crate::BrokerService,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Status> {
    let router = tonic::transport::Server::builder()
        .add_service(service.rpc_service())
        .add_service(service.control_rpc_service())
        .add_service(service.query_rpc_service());
    let result = serve_router(name, router, shutdown).await;
    service.drain().await?;
    result
}
pub(super) async fn serve_router(
    name: &str,
    router: tonic::transport::server::Router,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), Status> {
    // Bind synchronously before returning control to the accept loop. A
    // pre-existing instance always fails; never attach to somebody else's pipe.
    let mut listener = create_pipe(name, true)
        .map_err(|_| Status::failed_precondition("cannot reserve private native broker pipe"))?;
    let name = name.to_owned();
    let incoming: Pin<Box<dyn futures::Stream<Item = io::Result<AcceptedPipe>> + Send>> =
        Box::pin(async_stream::try_stream! {
            loop {
                listener.connect().await?;
                // Always retain a listening/connected handle while replacing the
                // accept instance, preventing a name-ownership gap between clients.
                let next = create_pipe(&name, false)?;
                let accepted = std::mem::replace(&mut listener, next);
                if verify_peer(accepted.as_raw_handle().cast(), false).is_ok() {
                    yield AcceptedPipe(accepted);
                }
            }
        });
    let result = router
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await
        .map_err(|_| Status::internal("native broker pipe listener failed"));
    result
}
