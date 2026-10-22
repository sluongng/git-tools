//! Listener recovery for transient local descriptor exhaustion.
//!
//! Recovery is confined to EMFILE/ENFILE. Dropping the incoming stream cancels
//! its pending accept or backoff; no detached accept producer survives shutdown.
use futures_core::Stream;
use std::{
    future::Future,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::net::{TcpListener, TcpStream};

const RETRY_DELAY: Duration = Duration::from_millis(100);
const WARNING_INTERVAL: Duration = Duration::from_secs(5);
const RESOURCE_WARNING: &str =
    "local descriptor budget exhausted while accepting connections; retrying";

#[derive(Default)]
struct Inner {
    exhausted: AtomicU64,
    last_warning: Mutex<Option<Instant>>,
}
/// Shared, privacy-safe accept diagnostics across listeners and server clones.
#[derive(Clone, Default)]
pub struct AcceptState(Arc<Inner>);
impl AcceptState {
    pub fn resource_exhaustions(&self) -> u64 {
        self.0.exhausted.load(Ordering::Relaxed)
    }
    fn exhausted(&self) {
        self.0.exhausted.fetch_add(1, Ordering::Relaxed);
        let warn = {
            let mut last = self
                .0
                .last_warning
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            if last.is_none_or(|at| now.duration_since(at) >= WARNING_INTERVAL) {
                *last = Some(now);
                true
            } else {
                false
            }
        };
        if warn {
            eprintln!("{RESOURCE_WARNING}");
        }
    }
}
/// Only Unix process/system descriptor exhaustion is safe to retry here.
pub fn is_descriptor_exhaustion(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(error.raw_os_error(), Some(libc::EMFILE | libc::ENFILE))
    }
    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}
async fn retry_accept<T, F, Fut>(state: &AcceptState, mut accept: F) -> io::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = io::Result<T>>,
{
    loop {
        match accept().await {
            Err(error) if is_descriptor_exhaustion(&error) => {
                state.exhausted();
                tokio::time::sleep(RETRY_DELAY).await;
            }
            result => return result,
        }
    }
}
#[cfg(unix)]
pub async fn accept_unix(
    listener: &tokio::net::UnixListener,
    state: &AcceptState,
) -> io::Result<tokio::net::UnixStream> {
    retry_accept(state, || async {
        listener.accept().await.map(|(stream, _)| stream)
    })
    .await
}
trait Acceptor: Send + Sync {
    type Connection: Send;
    fn accept(&self) -> impl Future<Output = io::Result<Self::Connection>> + Send;
}
impl Acceptor for TcpListener {
    type Connection = TcpStream;
    async fn accept(&self) -> io::Result<TcpStream> {
        TcpListener::accept(self).await.map(|(stream, _)| stream)
    }
}
#[cfg(unix)]
impl Acceptor for tokio::net::UnixListener {
    type Connection = tokio::net::UnixStream;
    async fn accept(&self) -> io::Result<Self::Connection> {
        tokio::net::UnixListener::accept(self)
            .await
            .map(|(stream, _)| stream)
    }
}
fn incoming<A: Acceptor>(
    listener: A,
    state: AcceptState,
) -> impl Stream<Item = io::Result<A::Connection>> + Send {
    async_stream::stream! {
        loop {
            // Preserve listener-stream behavior for nonresource errors: expose
            // the error to the server, then allow its next poll to accept again.
            yield retry_accept(&state, || listener.accept()).await;
        }
    }
}
#[cfg(unix)]
pub fn unix_incoming(
    listener: tokio::net::UnixListener,
    state: AcceptState,
) -> impl Stream<Item = io::Result<tokio::net::UnixStream>> + Send {
    incoming(listener, state)
}
pub fn tcp_incoming(
    listener: TcpListener,
    state: AcceptState,
) -> impl Stream<Item = io::Result<TcpStream>> + Send {
    incoming(listener, state)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    #[tokio::test]
    async fn descriptor_errors_recover_and_clones_share_observations() {
        let state = AcceptState::default();
        let shared = state.clone();
        let mut calls = 0;
        let result = retry_accept(&state, || {
            calls += 1;
            std::future::ready(match calls {
                1 => Err(io::Error::from_raw_os_error(libc::EMFILE)),
                2 => Err(io::Error::from_raw_os_error(libc::ENFILE)),
                _ => Ok(7),
            })
        })
        .await
        .unwrap();
        assert_eq!((result, calls, shared.resource_exhaustions()), (7, 3, 2));
    }
    #[tokio::test]
    async fn incoming_error_does_not_end_later_accepts() {
        struct Scripted(std::sync::atomic::AtomicUsize);
        impl Acceptor for Scripted {
            type Connection = usize;
            async fn accept(&self) -> io::Result<usize> {
                match self.0.fetch_add(1, Ordering::Relaxed) {
                    0 => Err(io::Error::new(io::ErrorKind::PermissionDenied, "fixture")),
                    n => Ok(n),
                }
            }
        }
        let state = AcceptState::default();
        let mut stream = Box::pin(incoming(Scripted(Default::default()), state.clone()));
        let error = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        for expected in [1, 2] {
            let accepted = std::future::poll_fn(|cx| stream.as_mut().poll_next(cx))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(accepted, expected);
        }
        assert_eq!(state.resource_exhaustions(), 0);
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_drops_backoff_and_owned_accept_state() {
        struct Dropped(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let state = AcceptState::default();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let marker = Dropped(dropped.clone());
        let mut pending = Box::pin(retry_accept::<(), _, _>(&state, move || {
            let _ = &marker;
            std::future::ready(Err(io::Error::from_raw_os_error(libc::EMFILE)))
        }));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(pending.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        assert_eq!(state.resource_exhaustions(), 1);
        drop(pending);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
