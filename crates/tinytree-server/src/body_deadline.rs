//! A body lifetime budget independent of HTTP/2 flow control and polling.
use bytes::Bytes;
#[derive(Clone)]
pub(crate) struct ResponseBudget(#[allow(dead_code)] Arc<dyn std::any::Any + Send + Sync>);
impl ResponseBudget {
    pub(crate) fn new(value: impl std::any::Any + Send + Sync) -> Self {
        Self(Arc::new(value))
    }
}
use http::{Request, Response};
use http_body::{Body, Frame};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    time::Duration,
};
use tower::{Layer, Service};

#[derive(Clone)]
pub struct DeadlineLayer {
    budget: Duration,
    frame_bytes: usize,
    idle: Duration,
}
impl DeadlineLayer {
    pub fn new(operation_timeout_ms: u64, stream_idle_timeout_ms: u64, frame_bytes: usize) -> Self {
        Self {
            budget: Duration::from_millis(operation_timeout_ms),
            idle: Duration::from_millis(stream_idle_timeout_ms),
            frame_bytes,
        }
    }
}
impl<S> Layer<S> for DeadlineLayer {
    type Service = DeadlineService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        DeadlineService {
            inner,
            config: self.clone(),
        }
    }
}
#[derive(Clone)]
pub struct DeadlineService<S> {
    inner: S,
    config: DeadlineLayer,
}
impl<S, I, B> Service<Request<I>> for DeadlineService<S>
where
    S: Service<Request<I>, Response = Response<B>>,
    S::Future: Send + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: 'static,
{
    type Response = Response<DeadlineBody<B>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: Request<I>) -> Self::Future {
        let mut budget = self.config.budget;
        if let Some(value) = request
            .headers()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
        {
            if let Some(duration) = parse_timeout(value) {
                budget = budget.min(duration);
            }
        }
        let deadline = tokio::time::Instant::now() + budget;
        let frame_bytes = self.config.frame_bytes;
        let idle = self.config.idle;
        let response = self.inner.call(request);
        Box::pin(async move {
            let mut response = response.await?;
            let budget = response.extensions_mut().remove::<ResponseBudget>();
            Ok(response.map(|body| DeadlineBody::new(body, deadline, idle, frame_bytes, budget)))
        })
    }
}
fn parse_timeout(value: &str) -> Option<Duration> {
    if !(2..=9).contains(&value.len()) {
        return None;
    }
    let (n, unit) = value.split_at(value.len() - 1);
    if !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = n.parse().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n.saturating_mul(3600)),
        "M" => Duration::from_secs(n.saturating_mul(60)),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}
struct State<B> {
    body: Option<Pin<Box<B>>>,
    pending: Bytes,
    expired: bool,
    done: bool,
    waker: Option<Waker>,
    idle_deadline: tokio::time::Instant,
    budget: Option<ResponseBudget>,
}
pub struct DeadlineBody<B> {
    state: Arc<Mutex<State<B>>>,
    timer: tokio::task::JoinHandle<()>,
    frame_bytes: usize,
    idle: Duration,
}
impl<B: Body<Data = Bytes> + Send + 'static> DeadlineBody<B> {
    fn new(
        body: B,
        deadline: tokio::time::Instant,
        idle: Duration,
        frame_bytes: usize,
        budget: Option<ResponseBudget>,
    ) -> Self {
        let state = Arc::new(Mutex::new(State {
            body: Some(Box::pin(body)),
            budget,
            pending: Bytes::new(),
            expired: false,
            done: false,
            waker: None,
            idle_deadline: tokio::time::Instant::now() + idle,
        }));
        let weak = Arc::downgrade(&state);
        let timer = tokio::spawn(async move {
            loop {
                let next = match weak.upgrade() {
                    Some(state) => state.lock().unwrap().idle_deadline.min(deadline),
                    None => return,
                };
                tokio::time::sleep_until(next).await;
                if let Some(state) = weak.upgrade() {
                    let (body, pending, budget, waker) = {
                        let mut state = state.lock().unwrap();
                        if state.done {
                            return;
                        }
                        if tokio::time::Instant::now() < state.idle_deadline.min(deadline) {
                            continue;
                        }
                        state.expired = true;
                        (
                            state.body.take(),
                            std::mem::take(&mut state.pending),
                            state.budget.take(),
                            state.waker.take(),
                        )
                    };
                    // Dropping a stream can release a store read lease. Never do so under our mutex.
                    drop(body);
                    drop(pending);
                    drop(budget);
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                    return;
                } else {
                    return;
                }
            }
        });
        Self {
            state,
            timer,
            frame_bytes: frame_bytes.max(1),
            idle,
        }
    }
}
impl<B> Drop for DeadlineBody<B> {
    fn drop(&mut self) {
        self.timer.abort();
    }
}
impl<B: Body<Data = Bytes> + Send + 'static> Body for DeadlineBody<B> {
    type Data = Bytes;
    type Error = B::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, B::Error>>> {
        let this = self.get_mut();
        let mut state = this.state.lock().unwrap();
        if state.done {
            return Poll::Ready(None);
        }
        if state.expired {
            state.done = true;
            let mut trailers = http::HeaderMap::new();
            trailers.insert("grpc-status", http::HeaderValue::from_static("4"));
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        state.waker = Some(cx.waker().clone());
        if !state.pending.is_empty() {
            state.idle_deadline = tokio::time::Instant::now() + this.idle;
            let n = this.frame_bytes.min(state.pending.len());
            return Poll::Ready(Some(Ok(Frame::data(state.pending.split_to(n)))));
        }
        let result = state.body.as_mut().unwrap().as_mut().poll_frame(cx);
        let output = match result {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(mut bytes) => {
                    state.idle_deadline = tokio::time::Instant::now() + this.idle;
                    let chunk = bytes.split_to(this.frame_bytes.min(bytes.len()));
                    state.pending = bytes;
                    Poll::Ready(Some(Ok(Frame::data(chunk))))
                }
                Err(frame) => {
                    if frame.is_trailers() {
                        state.done = true;
                        this.timer.abort();
                    }
                    Poll::Ready(Some(Ok(frame)))
                }
            },
            Poll::Ready(None) => {
                state.done = true;
                this.timer.abort();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                state.done = true;
                this.timer.abort();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Pending => Poll::Pending,
        };
        let finished = if state.done {
            Some((state.body.take(), state.budget.take()))
        } else {
            None
        };
        drop(state);
        drop(finished);
        output
    }
    fn is_end_stream(&self) -> bool {
        self.state.lock().unwrap().done
    }
}
