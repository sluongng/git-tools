//! Shared, bounded policy for replay-safe REAPI calls.
//!
//! A deadline belongs to the complete operation, not to each retry. Callers must
//! keep admission, body consumption and verification inside that same deadline.
use std::{fmt, future::Future, io::Read, path::Path, time::Duration};

use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tonic::{Code, Status};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub connect_timeout_ms: u64,
    pub rpc_timeout_ms: u64,
    pub stream_idle_timeout_ms: u64,
    pub operation_timeout_ms: u64,
    pub background_timeout_ms: u64,
    /// Advisory positive-presence suppression, never a remote retention lease.
    /// Zero disables suppression, so every operation checks the backend.
    pub presence_ttl_seconds: u64,
    pub retry: RetryPolicy,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct RetryPolicy {
    /// Total attempts, including the original call.
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            rpc_timeout_ms: 60_000,
            stream_idle_timeout_ms: 60_000,
            operation_timeout_ms: 120_000,
            background_timeout_ms: 600_000,
            presence_ttl_seconds: 0,
            retry: RetryPolicy::default(),
        }
    }
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 4,
            initial_backoff_ms: 100,
            max_backoff_ms: 5_000,
        }
    }
}

/// Configuration errors deliberately omit file contents and parser diagnostics:
/// a misconfigured file may contain credentials even though this schema cannot.
#[derive(Debug)]
pub struct ConfigError(&'static str);
impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ConfigError {}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ConfigFile {
    reapi: Policy,
}

impl Policy {
    /// Read an explicit TOML file. Unknown sections/keys and invalid values fail
    /// closed; no implicit discovery, environment expansion or credentials.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let file = std::fs::File::open(path)
            .map_err(|_| ConfigError("cannot open REAPI configuration file"))?;
        let mut bytes = Vec::new();
        file.take(65_537)
            .read_to_end(&mut bytes)
            .map_err(|_| ConfigError("cannot read REAPI configuration file"))?;
        if bytes.len() > 65_536 {
            return Err(ConfigError("REAPI configuration exceeds 64 KiB"));
        }
        let input = std::str::from_utf8(&bytes)
            .map_err(|_| ConfigError("REAPI configuration is not UTF-8"))?;
        Self::parse(input)
    }
    fn parse(input: &str) -> Result<Self, ConfigError> {
        let file: ConfigFile = toml::from_str(input).map_err(|_| {
            ConfigError("invalid REAPI TOML configuration; check schema and value types")
        })?;
        file.reapi.validate()?;
        Ok(file.reapi)
    }
    pub fn validate(&self) -> Result<(), ConfigError> {
        // Bound every timer to one day, and positive presence hints to 30 days.
        // These upper limits also keep Instant arithmetic and backoff finite.
        for value in [
            self.connect_timeout_ms,
            self.rpc_timeout_ms,
            self.stream_idle_timeout_ms,
            self.operation_timeout_ms,
            self.background_timeout_ms,
        ] {
            if !(1..=86_400_000).contains(&value) {
                return Err(ConfigError(
                    "REAPI timeouts must be between 1 ms and 24 hours",
                ));
            }
        }
        if self.presence_ttl_seconds > 2_592_000 {
            return Err(ConfigError(
                "REAPI presence TTL must be between zero and 30 days",
            ));
        }
        if !(1..=16).contains(&self.retry.max_attempts) {
            return Err(ConfigError("REAPI max_attempts must be between 1 and 16"));
        }
        if self.retry.initial_backoff_ms == 0
            || self.retry.max_backoff_ms > 60_000
            || self.retry.initial_backoff_ms > self.retry.max_backoff_ms
        {
            return Err(ConfigError(
                "REAPI backoff must satisfy 1 <= initial <= maximum <= 60000 ms",
            ));
        }
        Ok(())
    }
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }
    pub fn rpc_timeout(&self) -> Duration {
        Duration::from_millis(self.rpc_timeout_ms)
    }
    pub fn stream_idle_timeout(&self) -> Duration {
        Duration::from_millis(self.stream_idle_timeout_ms)
    }
    pub fn operation_timeout(&self) -> Duration {
        Duration::from_millis(self.operation_timeout_ms)
    }
    pub fn background_timeout(&self) -> Duration {
        Duration::from_millis(self.background_timeout_ms)
    }
    pub fn presence_ttl(&self) -> Duration {
        Duration::from_secs(self.presence_ttl_seconds)
    }

    /// Retry only replay-safe operations. The closure must recreate its request
    /// and consume its full response; uploads must reopen immutable source data.
    /// Dropping this future cancels the attempt/backoff: no work is detached.
    pub async fn retry<T, F, Fut>(
        &self,
        deadline: Instant,
        attempt_timeout: Duration,
        mut operation: F,
    ) -> Result<T, Status>
    where
        F: FnMut(Duration) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        self.validate()
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        if attempt_timeout.is_zero() {
            return Err(Status::invalid_argument("attempt timeout must be nonzero"));
        }
        let mut backoff_ms = self.retry.initial_backoff_ms;
        for attempt in 1..=self.retry.max_attempts {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .filter(|d| !d.is_zero())
                .ok_or_else(deadline_exceeded)?;
            let allowance = attempt_timeout.min(remaining);
            let attempt_deadline = Instant::now()
                .checked_add(allowance)
                .unwrap_or(deadline)
                .min(deadline);
            let result = timeout(attempt_deadline, operation(allowance)).await;
            let status = match result {
                Ok(value) => return Ok(value),
                Err(status) => status,
            };
            if attempt == self.retry.max_attempts || !retryable(&status) {
                return Err(status);
            }
            let jitter = Duration::from_millis(fastrand::u64(0..=backoff_ms));
            let delay = retry_hint(&status).map_or(jitter, |hint| hint.max(jitter));
            // A server hint may be arbitrarily large. Clamp before constructing
            // a Tokio timer, which otherwise can overflow its Instant range.
            let wake = Instant::now()
                .checked_add(delay)
                .unwrap_or(deadline)
                .min(deadline);
            timeout(deadline, async {
                tokio::time::sleep_until(wake).await;
                Ok(())
            })
            .await?;
            backoff_ms = backoff_ms.saturating_mul(2).min(self.retry.max_backoff_ms);
        }
        unreachable!("validated nonzero max_attempts")
    }
}

fn deadline_exceeded() -> Status {
    Status::deadline_exceeded("REAPI operation deadline exceeded")
}

/// Bound a complete asynchronous stage, including response bodies and sleeps.
pub async fn timeout<T>(
    deadline: Instant,
    future: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    if Instant::now() >= deadline {
        return Err(deadline_exceeded());
    }
    let result = tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| deadline_exceeded())?;
    // Tokio cannot preempt synchronous CPU work inside a poll. Do not report a
    // late success if final verification returned after the operation budget.
    if Instant::now() >= deadline {
        return Err(deadline_exceeded());
    }
    result
}

/// Permanent failures (including cancellation, integrity and authentication)
/// never retry. Resource exhaustion needs an explicit, valid server retry hint.
pub fn retryable(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Aborted => true,
        Code::ResourceExhausted => retry_hint(status).is_some(),
        _ => false,
    }
}

/// Marks a rejected, unaccepted operation as transient admission pressure.
/// Do not use for permanent quotas, oversized requests or accepted work. The
/// hint is bounded to 1 ms..60 s; callers still retain their original deadline.
pub fn resource_exhausted_with_retry(message: impl Into<String>, delay: Duration) -> Status {
    let delay = delay.clamp(Duration::from_millis(1), Duration::from_secs(60));
    let info = RetryInfo {
        retry_delay: Some(prost_types::Duration {
            seconds: delay.as_secs() as i64,
            nanos: delay.subsec_nanos() as i32,
        }),
    };
    let message = message.into();
    let rich = crate::proto::google::rpc::Status {
        code: Code::ResourceExhausted as i32,
        message: message.clone(),
        details: vec![prost_types::Any {
            type_url: "type.googleapis.com/google.rpc.RetryInfo".into(),
            value: info.encode_to_vec(),
        }],
    };
    Status::with_details(
        Code::ResourceExhausted,
        message,
        rich.encode_to_vec().into(),
    )
}

// RetryInfo is not part of the pinned generated proto set. Decode only its
// standard wire shape instead of expanding the generated API dependencies.
#[derive(Clone, PartialEq, Message)]
struct RetryInfo {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<prost_types::Duration>,
}
fn retry_hint(status: &Status) -> Option<Duration> {
    let rich = crate::proto::google::rpc::Status::decode(status.details()).ok()?;
    if rich.code != status.code() as i32 {
        return None;
    }
    rich.details
        .iter()
        .filter(|d| d.type_url.rsplit('/').next() == Some("google.rpc.RetryInfo"))
        .filter_map(|d| RetryInfo::decode(d.value.as_slice()).ok()?.retry_delay)
        .filter_map(|d| {
            if d.seconds < 0 || !(0..1_000_000_000).contains(&d.nanos) {
                return None;
            }
            Some(Duration::new(d.seconds as u64, d.nanos as u32))
        })
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_configuration_and_defaults() {
        assert_eq!(Policy::parse("").unwrap(), Policy::default());
        let p = Policy::parse("[reapi]\npresence_ttl_seconds=300\n[reapi.retry]\nmax_attempts=1")
            .unwrap();
        assert_eq!(p.presence_ttl(), Duration::from_secs(300));
        assert_eq!(p.retry.max_attempts, 1);
        for input in [
            "[reapi]\nrpc_timeout_ms=0",
            "[reapi]\nrpc_timeout_ms=-1",
            "[reapi]\nrpc_timeout_ms=86400001",
            "[reapi.retry]\nmax_attempts=0",
            "[reapi.retry]\nmax_attempts=17",
            "[reapi.retry]\ninitial_backoff_ms=6000",
            "[reapi]\napi_key='secret-marker'",
            "[credentials]\nkey='secret-marker'",
            "[reapi]\nrpc_timeout_ms='secret-marker'",
        ] {
            let error = Policy::parse(input).unwrap_err();
            assert!(!error.to_string().contains("secret-marker"));
        }
    }
    #[test]
    fn only_classified_failures_retry() {
        for code in [Code::Unavailable, Code::DeadlineExceeded, Code::Aborted] {
            assert!(retryable(&Status::new(code, "temporary")));
        }
        for code in [
            Code::Cancelled,
            Code::Unauthenticated,
            Code::PermissionDenied,
            Code::NotFound,
            Code::DataLoss,
            Code::InvalidArgument,
            Code::Unimplemented,
            Code::ResourceExhausted,
            Code::Internal,
            Code::Unknown,
        ] {
            assert!(!retryable(&Status::new(code, "permanent")));
        }
        let info = RetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: 0,
                nanos: 2_000_000,
            }),
        };
        let rich = crate::proto::google::rpc::Status {
            code: Code::ResourceExhausted as i32,
            message: String::new(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/google.rpc.RetryInfo".into(),
                value: info.encode_to_vec(),
            }],
        };
        let status =
            Status::with_details(Code::ResourceExhausted, "busy", rich.encode_to_vec().into());
        assert!(retryable(&status));
        assert_eq!(retry_hint(&status), Some(Duration::from_millis(2)));
        for (input, expected) in [
            (Duration::ZERO, Duration::from_millis(1)),
            (Duration::from_millis(25), Duration::from_millis(25)),
            (Duration::MAX, Duration::from_secs(60)),
        ] {
            let status = resource_exhausted_with_retry("admission busy", input);
            assert_eq!(status.code(), Code::ResourceExhausted);
            assert!(retryable(&status));
            assert_eq!(retry_hint(&status), Some(expected));
        }
    }
    #[tokio::test]
    async fn recovery_attempt_limit_and_permanent_errors() {
        let mut p = Policy::default();
        p.retry.initial_backoff_ms = 1;
        p.retry.max_backoff_ms = 1;
        let mut calls = 0;
        let answer = p
            .retry(
                Instant::now() + Duration::from_secs(1),
                p.rpc_timeout(),
                |_| {
                    calls += 1;
                    std::future::ready(if calls == 3 {
                        Ok(42)
                    } else {
                        Err(Status::unavailable("transient"))
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(answer, 42);
        assert_eq!(calls, 3);
        calls = 0;
        let err = p
            .retry::<(), _, _>(
                Instant::now() + Duration::from_secs(1),
                p.rpc_timeout(),
                |_| {
                    calls += 1;
                    std::future::ready(Err(Status::unavailable("persistent")))
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(calls, 4);
        calls = 0;
        let err = p
            .retry::<(), _, _>(
                Instant::now() + Duration::from_secs(1),
                p.rpc_timeout(),
                |_| {
                    calls += 1;
                    std::future::ready(Err(Status::data_loss("corrupt")))
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::DataLoss);
        assert_eq!(calls, 1);
    }
    #[tokio::test]
    async fn complete_body_obeys_deadline_and_attempt_allowance() {
        let p = Policy::default();
        let start = Instant::now();
        let mut calls = 0;
        let err = p
            .retry::<(), _, _>(
                start + Duration::from_millis(20),
                Duration::from_secs(60),
                |remaining| {
                    calls += 1;
                    assert!(remaining <= Duration::from_millis(20));
                    async { std::future::pending().await }
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::DeadlineExceeded);
        assert_eq!(calls, 1);
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn server_backoff_is_honored_but_cannot_extend_deadline() {
        let info = RetryInfo {
            retry_delay: Some(prost_types::Duration {
                seconds: i64::MAX,
                nanos: 0,
            }),
        };
        let rich = crate::proto::google::rpc::Status {
            code: Code::ResourceExhausted as i32,
            message: String::new(),
            details: vec![prost_types::Any {
                type_url: "type.googleapis.com/google.rpc.RetryInfo".into(),
                value: info.encode_to_vec(),
            }],
        };
        let status =
            Status::with_details(Code::ResourceExhausted, "busy", rich.encode_to_vec().into());
        let p = Policy::default();
        let mut calls = 0;
        let err = p
            .retry::<(), _, _>(
                Instant::now() + Duration::from_millis(20),
                p.rpc_timeout(),
                |_| {
                    calls += 1;
                    std::future::ready(Err(status.clone()))
                },
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::DeadlineExceeded);
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn expired_deadline_never_starts_and_cancellation_drops_attempt() {
        let p = Policy::default();
        let mut calls = 0;
        let err = p
            .retry::<(), _, _>(Instant::now(), p.rpc_timeout(), |_| {
                calls += 1;
                std::future::ready(Ok(()))
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::DeadlineExceeded);
        assert_eq!(calls, 0);

        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct Guard(Arc<AtomicBool>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let seen = dropped.clone();
        let handle = tokio::spawn(async move {
            p.retry::<(), _, _>(
                Instant::now() + Duration::from_secs(60),
                p.rpc_timeout(),
                |_| {
                    let guard = Guard(seen.clone());
                    async move {
                        let _guard = guard;
                        std::future::pending().await
                    }
                },
            )
            .await
        });
        tokio::task::yield_now().await;
        handle.abort();
        assert!(handle.await.unwrap_err().is_cancelled());
        assert!(dropped.load(Ordering::SeqCst));
    }
}
