# Remote client policy

Pass configuration explicitly; no repository-local discovery occurs. The Git
proxy and output service accept the shared [REAPI TOML](../examples/reapi-policy.toml).
The unified daemon uses the `[reapi]` table in
[`workspace.toml`](../examples/workspace.toml). CLI overrides take precedence over
TOML, then built-in defaults; unknown keys and invalid values fail startup.
Credentials belong in the environment or a scoped helper, never TOML or argv.

The [complete output-service config](../examples/bazel-output-service.toml) adds an
`[output_service]` table. That table is BOS-specific and rejected by the Git proxy.

## Deadlines and retries

| `[reapi]` setting | Default | Scope |
| --- | ---: | --- |
| `connect_timeout_ms` | 10000 | Connection establishment |
| `rpc_timeout_ms` | 60000 | One unary attempt |
| `stream_idle_timeout_ms` | 60000 | ByteStream progress |
| `operation_timeout_ms` | 120000 | Foreground operation, including waits/retries |
| `background_timeout_ms` | 600000 | Background CAS publication |
| `presence_ttl_seconds` | 0 | Advisory positive-presence suppression; zero disables it |

`[reapi.retry]` defaults to four total attempts, 100ms initial and 5000ms maximum
backoff with full jitter. Retries and codec fallback remain within the original
operation budget. Replay-safe `Unavailable`, `DeadlineExceeded` and `Aborted`
failures can retry; `ResourceExhausted` requires valid server `RetryInfo`.
Authentication, missing content, cancellation and corruption are not generic
transient errors. Compression is negotiated with identity fallback.

Lazy-read deadlines include admission, download, decoding and verification.
Shared fetches have their own finite lifetime; one reader timing out must not
cancel another reader's work. Native publication/unmount barriers can outlive a
caller deadline; a timeout does not justify abandoning a busy server.

## Presence and retention

A positive-presence hint is **not a retention lease**. Bazel's remote-cache TTL is
also an operator assumption, not a server-discovered guarantee. Configure each
client explicitly. BOS still revalidates retained outputs at `StartBuild`.

The daemon's `[cache]` defaults are:

- Fixed AC validity: `action_cache_ttl_seconds = 3600`; local hits do not slide it.
- Assumed upstream retention: `upstream_retention_seconds = 14400`.
- Background refresh interval: `retention_refresh_seconds = 10800`.

Without a suitable retention assumption, set AC validity and upstream retention
to zero. Successful upstream confirmation alone renews freshness. Missing outputs
invalidate cached AC metadata. Best-effort background maintenance checks recently
used AC/output references and CAS bytes; missing CAS content can be uploaded from
verified resident bytes. It never hydrates evicted bytes or republishes missing
AC metadata. Queued work does not pin the cache or delay foreground reads.

## Unified RPC forwarding

`rbe.sock` serves cache, Execution/Operations, Remote Asset, BES and optional BOS.
Execution streams inherit caller deadlines; `cache.execution_timeout_ms = 0`
adds no cap or message-idle timeout. Execute/WaitExecution/Operations forward once;
Bazel owns retries and resume. Independent upstream connections keep long
execution streams from blocking cache/downloader traffic.

Downloader mappings have fixed TTL and bounded metadata. Reuse needs verified
local bytes and an upstream presence check. Directory fetches always forward:
a cached root does not prove availability of its contents.

`cache.max_concurrent_reads = 128` bounds CAS serving-file operations across all
connections, shared by streaming and batch reads. It is independent of download
slots and the per-connection RPC limit. Admission uses the existing operation
budget; active streams retain their slot until the file closes. This is not a
process-wide file-descriptor quota: sockets and other services need headroom too.

The daemon best-effort raises only its own soft open-file limit to
`runtime.target_open_files = 16384`, clamped to the inherited hard limit. It
never lowers a higher limit or changes the hard limit; `0` retains the inherited
limit. `workspace status` reports the requested and effective limits and warnings.
Socket accepts retry descriptor exhaustion with bounded backoff instead of
terminating the service. Status separates accept exhaustion and serving-read
failures from download failures; these observations are not unique failed blobs.

## Durable build events

BES uses the separate `[bes]` policy, not REAPI retry settings. Events are ACKed
only after durable local persistence, then forwarded asynchronously. Payloads are
deleted only after upstream ACKs; pending events are never LRU-evicted. Quota
exhaustion rejects new data before ACK. Delivery is at least once across failures.

Defaults: four retries after the first attempt, exponential delays from 1s with
factor 1.6, 10s connection and 15s lifecycle RPC timeouts. Streaming has no ACK-idle
timeout because the upstream may ACK only after request EOF. After terminal EOF,
`post_half_close_timeout_ms` bounds completion to five minutes per attempt by
default; `0` disables it. Expiry retries unacknowledged events without deleting
them. Invalid requests,
permission failures and failed preconditions stop retries.

Inspect `workspace status`; correct forwarding failures and run `workspace retry-bes`.
Restart retries complete pending streams; unfinished streams wait for the publisher
to reconnect and continue consuming quota. Pending data prevents changing upstream
or credential scope. Keep `bes_results_url` pointed at the upstream UI; redirect
only `bes_backend` locally. This relay targets BuildBuddy, not generic strict-order
BES backends. Protect free disk space independently of logical spool quotas.
