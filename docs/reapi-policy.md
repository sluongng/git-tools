# REAPI client policy

The Git proxy and Bazel output service share a conservative REAPI client policy.
Pass a TOML file explicitly; neither daemon searches the working directory or
loads a repository-supplied file automatically.
See the complete [example](../examples/reapi-policy.toml).
For the unified `workspace` entry point, use the same `[reapi]` table in
[`workspace.toml`](../examples/workspace.toml), alongside endpoints and cache limits.

```sh
git-output-service --config /path/to/reapi-policy.toml \
  --root /path/to/outputs --state-dir /path/to/output-state \
  --cas-endpoint grpcs://cache.example.com
```

Configuration precedence is explicit CLI overrides, then TOML, then built-in
defaults.
An omitted field keeps its built-in default.
Unknown keys and invalid values fail startup before mounting storage.
The shared example is a policy-only file: proxy endpoints, paths and resource
limits keep their CLI options.
The output service also accepts an `[output_service]` table for complete daemon
configuration; see its [example](../examples/bazel-output-service.toml).
That complete file is BOS-specific: the proxy intentionally rejects its extra
table. Existing `[reapi]`-only files remain compatible with both daemons.
Credentials stay in environment variables; do not put keys in TOML or CLI flags.

## Defaults

All durations below use integer milliseconds, except `presence_ttl_seconds`.
Timeouts must be between 1 ms and 24 hours.
Presence TTL accepts zero through 30 days; retry counts accept 1 through 16.
Backoff values must satisfy
`1 <= initial_backoff_ms <= max_backoff_ms <= 60000`.

| TOML key under `[reapi]` | Default | Scope |
| --- | ---: | --- |
| `connect_timeout_ms` | 10000 | Establishing a CAS connection |
| `rpc_timeout_ms` | 60000 | One unary RPC attempt |
| `stream_idle_timeout_ms` | 60000 | ByteStream progress, not complete-file transfer time |
| `operation_timeout_ms` | 120000 | Foreground operation, including retry waits |
| `background_timeout_ms` | 600000 | Background proxy CAS publication operation |
| `presence_ttl_seconds` | 0 | Positive-presence suppression; zero disables it |

Retry keys under `[reapi.retry]`:

| Key | Default | Meaning |
| --- | ---: | --- |
| `max_attempts` | 4 | Total attempts, including the first |
| `initial_backoff_ms` | 100 | Initial exponential backoff ceiling |
| `max_backoff_ms` | 5000 | Maximum backoff ceiling |

Backoff uses full jitter and remains inside the operation's original budget.
Increasing attempt count does not multiply that budget.
The output service's legacy `--rpc-timeout-secs` overrides only
`rpc_timeout_ms`, not foreground, stream-idle or connection budgets.
Use TOML to configure these scopes independently.

## Deadlines and retries

Interactive reads and background publication have separate operation budgets.
For lazy output reads, the foreground deadline includes queue admission,
inode-lock admission, download, decoding and verification.
A shared fetch has its own finite deadline, independent of each reader's wait;
one reader timing out must not cancel work needed by another reader.
No connection keepalive setting replaces an operation deadline.

Retries apply to replay-safe operations returning `Unavailable`,
`DeadlineExceeded` or `Aborted`.
`ResourceExhausted` is retryable only with valid server `RetryInfo`.
Authentication failures, invalid requests, missing blobs, cancellation and
corrupt content are not generic transient failures.
Successful items in partial batch responses are retained; a retry must not
discard completed work.
Codec fallback also consumes the original operation budget.
Retry policy does not make a missing CAS blob available: callers still need
authoritative missing-object handling and repair.

Native mounts keep safe shutdown behavior.
A timed-out reader does not justify tearing down a Windows paging operation or
abandoning a busy NFS server.
Blocking filesystem calls cannot be preempted mid-syscall. Once an NFS
publication commits, its native cache-invalidation barrier finishes independently
of the caller's deadline; later publications and unmount wait for that barrier.
Remote presence checks do not hold the native publication lock. Refresh applies
their results only after revalidating node identity, path and native dirty state.

## Presence is not retention

The default `presence_ttl_seconds = 0` makes the proxy ask the remote CAS again
instead of suppressing `FindMissingBlobs` using old positive observations.
A nonzero value is an advisory optimization, not a retention lease.
Only enable it when the backend's minimum retention and repair behavior are
understood.
Local cache hits do not renew a remote-confirmation timestamp.
An authoritative missing response invalidates the corresponding positive hint.

The output service still revalidates retained remote outputs on each
`StartBuild`; a proxy presence hint must not replace that protocol check.
Bazel's remote-cache TTL is also an operator retention assumption, not a value
discovered from REAPI capabilities.
Configure Bazel and the backend separately rather than assuming this TOML file
changes either one.

## Unified execution and downloader transport

The daemon exposes cache, Execution/Operations, Remote Asset, BES and optional BOS
on `rbe.sock`. Cache, downloader and operation control use separate upstream
connections so long execution streams cannot block them. Execution uses no cache-derived
RPC or message-idle deadline. Caller deadlines cover the complete response stream;
`cache.execution_timeout_ms` optionally caps lifetime (zero leaves it to the caller).
Execution admission queues under the caller budget and shutdown signal; it does
not reject ordinary bursts to consume the client retry budget. The global stream
cap is configurable independently of per-connection HTTP/2 limits.
Execute/WaitExecution and Operations are forwarded once; Bazel owns retries/resume.
Requests use gzip only after that RPC method advertises support; response compression
is accepted independently. No execution is replayed to probe compression.

Downloader fetches preserve inline errors, qualifiers, mirror order and freshness
constraints. Blob mappings have fixed TTL and bounded LRU metadata. Reuse requires
verified local bytes and an upstream presence check; CAS eviction forces another
fetch. Normal CAS reads populate the shared byte cache lazily. Directory fetches
always forward; a cached root does not prove full-tree availability.

## Background retention maintenance

The daemon defaults to a fixed one-hour AC TTL after an upstream read; local hits
do not slide it. Used entries may be renewed in the background every three hours,
along with recently accessed shared-CAS bytes. Foreground upstream reads count
toward this interval; required AC revalidation is never delayed until maintenance.
Output references receive an initial asynchronous check, then use their own
refresh clock so hourly AC reads cannot postpone output maintenance. While AC
metadata is fresh, output-only checks reuse it without another AC lookup or
extending its expiry.

`[cache]` configures `action_cache_ttl_seconds = 3600`,
`upstream_retention_seconds = 14400`, `retention_refresh_seconds = 10800` and
`retention_max_entries = 16384`. Retention is an operator assumption, not a
server-reported lease. The refresh interval must be shorter than that window.
Without a suitable assumption, set both TTL and upstream retention to zero.

One background worker uses a separate connection, bounded metadata/work sets,
batched presence checks, configured retry policy and a 30-second maximum operation
budget (also capped by the configured background budget and retention headroom).
No foreground read waits for it. AC renewal checks direct outputs, stdout/stderr,
Tree contents and Directory closure, downloading bounded metadata only. CAS hits
notify the worker even through a per-read source override, including lazy BOS.
No output payload is eagerly fetched. If upstream reports a locally used CAS blob
missing, the worker verifies and streams resident bytes back using the same
background connection. It never hydrates an evicted local blob. Only an active
upload holds a lease; queued work does not pin bytes against LRU eviction.
Resident-byte re-verification has a separate single-slot admission lane and
inherits the repair deadline; cancellation stops hashing between file-read chunks.

Only successful upstream confirmation renews timestamps. Failures preserve the
old expiry; missing outputs invalidate cached AC metadata, not verified local
bytes. Missing CAS blobs can be repaired from verified resident bytes, but missing
AC metadata is not republished. Uploads support zstd with identity fallback and
bounded retries; repair batches use the configured background timeout, capped by
retention headroom. Failed or mismatched upload acknowledgements do not advance
confirmation. Maintenance reads do not generate new user-access notifications. The
bounded hot set is in memory; restart begins with unknown remote retention.
The worker is cancelled on shutdown. REAPI defines no standard cache-RPC urgency
field: low priority is local scheduling, not a server-priority guarantee.

## BES is a separate policy

The unified daemon's `[bes]` table uses Bazel BES defaults, not the REAPI policy
above: **four retries after the first attempt**, delays of 1s, 1.6s, 2.56s and
4.096s without jitter, and a 15-second unary lifecycle timeout. Streaming has no
deadline or ACK-idle timeout: BuildBuddy normally ACKs only after request EOF.
`connect_timeout_ms` defaults to 10 seconds. `retry_max_attempts` counts retries,
unlike `[reapi.retry].max_attempts`, which counts total attempts.
`INVALID_ARGUMENT`, `PERMISSION_DENIED` and `FAILED_PRECONDITION` stop retries.
Delivery failures retain locally ACKed data; inspect `workspace status` and run
`workspace retry-bes` after correcting the cause. Restart retries complete pending
streams. Unfinished streams wait for their publisher to reconnect and still
consume the pending-stream quota; there is no automatic orphan discard or expiry.
These timeouts do not change Bazel's own flags or remote CAS policy.

The default BES spool admits 1 GiB of pending encoded payloads, 128 streams and
16 concurrent upstream streams. Admission groups up to 64 ready events /
256 KiB per fsync, with no wait to fill a batch. An individual larger event
commits alone. Configure `batch_max_events`, `batch_max_bytes` and
`batch_max_delay_ms` in `[bes]`; an event limit of 1 disables grouping.
Limits reject new data before ACK; they never
evict pending events. Upstream ACKs logically delete payloads; Fjall compaction
reclaims physical disk space later. Small completed receipts are retained for up
to one day / 4096 streams for reconnects. These are logical limits, not a total
filesystem quota. Protect free disk space independently.

Keep `bes_results_url` pointed at the real BuildBuddy UI in the provisioned
Bazelrc; only `bes_backend` is redirected to the local private socket. Changing
the upstream or credential scope while data is pending is rejected. Delivery is
at least once across failures. BuildBuddy's current reconnect window for an
already-started incomplete invocation is four hours; durable spooling cannot
override that backend rule. This relay targets BuildBuddy; generic BES backends
with strict lifecycle/tool-stream ordering are not yet supported.
