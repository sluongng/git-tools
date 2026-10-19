# REAPI client policy

The Git proxy and Bazel output service share a conservative REAPI client policy.
Pass a TOML file explicitly; neither daemon searches the working directory or
loads a repository-supplied file automatically.
See the complete [example](../examples/reapi-policy.toml).

```sh
git-output-service --config /path/to/reapi-policy.toml \
  --root /path/to/outputs --cas-endpoint grpcs://cache.example.com
```

Configuration precedence is explicit CLI overrides, then TOML, then built-in
defaults.
An omitted field keeps its built-in default.
Unknown keys and invalid values fail startup before mounting storage.
This is a policy file, not a complete daemon configuration: endpoints, paths and
resource limits keep their existing CLI options.
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
