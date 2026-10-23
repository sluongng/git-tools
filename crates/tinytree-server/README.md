# tinytree-server · library and executable

Native repository storage and gRPC serving, separate from Git storage and future
transformation services. Implements bounded metadata/graph reads, lazy file
streams, verified durable uploads and generation-checked native ref publication.
It does not implement the jj client or source mount.

```sh
bazel build --config=rbe //crates/tinytree-server:tinytree
bazel-bin/crates/tinytree-server/tinytree --config server.toml
```

Start from [the example](examples/server.toml). Tokens come from named environment
variables, never from logged configuration. Non-loopback TCP requires both TLS
and authorization. Unix sockets require an existing owner-only parent directory
with no symlink ancestors. Existing endpoints are never removed. Shutdown drains active requests and streams.

The service negotiates zstd and enforces byte/count/concurrency and operation/idle
budgets. Configured GC retains published refs and staging/read leases; it is not
LRU eviction. Native publication is not Git publication. With `retain = true`,
principals can hold their own `_tinytree/hold/v1/<client>/<commit>` references
without public-write permission. Holds persist until explicit release; quotas
reject new promises, not existing ones. A historical receipt is not current proof.

The separate query service supports retained root scopes, exact graph facts and
prefix resolution. Its rebuildable index warms asynchronously; unavailable
coverage is an error, not an empty result. Scope TTLs and work limits are TOML
configurable. Private parent-first draft overlays and immutable forks are scoped
to the authenticated caller; they are ephemeral query data, not repository uploads
or backup receipts. Closing a parent preserves live forks without extending
their absolute expiry. Active-scope quotas
and bounded replay-history quotas are separate; closed keys remain closed until
their absolute expiry. Paged evaluation is not implemented yet.

Derived state lives in `derived-index-v{schema}/generation-{index_rebuild_generation}`
under storage. Restarting with the same generation reuses it; increment
`service.query.index_rebuild_generation` and restart to rebuild asynchronously with
a fresh epoch. Older generations and legacy `derived-index` data remain untouched;
remove them only through explicit offline maintenance. There is no automatic eviction
or in-place format conversion. An unavailable index does not disable object serving.
