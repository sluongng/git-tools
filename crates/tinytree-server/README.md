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
LRU eviction. Native publication is not Git publication. `_tinytree/` is reserved
for future owner-private retention, not ordinary bookmarks.
