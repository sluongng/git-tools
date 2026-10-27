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

Landing is opt-in: use [the bounded TOML](examples/landing.toml), granting
repository write and explicit `landing_profiles`. Start/Get are owner-scoped;
accepted authorization survives launch-grant removal. By default preparation
stops at pending checks. Optional `service.landing.checker` runs a pinned
[trusted checker](../tinytree-check-runner/README.md) and automatically publishes
only its exact verified candidate with destination-generation CAS. No client
verdict or raw finalization RPC is exposed.

To require checked landing for a destination, add an exact `repository` and
`reference` pair to `service.protected_references` as shown in the landing
example. Ordinary publication cannot create, change, delete or replay that
destination, even for a writer; uploads, private holds and other destinations
are unaffected. The default list is empty. Seed a destination before protecting
it. Protection remains effective with landing disabled, so removing a checker
does not reopen ordinary writes; it can leave the destination read-only until
checked landing is available again.

Checker activation upgrades storage to format7. Its fixed Store-owned journals
survive configuration removal; uncertain cleanup retains roots past TTL.
Restart reconciles the same attempt before retrying, and shutdown drains physical
work. Check slots are independent of preparation/status; canonical transitions
share the bounded Store-work pool. Capacity exhaustion fails closed, without
pruning history or silently discarding recovery obligations. Initially checks
support operator-trusted Linux source only, not hostile-source isolation.

Private workspace history is opt-in: start from [the cloud profile](examples/cloud.toml),
set `service.cloud.enabled = true` and grant each principal `cloud = true`.
Reads and uploads are owner-scoped; uploads/publication also require `write = true`.
All published versions are retained. Quotas reject new promises rather than
expiring history. The profile explicitly bounds Store validation work and its
separate RPC memory pool; it does not silently shrink existing native limits.

Set `indexed_versions = true` for portable indexes, then `captured_versions = true`
to accept bound capture capsules. Configure `max_capture_bytes` and the RPC pool
together; the cloud profile includes bounded opt-in settings. The first captured
publication upgrades Store format to 5. The daemon's opt-in asynchronous capture
uploader is configured separately in the [workspace daemon](../workspace-daemon).

The separate query service supports retained root scopes, exact graph facts and
prefix resolution. Its rebuildable index warms asynchronously; unavailable
coverage is an error, not an empty result. Warming caches raw canonical payloads
up to `max_warm_bytes`, evicting old payloads so long histories can progress
without full residency.
`max_warm_read_bytes` separately bounds actual discovery and cache-miss rereads.
Scope TTLs and work limits are TOML configurable. Private parent-first draft
overlays and immutable forks are scoped
to the authenticated caller; they are ephemeral query data, not repository uploads
or backup receipts. Closing a parent preserves live forks without extending
their absolute expiry. Active-scope quotas and bounded replay-history quotas
are separate; closed keys remain closed until
their absolute expiry. IDs, selected-graph and canonical evaluation are incremental
and bounded; empty pages with continuations signal progress. Cursors expire
absolutely, cache only their latest
page for replay, and retain roots independently of a closed scope. `CloseQuery`
releases a partial query without draining it; close is distinct from proven
exhaustion and prevents replay until its absolute tombstone expiry. Closing before
the first evaluation also prevents a late request from creating that query. Canonical rows
include verified commit envelopes, while graph edges describe the selected set.
Response-byte limits can shorten pages; unsupported operators return explicit errors.
Revision-pinned `PathHistory` and `Blame` support bounded fixed-path linear
history. Only the selected file body is returned; private metadata-only overlays
do not grant file access. Unsupported ancestry and exhausted budgets carry
explicit incomplete coverage. Limits are configurable under `[service.query]`.
`Search` pages literal matches at an immutable revision. Its separate background
indexer builds bounded, rebuildable manifests and per-file trigram filters;
positive candidates still require verified content reads. Warming, eviction and
resource limits never become complete empty results. Search cache and work
limits are configurable in the same TOML example.
Active graph/canonical work and retained replay bytes share a configurable cursor
memory pool; completed queries retain their actual cached page size.

Derived state lives in `derived-index-v{schema}/generation-{index_rebuild_generation}`
under storage. Restarting with the same generation reuses it; increment
`service.query.index_rebuild_generation` and restart to rebuild asynchronously with
a fresh epoch. Older generations and legacy `derived-index` data remain untouched;
remove them only through explicit offline maintenance. There is no automatic eviction
or in-place format conversion. An unavailable index does not disable object serving.

## Annotation budgets

Paged annotation holds one immutable path/anchor context across bounded advances.
`service.query.max_annotation_commits` bounds its aggregate traversal (default
4096), separately from the legacy per-request `max_history_commits` setting.
Page size does not renew work, read, diff, allocation or deadline allowances.
Canonical and retained-only attribution returns the verified target and origin
ranges without transferring the ancestry graph. Mixed private queries can request
a bounded frontier, while private bodies remain local. Closing leaves a replay
binding until expiry rather than allowing a fresh budget on the same scope.

## Browser UI

Open `/ui/` on the server's TCP address for the embedded WebAssembly browser.
Search and the repository README are the initial view. Select an authorized
repository and an exact public reference (initially `main`); later file reads and
literal content searches stay pinned to that immutable revision. The browser
keeps an entered bearer token in memory only. It does not create commits or
publish references. The existing TLS and principal requirements also apply to
browser access; unauthenticated loopback access requires a loopback-literal or
`localhost` browser address.

Autocomplete ranks the snapshot's bounded file-path list locally, without a
network round trip for each keystroke. README/file previews accept UTF-8 regular
files up to 1 MiB and do not follow symlinks. Markdown presentation is limited to
256 KiB and 20,000 formatting events; source views show up to 3,000 lines around
the selected match. Truncated views are labeled. Raw HTML is displayed as text,
and images as alt text without fetching remote resources. Conflicted root trees
are reported as unsupported. The browser's read-only JSON facade bounds path discovery to
10,000 entries and 1 MiB of paths, each metadata object and path-discovery walk
to 8 MiB, and each content search to 8 MiB of file bytes and 100 matching lines. Large files, exhausted
budgets, and truncated path lists carry explicit incomplete coverage; this
preview facade is separate from the native paginated indexed `Search` RPC.
Refresh the snapshot after reference movement or unavailable historical content.
