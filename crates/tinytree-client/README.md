# tinytree-client · library

A bounded, zstd-enabled native repository client for brokers and jj integrations.
It verifies canonical metadata and complete file hashes, retries replay-safe
operations under one deadline, and reconciles uncertain reference publications.
Presence and publication receipts are not retention leases.
`canonical_commit_summaries` verifies each entry against canonical metadata, not
the completeness or reachability of the server-selected set. A history importer
must traverse/verify the complete canonical closure before admitting history.

Root-scoped graph facts and prefix queries use checked, expiring handles and
bounded compressed RPCs. Exact negative answers trust the authenticated server.
Private parent-first draft overlays and forks share an immutable remote checkpoint;
they never upload objects or publish changes. Verify local drafts with
`NativeClient::verify_metadata`, then use `open_query_scope_with_overlay` or
`fork_query_scope`. Forks do not extend the parent scope's expiry. These APIs do
not yet replace jj's local index. `begin_query` / `evaluate_page` expose bounded
IDS-only graph pages using version-1 `query_protocol` structural ASTs. A page
size of 128 matches the default server cap; empty progress pages are explicit.
One deadline covers the query and caller pauses between pages. Dropping a query
releases local state; unfinished server cursors remain until their absolute TTL.
`begin_graph_query` / `evaluate_graph_page` add ordered
selected-set edges and optional verified canonical commits. Root has no canonical
payload. Indirect/missing edges and completeness still trust the authenticated
server; they are not canonical parent summaries. Pages retain byte reservations
until dropped, without retaining an inflight RPC slot.

Load [the TOML example](examples/client.toml) with `Config::load`, then call
`NativeClient::connect`. Plaintext is loopback-only; other TCP endpoints require
explicit TLS roots. Unix endpoints must have private owner-controlled parents.
Tokens are read from a named environment variable, never embedded in config.

Downloads return an owned `VerifiedFile`, not an installed destination. Explicit
synchronous `persist_noclobber` installs it without replacing existing files;
call it on a blocking thread if needed. Uploads reopen owned immutable snapshots,
not mutable source paths. Temporary files and open readers retain aggregate disk
quota until drop.
Staging requires an owner-private directory with trusted ancestors. File staging
currently supports Unix permission checks; other platforms fail closed pending
private-directory ACL verification.
