# jj-native-index · library

A jj 0.46 index over immutable remote checkpoints and all admitted local drafts,
including hidden siblings. Only local broker endpoints and provider identities
are persisted. Queries never fall back to a truncated DefaultIndex or hydrate
complete public history.

Hosts inject matching `NativeQueryIo` and `NativeObjectIo` instances. For CLI
use, both must dispatch I/O independently of the caller's Tokio runtime; the
broker clients provide that contract. A separate four-job bounded worker bridge
handles jj's synchronous `is_empty` inside current-thread and futures runtimes.
`sync_query_timeout_ms` bounds the caller wait (120 seconds by default); timed-out
physical jobs retain their owners and admission until they actually finish. Scopes reopen from fixed checkpoints,
not current references. `with_remote_roots` verifies explicit admitted roots,
retains old checkpoints, and filters newly remote-covered drafts from uploads
without deleting local history. `init_fresh_for_root_operation` seeds and associates the captured
root operation during journal-owned initialization.

Checksummed immutable segments and no-clobber, synced operation associations
support multiple processes. Unix publication also syncs directory entries,
including identical-entry retries; hosts must supply already-durable parent
ancestors. Durable writer APIs return `Unsupported` on non-Unix platforms until
an equivalent publisher is implemented and verified; read-only loading does not
promise durable publication. Missing associations and unsupported revset
predicates return errors. Complete index GC enumeration is unsupported; this
crate never deletes segments or operation associations. View-only checkpoints
reuse existing segments; changed checkpoints periodically flatten into bounded
parentless shards. Every historical operation remains exact and readable, so
disk usage still grows with retained operations. Default limits include 4096
local drafts, 4096 roots, 4 MiB per segment and 16 MiB per checkpoint; exceeding
limits is an error, not truncation. Storage collection is not implemented.
Rewrite inference remains opted out pending integrated acceptance.

Streams explicitly close queries at EOF; early drops use one bounded cleanup
worker per store context, with absolute queue deadlines and expiry fallback.
Hosts must drop repositories/helpers, then drain `cleanup_handle().wait(timeout)`
(or async `drain`) before normal process exit; the handle retains no index cycle.
Canonical draft bytes remain in the authoritative object provider. Descriptors
and checkpoints are not retention leases.

This crate is a prerequisite, not the production default index. Broker-backed
CLI/factory wiring and end-to-end no-history-hydration acceptance remain separate.
