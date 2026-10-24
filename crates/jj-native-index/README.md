# jj-native-index · library

An experimental jj 0.46 index over immutable remote checkpoints and admitted
local drafts, including hidden siblings. The daemon/CLI can select it with
`[native.workspaces] index_mode = "federated"`; it is not the default. Queries
never fall back to a truncated index or download complete public history.

Hosts inject matching `NativeQueryIo` and `NativeObjectIo` providers that dispatch
I/O independently of the caller's runtime. Queries use fixed checkpoints, not
current references. `import_fresh_operation_indexes`
stages exact retained indexes without live-reference fallback; it does not by
itself restore operation/view data or establish backup completion.

Selections from one exact retained version can merge lazily. Cross-version and
live/cloud mixing fail closed. Merges preserve canonical metadata and frozen
ordinals; normalization can rewrite bounded local metadata while retaining old
operation associations. Older readers reject multi-selection checkpoints.

Checksummed immutable segments and synced, no-clobber operation associations
preserve historical operations. Missing/corrupt parents and unsupported predicates
return errors, not incomplete results. Durable publication is Unix-only; storage
collection and automatic client offloading are not implemented, so retained
history consumes disk. Default limits include 4096 local drafts, 4096 roots,
4 MiB per segment and 16 MiB per checkpoint; excess is rejected, not truncated.
Rewrite inference remains opted out.

Timed-out physical work retains its owners and admission until completion.
Streams close queries at EOF; early drops use bounded cleanup with expiry
fallback. Hosts must drop repositories/helpers, then drain
`cleanup_handle().wait(timeout)` or async `drain` before normal process exit.
Canonical draft bytes remain in the authoritative provider; descriptors and
checkpoints are not retention leases.
