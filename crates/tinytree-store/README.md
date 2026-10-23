# tinytree-store · library

Authoritative native objects and generation-checked references—not Git storage
or an LRU cache. One process owns each directory; cloned handles share it.

Uploads verify and sync bytes before durable metadata/receipts. Reference updates
validate their full closure and atomically record a generation and replay receipt.
GC retains published references, staged closures and active readers. Ambiguous
write failures require reopening before further operations.

`open` initializes new/empty storage or reopens a recognized store.
`open_existing` never initializes missing storage. Unrecognized directories and
final symlinks are rejected without mutation. Traversal and staging are bounded;
payload quotas exclude database/receipt overhead and interrupted-file orphans.

`publish_retention` keeps owner/client-scoped roots until explicit release, with
atomic namespace, receipt and unique-closure byte quotas. Released names keep
generation tombstones; existing holds are never evicted to admit another client.
Holds use no staging leases. Reopening checks exact counters and replay history
with configurable record/byte scan limits; public receipts also consume that
startup budget. A mismatch or exhausted budget fails closed, without repair.

Callers must authorize repository access and protect local jj operation, undo and
workspace roots. An upload receipt alone is not permission to discard local data.
Process-crash/write-failure tests are not proof of machine power-loss durability.

`client_gc` is a separate, bounded unreachable-only collector seam for lazy
clients; it is not enabled by a broker or workspace API. Callers must fence the
complete workspace/operation/in-flight root registry through completion. Exact
remote FILE promises permit absent indexes, never corrupt indexed payloads or
reachable offload. Durable ages protect recent/legacy objects; deletion records
make interrupted unlink recoverable on a later successful collection. Its
logical scan/deletion limits do not cap database or crash-orphan disk overhead.
