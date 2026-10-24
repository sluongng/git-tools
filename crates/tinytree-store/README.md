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

`Store::pack` explicitly packs small loose objects during quiescent maintenance;
it is not automatic background work or a server command. Canonical IDs and RPC
payloads stay unchanged. Repeat bounded calls until `PackReport.scan_complete`;
the persisted scan cursor advances even across pages with no eligible objects.
[Indexed packs](../packblob) use independent compression
or one same-pack dictionary anchor, with bounded reads and memory admission.
The first pack upgrades the store format to version 2; older binaries must not
reopen it. Large objects remain loose. GC reclaims a pack only after its final
logical member dies, retaining dictionary bytes needed by surviving members.
Partially dead packs are not repacked, and crash-orphan cleanup is not automatic.

For a checked synthetic storage/read comparison, run
`bazel test --config=rbe //crates/tinytree-store:pack_benchmarks --test_output=all`.
This component benchmark does not measure Bazel startup or wire savings.

`publish_retention` keeps owner/client-scoped roots until explicit release, with
atomic namespace, receipt and unique-closure byte quotas. Released names keep
generation tombstones; existing holds are never evicted to admit another client.
Holds use no staging leases. Reopening checks exact counters and replay history
with configurable record/byte scan limits; public receipts also consume that
startup budget. A mismatch or exhausted budget fails closed, without repair.

Callers must authorize repository access and protect local jj operation, undo and
workspace roots. An upload receipt alone is not permission to discard local data.
Process-crash/write-failure tests are not proof of machine power-loss durability.

Captured recovery versions atomically bind portable workspace metadata and native
roots. The first such publication upgrades storage to format 5; earlier binaries
cannot reopen it. Capture data remains retained with its immutable version.

`client_gc` is a separate, bounded unreachable-only collector seam for lazy
clients; it is not enabled by a broker or workspace API. Callers must fence the
complete workspace/operation/in-flight root registry through completion. Exact
remote FILE promises permit absent indexes, never corrupt indexed payloads or
reachable offload. Durable ages protect recent/legacy objects; deletion records
make interrupted unlink recoverable on a later successful collection. Its
logical scan/deletion limits do not cap database or crash-orphan disk overhead.
`plan_client_gc` is nonmutating and grants no deletion permission; apply recomputes
the selection under its gate and revalidates the supplied epoch.
