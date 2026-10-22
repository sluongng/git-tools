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

Callers must authorize repository access and protect local jj operation, undo and
workspace roots. An upload receipt alone is not permission to discard local data.
Process-crash/write-failure tests are not proof of machine power-loss durability.
