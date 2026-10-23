# jj-native-remote · library

Broker-hosted `NativeObjectIo` plus the private `NativeRepositoryControl` adapter.
Compose it with the broker's sole `DirectObjectIo` owner and shared `LocalCas`.

Admission first establishes a current private hold, verifies the complete
canonical metadata closure, installs metadata durably, then records completion.
File payloads remain lazy. Their size hints come only from admitted canonical
trees, with provenance checked on lookup; the index is rebuildable.

Reads prefer authoritative local files, then verified scoped CAS bytes. New
writes are durable locally without a network dependency. Publication uploads
missing content and uses generation CAS with persistent idempotent reconciliation.
An ACK never deletes sole local copies. Existing cached work remains usable offline.
Reference observations preserve the original generation, including absence;
publication lookup reconciles only its exact durable replay key.

Start from [the broker configuration](examples/remote.toml). Authorization profiles
are trusted broker configuration, not request-selected labels. Cache scope also
includes repository, credential identity and captured TLS trust. Control
registration checks conservative whole-operation memory and staging requirements;
larger limits require matching broker pools. Versioned, exclusively owned staging
reclaims only recognized crash leftovers; upload budgets include both snapshots.
Staging currently requires Unix
owner-private directories; other platforms fail closed pending ACL verification.

**Not complete client GC:** all authoritative local objects are initially retained.
Never run ordinary `Store::gc` on this partial lazy store. Root-aware client GC
and safe offloading remain separate work; no upload receipt is a retention lease.
