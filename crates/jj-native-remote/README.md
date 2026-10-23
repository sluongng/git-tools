# jj-native-remote · library

Broker-hosted `NativeObjectIo` plus the private `NativeRepositoryControl` adapter.
Compose it with the broker's sole `DirectObjectIo` owner and shared `LocalCas`.

Admission establishes a current private hold and verifies only the selected
commit. Other metadata loads on demand through verified canonical parent edges.
A retained root is not a complete local closure. File payloads remain lazy; size
hints come only from verified trees, with provenance checked on lookup. The file
index rebuilds from resident verified trees without fetching history.

Copy-history parents install before children. The legacy complete copy-catalog
interface explicitly hydrates bounded metadata closures; it never returns an
incomplete catalog as complete. Publication also validates and installs its full
metadata closure. These paths may fetch historical trees; ordinary pinning does
not. The admission ledger upgrades to format 2; older readers must reject it.

Reads prefer authoritative local files, then verified scoped CAS bytes. New
writes are durable locally without a network dependency. Publication uploads
missing content and uses generation CAS with persistent idempotent reconciliation.
An ACK never deletes sole local copies. Existing cached work remains usable offline.
Reference observations preserve the original generation, including absence;
publication lookup reconciles only its exact durable replay key.

Stop callers before `wait_for_idle()` when closing the provider; cancelling an
awaiter does not cancel its owned metadata job.

Start from [the broker configuration](examples/remote.toml). Authorization profiles
are trusted broker configuration, not request-selected labels. Cache scope also
includes repository, credential identity and captured TLS trust. Metadata/descriptor/catalog and control
registration declare conservative whole-operation memory and staging requirements;
larger limits require matching broker pools. Versioned, exclusively owned staging
reclaims only recognized crash leftovers; upload budgets include both snapshots.
Staging currently requires Unix
owner-private directories; other platforms fail closed pending ACL verification.

**Not complete client GC:** all authoritative local objects are initially retained.
Never run ordinary `Store::gc` on this partial lazy store. Root-aware client GC
and safe offloading remain separate work; no upload receipt is a retention lease.
