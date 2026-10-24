# jj-native-backend · local prototype library

A jj 0.46 `Backend` backed by canonical Tinytree objects and authoritative local
storage. `NativeBackend::init/load` and `register(&mut StoreFactories, Config)`
preserve jj's normal per-workspace operation, index and working-copy stores.

This local provider is independent of remote I/O and mount management. One
process owns each store; the daemon coordinates shared workspace access. It
neither opens a Git database nor launches Git, though workspace-wide features
can still compile Git support.

Files stream through bounded buffers with aggregate staging admission and durable
write acknowledgments. Blocking I/O works without a caller Tokio runtime and
retains read leases through cancellation. Native metadata preserves merge terms,
labels, ChangeIds, signatures and copy history; Git submodules fail explicitly.
Copy queries apply merge semantics and bounded provenance scans, not a separate
catalog. These correctness-first scans still need performance benchmarking.

Explicit owned plan/apply operations support bounded unreachable-object
collection. The caller must supply and retain a complete root-registry guard,
including operation history, undo, workspaces and in-flight work. Physical jobs
keep that ownership through cancellation or timeout. There is no automatic
daemon GC or reachable-object offloading.

Use [the native CLI](../jj-native-cli) through mandatory daemon-owned lazy mounts,
without a materialized fallback. `//crates/jj-native-backend:unit_tests` exercises
the Backend and Workspace; it is not mounted or multi-process acceptance.
