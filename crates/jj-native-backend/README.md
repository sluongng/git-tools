# jj-native-backend · local prototype library

A jj 0.46 `Backend` backed by canonical Tinytree objects and authoritative local
storage. `NativeBackend::init/load` and `register(&mut StoreFactories, Config)`
preserve jj's normal per-workspace operation, index and working-copy stores.

This is **local-only**: no remote fetch/upload, VFS, offloading or GC. Every local
object is retained. One process owns each store; concurrent clients need the
future daemon broker. It neither opens a Git database nor launches Git, though
workspace-wide features can still compile Git support.

Files stream through bounded buffers with aggregate staging admission and durable
write acknowledgments. Blocking I/O works without a caller Tokio runtime and
retains read leases through cancellation. Native metadata preserves merge terms,
labels, ChangeIds, signatures and copy history; Git submodules fail explicitly.
Copy queries apply merge semantics and bounded provenance scans, not a separate
catalog. These correctness-first scans still need performance benchmarking.

Use [the local CLI](../jj-native-cli) to try ordinary materialized workspaces.
`//crates/jj-native-backend:unit_tests` exercises the actual Backend and Workspace;
this does not establish remote, multi-process or mounted acceptance. Future
retention must protect operation, undo and workspace roots before offloading.
