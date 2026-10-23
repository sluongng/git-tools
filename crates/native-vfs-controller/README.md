# native-vfs-controller · library

Connects native jj working copies to daemon-owned lazy source mounts. Snapshots
reuse clean object IDs and upload changed local files; durable receipts bridge
projection updates and jj state. Snapshot acknowledgement never evicts edits or
removes ignored/untracked paths.

Checkout currently rejects unresolved roots and unsnapshotted/untracked
collisions. Each platform needs an authoritative provider capture fence; missing
support is an error, never a materialized checkout. The `protocol-test-support`
feature is only for declared unmounted test fixtures, not native acceptance.
