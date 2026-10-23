# packblob · library

Immutable indexed BLAKE3 blob packs with raw, independent zstd and one-anchor
raw-prefix zstd representations. Logical object identities remain unchanged.
Opening reads only the header/footer; lookup reads individual index records;
extraction reads the requested payload and at most one independent anchor.

This is a distinct indexed format, **not** Mononoke's Thrift envelope or the
SHA-256 indexed format in `packblob-go`. See [FORMAT.md](FORMAT.md).

`Writer` streams payloads and seals an index/footer. `Reader` verifies decoded
bytes against the caller's expected digest and size, with explicit allocation
and zstd-window limits. `read_encoded` exposes a selected frame and base digest
for experiments; it is not a negotiated or authenticated transfer protocol.

Callers own immutable-file lifetime, aggregate memory admission, sync/atomic
publication, catalogs, retention and compaction. Large objects can remain loose;
the default per-object and anchor limits are 64 KiB. The codec does not enable
background maintenance or change server RPCs.
