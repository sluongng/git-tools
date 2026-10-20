# Lazy output FUSE

Linux writable `output_core::Projection`, used by the default BOS storage mode.
Requires unprivileged `/dev/fuse` and `fusermount3`; no sudo or kernel module
installation is performed. Mount an empty directory outside namespace/cache state.

`output_fuse::start(tree, mountpoint).await` returns an `Arc<FuseMount>`.
Metadata operations do not hydrate remote files. First content access uses the
shared verified CAS; mmap/execution use normal kernel page caching. Stable inode
generations preserve retired handles across replacement, while local writes use
copy-on-write state outside disposable cache storage.

Payload operations await bounded worker admission; metadata is served separately.
Publication invalidates kernel entries before acknowledgement. Keep the runtime
and mount owner alive, and call `Projection::shutdown` to drain and unmount.
A busy/failed unmount retains a working server for retry rather than forcing it.

Names are UTF-8. Hard links, special files and nonzero rename flags are unsupported;
mtime is supported, independent atime is not. Queued synchronous-request memory
scales with caller concurrency; only active workers and CAS hydration are bounded.

```sh
cargo test --locked -p output-fuse
cargo test --locked -p output-fuse --test mounted -- --ignored --nocapture
```

The second command runs actual kernel acceptance, including metadata-only access,
mmap/ELF execution, local edits, old handles, replacement/clean invalidation and
96 concurrent readers while CAS is stalled.
