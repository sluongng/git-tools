# output-fuse

Linux library implementing lazy writable `output_core::Projection` for BOS and
native source mounts.
Requires `/dev/fuse`, `fusermount3`, and an empty mountpoint outside state/cache.

`output_fuse::start(tree, mountpoint).await` returns an `Arc<FuseMount>`.
Metadata never hydrates remote files; first reads use shared verified CAS bytes.
Local writes use copy-on-write state, not disposable cache. Publication
invalidates kernel entries before acknowledgement. Source directory metadata and
symlink targets also load on demand; source checkout drains metadata work and
retains one exclusion fence through replacement.

Keep the runtime and mount owner alive; call `Projection::shutdown` to drain and
unmount. Failed/busy unmount retains a working server for retry.
UTF-8 names are required; hardlinks, special files and nonzero rename flags are
unsupported. See [BOS setup](../bazel-output-service/README.md),
[platform support](../../docs/platforms.md) and [tests](../../docs/development.md).
