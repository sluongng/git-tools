# workspace-fuse

Library and `git-vfs` executable for a writable Linux FUSE view of a blobless Git
clone. Reads load files lazily; edits persist in a copy-on-write overlay.
Native `git status` uses the durable change journal through fsmonitor.

```sh
cargo build --release -p workspace-fuse
target/release/git-vfs clone https://github.com/bazelbuild/bazel.git \
  /tmp/project.git --depth 1
mkdir -p /tmp/project
target/release/git-vfs mount --repo /tmp/project.git \
  --overlay /tmp/project-overlay --prefetch-bazel /tmp/project
```

Requires native Git, `/dev/fuse` and `fusermount3`. Clone destination must not exist;
mountpoint must be empty. Ctrl-C unmounts; remount the same clone/overlay to retain
edits and index. `--read-only` omits writable Git state. Keep outputs outside the mount.

GitHub sizes use its tree API before mount. Other origins need native/server
`remote-object-info` support or trusted proxy mappings; protocol v2 alone is not
enough. Private GitHub metadata uses explicit `--github-token-env NAME`.
`--prefetch-bazel` batches likely inputs and symlink blobs asynchronously after mount.

A trusted BLAKE3 proxy can supply mappings via `--blake3-proxy-url` and
`--blake3-proxy-repo`. Bazel startup flags `--digest_function=BLAKE3` and
`--unix_digest_hash_attribute_name=user.blake3` reuse digests; a mapping does not
prove remote CAS residency. Xattrs expose Git SHA1 and optional BLAKE3; edits
hash current bytes. Missing BLAKE3 metadata does not trigger a network fetch.

Native status/add/basic commit/log are supported; commit can hydrate the indexed
snapshot. The base is fixed: general checkout/merge/rebase is unsupported.
Preserve clone and overlay; only size metadata is disposable. One process owns
Fjall state. Linux/SHA1 only; no LFS expansion, checkout filters, hardlinks or
shared writes. See [VFS reference](../../docs/vfs-reference.md).
