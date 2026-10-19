# git-vfs

Writable Linux FUSE view of a bare blobless Git clone.
Files load when read; edits live in a persistent copy-on-write overlay.
Native `git status` uses a durable change journal through Git's fsmonitor hook.

## Start a workspace

Requires Rust/Cargo, native Git, Linux `/dev/fuse`, and `fusermount3`.
Run from the project root:

```sh
cargo build --release -p git-vfs
target/release/git-vfs clone https://github.com/bazelbuild/bazel.git \
  /tmp/project.git --depth 1
mkdir -p /tmp/project
target/release/git-vfs mount --repo /tmp/project.git \
  --overlay /tmp/project-overlay --prefetch-bazel /tmp/project
```

The clone destination must not exist; the mount directory must be empty.
Ctrl-C unmounts the foreground process. Remount the same clone and overlay to
retain edits and Git index. `mount --read-only` omits the writable `.git` view.
Without `--depth`, the blobless clone retains branch history.
Use global `--git /path/to/git` to select native Git.

GitHub.com sizes come from its tree API before mount, without downloading blobs.
Other remotes require `cat-file remote-object-info` and server protocol-v2
`object-info` support, unless trusted BLAKE3 proxy mappings supply the sizes.
Protocol v2 and blobless-clone support alone are insufficient.
Private GitHub metadata uses explicit `--github-token-env VARIABLE_NAME`;
Git credentials are not forwarded to the API.

## Bazel and digests

`--prefetch-bazel` asynchronously batches likely Bazel inputs and symlink blobs
after mount. Other files remain demand-loaded. Scope options and the blocking
`prefetch-bazel` command are in the [reference](../../docs/vfs-reference.md#bazel-metadata-prefetch-and-file-watching).

For a trusted proxy started with `--enable-blake3`:

```sh
target/release/git-vfs mount --repo /tmp/proxy-project.git \
  --overlay /tmp/proxy-project-overlay --prefetch-bazel \
  --blake3-proxy-url http://127.0.0.1:8082 --blake3-proxy-repo bazel \
  /tmp/project
```

Mount imports size/BLAKE3 mappings before FUSE starts; cached mappings avoid HTTP.
Use Bazel startup flags `--digest_function=BLAKE3` and
`--unix_digest_hash_attribute_name=user.blake3` before `query` or `build`.
Keep Bazel outputs/caches outside the mount. A remote cache/executor must support
the selected digest function and receive the content separately.

| Interface | Value |
|---|---|
| `stat().st_size` | Current content size |
| `user.gitsha1` / `.hex` | Raw 20-byte Git blob digest / 40-byte hex |
| Opt-in `user.blake3` / `.hex` | Raw 32-byte content digest / 64-byte hex |

Untouched digests use metadata; edits hash current overlay bytes, including
same-size edits. Neither raw digest includes size. Missing BLAKE3 mappings return
`ENODATA`; xattr callbacks never fetch them over HTTP.

## Git and storage limits

Native `status`, `add`, basic `commit`/`log`, and remount are validated.
A first commit can fetch the entire indexed snapshot. Base commits are fixed at
mount; general checkout/merge/rebase compatibility is not promised.
Bazel watchfs uses normal filesystem notifications, not Git's fsmonitor hook.

Fjall permits one owning process: run standalone cache/prefetch commands before
mount or after unmount. Preserve the base clone and overlay; the overlay contains
uncommitted/staged work. Only `git-vfs-sizes.fjall/` is disposable metadata.
Linux/SHA1 only; no LFS expansion, checkout filters, hard links, or shared writes.
Namespace metadata is eager; native Git and first-write copy-up have separate memory costs.

## Reference and validation

- [Full reference](../../docs/vfs-reference.md): provider limits, proxy API,
  authentication, prefetch scopes, storage formats, compatibility, and recovery.
- [Project quick start](../../README.md) / [benchmarks](../../tools/README.md).
- `cargo test -p git-vfs`; kernel tests require `/dev/fuse`:
  `cargo test -p git-vfs --test integration -- --ignored --nocapture`.
