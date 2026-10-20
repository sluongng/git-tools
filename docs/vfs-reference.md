# VFS reference

[Quick start](../crates/workspace-fuse/README.md) · [Platform support](platforms.md)

## Clone, mount and lifetime

`git-vfs clone URL DEST` uses a bare, filtered, single-branch clone. Without
`--depth` it retains branch history; `--depth 1` is independent of `blob:none`.
`--branch` selects a branch/tag. Git can fall back to unfiltered transport if the
origin lacks filters; inspect warnings. Global `--git /path/to/git` selects Git.

`mount --repo REPO --overlay STATE MOUNTPOINT` resolves a fixed base once.
`--revision` selects it explicitly; one overlay cannot be reused with another
base or by two writable mounts. The default overlay is
`REPO/git-vfs-overlays/COMMIT`. Remount preserves edits, deletions and private Git
index/refs/objects. `--read-only` omits the writable `.git` view.

The foreground daemon owns the mount. Ctrl-C or `fusermount3 -u MOUNTPOINT`
unmounts Linux FUSE. Keep the base clone available: private Git uses alternates.
**Never delete the overlay to clear a cache.** Native status/add/basic commit/log
work; a first commit can fetch the whole indexed snapshot into private Git.
General checkout, merge and rebase are not compatibility guarantees.

## Sizes without downloading files

Blob sizes persist by SHA1 in `REPO/git-vfs-sizes.fjall/`. With
`--size-source=auto`, GitHub.com remotes use a commit-pinned tree API; others use
native Git metadata. `warm-sizes --repo REPO` performs an explicit warm phase.
Mount warms GitHub or explicitly selected Git metadata before becoming ready.

Native lookup checks Fjall, local Git with lazy fetch disabled, then batched
protocol-v2 `remote-object-info`. It requires a suitable Git build **and server
object-info support**; protocol v2/filtering alone do not suffice. A controlled
server can enable `transfer.advertiseObjectInfo=true`.
`--size-fallback=fetch` explicitly permits downloading contents to learn sizes
and can hydrate a whole snapshot; failures otherwise do not silently fetch.

GitHub responses must match local paths, modes and object IDs; truncated results
use validated subtree requests. Bounds: 8MiB/response, 64MiB total, 1,024 requests,
500,000 manifest entries. Non-UTF-8 REST paths fail explicitly. Fully local sizes
need no API request. Use `--size-source=git|github` to require a provider and
`--github-token-env VARIABLE_NAME` for private GitHub metadata. Credentials are
not taken from Git or forwarded automatically; no token value belongs in flags.
Metadata works offline after warming; file reads require local content or origin.

## Digests and Bazel

| Interface | Untouched file | Edited/new file |
| --- | --- | --- |
| `stat().st_size` | Cached size | Current local size |
| `user.gitsha1` / `.hex` | Tree blob ID | Hash current bytes |
| Opt-in `user.blake3` / `.hex` | Trusted cached mapping | Hash current bytes |

Raw hashes are 20/32 bytes; `.hex` is 40/64 lowercase characters. Size comes
separately from stat. GitSHA1 is `SHA1("blob " + size + NUL + contents)`, not plain
content SHA1. Symlink digests describe target text without following the link.
Directories/submodules/private Git control files do not expose file digests.

For BLAKE3 mappings, explicitly trust a proxy started with `--enable-blake3`:

```sh
git-vfs mount --repo /tmp/project.git --overlay /tmp/project-state \
  --blake3-proxy-url http://127.0.0.1:8082 --blake3-proxy-repo project \
  --prefetch-bazel /tmp/project
```

All snapshot mappings import **before mount**, in batches of at most 16,384 IDs;
a warm Fjall cache avoids HTTP. `warm-blake3 --repo REPO --proxy-url URL
--proxy-repo ALIAS` separates this phase. Identity/type/size/hash shape are checked,
but verifying the provider's content digest would require downloading bytes.
Only trusted HTTP(S) origins are supported: no redirects, URL credentials,
path prefixes or environment HTTP proxies. Filesystem xattr callbacks never fetch
mappings; absent BLAKE3 returns `ENODATA`. Mappings do not prove CAS presence.

Use Bazel startup options **before** `query`/`build`:

```sh
bazel --digest_function=BLAKE3 \
  --unix_digest_hash_attribute_name=user.blake3 build //:target
```

Digest-xattr startup support requires Bazel 8.7+ on 8.x or 9.2+ on 9.x. GITSHA1
can use `user.gitsha1` instead; the remote backend must advertise that function.
Keep outputs/caches outside the source mount. Query/aquery can read Starlark
without requesting file digests: validate reuse with real builds and counters.

## Bazel metadata prefetch and file watching

`mount --prefetch-bazel` starts optional content prefetch after mount. The
standalone `prefetch-bazel --repo REPO` blocks and runs before/after mount, not
concurrently with its Fjall owner.

Selection includes BUILD/WORKSPACE/MODULE/REPO files, `.bazelrc`, `.bazelversion`,
`.bzl`/`.MODULE.bazel` files and **all symlink target-text blobs**. A bounded
literal parser also discovers module patches/requirements and rc imports/downloader
configuration. It does not evaluate Starlark or follow symlink targets.

| Standalone option | Mount equivalent | Effect |
| --- | --- | --- |
| repeatable `--path` | `--prefetch-path` | Select metadata/symlinks within file/subtree scopes |
| repeatable `--include` | `--prefetch-include` | Add known file/subtree contents, even outside scopes |
| `--no-discover` | `--prefetch-no-discover` | Disable literal discovery |

Missing includes fail before fetching/mounting. No scopes means whole snapshot.
Standalone initial selection is one deduplicated native fetch; known includes
plus no discovery keep it to one batch. Discovery can add waves (1MiB/seed,
8MiB total, 8 waves, 10,000 paths); reports record selections/reasons/omissions.
Mounted prefetch uses demand-prioritized bounded batches, not one guaranteed fetch.
Already-local reads continue during remote waits; overlapping requests coalesce.
Failed speculation is nonfatal; unmount cancels/reaps owned fetch processes.

Linux Bazel `--watchfs` consumes normal inotify events from edits through the
mount, **not Git fsmonitor**. Keep the mount alive across incremental builds;
restart Bazel after remount to reestablish watches. Direct Fjall edits are unsupported.

## Writable state and resource limits

Fjall stores namespace/journal records; ranged payload files store mutable data.
Durable redo precedes metadata publication. Uncertain mutations require remount
recovery. Unlinked content is reclaimed only after references permit it. Journal
overflow requests conservative Git rescans rather than silently losing changes.
Git's protected `.git` pointer leads to isolated admin state; upstream refs are
not updated. Same-size edits invalidate digest/index fast paths.

Immutable content cache defaults to 64MiB / 256 files; reply buffers have an
independent 64MiB limit. Configure `--content-cache-bytes` and
`--content-reply-bytes`; zero cache disables admission. Oversized immutable blobs
bypass that cache. Reply saturation can return `EAGAIN`; tiny limits may reject
ordinary read ranges. First-write copy-up and native Git memory are separate costs.

Source NFS and ProjFS use the same bounded scheduler with its default budgets.
NFS fetches immutable bytes outside namespace locks and rechecks edits afterward;
ProjFS uses ranged replies tied to the original stream identity. Blobs above the
64MiB decoded-cache budget can still be re-extracted for later ranges. Bounded
reply memory does not promise one extraction per large blob.

Decoded metadata residency defaults to 4,096 entries / 4MiB estimated bytes;
`GIT_VFS_METADATA_CACHE_ENTRIES` / `GIT_VFS_METADATA_CACHE_BYTES` override it.
Namespace indexes and the immutable snapshot remain eager; these are not RSS bounds.
Fjall permits one process owner. Run cache inspection/warming commands before
mount or after unmount. Only size/mapping metadata is disposable. Legacy
SQLite/LMDB overlays are rejected; older inline Fjall overlays migrate resumably
and must not then be opened with an older binary.

## Inspection and limits

`list`, `digest`, `stat`, `cat` and `cas-digest` inspect the **base snapshot**, not
mounted edits. `list`/`digest` do not hydrate; `stat` follows size policy; `cat`
allows native promisor fetch. Use mounted xattrs/current files for edits.

Linux source FUSE supports native-byte names, executable bits, symlinks and common
file mutations. Submodules are empty placeholders; no LFS expansion, CRLF or
checkout filters. No hardlinks, device nodes, arbitrary xattr writes, independent
atime or shared-writable mmap. Positive metadata caching relies on one owner and
in-band mutations. Git child operations and source first-write copy-up retain
serialization costs; do not assume whole-mount memory or throughput bounds.

See [build/test instructions](development.md), [platform differences](platforms.md)
and [benchmark qualifications](benchmarks.md). Protocol references:
[partial clone](https://git-scm.com/docs/partial-clone),
[object-info](https://git-scm.com/docs/gitprotocol-v2#_object_info),
[fsmonitor](https://git-scm.com/docs/githooks#_fsmonitor_watchman),
[REAPI](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto).
