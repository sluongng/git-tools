# VFS reference

[Quick start](../crates/git-vfs/README.md) · [Project overview](../README.md)

## Mount and clone behavior

Requirements: Rust/Cargo, Linux `/dev/fuse`, `fusermount3`, and native Git.
GitHub.com repositories use the GitHub tree API for size metadata. Other remotes
need Git with `cat-file remote-object-info` support and a server advertising
protocol-v2 `object-info`; protocol-v2 support alone is not sufficient.
For a Git server you control, enable `transfer.advertiseObjectInfo=true`.

For the basic build/clone/mount commands, use the [quick start](../crates/git-vfs/README.md#start-a-workspace).
Use `warm-sizes --repo PATH` before mount for an explicit metadata-only phase.


The mount stays in the foreground.
Ctrl-C unmounts it; `fusermount3 -u /tmp/git-vfs-mount` also unmounts it.
The base commit is resolved once at startup; writes are layered over that snapshot.
Use `--revision <commit-or-ref>` to select another commit.
Use the global `--git /path/to/git` option if a newer Git executable is needed.
Without `--overlay`, writable state lives under `<bare-repo>/git-vfs-overlays/<base-commit>`.
Remounting the same overlay preserves edits, deletions, renames, and the native Git index.
A custom overlay is bound to one base commit and rejects a different revision.
Only one writable mount can hold an overlay at a time.

Use `mount --read-only` for the original immutable view; that view has no `.git` working-tree entry.

The clone uses `--bare --filter=blob:none --single-branch --no-local`.
By default it retains the selected branch's history; it is not a shallow clone.
Use `clone --depth 1` when only the selected tip's history is needed, independently of the blob filter.
The first writable mount copies the clone's shallow boundary into its isolated Git admin.
Remounts preserve that admin's boundary, including subsequent native Git deepening.
`--branch <branch-or-tag>` selects a non-default branch/tag.
An existing bare SHA-1 repository can also be mounted directly.
Git may fall back to an unfiltered clone if the remote does not support filters; inspect its warnings.

## Native Git status from filesystem state

In another terminal:

```sh
git -C /tmp/git-vfs-mount status --short
printf 'new content\n' > /tmp/git-vfs-mount/new-file.txt
git -C /tmp/git-vfs-mount status --short
git -C /tmp/git-vfs-mount add new-file.txt
git -C /tmp/git-vfs-mount status --short
```

The mount exposes a protected `.git` pointer to an isolated Git admin directory in the overlay.
Its HEAD starts detached at the base commit, and its index is populated from that tree without checkout.
The admin shares base objects through Git alternates; staging writes new objects into the isolated admin,
not the base clone. No upstream push or source-clone ref update is performed by the VFS.

For every acknowledged mutation, one Fjall transaction persists the namespace/content change and a
generation-tagged path journal. The fsmonitor hook returns an opaque generation token plus NUL-separated
paths changed since the previous token. Renames report both names, including descendants for directory renames.
Git checks those candidate paths against its normal index; the VFS does not invent Git status results.
Changing a file and then restoring its original bytes can therefore become clean again.

Untouched baseline index entries are initially marked **fsmonitor-valid**, not assume-unchanged or skip-worktree.
Subsequent journal events invalidate the affected paths, including same-size edits within one timestamp tick.
Unknown, foreign, or future tokens request a conservative full rescan rather than silently missing changes.
The isolated admin sets `fsmonitor.allowRemote=true` because Git classifies FUSE as remote, while this
custom hook obtains changes from a local transactional journal. Global Git configuration is not modified.

Clean status does not need to hash untouched baseline files. Git may still read `.gitignore`, `.gitattributes`,
or other policy files to interpret the working tree; such reads lazily fetch those blobs if absent.
Disabling the hook or using a Git build without fsmonitor support falls back to normal scanning and can
hydrate many files. Status/add/large-file hashing and remount persistence are validated; complete
branch-switching/merge/rebase/commit workflows are not yet a compatibility guarantee.

Native `git log` reads the isolated admin's history. A simple `git add` / `git commit` /
`git log` sequence and remount are validated, but **ordinary commit is not content-lazy**:
stock Git's cache-tree update prefetches missing non-skip-worktree index objects in
a promisor repository, including unchanged files. Those blobs land in the private
admin, so auditing only the base clone misses this transfer. A first commit can
therefore fetch the entire indexed snapshot. The VFS does not set skip-worktree
bits or weaken Git's object checks to suppress this behavior; doing so would
change ordinary edit detection or commit integrity.

## Digest and size interface

For regular files:

| Interface | Value | Reads blob contents? |
| --- | --- | --- |
| `stat().st_size` | Current file size | No; base sizes use metadata, edited sizes are local |
| `user.gitsha1` xattr | **20 raw digest bytes for current contents** | Untouched: no; edited: hash local overlay bytes |
| `user.gitsha1.hex` xattr | 40 lowercase hexadecimal characters | Same as the raw xattr |
| `read()` | Raw Git blob bytes | Yes, fetching only if absent locally |

```sh
python3 - /tmp/git-vfs-mount/path/to/file <<'PY'
import os, sys
path = sys.argv[1]
print("gitsha1:", os.getxattr(path, "user.gitsha1").hex())
print("size:", os.stat(path).st_size)
PY
```

The hash is `SHA1(b"blob " + decimal(size) + b"\0" + contents)`.
Untouched files return their tree's blob object ID; modified/new files hash their current overlay bytes.
It is not ordinary SHA-1 of the contents, and an edited file never retains a stale base digest.
Xattrs contain only the hash; file size comes from `stat`, not from a combined xattr.
Directory, submodule, and `.git` control entries have no digest xattr.
Symlink xattrs describe the symlink's target text when queried without following the link.

On a supporting Bazel version (8.7+ on the 8.x line, 9.2+ on the 9.x line), configure:

```text
startup --digest_function=GITSHA1
startup --unix_digest_hash_attribute_name=user.gitsha1
```

An REAPI remote cache/executor must advertise `GITSHA1` too.
Keep Bazel's output base/cache on a normal filesystem; the mount is a writable source tree prototype,
not an optimized backing store for build outputs or large databases.

## Git proxy size and BLAKE3 mapping

Build from the workspace root with `cargo build --release -p git-vfs`.
A configured `git-proxy` repository can serve the native Git clone and protocol-v2
`object-info` size queries, including when its upstream does not advertise them:

```sh
target/release/git-vfs clone http://127.0.0.1:8082/git/project.git /tmp/project.git
target/release/git-vfs warm-sizes --repo /tmp/project.git

# Optional content-digest lookup; the proxy requires --enable-blake3.
target/release/git-vfs cas-digest --repo /tmp/project.git \
  --proxy-url http://127.0.0.1:8082 --proxy-repo project path/to/file
```

`cas-digest` reads the selected base snapshot's Git SHA-1 from tree metadata and
requests its BLAKE3 mapping from `POST /api/v1/repos/project/objects`.
It does not fetch the blob into the VFS clone; the proxy may fetch and read the
blob to compute a missing mapping.
The result is an REAPI-compatible digest description:

```json
{"hash":"<64 hexadecimal characters>","size_bytes":123,"digest_function":"BLAKE3","git_sha1":"<40 hexadecimal characters>"}
```

The BLAKE3 hash describes raw blob contents, without Git's object header.
For a symlink, it describes the raw target-text blob, not the file reached by
following the link. Directories and submodules are rejected.
Like `digest`, this command describes the base snapshot, not writable overlay edits.
It validates the returned object identity, blob type, hash shape, and matching
nonnegative sizes within REAPI's signed 64-bit limit. The proxy is trusted to
compute the mapping correctly; verifying the content hash locally would require
reading the blob.

`--proxy-url` accepts only an explicit HTTP(S) origin, without credentials, query,
fragment, or path prefix. Aliases contain 1-128 ASCII letters, digits, `_`, and `-`.
Requests have a 30-second timeout and bounded responses. Redirects and environment
HTTP proxies are disabled; no Git credential helper or other authentication is
invoked for this API. Keep an unauthenticated proxy on a trusted local network.
This lookup does **not** upload anything to a CAS, change Bazel's digest mode, or
replace the existing `user.gitsha1` xattr.

## Bazel metadata prefetch and file watching

```sh
# Optional: download likely package-loading inputs in native Git batches.
target/release/git-vfs prefetch-bazel --repo /tmp/project.git
```

This selects `BUILD`, `BUILD.bazel`, `WORKSPACE`, `WORKSPACE.bazel`, `MODULE.bazel`,
`MODULE.bazel.lock`, `REPO.bazel`, `.bazelrc`, `.bazelversion`, and files ending in
`.bzl` or `.MODULE.bazel`, plus every symlink blob, anywhere in the base snapshot.
Symlink blobs contain target strings needed for `readlink`; prefetch does not follow
the links or download their target files. Duplicate blob IDs are fetched once;
existing local blobs are reused. The initial union is combined into one native
`git fetch --stdin` invocation, without updating branch refs or `FETCH_HEAD`.
Their sizes are cached too.

By default, a bounded second pass discovers literal repository-loading inputs:
`patches` and `requirements_lock` in module files, plus downloader configurations
and `import` / `try-import` in rc files. Included module fragments resolve labels
from the root module package. Newly discovered inputs are deduplicated and fetched
in one additional batch per discovery wave; imported rc files can require further
waves. Discovery reads at most 1 MiB per seed and 8 MiB total, visits at most eight
waves, and adds at most 10,000 paths. Limits skip optional discovery and are reported
without discarding the initial prefetch.

The JSON report includes selected paths, per-path reasons, unique blob/fetch
counts, discovery waves, and bounded-discovery omissions.

Use repeatable repository-relative file or subtree scopes to limit selection:

```sh
target/release/git-vfs prefetch-bazel --repo /tmp/project.git \
  --path MODULE.bazel --path BUILD.bazel --path contrib/bazel --path t
```

Scopes are combined before the initial fetch. A subtree matches path components,
not similarly named siblings. No scopes means the whole snapshot. Scopes select
only matching Bazel metadata and symlink blobs, not every source file underneath;
discovered references stay inside these scopes.

Add known file/subtree contents with repeatable `--include`. These are explicit
additions, including outside metadata scopes. Missing includes fail before fetching.
With known includes and `--no-discover`, the whole union uses a single native fetch:

```sh
target/release/git-vfs prefetch-bazel --repo /tmp/project.git \
  --include third_party/dependency.patch --include requirements.txt --no-discover
```

Unlike `warm-sizes`, this **downloads contents**. It is a filename heuristic, not a
complete dependency graph: it does not evaluate Starlark. Computed attributes,
unsupported string syntax, arbitrary file-valued attributes, contents reached
through symlinks, generated metadata, and external repositories can still need lazy
reads. Inactive literal calls can overfetch. It does not overwrite overlay
edits. Whether prefetch improves time to the first query/build depends on the
workload and transport.

To overlap content transfer with Bazel startup, let the mount own prefetch:

```sh
target/release/git-vfs mount --repo /tmp/project.git \
  --prefetch-bazel --prefetch-include requirements.txt \
  --overlay /tmp/project-overlay /tmp/git-vfs-mount
```

The filesystem becomes available before content prefetch finishes. GitHub size
metadata still warms before mounting, so `stat` returns correct sizes. Use
repeatable `--prefetch-path` and `--prefetch-include` for the same policy as the
standalone command; `--prefetch-no-discover` disables literal input discovery.
Invalid includes fail before mount. Prefetch is optional; ordinary mounts remain
fully lazy.

Background prefetch and demand reads share a content-transfer lock. An early
content read waits for an active batch, then reuses its objects. Metadata does
not take that lock, but FUSE dispatch is serial: a waiting content read also
queues later filesystem requests. This first implementation also delays
already-local blob reads behind an active batch. Failure is reported without
taking down the mount; later reads can still fetch on demand. Unmount cancels
and reaps native Git process groups in the shared shutdown scope. This overlaps
work; it does not eliminate transfer
time. Benchmark clone-to-successful-query/build, not just mount readiness.

`query` loads packages; `aquery` additionally analyzes the action graph. Neither is
a substitute for validating action-input digests during a real build. Even with
digest xattrs, Bazel must read Starlark content to evaluate it, and executing a
local action must read whatever source contents that action actually uses.

On Linux, Bazel's `--watchfs` uses Java `WatchService`/inotify. Normal edits through
this writable mount produce those kernel events; no Git fsmonitor hook integration
is needed. Git consumes the persistent overlay journal, while Bazel consumes
ephemeral kernel watches. Keep the mount alive across incremental builds and
restart Bazel after unmount/remount to establish fresh watches. Editing the backing
Fjall database directly is unsupported and does not emit mounted-filesystem
events. Initial watch registration traverses directory metadata; warming sizes
avoids one remote size lookup per uncached file during that traversal.

## Persistent blob-size cache

**Yes: cache `Git blob SHA-1 -> content size` locally.**
Blob identities and sizes are immutable, so entries can be reused across paths, commits, and mount sessions.
The Fjall cache lives at `<bare-repo>/git-vfs-sizes.fjall/`, using raw 20-byte
SHA-1 keys and big-endian eight-byte sizes. Duplicate content shares one entry.
Snapshot reads, atomic conflict-checking transactions, and explicit `SyncAll` durability support safe reuse.
The database is a local metadata cache; the Git object database remains the content cache.

For a missing size, the lookup order is:

1. Fjall cache.
2. Local Git object metadata with `GIT_NO_LAZY_FETCH=1`.
3. Protocol-v2 `remote-object-info`, in batches of up to 128 OIDs.
4. Error by default. `--size-fallback=fetch` explicitly allows fetching content to learn its size.

```sh
target/release/git-vfs cache --repo /tmp/project.git
target/release/git-vfs stat --repo /tmp/project.git path/to/file

# This can download blobs during stat/lookup. It is an explicit tradeoff.
target/release/git-vfs mount --repo /tmp/project.git \
  --size-fallback=fetch /tmp/git-vfs-mount
```

`warm-sizes` fills the cache for all blobs in the selected snapshot without fetching content.
With the default `--size-source=auto`, a recognized GitHub.com remote uses its
tree API; other remotes use the native Git lookup order above. A successful
`blob:none` clone does not establish `object-info` support.

For GitHub, `mount` also warms missing sizes automatically before starting FUSE.
Fjall is checked first, then all uncached local objects are checked in one native
Git batch with lazy fetching disabled. A fully local clone therefore needs no API
request or token, even when its Fjall cache is empty. Bare repositories without a
configured remote retain the ordinary local/lazy mount behavior.
The tree API request is pinned to the selected commit's root tree, not a moving
branch. Responses must match the local snapshot's paths, modes and object IDs;
truncated recursive results fall back to validated non-recursive subtree requests.
Sizes are staged and persisted in one conflict-checked, durable Fjall transaction.
No file contents are downloaded by this metadata step. Non-UTF-8 paths cannot be
represented reliably by this REST provider and cause an explicit error.
Discovery is bounded to 8 MiB per response, 64 MiB in aggregate, 1,024 requests,
and 500,000 local manifest entries. Exceeding a limit fails explicitly rather than
accepting incomplete metadata or hydrating contents.

Use `--size-source=git` to retain native Git size discovery and skip automatic
GitHub warming on mount, or `--size-source=github` to require the GitHub provider.
The API endpoint is fixed to `https://api.github.com`, redirects are not followed,
and no Git credentials are automatically forwarded. Public metadata needs no token.
For a private repository or higher rate limit, explicitly select a token-bearing
environment variable, for example `--github-token-env GH_TOKEN`; pass its **name**,
never the token value. Provider failures do not silently download blob contents.

```sh
target/release/git-vfs warm-sizes --repo /tmp/project.git \
  --size-source=github --github-token-env GH_TOKEN
```

The JSON output retains the cache statistics and adds a `warm` report describing
the selected provider, pinned identity, cache reuse, and HTTP requests/bytes.
Once warmed, metadata works offline even if blobs have never been downloaded.
File contents work offline only for blobs already in the local object database.
Deleting the cache while no mount/process uses it is safe; the next session rebuilds metadata on demand.
Do not edit cached sizes manually.

## Fjall storage

Fjall is the only storage engine; there is no `--storage` selector. The writable
state lives at `<overlay>/overlay.fjall/`. Namespace changes, file bytes, generation
advancement, and journal paths commit atomically with `PersistMode::SyncAll` before
updating the mounted in-memory view. Default buffered-only writes are not used for
acknowledged mutations.

The existing Fjall cache and overlay formats are unchanged. SQLite and LMDB are no
longer supported: legacy cache files are left untouched but are not imported, and
legacy writable overlays are rejected rather than treated as empty workspaces.
There is no automatic migration. Keep the old overlay and an older binary if you
need to recover its uncommitted or staged work; a new overlay directory starts a
separate working tree.

Fjall allows one owning process per database. The mount owns its databases, while
Git's separate fsmonitor hook requests a consistent journal snapshot through a
read-only Unix socket. Socket names are hashed under a private, owner-only runtime
directory, so long overlay paths work. The service checks peer identity and bounds
messages; it exposes no database-write operation. When unmounted, journal queries
open the database directly under an ownership lock. A dead/unreachable active service
returns an error rather than reporting a false empty change set.

Standalone size-cache commands cannot open the same Fjall cache
concurrently with its mount. Run `warm-sizes`, `prefetch-bazel`, and cache inspection
before mounting or after unmounting; mounted filesystem operations and native Git
status remain supported.
Fjall values have a roughly 4 GiB per-record limit; this prototype stores a changed
file as one record, including metadata, and rejects oversized records.

## Inspect the base snapshot without mounting

```sh
target/release/git-vfs list --repo /tmp/project.git
target/release/git-vfs digest --repo /tmp/project.git path/to/file
target/release/git-vfs digest --raw --repo /tmp/project.git path/to/file
target/release/git-vfs cat --repo /tmp/project.git path/to/file
```

`list` and `digest` do not query sizes or fetch blobs.
`stat` uses the metadata policy; `cat` allows Git's native promisor fetch.
These commands inspect the base Git snapshot, not edits in a mounted overlay; use filesystem operations
and digest xattrs on the mount to inspect current writable contents.
JSON output is for inspection; paths use lossy UTF-8 display strings.
The mounted filesystem and path-taking commands preserve the original native filename bytes.

## Scope and current limits

- Linux only, SHA-1 repositories only. Immutable base plus persistent writable overlay; optional read-only view.
- Tree/commit metadata is loaded at startup; blob contents remain lazy.
- Executable bits, regular files, empty files, native filenames, and symlinks are preserved.
- Submodules begin as empty directory placeholders; their repositories are not fetched or managed.
- No checkout filters, CRLF transformation, or LFS payload expansion. An LFS file exposes its pointer blob.
  This keeps the advertised digest consistent with the visible bytes.
- Regular-file creation, reads/writes, append, truncation, chmod/mtime, rename/replacement, deletion,
  directories, and symlinks are supported. Open handles retain inode identity after unlink or replacement.
- **Prototype I/O:** reading a base file buffers the whole blob through Git. A partial edit materializes
  that base once; truncation to zero does not fetch old content. Edited content is stored as Fjall values
  and retained in memory while mounted. Very large files and many small writes are not optimized.
  Each repository reuses one serialized native `cat-file --batch` process for blob
  reads, without retaining an additional content cache. A failed response discards
  the process; the failed operation is not silently retried.
- Writable handles use direct I/O with a one-second positive attribute/entry TTL.
  Linux FUSE updates or invalidates these caches for mutations through the mount;
  immediate write/truncate/chmod/rename/unlink and parent-link-count tests exercise
  that behavior on the tested kernel. Missing-name replies are not timed negative
  entries. This depends on the single-owner overlay and in-band-only mutation
  model; external overlay writers would require explicit invalidation. Native Git's
  private mmap hashing works on the tested kernel; writable/shared mmap, hard links, device nodes,
  arbitrary user xattr mutations, and atime tracking are not implemented.
- Metadata requests currently query one OID at a time. Use `warm-sizes` to batch before traversal.
- Native Git handles transport, authentication, and object retrieval. No credential extraction or new auth layer.
- Backend operations are serialized within a mount. Git subprocesses have no timeout/cancellation layer yet;
  a blocked authentication/network operation can delay a FUSE request.

The overlay directory contains `overlay.fjall/` (namespace, current edited bytes, tombstones, and journal),
`git/` (isolated index/refs/objects), and a mount lock. **Do not delete it to clear a cache:** it contains
your uncommitted and staged work. Keep the base clone available while its objects are referenced by alternates.
The immutable `git-vfs-sizes.fjall/` directory described above remains a separate, disposable metadata cache.

## Validation

```sh
cargo fmt --check
cargo clippy -p git-vfs --all-targets -- -D warnings
cargo test -p git-vfs

# Explicit kernel-level integration tests (require working /dev/fuse).
cargo test -p git-vfs --test integration -- --ignored --nocapture

```

The local Git fixture enables partial-clone filtering and object-info on the server.
It checks that namespace/digest/size operations leave blobs absent, the Fjall cache survives reopening with an
unavailable remote, a content read fetches just its blob, and unsupported metadata requires an explicit fallback.
It includes duplicate content, empty/binary/large files, executables, symlinks, native-byte filenames, and
more than one metadata batch.
Mount tests exercise kernel reads/xattrs, read-only enforcement, writable file/namespace operations,
native Git clean/dirty/staged status without hydrating the baseline, same-size edits, restored-content
cleanliness, large-file hashing, native-byte filenames, persistent remounts, protected `.git`, single-writer
locking, open/unlinked/replaced handles, and recovery from interrupted Git admin setup.
Fjall is checked for daemon-kill recovery, cross-process journal reads, and rejection
of legacy SQLite/LMDB overlays without loss of their contents.

## References

- [Git partial-clone design](https://git-scm.com/docs/partial-clone)
- [Git protocol-v2 object-info](https://git-scm.com/docs/gitprotocol-v2#_object_info)
- [Git fsmonitor v2 hook contract](https://git-scm.com/docs/githooks#_fsmonitor_watchman)
- [REAPI digest functions](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto)
- [Bazel GITSHA1 implementation](https://github.com/bazelbuild/bazel/blob/master/src/main/java/com/google/devtools/build/lib/vfs/GitSha1HashFunction.java)

## Opt-in BLAKE3 mappings from a trusted proxy

A proxy started with `--enable-blake3` can supply raw-content BLAKE3 digests
without the VFS downloading the corresponding Git blobs. Explicitly opt in:

```sh
# Optional separate phase for measuring or reusing the persistent metadata cache.
target/release/git-vfs warm-blake3 --repo /tmp/project.git \
  --proxy-url http://127.0.0.1:8080 --proxy-repo project

target/release/git-vfs mount --repo /tmp/project.git \
  --blake3-proxy-url http://127.0.0.1:8080 --blake3-proxy-repo project \
  --prefetch-bazel /tmp/git-vfs-mount

# In another terminal, with a Bazel build that supports BLAKE3:
cd /tmp/git-vfs-mount
bazel --digest_function=BLAKE3 query \
  --unix_digest_hash_attribute_name=user.blake3 //...
```

Mount startup first warms every unique file/symlink mapping in batches of at
most 16,384 object IDs. This is a **blocking metadata phase**, separate from
optional asynchronous content prefetch. A warm local mapping cache needs no
HTTP request. Sizes and 32-byte hashes are persisted together in the existing
Fjall size database, with conflict checks against previously cached values.
The provider is trusted to report the correct content hash: the VFS validates
identities, blob types, hash encoding and size agreement, but cannot verify a
content hash without downloading the bytes. Use a trusted origin and transport.
Redirects, URL credentials and path prefixes are rejected.

Only opted-in mounts expose `user.blake3` (32 raw bytes) and
`user.blake3.hex` (64 hexadecimal bytes). Untouched base files use cache-only
lookups; an absent mapping returns `ENODATA`, never an HTTP request from a
filesystem callback. Edited and new files hash their current overlay bytes,
including same-size edits. Directories and the private `.git` control files do
not expose a BLAKE3 digest. These mappings neither upload content to a remote
CAS nor assert that a remote CAS already holds it.

At shutdown, `BLAKE3 xattrs: {...}` reports per-mount `base_hits`,
`local_hashes` and `misses`. Counters include xattr size probes and reads, not
unique paths. Stop filesystem users before shutdown to measure a quiescent
mount: these are best-effort snapshots, not a dispatch-thread join guarantee.
A query may load BUILD/Starlark files without requesting content
digests; zero hits means that run did not exercise digest reuse, regardless of
its configured digest function. Use an actual build/action-input workload to
measure digest reuse when query alone does not request the attributes.
