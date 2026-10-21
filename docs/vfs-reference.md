# Source VFS reference

[Linux setup](../crates/workspace-fuse/README.md) · [Platform support](platforms.md)

## Clone and mount

`git-vfs clone URL DEST` creates a bare, filtered, single-branch clone without a
checkout. `--depth 1` also limits history; filtering alone does not. `--branch`
selects a branch/tag. Origins without filtering may send all blobs; inspect Git's
warnings. Global `--git /path/to/git` selects the native Git executable.

```sh
git-vfs mount --repo /tmp/project.git --overlay /tmp/project-state \
  --revision FULL_COMMIT_SHA --prefetch-bazel /tmp/project
```

A writable overlay belongs to one fixed base and one mount owner. Remounting
preserves edits, deletions and private Git index/refs/objects. `--read-only` omits
the writable `.git` view. Keep the base clone available: private Git uses alternates.
**Never delete the overlay to clear a cache.** Status/add/basic commit/log work;
a first commit can hydrate the indexed snapshot. General checkout/merge/rebase
are not supported workflows. Ctrl-C or `fusermount3 -u MOUNTPOINT` unmounts FUSE.
If readiness metadata is missing, only a provably pristine Git admin can resume
initialization. Existing commits/index state is preserved and mounting fails
closed; do not delete that state or invent a readiness marker to force recovery.

## Sizes and digests

Sizes persist by SHA1 in `REPO/git-vfs-sizes.fjall/`. `--size-source=auto` uses
GitHub's commit-pinned tree API for GitHub.com origins, otherwise native Git.
`warm-sizes --repo REPO` warms explicitly; GitHub metadata warming can block mount
readiness. Other origins need a suitable Git build **and server object-info
support**. Filtering/protocol v2 alone is insufficient.

`--size-fallback=fetch` explicitly allows downloading blobs to learn sizes;
otherwise failures do not silently fetch them. Private GitHub metadata uses
`--github-token-env VARIABLE_NAME`, not a token value. Metadata works offline once
warmed; content reads still need local bytes or origin access.

| Mounted interface | Untouched file | Edited/new file |
| --- | --- | --- |
| `stat().st_size` | Cached size | Current local size |
| `user.gitsha1` / `.hex` | Git tree blob ID | Hash current bytes |
| Opt-in `user.blake3` / `.hex` | Trusted cached mapping | Hash current bytes |

Raw digests are 20/32 bytes; `.hex` is 40/64 lowercase characters. GitSHA1 includes
Git's blob header, unlike REAPI content hashes. Symlink digests describe target
text, not the destination. Directories and Git control files expose no file digest.

For BLAKE3, trust a proxy started with `--enable-blake3` and add
`--blake3-proxy-url http://127.0.0.1:8082 --blake3-proxy-repo project` when mounting.
Snapshot mappings import before mount; `warm-blake3` separates that warm phase.
The provider is trusted for content hashes; validating them locally would require
downloading bytes. No redirects, URL credentials or HTTP path prefixes are accepted.
Xattr reads never fetch mappings; absent BLAKE3 returns `ENODATA`.
A mapping does not prove remote CAS presence.

With a compatible Bazel, set startup options before `build`:

```sh
bazel --digest_function=BLAKE3 \
  --unix_digest_hash_attribute_name=user.blake3 build //:target
```

GITSHA1 can use `user.gitsha1` if the remote backend advertises it. Keep outputs
and caches outside the source mount. Query/aquery need not request file digests;
use actual builds when validating digest reuse.

## Prefetch and watching

`mount --prefetch-bazel` starts bounded background prefetch after mounting.
Selection includes Bazel/Starlark metadata and symlink target-text blobs. Literal
imports/patches/requirements can add discovery waves; Starlark is not evaluated.

| Mount option | Purpose |
| --- | --- |
| Repeatable `--prefetch-path` | Limit metadata/symlink selection to scopes |
| Repeatable `--prefetch-include` | Add known file/subtree contents |
| `--prefetch-no-discover` | Disable literal discovery |

Standalone `prefetch-bazel` uses `--path`, `--include`, `--no-discover` and blocks.
Known includes without discovery can use one fetch; mounted prefetch is bounded,
demand-prioritized and not guaranteed to be one batch. Missing includes fail
before fetching. Failed speculation is nonfatal; unmount cancels owned fetches.

Linux Bazel `--watchfs` uses normal inotify events from mounted edits, not Git
fsmonitor. Keep the mount alive for incremental builds; restart Bazel after remount.
Direct Fjall edits are unsupported.

## Storage and limits

Fjall stores namespace/journal metadata; mutable bytes live in ranged backing
files. Journal overflow requests a conservative rescan. Local edits, private Git
state and commits are authoritative, not disposable cache data. Only immutable
size/digest mappings can be rebuilt safely. Fjall permits one process owner:
inspect/warm its databases before mounting or after unmounting.

`--content-cache-bytes` and `--content-reply-bytes` independently default to 64MiB.
Zero disables decoded-content caching; oversize blobs bypass it. Saturated reply
budgets can return `EAGAIN`. These are not whole-process memory limits: namespace
indexes, Git extraction and first-write copy-up have separate costs.

`list`, `digest`, `stat`, `cat` and `cas-digest` inspect the base snapshot, not mounted
edits. Use mounted files/xattrs for current state. Submodules are placeholders;
LFS expansion, checkout filters, hardlinks, arbitrary xattr writes and writable
shared mmap are unsupported. See [platform differences](platforms.md).
