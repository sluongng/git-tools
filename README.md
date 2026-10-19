# git-tools

Rust tools for starting work on Git repositories without a full checkout.

- [`git-proxy`](crates/git-proxy/): shared Git mirror, fetch-response cache,
  batch object sizes, and optional Git SHA1 → BLAKE3 mappings.
- [`workspace-fuse`](crates/workspace-fuse/): writable Linux FUSE view of a blobless clone,
  with lazy reads and persistent per-agent edits.

Additional crates: [`workspace-nfs`](crates/workspace-nfs/) for macOS NFS,
[`workspace-projfs`](crates/workspace-projfs/) for Windows,
[`jj-vfs`](crates/jj-vfs/) for Jujutsu working copies, and
[`bazel-output-service`](crates/bazel-output-service/) for BuildBuddy-backed Bazel outputs.
These are experimental; native macOS kernel acceptance is still pending.
Crate names describe responsibilities; executable names remain `git-vfs`,
`git-nfs`, `git-projfs`, `git-jj`, and `git-output-service` for compatibility.

Native Git stores and transfers objects; Fjall stores local metadata.
The VFS also works directly with GitHub, without a proxy.

## Quick start

Requires the pinned Rust toolchain, native Git, Linux `/dev/fuse`,
and `fusermount3` (usually the `fuse3` package).
Run commands from this repository's root.

**Shared proxy — run once, in its own terminal:**

```sh
cargo build --release -p git-proxy -p workspace-fuse
target/release/git-proxy --cache-dir /tmp/git-tools-proxy \
  --repo bazel=https://github.com/bazelbuild/bazel.git --enable-blake3
```

The first request creates a **full all-refs mirror**; it is not a cheap cold start.
Later agents share the mirror and eligible cached fetch responses.
The unauthenticated proxy defaults to loopback; keep it on a trusted network.

**Per agent — use fresh clone/overlay paths and an empty mount directory:**

```sh
target/release/git-vfs clone http://127.0.0.1:8082/git/bazel.git \
  /tmp/agent-bazel.git --depth 1
mkdir -p /tmp/agent-bazel
target/release/git-vfs mount --repo /tmp/agent-bazel.git \
  --overlay /tmp/agent-bazel-overlay --prefetch-bazel \
  --blake3-proxy-url http://127.0.0.1:8082 --blake3-proxy-repo bazel \
  /tmp/agent-bazel
```

The mount stays in the foreground; Ctrl-C unmounts it.
BLAKE3 mapping import is blocking metadata work; content prefetch runs after mount.
Other contents load on demand. Remount the same overlay to retain edits and index.
For direct GitHub use, change the clone URL and omit both BLAKE3 proxy options.
See the [VFS guide](crates/workspace-fuse/README.md) for metadata-provider requirements.

**Another terminal — Git and Bazel:**

```sh
git -C /tmp/agent-bazel status --short
cd /tmp/agent-bazel
bazel --output_user_root=/tmp/agent-bazel-user \
  --output_base=/tmp/agent-bazel-output --digest_function=BLAKE3 \
  --unix_digest_hash_attribute_name=user.blake3 query //...
```

Use the repository's supported Bazel version with digest-xattr startup support
(8.7+ on 8.x, 9.2+ on 9.x). Keep outputs and caches outside the mount.
Configure remote execution/cache and external-dependency caching separately.
Digest mappings alone neither upload blobs nor prove remote CAS residency.

## Measured performance

Pinned `bazel.git`, warm dependency caches, fresh clients and Bazel output bases:

| Experiment | Shallow checkout | VFS |
|---|---:|---:|
| Direct GitHub, background-prefetch query (Oct 16, five pairs) | 8.384 s | 4.964 s |
| Warm local proxy, BLAKE3-xattr query (Oct 19, three trials) | 3.277 s | 2.896 s |
| Warm proxy/CAS, first narrow RBE build (Oct 19, three trials) | 24.969 s | 24.316 s |

These are clone-to-query/build medians under different cache conditions.
The RBE difference is only 2.62%, with overlapping ranges; see
[agent-startup results](docs/proxy-agent-startup.md) for phases and caveats.
Proxy experiments exclude mirror population/server prewarming. In the query
experiment, xattrs saved only 27 ms versus normal VFS hashing, not a reliable win.
Full-snapshot BLAKE3 mapping creation took 12.481 s; warm lookup took 86 ms.
See [direct-GitHub results](docs/github-bazel-async-prefetch.md),
[BLAKE3 results and caveats](docs/proxy-blake3-benchmarks.md), and
[benchmark reproduction](tools/README.md).

For repeated edits, see the [VFS iteration results](docs/vfs-iteration-benchmarks.md),
including reclamation, rename/digest work and real incremental RBE builds.

## Guides and limits

- [Cloud Bazel agent example skill](examples/skills/cloud-bazel-agent/SKILL.md)
- [VFS guide](crates/workspace-fuse/README.md) / [full reference](docs/vfs-reference.md)
- [Proxy guide](crates/git-proxy/README.md) / [full reference](docs/proxy-reference.md)
- [Proxy design and research](docs/proxy-design.md)
- [VFS lifecycle design and measurements](docs/design/vfs-iteration-lifecycle.md)

This is a source-tree prototype: SHA1 repositories, fixed base commit per mount,
no general checkout/merge/rebase guarantee. Basic Git commit/log work, but a first
commit can hydrate the entire indexed snapshot. The overlay contains uncommitted
and staged work: **do not delete it as a cache**.

For background workspace publication, see [CAS sync](docs/proxy-cas-sync.md).
It complements r2c2 external-repository caching; neither a digest mapping nor a
queued upload proves that a remote CAS holds the content.

## Development

Bazel uses pinned Rust/LLVM toolchains and the same Cargo lockfile:

```sh
bazel build //:workspace-fuse //:git-proxy
bazel test --config=rbe //...
```

Put BuildBuddy credentials in the **gitignored** `user.bazelrc`.
See [build setup and crate boundaries](docs/development.md) and
[local/RBE build measurements](docs/developer-build-benchmarks.md), plus
[next EdenFS lessons](docs/design/edenfs-next-steps.md).
