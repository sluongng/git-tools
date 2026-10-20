# git-tools

Start coding on Git repositories without checking out every file. Experimental
Rust tools for private, writable agent workspaces and Bazel remote execution.

| Tool | What it does |
| --- | --- |
| [`agent-session`](crates/agent-session/README.md) | Create, resume, build, save and stop private Linux agent sessions |
| [`git-vfs`](crates/workspace-fuse/README.md) | Linux FUSE workspace: lazy Git reads, persistent edits, native Git status |
| [`git-proxy`](crates/git-proxy/README.md) | Shared Git mirror and fetch cache, object sizes, BLAKE3 mappings, optional CAS uploads |
| [`git-output-service`](crates/bazel-output-service/README.md) | Lazy Bazel outputs backed by a BuildBuddy/REAPI CAS |

macOS uses [NFS](crates/workspace-nfs/README.md); Windows uses
[ProjFS](crates/workspace-projfs/README.md). [Jujutsu](crates/jj-vfs/README.md)
requires our custom `git-jj` CLI. Native acceptance and restrictions differ by
platform; see [platform support](docs/platforms.md).

## Quick start: Linux workspace

Requires native Git, `/dev/fuse`, `fusermount3`, and the pinned Rust toolchain.
Run from this repository:

```sh
cargo build --release -p workspace-fuse
target/release/git-vfs clone https://github.com/bazelbuild/bazel.git /tmp/agent.git --depth 1
mkdir -p /tmp/agent
target/release/git-vfs mount --repo /tmp/agent.git \
  --overlay /tmp/agent-state --prefetch-bazel /tmp/agent
```

The foreground process owns the mount; Ctrl-C unmounts. In another terminal:

```sh
git -C /tmp/agent status --short
cd /tmp/agent
bazel --output_user_root=/tmp/agent-bazel-user \
  --output_base=/tmp/agent-bazel-output query //...
```

GitHub size metadata loads before mount; likely Bazel inputs prefetch afterward.
Other contents load on demand. Remount the same clone and overlay to retain edits.
**The overlay contains your work, not disposable cache data.** Base commits are
fixed per mount. Basic Git commit/log work, but a first commit can hydrate the
whole indexed snapshot; general checkout/merge/rebase are not supported.

## Multiple agents and remote builds

Run one [proxy](crates/git-proxy/README.md), then give each agent a private clone,
overlay, mount and Bazel output base. The first proxy request creates a **full
all-refs mirror**; prewarm it rather than charging that cost to every agent.

Use the [cloud-agent example skill](examples/skills/cloud-bazel-agent/SKILL.md)
for BLAKE3 digest reuse, BuildBuddy RBE and remote repository-content caching
(r2c2). [CAS sync](docs/proxy-cas-sync.md) can publish workspace blobs in the
background. A mapping or queued upload is not proof of remote CAS residency.
Keep build outputs and caches outside the source mount.

[`agent-session`](crates/agent-session/README.md) wraps that lifecycle without
sharing mutable state or deleting edits on stop. RBE/r2c2 remain provisioned services.

## Learn more

- [VFS options, prefetch and storage](docs/vfs-reference.md)
- [Proxy API and cache behavior](docs/proxy-reference.md)
- [Bazel output-service setup and TOML](crates/bazel-output-service/README.md)
- [Timeouts, retries and presence TTL](docs/reapi-policy.md)
- [Architecture and cache ownership](docs/architecture.md)
- [Benchmarks and tradeoffs](docs/benchmarks.md) · [Reproduce measurements](tools/README.md)
- [Build and test with RBE](docs/development.md)

Measured direct-GitHub clone-to-query median: **4.964s VFS vs 8.384s shallow
checkout** in five paired trials. A warm-proxy narrow RBE build was **24.316s vs
24.969s**, with overlapping ranges: no reliable broad RBE speedup is established.
These are different pinned workloads/cache conditions, not general guarantees.
