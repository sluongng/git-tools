# git-tools

Start coding on Git repositories without checking out every file. Experimental
Rust tools for private, writable agent workspaces and Bazel remote execution.

| Tool | What it does |
| --- | --- |
| [`workspace`](crates/workspace-daemon/README.md) | One entry point: private sessions, shared Git/REAPI/BES proxies, lazy mounts and bounded local CAS |
| [`agent-session`](crates/agent-session/README.md) | Create, resume, build, save and stop private Linux agent sessions |
| [`git-vfs`](crates/workspace-fuse/README.md) | Linux FUSE workspace: lazy Git reads, persistent edits, native Git status |
| [`git-proxy`](crates/git-proxy/README.md) | Shared Git mirror and fetch cache, object sizes, BLAKE3 mappings, optional CAS uploads |
| [`git-output-service`](crates/bazel-output-service/README.md) | Lazy Bazel outputs backed by a BuildBuddy/REAPI CAS |

macOS uses [NFS](crates/workspace-nfs/README.md); Windows uses
[ProjFS](crates/workspace-projfs/README.md). [Jujutsu](crates/jj-vfs/README.md)
requires our custom `git-jj` CLI. Native acceptance and restrictions differ by
platform; see [platform support](docs/platforms.md).

## Quick start: shared Linux daemon

Requires native Git, `/dev/fuse`, `fusermount3`, compatible Bazel, and provisioned
BuildBuddy RBE credentials/toolchains. Build the single entry point:

```sh
bazel build --config=rbe //:workspace
export PATH="$PWD/bazel-bin/crates/workspace-daemon:$PATH"
workspace init
# Edit ~/.config/workspace/config.toml; see examples/workspace.toml.
# Provision BUILDBUDDY_API_KEY using your normal secret mechanism.
workspace create /tmp/agent-1 --repo bazel --revision FULL_COMMIT_SHA \
  --bazelrc /absolute/path/to/provisioned.bazelrc
workspace build /tmp/agent-1 -- //src:target
workspace status
```

`create` starts the daemon automatically. Each agent keeps its own
source edits, Git state and Bazel output base. Source/output payloads are lazy;
likely Bazel source inputs prefetch asynchronously after mounting.

```sh
git -C /tmp/agent-1/workspace status --short
workspace test /tmp/agent-1 -- //...
workspace stop /tmp/agent-1
```

Defaults: 4 GiB verified-byte LRU cache, eight fetches and one private `rbe.sock` for cache, execution, downloader, BES and BOS.
Linux BOS is opt-in (`[bos] enabled = true`) and strictly lazy when enabled;
its current metadata-publication overhead can outweigh download savings.
Action-result metadata has a fixed 60-second TTL; set it to zero without a
backend-retention assumption. **Edits and commits are not disposable cache data.** Native
commit can hydrate indexed files; general checkout/merge/rebase are not supported.
BES events are ACKed after durable local persistence, forwarded asynchronously
to BuildBuddy and deleted only after its ACK. Pending events are never LRU-evicted;
check `workspace status` and use `workspace retry-bes` after a forwarding failure.

Bazel must include the [Unix-socket authority fix](https://github.com/bazelbuild/bazel/commit/242563f1a14378801725e5f6b0ee397ef0d307f2)
as well as BLAKE3/xattr/r2c2 support. Bazel 9.3.0rc2 lacks this fix; select a
compatible binary with `session.bazel` in the TOML.

## Multiple agents and remote builds

Configure repository aliases and the BuildBuddy upstream in
[`workspace.toml`](examples/workspace.toml). One daemon hosts the Git proxy,
source/BOS mounts and local AC/CAS front end, sharing immutable bytes across
agents. The first repository request creates a **full all-refs mirror**; preparation
and native Git storage are separate from the CAS quota.

Use the [cloud-agent example skill](examples/skills/cloud-bazel-agent/SKILL.md)
for BLAKE3 digest reuse, BuildBuddy RBE and remote repository-content caching
(r2c2). [CAS sync](docs/proxy-cas-sync.md) can publish workspace blobs in the
background. A mapping or queued upload is not proof of remote CAS residency.
Keep build outputs and caches outside the source mount.

The standalone component tools remain available. Unified source-session lifecycle
is currently Linux-only; native NFS/ProjFS adapters have separate platform setup.
BuildBuddy scheduling/execution stays remote; local caching never proves that an
input has reached the remote CAS.

## Learn more

- [VFS options, prefetch and storage](docs/vfs-reference.md)
- [Proxy API and cache behavior](docs/proxy-reference.md)
- [Bazel output-service setup and TOML](crates/bazel-output-service/README.md)
- [Timeouts, retries and presence TTL](docs/reapi-policy.md)
- [Architecture and cache ownership](docs/architecture.md)
- [Benchmarks and tradeoffs](docs/benchmarks.md) · [Reproduce measurements](tools/README.md)
- [Build and test with RBE](docs/development.md)

The latest cache-endpoint comparison measured BuildBuddy CI `build //...` at
**93–98s direct / 86–92s through the daemon**. A narrow Bazel target was
**5.78s direct / 6.82s daemon-cold / 5.67s warm**. These are small samples with
prepared dependency caches, not a cold-clone or general RBE speedup claim.
See [scope, failures and historical checkout comparisons](docs/benchmarks.md).
