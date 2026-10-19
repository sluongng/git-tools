# Building git-tools

Use Bazelisk or Cargo; the repository pins both Bazel and Rust toolchains.
Linux x86-64 is the initial Bazel target. Native Git and `/dev/fuse` are runtime
requirements, not dependencies of every library.

```sh
bazel build //:workspace-fuse //:git-proxy
bazel test //...
```

The root aliases build executable targets; their library counterparts live in
`//crates/workspace-fuse:workspace-fuse` and `//crates/git-proxy:git-proxy`.
Cargo remains supported: `cargo test --locked --workspace`.

Tests requiring native Git run on the host; their Rust compilation remains
eligible for remote execution. Use Git with `cat-file remote-object-info`
support on `PATH`. The FUSE cases are opt-in:

```sh
bazel test //crates/workspace-fuse:integration_tests \
  --test_arg=--ignored --test_arg=--test-threads=1
```

Host scratch defaults to `/tmp/git-tools-bazel-tests`, overridable with
`--test_tmpdir`. An existing scheduler fixture has a marker-ordering race that
reproduces frequently on Btrfs and can still occur on tmpfs. This build migration
leaves that unrelated fixture unchanged; see the build results for failed runs.

[Measured build results and validation caveats](developer-build-benchmarks.md).

## BuildBuddy remote execution

Create `user.bazelrc` locally; it is ignored by Git. Configure the credential
under `build:rbe --remote_header=x-buildbuddy-api-key=YOUR_KEY`, or use a
credential helper. Override the remote/BES endpoints there when using a custom
BuildBuddy deployment. Do not enable `--announce_rc` with a key-bearing rc.

```sh
chmod 600 user.bazelrc
git check-ignore user.bazelrc
bazel build --config=rbe //:workspace-fuse //:git-proxy
bazel test --config=rbe //...
```

Rust and LLVM are pinned for both local and remote actions. The REAPI bindings
are generated from checked-in protocol sources with a vendored protoc.
The patched local `third_party/fuser` is used by both build systems.
The remote execution platform pins a standard BuildBuddy Ubuntu image by digest.

## Crate boundaries

| Crate | Responsibility | Local dependencies |
|---|---|---|
| `workspace-core` | Git object access, immutable trees, metadata, prefetch and shared Fjall registry | None |
| `workspace-overlay` | Persistent writable namespace, journal, payloads and native Git worktree | `workspace-core` |
| `workspace-fuse` | FUSE adapters and CLI | `workspace-core`, `workspace-overlay` |
| `git-pack-cache` | Smart-HTTP keys, framing and bounded generated responses | None |
| `reapi` | Generated REAPI/ByteStream bindings | None |
| `git-proxy` | HTTP server, native Git orchestration and CAS transport/sync | `git-pack-cache`, `reapi` |
| `workspace-nfs` | Loopback writable NFSv3 frontend | `workspace-core`, `workspace-overlay` |
| `workspace-projfs` | Windows ProjFS provider and native Git admin | `workspace-core` |
| `jj-vfs` | Custom Jujutsu CLI/working-copy backend | `workspace-overlay` |
| `bazel-output-service` | Bazel output-tree lifecycle and operator-selected CAS | `reapi`, `output-core`, native adapter |
| `output-core` | Durable output generations, local mutations and bounded verified CAS cache | Fjall |
| `output-nfs` / `output-projfs` | Native lazy output publication, reads and mutations | `output-core` |

Each frontend has a root executable alias and crate-local verification instructions.
See [platform contracts and acceptance](design/platform-frontends.md); compiling
a platform target does not establish native runtime support. The pinned NFS fork
and its local changes live in `third_party/nfsserve`.

The lower VFS crates do not depend on FUSE. `workspace-fuse` reexports shared
backend and overlay APIs; the proxy reexports its protocol/config APIs. All workspace
metadata owners share one Fjall registry, rather than opening the same database
independently. Protocol generation and pack parsing do not rebuild merely
because the proxy's routing or CAS orchestration changes.

## Measuring iteration

`tools/benchmark_developer_build.py --help` describes separate population,
clean-local reuse, local no-op, frontend-edit and lower-layer-edit phases.
It defaults to a dry run. Keep failed attempts and distinguish actual remote
actions from cache hits; a fast no-op is not evidence of remote-cache speed.
Run benchmarks without concurrent compilation. The default edit adds a public
constant, changing Rust metadata without changing CLI behavior. This exercises
dependency rebuild boundaries, not application throughput. Optional comment-only
edits can produce unchanged dependency outputs and cut off downstream work.
