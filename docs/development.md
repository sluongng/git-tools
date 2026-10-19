# Building git-tools

Use Bazelisk (the repository pins Bazel) or Cargo with Rust edition 2024 support.
Linux x86-64 is the initial Bazel target. Native Git and `/dev/fuse` are runtime
requirements, not dependencies of every library.

```sh
bazel build //:git-vfs //:git-proxy
bazel test //...
```

The root aliases build executable targets; their library counterparts live in
`//crates/git-vfs:git-vfs` and `//crates/git-proxy:git-proxy`.
Cargo remains supported: `cargo test --locked --workspace`.

Tests requiring native Git run on the host; their Rust compilation remains
eligible for remote execution. Use Git with `cat-file remote-object-info`
support on `PATH`. The FUSE cases are opt-in:

```sh
bazel test //crates/git-vfs:integration_tests \
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
bazel build --config=rbe //:git-vfs //:git-proxy
bazel test --config=rbe //...
```

Rust and LLVM are pinned for both local and remote actions. The REAPI bindings
are generated from checked-in protocol sources with a vendored protoc.
The patched local `third_party/fuser` is used by both build systems.
The remote execution platform pins a standard BuildBuddy Ubuntu image by digest.

## Crate boundaries

| Crate | Responsibility | Local dependencies |
|---|---|---|
| `git-workspace` | Git object access, immutable trees, metadata, prefetch and shared Fjall registry | None |
| `git-overlay` | Persistent writable namespace, journal, payloads and native Git worktree | `git-workspace` |
| `git-vfs` | FUSE adapters and CLI | `git-workspace`, `git-overlay` |
| `git-pack-cache` | Smart-HTTP keys, framing and bounded generated responses | None |
| `git-reapi` | Generated REAPI/ByteStream bindings | None |
| `git-proxy` | HTTP server, native Git orchestration and CAS transport/sync | `git-pack-cache`, `git-reapi` |

The lower VFS crates do not depend on FUSE. Existing `git_vfs` module paths and
proxy protocol/config exports are reexported for compatibility. All workspace
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
