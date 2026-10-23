# Build and test

Bazelisk selects the pinned Bazel/Rust/LLVM toolchains. `Cargo.toml` and
`Cargo.lock` supply dependency metadata to `rules_rs`; native Cargo builds are
not supported because local dependency patches are applied by Bzlmod.
The `rbe` configuration executes on Linux x86-64. Native macOS and Windows
builds use the platform selections in [platforms.md](platforms.md); do not
combine those native test commands with `--config=rbe`.

```sh
bazel build --config=rbe //:workspace
bazel test --config=rbe //...
```

Ordinary unit and loopback integration suites run remotely with declared fixture
tools. Standalone executables have root labels such as `//:git-proxy` and
`//:bazel-output-service`.

## Credentials

Keep endpoint overrides and credentials in **gitignored** `user.bazelrc`.
Prefer a host-scoped credential helper; never commit keys or enable
`--announce_rc` with a key-bearing rc. Restrict the file with `chmod 600` and
confirm `git check-ignore user.bazelrc` before use.

## Native mounts

Kernel/provider acceptance needs the actual platform. On Linux, install
`fusermount3` and provide `/dev/fuse`, then select the local acceptance targets:

```sh
bazel test --config=rbe \
  //crates/workspace-fuse:mounted_tests \
  //crates/jj-vfs:mounted_tests \
  //crates/jj-native-cli:mounted_tests \
  //crates/native-vfs:mounted_tests \
  //crates/output-fuse:mounted_tests --test_output=all
```

Compilation can still run remotely. Native macOS/Windows adapters require a
binary built for that target and native execution for provider tests. Follow the [platform guides](platforms.md) for NFS/ProjFS;
cross-compilation and protocol tests do not establish native acceptance.
Keep development plans, benchmark records and private evidence in ignored
`dev-notes/`, not user documentation.
