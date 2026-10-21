# Build and test

Bazelisk selects the pinned Bazel/Rust/LLVM toolchains and Cargo dependencies.
The Bazel execution target is Linux x86-64.

```sh
bazel build --config=rbe //:workspace
bazel test --config=rbe //...
```

Ordinary unit and loopback integration suites run remotely with declared fixture
tools. Cargo is also supported: `cargo test --locked --workspace`.

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
  //crates/output-fuse:mounted_tests --test_output=all
```

Compilation can still run remotely. Follow the [platform guides](platforms.md)
for NFS/ProjFS; cross-compilation and protocol tests do not establish native
acceptance. Keep development plans, benchmark records and private evidence in
ignored `dev-notes/`, not user documentation.
