# Build and test

Bazelisk selects pinned Bazel/Rust/LLVM toolchains and Cargo dependencies.
Bazel's execution target is Linux x86-64.

```sh
bazel build --config=rbe //:workspace
bazel test --config=rbe //...
```

Ordinary unit and loopback integration suites run on RBE with declared Git/Python
fixture tools, not inherited host PATH. Cargo remains supported:
`cargo test --locked --workspace`.

## BuildBuddy credentials

Create private, **gitignored** `user.bazelrc`. Prefer a host-scoped credential
helper, or put `build:rbe --remote_header=x-buildbuddy-api-key=YOUR_KEY` there.
Override remote/BES endpoints for your deployment. Do not enable `--announce_rc`
with a key-bearing rc.

```sh
chmod 600 user.bazelrc
git check-ignore user.bazelrc
```

## Native mount acceptance

Only actual kernel/provider tests need a local machine. Their compilation remains
eligible for RBE; explicitly selected test actions run locally:

```sh
bazel test --config=rbe \
  //crates/workspace-fuse:mounted_tests \
  //crates/jj-vfs:mounted_tests \
  //crates/output-fuse:mounted_tests --test_output=all
```

Linux needs `/dev/fuse` and `fusermount3`. For NFS/ProjFS use each
[platform guide](platforms.md)'s native instructions. Cross-compilation and portable
protocol tests are not native acceptance.

## Further work

See [architecture/cache ownership](architecture.md) and [benchmark tools](../tools/README.md).
Keep active development plans, unresolved follow-ups and evidence pointers in
**gitignored `dev-notes/`**, pruning completed/superseded tracking. Stable user
instructions belong in tracked docs. Private reports and credentials stay outside Git.
