# tinytree-fixture · library and executable

Builds a deterministic native repository bundle without Git or a server. The
fixture contains a small Bazel C++ target and configurable unused assets for
lazy-read tests. It is a synthetic workload, not a Git import or production
startup benchmark.

```sh
bazel build --config=rbe //crates/tinytree-fixture:tinytree-fixture-bin
bazel-bin/crates/tinytree-fixture/tinytree-fixture-bin \
  --out /tmp/native-seed --files 64 --bytes-per-file 4096
```

The selected output must be new or empty. Existing contents are never removed
or overwritten. `manifest.json` records stable paths, modes, content IDs, lengths
and native object descriptors; payloads live under `objects/KIND/ID`. The JSON
report on stdout records seed duration and counts separately from identity.

Library callers use `build_fixture(FixtureConfig)` and can seed loopback tests
directly from `Fixture::objects()`. Generating/writing the bundle does not upload
objects, publish refs, configure RBE or establish durable server acceptance.
