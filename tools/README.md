# Benchmark tools

Run from the project root. Use a fresh output directory and independently
verified release binaries. Commands below are dry runs; add `--run` to execute.
Do not run matrices concurrently with builds, profilers, or other benchmarks.

## Direct GitHub: checkout versus VFS

Copy [the example config](benchmark_github_bazel.example.json), set actual Git,
Bazel, VFS binary, shared repository cache, and output-root paths, then verify a
Bazel/dependency pilot. The pinned workload requires Bazel 9.2.0.

```sh
python3 tools/benchmark_github_bazel.py \
  --config CONFIG.json --output FRESH_OUTPUT --native-probe
```

Default: five rotated pairs, fresh source/overlay/output bases, three warm
queries. `prefetch_mode` selects blocking `before-mount` or async `after-mount`.
For true clone-to-first-build, use `--workflow build-first --warm-builds 3` and
configure a build target plus nonempty output-path manifest.

## Proxy mapping generation and reuse

Supply a closed, full, self-contained bare mirror and optionally a matching
query-presence manifest from the VFS harness:

```sh
python3 tools/benchmark_proxy_mapping.py \
  --output FRESH_OUTPUT --proxy-binary target/release/git-proxy --git /usr/bin/git \
  --mirror CLOSED_CACHE/bazel.git --source-revision VERIFIED_BINARY_SOURCE_SHA \
  --subset-json QUERY_PRESENCE.json --trials 3 --metadata-baseline
```

Each trial measures cold mapping creation, three warm requests, and persisted
reuse after restart. The mirror is warm; mapping databases start empty.
Mirror population/copy and correctness checks are outside HTTP request timings.
The optional metadata baseline warms sizes/types before mapping creation.
Reported logical MiB/s includes extraction and persistence, not just hashing.

## Warm proxy + BLAKE3 Bazel

Use the same config, add an absolute `proxy_binary` path, and verify dependency
cache warmth before setting `pilot_dependencies_warm_verified`.

```sh
python3 tools/benchmark_warm_proxy_bazel.py \
  --config CONFIG.json --output FRESH_OUTPUT --mirror CLOSED_CACHE/bazel.git \
  --trials 3 --warm-queries 1 --build-proof
```

Rotates checkout, VFS normal hashing, and VFS digest-xattr arms, all with BLAKE3.
Both VFS arms import identical mappings and use blocking content prefetch.
Mirror population and server prewarming are excluded from fresh-client latency.
`--build-proof` checks matched outputs and a same-size edit/rebuild after queries.
Digest counters distinguish actual reuse from merely configuring BLAKE3.

## Fresh-agent BuildBuddy RBE

Use a config with a verified provisioned RBE toolchain and matching manifest:

```sh
python3 tools/benchmark_proxy_agent_rbe.py \
  --config CONFIG.json --output FRESH_OUTPUT --mirror CLOSED_CACHE/bazel.git \
  --credential-rc EXISTING_PRIVATE_RC --trials 3 --proxy-cas-sync
```

Rotates the same three arms with async prefetch and a build **before any query**.
Every cell has a fresh local r2c2 cache, verifies three uncached remote BLAKE3
C++ actions and compares output bytes. Proxy/mapping/CAS preparation is separate.
The rc selects one existing BuildBuddy credential; it is never copied into output.
Private RPC logs become sanitized status/count summaries, then are removed.
See [agent startup results](../docs/proxy-agent-startup.md) for scope and setup.

## Iterative workspaces

[Lifecycle results and protocol](../docs/vfs-iteration-benchmarks.md) cover the
`benchmark_vfs_{iteration,ranges,journal,metadata,hydration,content}.py` local
harnesses. Each takes frozen baseline/candidate binaries; use `--help` for its
workload and strict-counter flags. Three rotated trials are the default.

`benchmark_iterative_agent_rbe.py` uses the fresh-agent setup above and adds
`--baseline-vfs`, `--candidate-vfs`, their verified source commits, and `--cycles 8`.
Primary runs require at least three trials. `--functional-pilot` explicitly marks
short readiness checks as non-headline evidence.

## Evidence and interpretation

For this project's own local versus BuildBuddy Rust build matrix, use
`benchmark_developer_build.py --config rbe --output FRESH_OUTPUT --source-edit
crates/git-vfs/src/main.rs crates/git-workspace/src/backend.rs`. Add `--run`
after reviewing the dry run. It separates clean-output reuse, no-op and
metadata-changing edits; see [developer build setup](../docs/development.md).

Harnesses retain frozen binary hashes, raw commands/logs, TRACE2, object-presence
manifests, profiles, and checkpointed results on success or failure.
Do not call logical blob bytes network bytes. Keep failures/outliers; do not
extend timeouts, filter workloads, or retry cells to produce a passing matrix.
Warm archive caches do not prevent mutable external catalogue downloads.

See [the full protocol reference](../docs/benchmark-reference.md) for cache
boundaries, provenance, audit fields, no-op verification, and resource accounting.
Results: [GitHub query](../docs/github-bazel-async-prefetch.md),
[first build](../docs/github-bazel-performance.md),
[proxy/BLAKE3](../docs/proxy-blake3-benchmarks.md).
