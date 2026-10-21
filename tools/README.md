# Benchmark tools

Run from the repository root with frozen release binaries and a **fresh output
directory**. Most scripts default to dry runs; add `--run` after checking the plan.
Use `--help` for options and [the measurement checklist](../docs/benchmark-reference.md)
for cache boundaries, provenance, output correctness and cleanup.

## Choose a workload

| Question | Harness |
| --- | --- |
| Direct GitHub clone → query/build | `benchmark_github_bazel.py` |
| SHA1 → BLAKE3 generation, warm lookup, restart | `benchmark_proxy_mapping.py` |
| Warm proxy: checkout vs VFS vs digest xattrs | `benchmark_warm_proxy_bazel.py` |
| Fresh agent → first BuildBuddy RBE build | `benchmark_proxy_agent_rbe.py` |
| Repeated source edit/status/RBE build | `benchmark_iterative_agent_rbe.py` |
| Private agents sharing a proxy | `benchmark_concurrent_agents.py` (smoke or opt-in paired performance) |
| Direct remote cache vs shared-daemon cache | `benchmark_shared_daemon.py` |
| BES durable ACK throughput/latency | `benchmark_bes_group_commit.py` (runs immediately; local fake upstream) |
| Local VFS subsystem costs | `benchmark_vfs_{iteration,ranges,journal,metadata,hydration,content}.py` |
| This project's local/RBE Rust build iteration | `benchmark_developer_build.py` |

`benchmark_concurrent_agents.py --performance --agent-counts 1 2 4 8 --trials 3`
enables paired warm-proxy checkout/VFS cohorts; default remains a two-agent
correctness smoke. Pilot one agent/one trial first. The full matrix retains
private outputs and executes many remote actions; it does not include upstream
mirror population. Save measures native Git commit, including possible hydration.
Select the provisioned credential class before running: `--credential-config rbe`
reads only `common:rbe` headers from `--credential-rc`; the default is
`remote-shared`. Neither mode probes another credential class on failure.

For trusted-host local cache sharing, add `--local-repo-cache-policy compare
--performance-arms vfs-blake3-xattr`. Remote r2c2 remains on in both measured
policies. A separately timed remote-r2c2-off seed populates eligible local
repositories; remote hits alone do not populate that directory cache. Add
`--probe-empty-shared-cache` to observe that distinction before priming.

For path-cardinality startup, use `benchmark_vfs_metadata.py --workloads startup
--entry-counts 100000 1000000`. Its three-blob fixture isolates namespace costs,
not a million distinct mappings. `GIT_TOOLS_METADATA_TIMINGS=1` enables phase JSON
on stderr without changing normal output or storage formats.

## Common commands

Copy [the example JSON](benchmark_github_bazel.example.json); set actual Git,
Bazel, VFS binary, cache and output-root paths. Verify dependency/toolchain
readiness separately. The historical pinned workload uses Bazel 9.2.0.

```sh
python3 tools/benchmark_github_bazel.py \
  --config CONFIG.json --output FRESH_OUTPUT --native-probe
```

Use `prefetch_mode: after-mount` for async prefetch. Add `--workflow build-first
--warm-builds 3` plus a target/nonempty output manifest for true first-build timing.

Proxy experiments require a **closed, self-contained full mirror**:

```sh
python3 tools/benchmark_proxy_mapping.py --output FRESH_OUTPUT \
  --proxy-binary target/release/git-proxy --git /usr/bin/git \
  --mirror CLOSED_CACHE/bazel.git --source-revision VERIFIED_SOURCE_SHA --trials 3

python3 tools/benchmark_warm_proxy_bazel.py --config CONFIG.json \
  --output FRESH_OUTPUT --mirror CLOSED_CACHE/bazel.git --trials 3 --build-proof

python3 tools/benchmark_proxy_agent_rbe.py --config CONFIG.json \
  --output FRESH_OUTPUT --mirror CLOSED_CACHE/bazel.git \
  --credential-rc EXISTING_PRIVATE_RC --trials 3 --proxy-cas-sync
```

Warm-proxy arms exclude mirror population/server preparation. RBE requires a
provisioned compatible toolchain; each cell must execute the intended uncached
actions and compare exact outputs. Repeated-edit harnesses additionally require
verified baseline/candidate binaries and commits. Run the two-agent smoke after,
not during, serial timing matrices.

Local CAS component measurement:

```sh
bazel run --config=rbe //crates/local-cas:cache_benchmark -- /tmp/fresh-cas-bench candidate
```

For the shared daemon, supply a private plan JSON with `cwd`, `source_revision`
and a Bazel `command` argv array. Placeholders are `{output_base}`,
`{output_user_root}`, `{artifacts}` and `{endpoint}`. Run the same plan against
the upstream endpoint and `unix:/absolute/state/cache.sock`, with fresh output
directories. Keep credentials in a helper/private rc, never the command array.
`--noop` measures a separate same-server incremental build; it is not another
remote-cache sample. Record mirror population and daemon cache priming separately.
This endpoint comparison isolates the proxy, not source VFS or BOS benefits.
Match actual action-hit/execution counts, not just flags. Check build-event
upload warnings even when Bazel exits successfully.

Do not run matrices alongside compilation/profiling. Keep failures/outliers and
raw receipts; never label logical blob sizes as network bytes or no-op cache
hits as executed remote work. See [historical results and tradeoffs](../docs/benchmarks.md).

BES group commit (component only, no production backend): build
`//crates/bes-proxy:integration_tests` with `--config=rbe`, then run
`python3 tools/benchmark_bes_group_commit.py --binary
bazel-bin/crates/bes-proxy/integration_tests --output FRESH_OUTPUT`. Use a
disk-backed output path: it also holds the measured spool, not `/tmp` tmpfs.
The harness compares per-event, ready-event and 1 ms coalesced commits; reports
local admission/ACK latency, separate upstream drain, CPU and peak RSS.
