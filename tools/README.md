# Developer tools

Benchmark and diagnostic scripts are optional developer utilities, not the user
entry point. Start with the [project README](../README.md) and use each script's
`--help` for its configuration. Most benchmarks default to a dry run; check the
plan before `--run`. Keep site configuration, credentials and raw results outside Git.

| Workload | Script |
| --- | --- |
| GitHub clone → query/build | `benchmark_github_bazel.py` |
| SHA1 → BLAKE3 mappings | `benchmark_proxy_mapping.py` |
| Warm proxy and first RBE build | `benchmark_warm_proxy_bazel.py`, `benchmark_proxy_agent_rbe.py` |
| Repeated edits or multiple agents | `benchmark_iterative_agent_rbe.py`, `benchmark_concurrent_agents.py` |
| Shared daemon cache | `benchmark_shared_daemon.py` |
| Durable BES ACKs | `benchmark_bes_group_commit.py` (runs immediately against a local fake upstream) |
| VFS metadata/content/storage | `benchmark_vfs_*.py` |
| This project's build iteration | `benchmark_developer_build.py` |

Use fresh output directories and matched workload/cache settings; account for
mirror population and preparation separately. Never put credentials in command
arrays or URLs. The [example config](benchmark_github_bazel.example.json) contains
only portable inputs. Build and test instructions are in
[development](../docs/development.md).
