# Measurement checklist

[Tools and commands](../tools/README.md) · [Historical results](benchmarks.md)

## Before running

- Freeze source revisions, release-binary hashes, Git/Bazel versions, workload,
  flags, output manifests and scopes. Use a separate pilot for dependency/toolchain
  readiness; do not tune the workload after observing timed results.
- Use fresh output directories, clones, overlays and Bazel output bases per cell.
  Share only explicitly documented server/dependency caches. Do not run competing
  builds, profilers or matrices. Record whether OS pages are retained.
- Separate direct GitHub, cold proxy population and warm proxy/fresh client.
  A full mirror and precomputed mappings/CAS blobs are substantial preparation.
- Review the harness dry run. Preserve raw commands, timings, statuses and partial
  results on success **and failure**; do not retry/filter cells to improve medians.

## Time the user outcome

Continuous clone-to-first-command starts at clone launch and ends at successful
query/build exit. Include metadata import, blocking prefetch, mount and handovers.
Do not subtract overlapping background prefetch. Report first-use output latency
as well as lazy build completion. Independent phase medians are not additive.

A build after query is not clone-to-first-build. Use `--workflow build-first`,
a target and a nonempty output manifest for the latter. Query validates package
loading; it may never request digests. Prove action-input reuse with actual build
counters and compare identical semantic outputs across checkout/VFS arms.

A no-op requires unchanged output hashes and a complete recognized action summary
with zero executed **content** actions. Workspace-status bookkeeping can still
run. Missing or malformed BEP metrics are not proof of a no-op.

For RBE, count executed remote actions separately from cache hits. Do not disable
all remote cache reads merely to force execution: that can also disable r2c2.
Use targeted no-remote-cache execution requirements and provision an identical
compatible RBE toolchain for all arms. Record image/config preparation separately.

## Audit bytes, state and cleanup

- Audit both base clone and private overlay Git admin, including alternates.
  `GIT_NO_LAZY_FETCH=1` prevents the audit hydrating missing objects. Legacy
  `present_*` fields may be base-only; use `union_present_*` / `private_only_*`
  when available. Native commit can hydrate private-only snapshot objects.
- Logical Git blob sizes, decoded CAS bytes and compressed application payloads
  are different from encrypted network traffic. Name the quantity actually measured.
- Count outputs/caches separately from source/overlay storage. Allocated bytes,
  shared extents, logical payload quotas and RSS are not interchangeable.
  Report parent/child process scope for CPU/RSS measurements.
- Retain fetched path/OID/reason manifests. Symlink targets and repository-loading
  patches/configs are not Starlark but can be justified; speculation is not use.
- Verify owned Bazel servers, mounts and child process groups shut down. A failed
  cleanup remains a failed acceptance check even when outputs match.

## Credentials and evidence

Never enable HTTP header tracing or put keys in commands/config snapshots. Use
selected environment variable names or a host-scoped credential helper. Raw RPC
logs may contain credentials: keep them private, extract allowlisted counts/status
summaries, and remove originals through the harness's cleanup path.

Public summaries may retain revisions, hashes, samples and caveats; development
campaign tracking belongs in ignored `dev-notes/`, not the user guide. A later
passing rerun does not erase an earlier failure. Label component microbenchmarks,
functional pilots and end-to-end primary matrices distinctly.
