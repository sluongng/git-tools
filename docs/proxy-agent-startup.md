# Fast cloud-agent startup with BuildBuddy

Use the [example agent skill](../examples/skills/cloud-bazel-agent/SKILL.md)
for isolated writable workspaces, and [CAS sync](proxy-cas-sync.md) for shared
workspace publication. Start the narrow build directly; do not run `query //...`
first when the task already supplies a target.

## Provision once, isolate per agent

Share a warm Git mirror, cached clone packs, SHA1→BLAKE3 mappings, and remote
CAS/r2c2. Each agent gets its own blobless clone, overlay, mount, Bazel output
base and local repository-contents cache. Content prefetch runs after mount;
mapping import remains blocking metadata work. Prewarm shared CAS before an
agent burst so publication does not compete with every first build.

Provision SDKs and a repository-compatible RBE toolchain in the agent image.
For the pinned Bazel workload, Ubuntu RBE config generation is not reproducible
and therefore is not remotely cached by r2c2. Its warm diagnostic spent 21.7 s
inside repository fetching/generation; this is a nested profile duration, not
an independently measured wall-time saving. We extracted its generated config
once, recorded every file/hash/mode, and passed the same immutable repository
override to every benchmark arm. Do not mark a host-dependent rule reproducible
just to make it cacheable.

## Measurement

Three rotated trials per arm, October 19, 2026:

| Fresh agent | Clone → first RBE build median | Range | Workspace blobs loaded |
|---|---:|---:|---:|
| Shallow checkout | 24.969 s | 24.664–25.109 s | 11,850 |
| VFS, normal hashing | 24.806 s | 24.178–54.016 s | 699 |
| VFS, cached BLAKE3 xattrs | 24.316 s | 23.907–24.832 s | 696 |

Mapped VFS was **0.654 s / 2.62% lower** than shallow checkout by median.
The ranges overlap and N=3 is small; this is not a reliable broad speedup claim.
The 54.016 s trial is retained: archive/link metrics each show about 29.25 s of
remote executor setup, overlapping rather than additive.
The comparison measures one pinned C++ target, not a complete Bazel build or
an arbitrary coding-agent task. All 27 remote actions executed uncached and
all output bytes matched. Every cell recorded 34 successful r2c2 lookup RPCs
across 25 repository IDs, plus 12 NOT_FOUND lookups.

| Median phase | Shallow checkout | Normal VFS | Mapped VFS |
|---|---:|---:|---:|
| Clone | 0.755 s | 0.063 s | 0.063 s |
| Mapping/size import | — | 0.169 s | 0.168 s |
| Mount readiness | — | 0.152 s | 0.147 s |
| First Bazel build | 24.213 s | 24.415 s | 23.928 s |

Medians of separate phases need not sum to the median total. Mapped VFS loaded
3,495,118 logical workspace bytes versus 138,576,774 for checkout; these are
uncompressed object bytes, not measured network traffic. Its 47 base digest hits
had zero misses. The three C++ blobs absent locally were `exit_code.h`,
`strings.cc` and `strings.h` under `src/main/cpp/util/`: RBE could use their
already-published CAS contents.

Server preparation took 24.410 s from an existing full mirror, including 12.492 s
of cold local mapping creation and confirmation of all 11,850 already-present
CAS blobs (zero new uploads). This excludes original GitHub mirror population.
A separate partially-cold CAS functional pilot published 11,651 blobs / 127,062,367
logical bytes; its entire server preparation took 59.213 s, with publication
overlapping mapping/pack preparation. That is not an isolated upload benchmark.
The live CDC test independently published two missing blobs / 8,021,601 bytes,
including a six-chunk SpliceBlob, and verified TTL reuse and forced revalidation.

See [compact machine-readable evidence](benchmarks/2026-10-19-agent-rbe.json)
for individual trials, binary/harness hashes, output hashes and toolchain
manifest. Raw local evidence is retained under
`artifacts/proxy-agent-rbe-primary-20261019/`.

Reproduce with [the RBE harness](../tools/README.md#fresh-agent-buildbuddy-rbe).
Its continuous timer includes clone, mapping import, mount readiness and the
first successful build. Server preparation and image provisioning are separate.
Warm server state is common to shallow checkout and both VFS arms; this does
not isolate the marginal benefit of CAS prewarming or toolchain provisioning.
No OS page caches are dropped. Retain failures and individual trial values.

The workload uses Bazel 9.2, GitHub Bazel revision
`c9af12d1db12d1b2df2137feb9ea8d39e3a25c79`, target
`//src/main/cpp/util:strings`, BLAKE3, BuildBuddy RBE and r2c2.
Three C++ actions are deliberately executed remotely in every cell; r2c2 reads
remain enabled. Output hashes must match the shallow-checkout reference.

## Remaining cost

A diagnostic confirmed 34 successful `GetActionResult` RPCs across 25 repository IDs
and 3,316 successful ByteStream reads (24.5 MB read), with a fresh local r2c2
cache. Remote repository caching still downloads and materializes files;
a flag alone does not establish a hit. Local SDK/configuration rules still run.
The dominant next investigation is dependency materialization and Bazel startup,
not additional main-workspace hashing for this small target.

## Two-agent correctness smoke

After the serial matrix, two agents shared one prepared proxy while using
private clones, overlays, mounts, output-user roots, output bases and local
repository-contents caches. Both completed three uncached remote BLAKE3 actions
and matched the serial checkout outputs. Different bytes written to the same
workspace-relative marker path remained private before and after both builds.
This establishes two-agent writable-state isolation, not throughput or a
multi-tenant security boundary. Raw evidence:
`artifacts/proxy-agent-rbe-concurrent-20261019/`.

Run `tools/benchmark_concurrent_agents.py` with the same config/mirror/credential
arguments and `--reference-results PRIMARY/results.json --proxy-cas-sync --run`.
This smoke is separate from, and must run after, the serial timing matrix.
