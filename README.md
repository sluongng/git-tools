# git-tools

Rust tools for using Git repositories without eagerly checking out every file.

| Project | Purpose |
|---|---|
| [`git-vfs`](crates/git-vfs/) | Writable Linux FUSE filesystem over a bare blobless clone. Files load on demand; a persistent copy-on-write overlay holds edits. |
| [`git-proxy`](crates/git-proxy/) | Optional read-only Git pull-through cache with batch object sizes, generated fetch-response reuse, and Git SHA1 → BLAKE3 digest lookup. |

Native Git handles object storage and transport. Fjall stores local metadata.
The VFS works directly with GitHub; **the proxy is not required**.

## Quick start: GitHub → writable mount

Requirements: a Rust toolchain supporting edition 2024, Cargo, native Git, Linux
with accessible `/dev/fuse`, and `fusermount3` (usually the `fuse3` package).
GitHub.com size metadata comes from its tree API, without fetching file contents.
Other remotes need a Git client supporting `cat-file remote-object-info` and a
server advertising protocol-v2 `object-info`; protocol v2 alone is not enough.
Use the global `--git /path/to/git` option to select a suitable Git build.

**Terminal 1 — build, clone, and mount:**

```sh
git clone https://github.com/sluongng/git-tools.git
cd git-tools
cargo build --release --workspace

# Clone/overlay paths must be fresh; the mount directory must be empty.
target/release/git-vfs clone https://github.com/bazelbuild/bazel.git \
  /tmp/bazel-vfs.git --depth 1
mkdir -p /tmp/bazel-vfs-mount
target/release/git-vfs mount --repo /tmp/bazel-vfs.git \
  --overlay /tmp/bazel-vfs-overlay --prefetch-bazel /tmp/bazel-vfs-mount
```

The clone is bare, depth-one, and `blob:none`: no checkout is created.
Mounting warms exact GitHub size metadata first, then starts FUSE. The optional
`--prefetch-bazel` worker fetches likely Bazel inputs and symlink blobs **after
mount**, overlapping user activity. Other files load when read. Omit that flag
for ordinary demand-only loading. The mount stays in the foreground; Ctrl-C
unmounts it. Remount the same clone/overlay to retain edits and the Git index.

**Terminal 2 — use normal filesystem and Git commands:**

```sh
git -C /tmp/bazel-vfs-mount status --short
head -n 10 /tmp/bazel-vfs-mount/README.md
printf 'local VFS edit\n' > /tmp/bazel-vfs-mount/vfs-example.txt
git -C /tmp/bazel-vfs-mount status --short

# Current-content digest: base files use metadata; edits hash local overlay bytes.
python3 - <<'PY'
import os
path = '/tmp/bazel-vfs-mount/vfs-example.txt'
print('gitsha1:', os.getxattr(path, 'user.gitsha1').hex())
print('size:', os.stat(path).st_size)
PY
```

`user.gitsha1` contains 20 raw bytes, not hexadecimal text. It is the Git blob
hash, `SHA1("blob " + size + NUL + contents)`, not plain SHA-1 of the contents.
`user.gitsha1.hex` is the text form. Neither xattr includes file size.

### Optional: Bazel query

Use a Bazel version supporting both digest startup flags: 8.7+ on the 8.x line
or 9.2+ on the 9.x line. Also respect the source repository's Bazel version
requirement; the benchmarks below used Bazel 9.2.0 and a pinned older snapshot.
From terminal 2:

```sh
cd /tmp/bazel-vfs-mount
bazel --nosystem_rc --nohome_rc \
  --output_user_root=/tmp/git-tools-bazel-user \
  --output_base=/tmp/git-tools-bazel-output \
  --digest_function=GITSHA1 \
  --unix_digest_hash_attribute_name=user.gitsha1 \
  query //... --watchfs --lockfile_mode=off \
  --repository_cache=/tmp/git-tools-bazel-repository-cache
```

Keep build outputs and caches **outside the mount**. These digest flags are
startup options, before `query`/`build`. A remote cache or executor must also
advertise REAPI `GITSHA1`; the xattr does not add server support. Bazel's file
watching uses normal filesystem notifications, while native `git status` uses
the VFS change journal through Git's fsmonitor hook.

## Measured performance versus shallow clone

**Starting with no clone, the VFS was faster in these workloads.** This mainly
saves source setup/transfer; it does not make Bazel evaluation itself faster.
Times below are medians of five paired, single-machine trials, from clone launch
to successful first command exit, including metadata, prefetch, and mount setup.

| Date / workload | Shallow checkout | VFS | Elapsed-time change |
|---|---:|---:|---:|
| Oct 16, 2026: default-policy `query //...`, background prefetch | 8.384 s | 4.964 s | **−40.80%** |
| Oct 15, 2026: target-tuned first build, blocking prefetch | 9.654 s | 6.034 s | **−37.49%** |

Both use GitHub directly, no proxy, depth-one clones, and Bazel 9.2.0 at commit
`c9af12d1db12d1b2df2137feb9ea8d39e3a25c79`. Each trial has fresh source,
overlay, and Bazel output base; shared Bazel installation/dependency/archive
caches are warm, and OS caches are not dropped. These are **different dated
experiments and workloads**, not rows from one interchangeable matrix. The
build target is `//src/main/cpp/util:strings`, with **21 explicit target-specific
prefetch includes**, not the default query policy or a general-build guarantee.

In the Oct 16 comparison, moving prefetch into the background reduced VFS
clone-to-query time from **5.192 s to 4.964 s (−4.39%)**. It won four of five
trials. Most prefetch latency shifted into query file-read waits rather than
vanishing; warm query runtimes were effectively the same as shallow checkout.
The default query fetched 696 unique blobs, not the full 11,850-blob snapshot.
Logical blob bytes are not measured network-transfer bytes.

See the [background-prefetch report](docs/github-bazel-async-prefetch.md) for
phase timings and caveats, the [original performance report](docs/github-bazel-performance.md)
for build tuning and fetched-file analysis, and the checked-in
[Oct 16 JSON](docs/benchmarks/github-bazel-async-prefetch-20261016.json) /
[Oct 15 JSON](docs/benchmarks/github-bazel-20261015.json) for exact results.
Historical source hashes in those reports identify pre-squash benchmark builds.
The [benchmark harness guide](tools/README.md) explains reproduction.

## Optional: shared Git proxy

In a separate terminal, from the `git-tools` directory:

```sh
target/release/git-proxy --cache-dir /tmp/git-tools-proxy-cache \
  --repo bazel=https://github.com/bazelbuild/bazel.git --enable-blake3
```

In another terminal, also from `git-tools`, create a separate client clone:

```sh
target/release/git-vfs clone http://127.0.0.1:8082/git/bazel.git \
  /tmp/bazel-proxy-vfs.git --depth 1
target/release/git-vfs warm-sizes --repo /tmp/bazel-proxy-vfs.git
mkdir -p /tmp/bazel-proxy-mount
target/release/git-vfs mount --repo /tmp/bazel-proxy-vfs.git \
  --prefetch-bazel /tmp/bazel-proxy-mount
```

The proxy's **first request populates a full all-refs bare mirror**, even for a
shallow/blobless client: first use can be substantially slower than direct GitHub.
Later clients share that mirror. Eligible complete fetches reuse a bounded
Packhorse-inspired generated-response cache; concurrent followers wait for the
completed response. Native `object-info` requires an appropriate server Git;
the proxy also exposes a batch JSON object-size API independently of that extension.

Optional base-snapshot digest lookup, from `git-tools` **after stopping the
proxy-client mount with Ctrl-C**. Standalone VFS commands cannot open the size
cache while the mount owns it:

```sh
target/release/git-vfs cas-digest --repo /tmp/bazel-proxy-vfs.git \
  --proxy-url http://127.0.0.1:8082 --proxy-repo bazel README.md
```

BLAKE3 hashes raw blob contents. This mapping neither uploads to a CAS nor proves
CAS residency, enables remote execution, or changes Bazel's digest mode. It
reflects the base snapshot, not overlay edits. Keep this unauthenticated prototype
on a trusted local network; see the [proxy guide](crates/git-proxy/README.md).

## Optional: Bazel BLAKE3 digest reuse

With an already populated proxy started with `--enable-blake3`, import its
mappings before mounting, then enable the digest attributes on the mount:

```sh
target/release/git-vfs warm-blake3 --repo /tmp/bazel-proxy-vfs.git \
  --proxy-url http://127.0.0.1:8082 --proxy-repo bazel
target/release/git-vfs mount --repo /tmp/bazel-proxy-vfs.git \
  --blake3-proxy-url http://127.0.0.1:8082 --blake3-proxy-repo bazel \
  --prefetch-bazel /tmp/bazel-proxy-mount
```

Run the earlier Bazel command with `--digest_function=BLAKE3` and
`--unix_digest_hash_attribute_name=user.blake3` instead of the GITSHA1 options.
Both are startup flags, before `query` or `build`. Base-file digests come from
local cached mappings without fetching content; edited/new files hash their
current overlay bytes. Mapping import is blocking metadata work, independent
of asynchronous content prefetch. Stop an existing mount before importing.
A remote cache/executor must separately support BLAKE3 and receive required
content; digest mappings alone do not populate its CAS.

### Warm-proxy measurements, October 19, 2026

For the same pinned Bazel snapshot, creating mappings for **11,850 unique blobs
(132.16 MiB raw content)** took **12.481 s** with a warm Git mirror and a fresh
mapping DB. Reusing them took **86 ms**, including after proxy restart. For the
query's 696 blobs, creation took **0.799 s** and warm lookup **8 ms**.

A separate three-trial, rotated comparison used fresh clients and Bazel output
bases against a warm local proxy, with BLAKE3 in every arm:

| Clone to first `query //...` | Median | Change vs checkout |
|---|---:|---:|
| Shallow checkout | 3.277 s | — |
| VFS, normal BLAKE3 hashing | 2.922 s | −10.82% |
| VFS, cached BLAKE3 xattrs | 2.896 s | −11.63% |

Both VFS arms imported identical mappings and used **blocking prefetch** to
isolate digest reuse. The xattr arm was only 27 ms (0.91%) faster than the other
VFS arm; this small experiment does **not** establish a reliable extra query
speedup from digest reuse. Query-only cells did make 617 cached-digest xattr
calls with zero misses. Both VFS arms still hydrated the same 696 blobs.

These are not direct-GitHub or cold-proxy timings. Full mirror population and
server prewarming are excluded. Go SDK catalogue downloads remained in query
time; one checkout trial took 9.284 s, with a 7.118 s download event. That run
is retained. See the [BLAKE3 benchmark report](docs/proxy-blake3-benchmarks.md)
for distributions, phases, correctness checks, and reproduction.

## Limits and further reading

Native `git status`, `add`, basic `commit`/`log`, and remount are validated, but
**a first native commit can fetch the entire indexed snapshot**. The base commit
is fixed at mount time; general checkout/merge/rebase compatibility is not
promised. This is a source-tree prototype, not a general-purpose filesystem.

- [VFS guide](crates/git-vfs/README.md): writable state, digests, prefetch scopes,
  GitHub authentication, fsmonitor/watchfs, and native Git compatibility.
- [Proxy guide](crates/git-proxy/README.md): APIs, refresh, cache admission/limits,
  and deployment boundaries.
- [Proxy design and research](docs/proxy-design.md): Canva Goblet, Cursor Origin,
  Datadog gitretriever, GitLab Packhorse, and protocol references.
