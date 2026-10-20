# Bazel Remote Output Service

A trusted, single-user loopback BOS v1 server for Bazel 9.2 and a configured
BuildBuddy / REAPI CAS. This manages **build outputs**, not the Git source VFS.

```sh
# Populate this variable from your secret store, not a command-line key.
export BUILDBUDDY_API_KEY
# Copy the example to a private location; edit its endpoint and paths.
cargo run -p bazel-output-service -- --config /path/to/bazel-output-service.toml

bazel --digest_function=BLAKE3 build //:target \
  --remote_cache=grpcs://YOUR_INSTANCE.buildbuddy.io \
  --experimental_remote_output_service=grpc://127.0.0.1:8377 \
  --experimental_remote_output_service_output_path_prefix=/path/to/outputs
```

The Bazel output path prefix must match the resolved TOML `root`.
Configure Bazel's host-scoped credential helper separately. SHA256 also works.
Set `--instance-name` to match a nonempty Bazel `--remote_instance_name`.
Use the complete [TOML example](../../examples/bazel-output-service.toml) for
paths, endpoint, cache limits and [REAPI policy](../../docs/reapi-policy.md).
CLI options override TOML; omitted values use built-in defaults.
Relative TOML paths resolve beside the file; CLI paths remain relative to the
working directory. No file is loaded automatically, and no environment or `~`
expansion occurs. Only the credential environment variable's **name** belongs
in TOML, never its value. Unknown fields, malformed TOML and files over 64 KiB
are rejected without echoing their contents.
Existing `[reapi]`-only policy files still work with endpoint/path CLI options.

## Storage

- `--storage lazy` (default): Linux FUSE, macOS NFS or Windows ProjFS. Staging,
  directory listing and untouched-file stat publish/read metadata only. First
  content access asynchronously fetches and verifies a whole blob; concurrent
  readers share that fetch. `--storage fuse`, `nfs` and `projfs` select explicitly.
- Lazy storage requires `--state-dir` outside `--root`, and an empty mountpoint.
  Linux needs `/dev/fuse` and `fusermount3`; macOS NFS needs no sudo; Windows
  needs ProjFS enabled. Keep the daemon running while accessing outputs.
  Restart by unmount/remount, not takeover. There is no silent eager fallback.
- Eager storage is **debug-only**: `--debug-eager` streams and verifies every
  staged blob before publication to an ordinary directory, with a startup warning.
  It cannot be selected via `--storage`, TOML, or alongside explicit `--storage`.

Bazel currently supplies flattened output paths and digests, including tree
children; BOS creates their parent directories without another CAS download.
It does not recursively prefetch file payloads. Each new build still checks
retained remote objects before acknowledging `StartBuild`, as the protocol requires.

Lazy state includes authoritative edits: **do not delete it as a cache**.
The shared [`local-cas`](../local-cas/) crate keeps only verified immutable bytes
and evicts least-recently-used, unpinned entries. Active read leases and in-flight
downloads are protected. It defaults to `--state-dir/cache`; use `--cas-cache-dir`
to separate disposable cache bytes from authoritative state. One process owns a
cache directory; library consumers share its `Arc<LocalCas>`, not independent
writers/GC. Batched recency hints survive graceful restart; retained bytes are
still independently reverified. A crash may lose recent access hints.

Backend identity now includes a credential fingerprint. Old namespace state or
a changed endpoint/instance/API key fails closed: use a fresh state directory,
and preserve/export any local edits from the old state rather than deleting it.

The verified cache defaults to 4 GiB. Metadata can describe larger blobs without
fetching them; a content read must fit both the cache and the 256 MiB in-flight
budget. Adjust `--max-cache-bytes`, `--max-inflight-bytes` and
`--max-concurrent-fetches` for the workload. Metadata staging does not prove CAS
residency; each build checks presence and requests conservative revalidation.
Both eager and lazy reads negotiate ZSTD with CAS capabilities; legacy or
identity-only servers keep working. Digests and limits describe decoded bytes.
Streaming decoding is bounded, checks complete frames and yields for deadlines.
The decoder accepts concatenated frames with windows up to **8 MiB**; valid
frames requiring larger windows are rejected. Corruption is never hidden by a
broad identity retry. Metadata gRPC calls support gzip with codec-aware fallback;
outbound ByteStream RPCs do not add gRPC compression to ZSTD payloads. Tiny
frames can be larger than the original content.

Local changes never retain a trusted remote digest. Staged files are executable
because the protocol has no executable bit.

Historical native acceptance: NFS and ProjFS pass the 12-phase real Bazel/CAS workflow
with zero-download initial cached builds. First consumption pays the whole-blob
verification cost; this is not an established eager-versus-lazy speedup.

**Limits:** experimental native mounts, native Git object storage remains separate,
serialized lifecycle/staging per output base, no TreeArtifactLocator staging or modification
journal. Retired IDs/local payloads are retained conservatively; cache leases do
not guarantee an indefinitely open NFS descriptor works after CAS expiration.
Windows permits local directory renames, but rejects moves containing projected
remote descendants until subtree rebasing exists. Native cache-client support
is not Darwin/Windows RBE worker provisioning.

Checks: `cargo test -p bazel-output-service`. Opt-in
[`tests/bazel_smoke.py`](tests/bazel_smoke.py) covers real Bazel + selected CAS,
SHA256/BLAKE3, execution, restart, tamper repair and clean; use `--storage` to select
native lazy mode. Environment-only credentials use `--credential-env NAME` with
`--cas-endpoint`; generated helpers/rc files contain no key values.
See [platform support and acceptance limits](../../docs/platforms.md).
