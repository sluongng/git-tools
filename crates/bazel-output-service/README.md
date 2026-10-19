# Bazel Remote Output Service

A trusted, single-user loopback BOS v1 server for Bazel 9.2 and a configured
BuildBuddy / REAPI CAS. This manages **build outputs**, not the Git source VFS.

```sh
# Populate this variable from your secret store, not a command-line key.
export BUILDBUDDY_API_KEY
cargo run -p bazel-output-service -- \
  --root "$HOME/.cache/bazel-outputs" \
  --cas-endpoint grpcs://YOUR_INSTANCE.buildbuddy.io

bazel --digest_function=BLAKE3 build //:target \
  --remote_cache=grpcs://YOUR_INSTANCE.buildbuddy.io \
  --experimental_remote_output_service=grpc://127.0.0.1:8377 \
  --experimental_remote_output_service_output_path_prefix="$HOME/.cache/bazel-outputs"
```

Configure Bazel's host-scoped credential helper separately. SHA256 also works.
Set `--instance-name` to match a nonempty Bazel `--remote_instance_name`.

## Storage

- `--storage eager` (default): stream and verify every staged blob before atomic
  publication to an ordinary local directory.
- `--storage nfs` (macOS) or `--storage projfs` (Windows): publish CAS metadata,
  fetch and verify a whole blob on first access. Add
  `--state-dir "$HOME/.cache/bazel-output-state"` outside `--root`. NFS needs an
  empty mountpoint and no sudo; Windows needs ProjFS enabled. Keep the daemon
  running while accessing lazy outputs. Restart NFS by unmount/remount, not takeover.

Lazy state includes authoritative edits: **do not delete it as a cache**.
The verified cache defaults to 4 GiB; a blob must also fit the 256 MiB in-flight
budget. Adjust `--max-cache-bytes`, `--max-inflight-bytes` and
`--max-concurrent-fetches` for the workload. Metadata staging does not prove CAS
residency; each build checks presence and requests conservative revalidation.
Local changes never retain a trusted remote digest. Staged files are executable
because the protocol has no executable bit.

Native acceptance: both lazy adapters pass the 12-phase real Bazel/CAS workflow
with zero-download initial cached builds. First consumption pays the whole-blob
verification cost; this is not an established eager-versus-lazy speedup.

**Limits:** experimental native mounts, independent source/output caches,
serialized lifecycle/staging, no TreeArtifactLocator staging or modification
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
See [design and native evidence](../../docs/design/edenfs-platforms-and-outputs.md).
