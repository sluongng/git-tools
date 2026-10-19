# Bazel Remote Output Service

A local BOS v1 server (Bazel 9.2 protocol) backed by a configured BuildBuddy /
REAPI CAS. Separate from the Git source VFS: this manages **build outputs**.

```sh
# Read the key from your secret store into this environment variable.
export BUILDBUDDY_API_KEY
cargo run -p bazel-output-service -- \
  --root "$HOME/.cache/git-output-service" \
  --cas-endpoint grpcs://YOUR_INSTANCE.buildbuddy.io

# --digest_function is a Bazel startup option; SHA256 also works.
bazel --digest_function=BLAKE3 build //:target \
  --remote_cache=grpcs://YOUR_INSTANCE.buildbuddy.io \
  --experimental_remote_output_service=grpc://127.0.0.1:8377 \
  --experimental_remote_output_service_output_path_prefix="$HOME/.cache/git-output-service"
```

Configure Bazel credentials separately with a host-scoped credential helper.
Do not put keys in command-line arguments. Set `--instance-name` to match
Bazel's `--remote_instance_name` when nonempty. The prefix flag is required for
Bazel 9.2's native client path handling. The service rejects other backend
origins/instances and unsupported digest functions.

**Behavior:** eager, streamed, size/digest-verified downloads; atomic replacement;
capability-confined filesystem access; isolated output bases; native clean and
finalization. All outputs remain local across builds, but each build requests
full revalidation, so edits cannot be hidden by stale incremental metadata.
Staged files are executable (the protocol carries no executable bit).

**Limits:** trusted single-user loopback service, not a multi-tenant server.
No lazy output hydration, remote-only retention, TreeArtifactLocator staging,
output eviction, or modification journal yet. Stage requests are serialized with
lifecycle changes. Source VFS and output-service caches are independent.

Checks: `cargo test -p bazel-output-service`. The opt-in
[`tests/bazel_smoke.py`](tests/bazel_smoke.py) exercises real Bazel + an
operator-selected BuildBuddy CAS with SHA256/BLAKE3, cached outputs,
incremental builds, tamper repair, and clean; `--help` lists inputs.
