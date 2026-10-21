# bazel-output-service

Library and `git-output-service` executable implementing Bazel's remote output
service (BOS) over a configured REAPI CAS. This projects build outputs, not sources.
Run a native `git-output-service` binary on the target host.

```sh
# Provision the credential in your environment through your normal secret mechanism.
export BUILDBUDDY_API_KEY
git-output-service --config /path/to/private/output-service.toml

bazel --digest_function=BLAKE3 build //:target \
  --remote_cache=grpcs://YOUR_INSTANCE.buildbuddy.io \
  --experimental_remote_output_service=grpc://127.0.0.1:8377 \
  --experimental_remote_output_service_output_path_prefix=/path/to/outputs
```

Copy the [TOML example](../../examples/bazel-output-service.toml) to a private location.
Its resolved `root` must match Bazel's output path prefix; configure the same remote
instance and a separate host-scoped Bazel credential helper. SHA256 also works.
CLI options override TOML; relative TOML paths resolve beside the file.
Only a credential environment variable's name belongs in config, never its value.

Storage is **lazy by default**: Linux FUSE, macOS NFS or Windows ProjFS.
Staging/listing/stat use metadata; first content access fetches and verifies a
whole blob through shared [`local-cas`](../local-cas/README.md).
Use an empty mountpoint and keep `state_dir` outside it. Keep the service running;
restart requires unmount/remount. No silent eager fallback exists.
`--debug-eager` is a diagnostic-only alternative, unavailable through TOML.

Preserve authoritative state and local edits separately from disposable CAS bytes.
Changed endpoint/instance/credentials reject reuse of old namespace state: use a
fresh state directory and preserve old edits. The default verified cache is 4 GiB;
artifacts over the default 1 GiB read limit require explicit budget adjustments.
Metadata staging alone does not prove remote CAS residency; each build checks it.

Experimental limits include serialized lifecycle per output base, no
TreeArtifactLocator staging or modification journal, and conservative retention
of retired IDs/local payloads. Windows rejects directory moves containing
projected descendants. This is a trusted single-user local service.
See [platform setup](../../docs/platforms.md), [transport policy](../../docs/reapi-policy.md),
and [build/test instructions](../../docs/development.md).
