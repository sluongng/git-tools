# Tinytree check runner

Executable and protocol library for operator-trusted Linux checks on sealed
native source mounts. It is not a sandbox for hostile repository code.

1. Copy [example.toml](example.toml) to an owner-private configuration directory.
   Set runner/workspace/Bazel SHA256 pins, daemon configuration, matching remote
   instance name, Unix RBE socket and exact build/test labels. Keep actual
   settings and credentials untracked. Provision the configured scratch path's
   parent as a trusted, owner-managed directory first: the runner creates only
   the private scratch leaf, not missing parent directories. `profile-version`
   validates the profile but does not provision scratch directories.
2. Build with `bazel build --config=rbe //:tinytree-check-runner`.
3. Obtain the frozen server profile version with
   `tinytree-check-runner --config CONFIG profile-version --profile unit`.
4. The service sends one bounded JSON `Request` to `run` on stdin and validates
   the complete returned `Receipt`. Only operator policy selects commands.

Sealed checks disable workspace Bazel rc loading. Operator profiles must
explicitly include required repository build settings in their `flags`; for this
repository, include `--compilation_mode=opt` and `--@curl//:ssl_lib=openssl`.
`force_remote_execution = true` disables remote action-cache reads and cached
test results to require fresh execution. Normal incremental checks can use
`force_remote_execution = false` to reuse the content-addressed remote cache.

Success requires exact immutable mount identity, completed BEP target/test
results, evidence digests, Bazel shutdown, unmount and candidate retirement.
`recover` consumes the same request and cleans an interrupted attempt without
rerunning checks or manufacturing a verdict. Failed cleanup preserves recovery
files; completed attempt replay returns the saved receipt. New infrastructure
attempts need a fresh `attempt_id`.

Timeouts and evidence limits are configurable. Scratch admission defaults to
1,024 retained attempts and fails closed when full; automatic evidence pruning
and hostile-source isolation are not implemented. This crate alone does not
publish a landing or establish server integration.

Additional pinned tools can use `[tools.NAME]` binary entries; a profile flag
such as `--repo_env=TOOL=@runner_tool(NAME)` resolves to its private snapshot.
The executable checks its own configured pin; the service must also pin it
before launch. A completed early build failure can have an explicitly absent
compact log, but success cannot. Configuration rotation can use retained cleanup
policy; unavailable rotated credentials and uncertain/live process groups still
require operator recovery. Keep the Tokio runtime alive and cancel/drain retained
`RunningCheck` handles when embedding the library.

## HTTPS candidate snapshots

The alternative HTTPS source mode is an execution-only client of the Forge
checker endpoint. It needs no local repository server, daemon configuration or
workspace binary. Omit `daemon_config` and `workspace`, and configure
`[https_snapshot]` with `origin`, `credential_environment`, a pinned
`[https_snapshot.readonly_helper]` binary and `[https_snapshot.limits]`.
Keep the common pinned Bazel, runner, scratch, execution-only proxy and exact
profile settings. Use an HTTPS origin; HTTP is accepted only on loopback for
local acceptance. Resolve the credential through its environment-variable
reference; never put its value in the snapshot, profile flags or execution
environment. The service grants the dedicated checker identity an exact account
and profile independently from Forge user membership.

Build the helper with
`bazel build --config=rbe //crates/tinytree-check-runner:readonly_snapshot_helper`.
After installing the pinned binaries, calculate the profile version and configure
that same version and target policy on the service. The backend selection,
helper pin, limits and forced batch execution enter the profile identity.
Unchanged native configurations retain their existing profile identity.

```sh
tinytree-check-runner --config CONFIG run-remote \
  --account ACCOUNT --profile PROFILE --claim-id SAVED_CLAIM
tinytree-check-runner --config CONFIG recover-remote \
  --account ACCOUNT --profile PROFILE --claim-id SAVED_CLAIM
```

Keep the same saved claim for retries and recovery. The controller persists its
claim, execution intent and result outbox in a private sibling of scratch;
heartbeat uncertainty cancels execution. Recovery cleans the owned attempt and
reconciles the saved result; it never downloads a fresh candidate or reruns Bazel.
An accepted result acknowledgement can be recovered without another execution.
Missing cleanup tools, rotated access or uncertain process ownership fail closed
and can require operator recovery. Preserve the original scratch and control
journals rather than deleting them or treating lease expiry as cleanup.

The materializer verifies native COMMIT/TREE coverage and streamed leaf hashes.
The Linux helper copies into an independent bounded tmpfs, remounts its
superblock readonly, drops capabilities and runs Bazel in a child PID namespace.
Bazel uses `--batch`, disabled rc files and `--lockfile_mode=off`; no persistent
server is allowed. The helper verifies source identity before and after execution
and records namespace teardown. Current snapshot policy rejects absolute symlinks
and targets containing `..`, including otherwise in-tree parent-relative links.
This remains a trusted-source check facility, not a hostile-code sandbox.

Explicit Linux acceptance uses the declared fixture, Worker, runner, helper and
pinned Bazel executable:

```sh
bazel test --config=rbe //cloudflare:checker_acceptance \
  --test_env=TT_CHECKER_ACCEPT_RBE_SOCKET --test_output=all
```

Set that environment variable to an existing execution-only Unix proxy socket.
The target executes locally for real kernel isolation, while compilation and
the sealed Bazel test use remote execution. Its production-workerd evidence is
not a deployed-provider acceptance result.
