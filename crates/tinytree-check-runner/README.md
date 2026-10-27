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
