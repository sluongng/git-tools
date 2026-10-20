---
name: cloud-bazel-agent
description: Start and resume private Linux coding-agent workspaces for Bazel RBE through the shared workspace daemon, without Git worktrees.
---

# Cloud Bazel agent

Use `workspace` as the entry point. One host daemon shares Git transport,
immutable source/output bytes and a BuildBuddy cache connection. Each agent owns
its source edits, Git admin state and Bazel output base; do not share those.

## Provision once per trusted host

Use Linux with `/dev/fuse`, `fusermount3`, native Git supporting
`cat-file remote-object-info`, and the `workspace` binary. Bazel needs BLAKE3,
digest xattrs, r2c2 and the [Unix-socket authority fix](https://github.com/bazelbuild/bazel/commit/242563f1a14378801725e5f6b0ee397ef0d307f2).
An older repository-pinned client, including 9.3.0rc2, is not sufficient.

Provision [workspace TOML](../../workspace.toml) outside all mounts: repository
aliases, BuildBuddy upstream, credential environment-variable name, compatible
`session.bazel`, and an external `session.bazelrc` enabling RBE with the correct
execution platforms. Never copy credentials into a repository or put them in
command arguments. The CLI does not provision SDKs or prove that RBE was used.

The first Git request prepares a full mirror; prewarm it when agent startup
latency matters, and account for that cost separately. An optional host-shared
repository-tree cache must be provisioned outside sessions. Remote r2c2 stays
on by default, but remote hits do not populate that extracted-tree cache.

## Start work

Inputs: a new absolute `AGENT_SESSION`, repository alias `PROXY_REPO`, branch/tag
`TASK_REF`, immutable full commit `TASK_REVISION`, and `TASK_TARGET`.

```sh
workspace create "$AGENT_SESSION" --repo "$PROXY_REPO" \
  --reference "$TASK_REF" --revision "$TASK_REVISION"
workspace build "$AGENT_SESSION" -- "$TASK_TARGET"
cd "$AGENT_SESSION/workspace"
git status --short
```

`create` starts the shared daemon automatically. Use global `--config FILE` for
an explicitly provisioned host configuration. If the external bazelrc requires
a named config, pass it after `--` in `build`/`test`. Inspect the first invocation
for actual execution/cache evidence; mount readiness is not build success.

Likely Bazel inputs prefetch asynchronously after mount. Other contents load
lazily. BOS is opt-in (`bos.enabled = true`); its outputs stay lazy, but current
metadata staging can slow builds. A digest mapping or queued upload is not proof
of remote CAS residency; Bazel/proxy still ensure inputs reach BuildBuddy.

## Iterate and retain work

```sh
workspace test "$AGENT_SESSION" -- //path/to:tests
# Stage only intended changes when a commit is requested.
git add path/to/changed-file
workspace save "$AGENT_SESSION" -- -m 'Describe the change'
workspace stop "$AGENT_SESSION"
workspace resume "$AGENT_SESSION"
```

`save` is native Git commit, not automatic staging or pushing. A first commit
can hydrate missing indexed blobs. General checkout/merge/rebase and native JJ
session orchestration are not supported by this lifecycle command.

Stop retains edits, index, commits and caches. Do not delete overlays or shut down
the shared daemon as routine agent cleanup. After a crash, run
`workspace recover "$AGENT_SESSION"` before resume. Recovery refuses live or unverified mounts;
do not bypass that check or claim transparent recovery.

Only disposable verified bytes enter the shared LRU CAS. Native Git packs and
repository trees have separate cleanup policies. Action-result TTL and positive
presence hints default to zero; configure longer retention only from an actual
backend contract, not inferred from a successful upload.
