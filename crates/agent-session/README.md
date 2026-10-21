# agent-session

Standalone Linux session CLI over `git-vfs`, native Git and Bazel.
Each session keeps its own clone, writable overlay, Git index and output roots.
Prefer the unified [`workspace`](../workspace-daemon/README.md) CLI for a shared host daemon.

```sh
agent-session create /var/tmp/agent-1 \
  --proxy-url http://127.0.0.1:8082 --proxy-repo bazel \
  --reference master --revision FULL_COMMIT_SHA --bazelrc /path/to/private.bazelrc
agent-session build /var/tmp/agent-1 -- //src:target
agent-session status /var/tmp/agent-1
agent-session stop /var/tmp/agent-1
agent-session resume /var/tmp/agent-1
```

Requires Linux 5.3+, `/dev/fuse`, `fusermount3`, suitable native Git, `git-vfs`
and compatible Bazel. The supplied bazelrc owns RBE, toolchains and credentials;
never embed credentials in the proxy URL. Remote repository-content caching is
on by default; disable it with `--remote-repo-contents-cache=false`.
For trusted host sharing, use `--repo-contents-cache /path/outside/sessions`.
Shared dependency trees are not an isolation boundary against other agents' edits.

Stage intended files with native `git add`, then use `agent-session save SESSION -- -m MESSAGE`.
Save runs native `git commit`, including hooks/signing, and can hydrate indexed files.
It neither stages nor pushes changes. Keep the pinned base fixed; general
checkout/merge/rebase is unsupported. Stop/resume preserves edits and commits;
never delete overlay state as cache cleanup. Keep outputs outside the source mount.

Commands return JSON; subprocess output goes to stderr. Operations lock the
session, and stop signals only the verified owned process, including across launcher
`exec` transitions. A live legacy receipt without a boot identity requires manual
recovery; stop/resume retain it without signalling or replacing the process.
For a failed create, use a fresh directory rather than overwrite retained state. NFS/ProjFS session
orchestration is not implemented. See [platform support](../../docs/platforms.md)
and the [cloud-agent workflow](../../examples/skills/cloud-bazel-agent/SKILL.md).
