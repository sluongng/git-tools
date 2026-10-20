# Agent sessions

A thin **Linux FUSE** lifecycle CLI over `git-vfs`, native Git and Bazel. Each
session owns a blobless clone, writable overlay, Git admin and private Bazel
roots. The Git proxy, RBE and r2c2 remain externally provisioned shared services.

```sh
agent-session create /var/tmp/agent-42 \
  --proxy-url http://127.0.0.1:8082 --proxy-repo bazel \
  --reference master --revision FULL_40_CHARACTER_COMMIT_SHA \
  --bazelrc /run/agent/bazelrc
agent-session status /var/tmp/agent-42
agent-session build /var/tmp/agent-42 -- //src:target
cd /var/tmp/agent-42/workspace
# Edit normally; stage only the intended changes.
git add path/to/file
agent-session save /var/tmp/agent-42 -- -m 'Describe the change'
agent-session stop /var/tmp/agent-42
agent-session resume /var/tmp/agent-42
```

The host needs `/dev/fuse`, `fusermount3`, a Git supporting `remote-object-info`,
`git-vfs`, compatible Bazel with BLAKE3 xattrs/r2c2 support, and Linux >= 5.3 for
safe pidfd-based stop. Select executables with `--git-vfs`, `--git`, `--bazel` at
creation. The external bazelrc configures RBE, execution platforms, credentials
and justified remote TTL. The CLI neither copies secrets nor proves RBE was used;
inspect the first BuildBuddy invocation. Do not put credentials in the proxy URL.

For cooperating agents on one trusted host, create a directory outside every
session and pass `--repo-contents-cache /absolute/host/repo-contents` at creation.
The canonical path is retained across resume and never deleted by stop. Cached
dependencies are symlink-shared, so this is not isolation against dependency
edits. Omitting the option preserves Bazel/rc defaults under the private roots.

Remote repository caching is **on by default**, independently of host sharing.
Use `--remote-repo-contents-cache=false` (or `--no-remote-repo-contents-cache`) at
creation to disable r2c2 without disabling RBE. The provisioned rc still supplies
the remote cache endpoint, credentials and instance; reads require
`--remote_accept_cached=true`. A remote hit stays lazy and does not populate the
local extracted-directory cache. Optional `--disk_cache` in the rc can reuse
downloaded remote blobs; scope it to trusted agents and configure its GC budget.
Bazel 9.2 ignores that disk cache when its remote output service is enabled.

Commands return JSON `{ "ok": true, "report": ... }` (or `ok: false` and
a nonzero exit status on failure), with revision, readiness, paths and phase wall times;
subprocess output goes to stderr. Create times clone, mapping import and mount
readiness separately. Mapping import is blocking; Bazel/Starlark and symlink
prefetch starts asynchronously after mounting. The proxy must already be warm.
`vfs.log` retains backend diagnostics; `session.json` is a versioned manifest.
Session operations fail rather than race when another operation holds the lock.
`edit_ready` means only that the mount/process are ready; last successful build
and save timestamps are separate and do not prove RBE usage. Operation timings
remain available from subsequent `status` calls.

`save` is **native `git commit`**, not an export, push or automatic `git add`.
It preserves the Git index, hooks and signing behavior. Contextual shell
overrides such as `GIT_DIR` and `GIT_INDEX_FILE` are removed so operations cannot
accidentally target another workspace. Stock Git may hydrate all
missing indexed blobs during commit; saving is not promised to be lazy. Unstaged
and untracked edits remain in the overlay. Do not change the pinned base revision
or use reset/checkout as a session-management operation.

Stop shuts down this session's existing Bazel server and signals only a verified
VFS process using pidfd identity checks. It never kills the proxy, force-kills a
process, recursively deletes state or discards edits. A manifest is bound to its canonical session directory; copying or moving it
cannot control processes from the original directory. Resume retains the original
clone, overlay, index, local commits and output roots. A partial failed clone is
retained for diagnosis; create a fresh session rather than overwriting it. A
mount timeout leaves its recorded process addressable with `stop`. If ownership
cannot be verified, manual diagnosis is required. NFS/ProjFS session orchestration
is not yet implemented; unsupported operating systems fail explicitly.
