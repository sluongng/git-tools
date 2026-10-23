# jj-native-cli · experimental executable

`tinytree-jj` runs ordinary jj commands with native objects, keeping each
workspace's operation, index and working-copy state independent. Use a configured
native remote alias in the [workspace daemon](../workspace-daemon); the CLI only
receives its private socket and alias, never upstream credentials.

```sh
bazel build --config=rbe //crates/jj-native-cli:tinytree-jj
JJ="$PWD/bazel-bin/crates/jj-native-cli/tinytree-jj"
"$JJ" tinytree clone project --broker /path/to/rbe.sock --store example \
  --reference main --bookmark main
cd project
"$JJ" status
"$JJ" tinytree fetch --reference main --bookmark main
# Make changes, then select the bookmark to publish.
"$JJ" bookmark set main -r @
"$JJ" tinytree push --reference main --bookmark main
```

User workspaces always use a daemon-owned lazy jj-vfs mount. Both `init` and
`clone` require `--broker SOCKET --store ALIAS`; there is no materialized or
debug fallback. The mount contains an opaque `.jj/native-workspace` marker;
operation, index and working-copy state live in private external administration.
Fetch an absent reference explicitly before its first push. Native publication
is not Git push.

For new remote workspaces, experimental `[native.workspaces] index_mode =
"federated"` uses immutable native index checkpoints and broker queries rather
than building a full local history index. The default remains `"legacy"`. Existing
workspaces keep their recorded index type; missing native state fails closed.

Push uses this workspace's durable observed generation, which jj undo cannot
roll back. Interrupted pushes retain their original replay intent. Retry that
push to reconcile it; `fetch --abandon-pending-push` explicitly archives the local
intent before fetching. This does **not** cancel or roll back upstream work.
Observations and replay archives are bounded; failures never silently discard them.

Use `tinytree unmount DEST` from outside the mount and `tinytree resume DEST`
to stop and resume the original workspace without resetting edits. Interrupted
mutations use `tinytree recover DEST`; an applied mutation may require its actual
retained operation ID as proof.

After a Linux daemon crash, select the workspace ID from `workspace status`, run
`workspace recover --native-workspace ID`, then `tinytree resume DEST`. Recovery
only detaches a proven disconnected mount with its matching persisted identity;
missing evidence requires manual inspection, not a reset.

Linux FUSE is the current integrated provider. Native macOS/Windows mutation
integration and acceptance remain incomplete; unsupported operations fail
without creating a checkout. Client GC/offloading is not automatic. Protocol
fixtures are separate test-only binaries, not proof of native mount acceptance.
