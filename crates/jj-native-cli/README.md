# jj-native-cli · experimental executable

`tinytree-jj` runs ordinary jj commands with native objects, keeping each
workspace's operation, index and working-copy state independent. Use a configured
native remote alias in the [workspace daemon](../workspace-daemon). Clone, fetch
and push use its private socket and alias; restore and scoped resume use the
matching daemon configuration. Keep upstream credentials outside command lines.

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
roll back. Retry an interrupted push to reconcile its original intent.
`fetch --abandon-pending-push` archives that local intent before fetching; it does
not cancel or roll back upstream work. Failures never silently discard replay data.

Use `tinytree unmount DEST` from outside the mount and `tinytree resume DEST`
to stop and resume the original workspace without resetting edits. Restored
workspaces require `--daemon-config CONFIG` when resuming. Interrupted mutations
use `tinytree recover DEST`; an applied mutation may require its retained operation
ID as proof.

## Restore a retained capture

To create backups, enable capture and asynchronous upload in the
[daemon configuration](../workspace-daemon); both are opt-in. To restore, obtain
the exact source workspace, generation, manifest ID, raw BLAKE3 digest and byte
length from the authenticated retained-version API.
Choose a new 64-hex destination workspace ID and retain it for retries.

```sh
"$JJ" tinytree restore restored --daemon-config CONFIG --store example \
  --workspace-id NEW_DESTINATION_ID --source-workspace SOURCE_WORKSPACE_ID \
  --generation GENERATION --manifest MANIFEST_ID \
  --manifest-blake3 RAW_BLAKE3 --manifest-bytes BYTE_LENGTH
"$JJ" tinytree unmount restored
"$JJ" tinytree resume restored --daemon-config CONFIG
```

Replace the uppercase placeholders with the selected values. Restore preserves
operation history and working-copy state in a fresh lazy mount. Later resumes
preserve its current edits rather than replaying the original capture. Historical
publication is not proof of current remote availability; local backup data is not
automatically evicted.

After a Linux daemon crash, select the workspace ID from `workspace status`, run
`workspace recover --native-workspace ID`, then resume with the matching daemon
configuration. Recovery only detaches a proven disconnected mount with its matching
persisted identity. Missing evidence requires manual inspection, not a reset.

Linux FUSE is the current integrated provider. Native macOS/Windows mutation
integration and acceptance remain incomplete; unsupported operations fail
without creating a checkout. Client GC/offloading is not automatic. Protocol
fixtures are separate test-only binaries, not proof of native mount acceptance.
