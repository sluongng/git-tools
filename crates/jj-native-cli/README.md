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
"$JJ" tinytree history path/to/file.rs
"$JJ" tinytree blame path/to/file.rs --json
"$JJ" tinytree search 'fn (open|close)_[a-z_]+' --path-prefix src --json
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

For new remote `init`, `clone` and convenience `cloud restore` destinations on
Linux, the default
`[native.workspaces] index_mode = "auto"` selects immutable native index checkpoints
and lazy broker queries. Automatic capture and background cloud publication are
also enabled for those new workspaces. Creation checks the remote server's
features and caller access, then verifies the exact selected query source before
creating the destination. Restore uses retained-cloud authority for the selected
version, never public-root admission for private captured commits. Unsupported servers or roots fail explicitly; there is
no eager or legacy fallback.

Local aliases remain legacy and are not automatically backed up. Explicit
`index_mode = "legacy"` also opts out of automatic portable backup; capture/upload
`policy = "disabled"` remain available independently.
Existing workspaces keep their recorded index type and are not enrolled merely
by upgrading the daemon. Restores keep their captured index even with an explicit
Legacy preference. Older pending restore requests and the lower-level exact
`tinytree restore` compatibility route remain unenrolled; use explicit
capture/upload policies to back them up. Missing native state always fails closed. A pending new initialization retains its selected mode
and backup eligibility across retries rather than adopting later config defaults.

With a federated workspace and an operator-configured landing profile:

```sh
"$JJ" tinytree land --base main@tinytree --reference main --bookmark main \
  --profile checks --profile-version v1
"$JJ" tinytree land --status
"$JJ" tinytree land --resume
```

Submission freezes the selected linear stack (default `@`) and leaves a fresh
working-copy child for continued edits. Checks and native publication are owned
by the server; the client never supplies a verdict. Resume imports exact checked
IDs and rebases local descendants. Amending submitted commits requires explicit
reconciliation; it never overwrites newer work. Interrupted operations retain
recovery roots. First submission upgrades local state; use a matching
landing-aware CLI afterward. There is no Git publication or materialized fallback.

`history` and `blame` pin the selected revision (`-r REV`, default `@`) and
compose local drafts and dirty edits over this workspace's immutable remote
baseline, without uploading them. They use fixed-path, linear-history semantics;
merges, conflicts and exhausted limits are explicit, not complete results.
Queries use an observed clone/fetch baseline or the exact retained version of a
restored cloud workspace; later private edits stay local. With the Federated
index, ordinary `jj file annotate` uses the same bounded linear attribution,
including dirty edits and `-r` selection, and renders jj's configured annotation
template or `-T` override. It requires complete attribution before output;
merges, conflicts and exhausted limits are unsupported, without an eager history
fallback. This does not yet provide jj's full multi-parent annotation semantics.
Explicit Legacy indexes retain jj's ordinary annotation implementation.

`search` interprets its pattern as a case-sensitive regular expression in regular
files at the selected revision, merging remote results with local additions,
edits, deletions and renames. Quote patterns to preserve shell metacharacters;
use inline flags such as `(?i)` for case-insensitive matching, or `-F` /
`--fixed-strings` to search literally (for example, `search -F 'object.method('`).
Character classes and word boundaries use ASCII byte semantics by default;
`(?u:...)` enables Unicode classes, but Unicode word boundaries are unsupported.
Matches are nonoverlapping and line-oriented: they never cross a newline, and
zero-width matches are skipped. Patterns must be nonempty, at most 1024 bytes,
and contain no literal newline. Results use byte offsets and byte columns, with
bounded line snippets; a match longer than 1024 bytes exceeds search limits
instead of yielding a truncated match. Search does not follow symlinks. Index
warming is asynchronous; output reports incomplete coverage when limits apply.
The same observed or exact retained-cloud baseline selection applies.
Private baseline discovery is currently linear-only; the server can search
resolved merge trees directly.

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

New remote clones and convenience cloud restores use automatic capture and
publication. To back up an existing workspace or a lower-level exact restore,
select explicit capture and asynchronous upload policies
in the [daemon configuration](../workspace-daemon). Use the matching daemon and
configured alias to discover backups without supplying server URLs, credentials
or raw manifest digests:

```sh
"$JJ" tinytree cloud --daemon-config CONFIG --store example workspaces
"$JJ" tinytree cloud --daemon-config CONFIG --store example versions WORKSPACE_ID
"$JJ" tinytree cloud --daemon-config CONFIG --store example \
  restore WORKSPACE_ID restored --generation GENERATION
"$JJ" tinytree unmount restored
"$JJ" tinytree resume restored --daemon-config CONFIG
```

Select `WORKSPACE_ID` from the first command and a generation from the second.
Omit `--generation` to select the latest published version once. Before creating
the destination, the daemon saves its new identity and exact source selection.
Retry the same destination to resume that selection, even if the source head has
moved; retries and later resumes preserve current edits. Automatic backup
eligibility is saved with that request and does not change merely because later
new-workspace defaults change. Explicitly disabled runtime capture/upload still
wins. A changed alias, source or explicit generation is rejected instead of
replacing that intent.

`workspaces` and `versions` return bounded pages; use the printed `--cursor`
with the same command and alias for the next page. `versions` keeps the first
page's generation upper bound, while the workspace catalog can move. All three
commands accept `--json`. Catalog rows and historical publication are selectors,
not retention leases or proof of current availability. Restore verifies the
selected retained capture before materializing its fresh lazy mount, preserving
operation history and working-copy state. Local backup data is not automatically
evicted.

The lower-level `tinytree restore` command remains available for an exact
manifest selection and explicitly chosen destination identity.

After a Linux daemon crash, select the workspace ID from `workspace status`, run
`workspace recover --native-workspace ID`, then resume with the matching daemon
configuration. Recovery only detaches a proven disconnected mount with its matching
persisted identity. Missing evidence requires manual inspection, not a reset.

Linux FUSE is the current integrated provider. Native macOS/Windows mutation
integration and acceptance remain incomplete; unsupported operations fail
without creating a checkout. Client GC/offloading is not automatic; see the
[daemon collection guide](../workspace-daemon#explicit-native-collection) for
explicit unreachable-only collection of a fresh, fully local cohort. Protocol
fixtures are separate test-only binaries, not proof of native mount acceptance.
