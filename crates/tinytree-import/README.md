# tinytree-import · library and executable

Seed one immutable Git revision into a native Tinytree reference. The importer
reads Git objects, never a checkout: dirty files, untracked credentials and local
configuration are not source content. Regular files, executable bits and UTF-8
symlink targets are preserved; unsupported entries fail before publication.
The default workflow is a one-way snapshot seed, not a Git storage backend.
Explicit history migration is described below.

```sh
bazel build --config=rbe //crates/tinytree-import:tinytree-import-bin
bazel-bin/crates/tinytree-import/tinytree-import-bin \
  --source-git /path/to/repository --revision FULL_COMMIT_ID \
  --config /path/to/client.toml --reference snapshots/example
```

Use the [client TOML](../tinytree-client/examples/client.toml) for transport,
credentials and RPC policy. Imports have explicit file, total-byte and time
limits. A bounded local hashing pass precedes upload so existing identical
references can be reused safely. Publication defaults to generation zero;
changing an existing reference requires its explicit `--expected-generation`.
A successful upload is not a permanent retention lease. Failed imports leave
source Git objects untouched and never publish a partial tree.

## Explicit history migration

On Unix, `--history --state /private/history-job` selects a separate, durable
history workflow. The default command and snapshot library API keep their original
behavior. History mode requires an exact lowercase SHA-1 commit ID and a complete,
non-promisor source without alternates. The Git executable must support the
global `--no-lazy-fetch` option; every reader command exercises that capability.
It reads verified immutable Git objects, not dirty, ignored or untracked
worktree contents.

```sh
tinytree-import --history --state /private/history-job \
  --source-git /path/to/repository --revision FULL_COMMIT_ID \
  --config /path/to/client.toml --reference main
```

The first publication requires an **empty** native target at the explicit expected
generation. Supplying the current generation does not authorize replacing an
existing history. Parent order, supported author/committer identities, timestamps,
timezones and messages are preserved. Git seconds become native milliseconds
exactly, and change IDs use a deterministic history-import domain. Exporting the
result with `tinytree-git` into an empty bare receiver reproduces the supported
original Git IDs, including merge parent order. It does not grant arbitrary
caller-supplied existing Git anchors.

V1 rejects signatures, extra commit headers, non-UTF-8 metadata/paths, noncanonical
identities, duplicate parents, submodules and unsupported tree semantics rather
than silently losing data. Git signature bytes are not native signatures. Native
canonical object formats and existing snapshot IDs are unchanged.

### Retention and recovery

Before public publication, a private native checkpoint retains both the imported
closure and a provenance manifest containing original raw commit/tree preimages,
typed associations and conversion/publication policy. Independent native readback
rehashes Git preimages and recomputes canonical native tree/parent associations.
The successful provenance hold is an intentional durable migration artifact;
it is not automatically released as a temporary upload lease.

One private checksummed/fsynced journal owns the source, target, policy and
credential/trust identity, stable request keys and pending intent. Repeating the
same command reconciles the original publication. Reports distinguish its immutable
`publication_generation` receipt from `current_generation`, `current_native_head`
and `is_current`; another writer's later advancement is never overwritten or
reported as the imported head still being current. An ambiguous or failed attempt
preserves the journal and retained provenance. Accepted children/blocking work own
the journal fence through cancellation and physical completion; interrupted native
spool artifacts are preserved under bounded admission, not silently discarded.

Save the reported `retention_hold`, `provenance_checkpoint` and
`provenance_manifest`, plus the private journal and configuration identity, in an
independent recovery packet. The Rust `verify_history` API verifies an explicit
`HistoryProof` read-only even after local source/journal/receiver loss. It does not
publish, discover unknown hold names, grant a mapping, or authenticate a Git
signature. Native public history can regenerate an empty Git receiver; complete
server disaster recovery still requires independent native storage/retention and
adapter-outbox backups. GitHub public history is not that private backup.

Credential, source-path or target identity changes require intentional recovery,
not automatic rebinding. Missing source storage may be resumed only when the
journal's exact provenance is already retained and independently verified. The
history library exposes aggregate object, commit, entry, depth, byte, memory,
manifest and whole-command deadline limits. Cold migration costs are separate
from normal native editing and incremental export.
