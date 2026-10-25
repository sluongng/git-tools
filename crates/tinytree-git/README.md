# Local public-history Git export

`tinytree-git export` mirrors one exact native public reference into one branch of
an initially empty, private local bare Git repository. Native storage remains the
source of truth. The adapter does not contact GitHub, push, watch in the background,
import original Git history, or switch an existing repository to native hosting.

```sh
tinytree-git export --config client.toml --state /private/adapter \
  --destination-git /private/receiver.git --source-reference main \
  --destination-reference refs/heads/main --git /path/to/git
tinytree-git status --state /private/adapter
```

The state and receiver must be separate absolute private directories. Git 2.38+
is required; all Git writes explicitly select object/reference fsync. The adapter
sanitizes Git configuration and environment, disables hooks and automatic
maintenance, and rejects alternates, replacement objects and partial repositories.
A conflicting receiver branch is never overwritten; unrelated references are left
alone. A fresh clone and `git fsck --strict` are useful independent receiver checks.

## Selection and conversion policy v1

Only the selected commit and its ordered **public parents** are converted.
Predecessors, copy history, workspace clouds, operation logs and sidecars are not
export selection. Native retention may cover a broader closure without making it
public in Git. Regular files, executable bits, symlinks and binary bytes are
preserved. Resolved trees are required. Native signatures, unresolved conflicts,
unsafe identities/paths and unsupported metadata fail explicitly rather than being
silently discarded. Typed native commit IDs are retained in the private mapping, not
added to public commit messages. Distinct native changes can map to identical Git
commits; duplicate resulting merge parents are rejected.

Author, committer, timezone, message and parent order are deterministic. V1 floors
nonnegative millisecond timestamps to whole Git seconds; subsecond precision is
not representable in Git. Negative dates and invalid timezone offsets are rejected.
This precision policy is part of the mapping format, not a history-preserving
original-Git-ID migration guarantee.

## Recovery and bounds

A single-owner lock protects a checksummed, fsynced private checkpoint and one
pending export. Before reading native payloads, a durable private native hold is
published and freshly verified. Objects and stable typed native-to-Git mappings
are installed before compare-and-swap publication of the receiver branch. An
independent readback precedes the delivered checkpoint; only then is the native
hold released and that release reconciled. Failures preserve the pending intent
and hold. Repeating the same command reconciles the original job and produces the
same Git object IDs, including after an ambiguous reference update. `status`
distinguishes delivery from pending cleanup.

Accepted work retains the writer lock until Git children and blocking writers
finish or are killed and reaped, even if the caller cancels. A surviving Git child
also inherits the physical lock across abrupt adapter death. Interrupted native
FILE spool artifacts are preserved and bounded by admission checks, not silently
removed as unrelated orphans. Exhaustion requires deliberate local reconciliation.
The library exposes explicit object, byte, memory, depth, disk and deadline limits;
the CLI uses conservative defaults and exposes the overall deadline.

The checkpoint binds the repository, endpoint, selected references, receiver path,
and credential/TLS configuration identity. Credential rotation or identity changes
require an intentional migration; they do not silently reuse an old authority.
Keep the private state and its retention intents with the receiver.

Original Git history requires a separate history-preserving importer plus a
verified commit/tree association before an existing Git anchor can be accepted.
Arbitrary caller-supplied native-to-Git mappings are not supported. Background
publication and GitHub delivery remain separate follow-up work.
