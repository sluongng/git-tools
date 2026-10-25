# Local public-history Git export

`tinytree-git export` mirrors one exact native public reference into one branch of
an initially empty, private local bare Git repository. Native storage remains the
source of truth. Export is local-only; optional foreground watcher commands
provide local replication and a separate opt-in remote backup. These commands do
not import original Git history or switch repository authority.

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

## Bounded local background replication

`watch` runs one configured public reference into the same local export mapping
and bare receiver. It is a foreground service process; it does not install or
start a daemon, push to GitHub, change native landing policy, or switch repository
authority. Native publication does not await Git progress.

```sh
tinytree-git watch --config client.toml --state /private/export-state \
  --watch-state /private/watch-state --destination-git /private/receiver.git \
  --source-reference main --destination-reference refs/heads/main --git /path/to/git
tinytree-git watch-status --watch-state /private/watch-state
```

All three directories must be absolute, normalized and pairwise disjoint. The
watcher reserves both its private status writer fence and the existing export
writer fence for its lifetime. A competing watcher or foreground exporter gets
an explicit busy error; independent Git changes are detected and never overwritten.
The existing foreground export API, checkpoint format and mapping remain usable
when the watcher is stopped.

Defaults are a five-second poll, five-second bounded probe and capped exponential
retry up to five minutes. `--poll-interval-ms`, `--probe-timeout-ms`,
`--max-backoff-ms` and `--timeout-seconds` configure bounded alternatives. Polls
range from 100 ms to one hour, probes from 100 ms to one minute, and maximum
backoff must be at least the poll interval and no more than one hour. One timer
and one admitted operation are active; there is no growing in-memory queue.
Failures, including permanent receiver divergence, retain a checksummed/fsynced
retry deadline across restart rather than repeatedly attempting conversion.
Filesystem/status persistence errors stop scheduling and return an error.

Unchanged polls use one authenticated exact-reference probe and a cheap local Git
reference check. They do not convert commits, download native FILEs, or repeatedly
fsck the whole receiver. A successful export performs the full existing integrity
checks. A new watcher also verifies existing delivery before reporting caught-up
state. Captured TLS trust and credential identity remain bound to the configured
source; startup does not silently rebind an old mapping.

### Observation, delivery and recovery status

`observed_desired` is a timestamped native observation, not a promise about which
head the exporter later selects. The exporter may select a newer public head.
Its durable outbox is authoritative for selected work and delivered generation,
native head and Git OID. After an attempt, the watcher reloads that outbox and
marks source lag unknown until a fresh observation reconciles it. Existing pending
Ready/Held/Release work is replayed before a new probe, including local Ready
publication while the native service is temporarily unavailable. Source absence
or deletion is an error, not permission to delete the Git branch.

`lag_at_last_observation` compares that observation with acknowledged delivery.
A positive gap can remain visible during an admitted export; zero requires a fresh
matching observation, receiver-ref check, no pending cleanup and a verified delivery
in the current run. `null` means unknown or not safely comparable, not zero.
`last_full_verification_unix_ms` identifies the last full graph check; a cheap probe
does not refresh it. Exporting may have a newer selected job than the last checked
`pending_export_phase`; timestamps and the export journal preserve that distinction.

The phase on disk is the last persisted phase, not proof of a live actor.
`writer_fence_held` can remain true for a surviving physical child after process
death. Status readers clear lag for an absent fence, stale observation or future
past-event timestamp, and expose clock skew explicitly. Retry recovery clamps
wall-clock rollback to the configured maximum delay rather than treating a future
observation as healthy age zero.

Ctrl-C stops scheduling, cancels logical work, kills and reaps accepted Git
children, and drains accepted SDK/blocking writers while both fences remain held.
Only then is Stopped fsynced and shutdown acknowledged. A replacement must wait for
both writer fences to be released. The Rust `start_watch` API returns a lifecycle
handle synchronously; cancelling `wait` keeps that handle reachable, and dropping
it requests shutdown. `shutdown().await` provides the drained acknowledgment.

Private pending export/hold intents survive interruption. Known partial staging
candidates can be discarded only after validating the committed checkpoint and
owned-file metadata; they are never adopted as authority. Independent native and
adapter-state backups are still required for disaster recovery. Local background
replication does not establish GitHub delivery or authorize unattended publication.

Interrupted Git writers can leave Git-created lockfiles. The watcher does not
automatically delete unknown Git locks: the durable outbox and native hold remain
safe, retries back off, and local lock reconciliation may be required before
replication can resume. Similarly, bounded interrupted native spool residues are
preserved for explicit reconciliation. The tests cover cancellation and process
death around accepted children, but not a power-loss or kill at every internal
Git ref-lock instruction.


## Optional one-way remote backup

On Linux, `push-watch` independently backs up **delivered exporter checkpoints**
to one public branch. It is disabled unless explicitly started. Run local
`watch` or `export` separately; native publication and local export do not wait
for remote pushes. No fetch, mirror, tag replication, force push, or source-side
reference update is performed.

```sh
tinytree-git push-watch --config client.toml --state /private/export-state \
  --push-state /private/push-state --destination-git /private/receiver.git \
  --remote https://example.invalid/owner/backup.git \
  --remote-reference refs/heads/main --git /path/to/git
tinytree-git push-status --push-state /private/push-state
```

The existing source/destination reference options must match the exporter.
`--remote-reference` selects the only remote branch. All local directories must
be absolute, private, normalized, and pairwise independent. A changed source,
credential identity, remote, branch, directory, Git executable path, or transport
policy requires explicit migration, not silently reusing the old outbox.

Only HTTPS and absolute local paths (including `file:///...`) are accepted.
URLs must not contain user information, query strings, fragments, or control
characters. SSH, custom protocol helpers and option-like destinations are not
supported. TLS verification is enabled and redirects are disabled. Ambient Git
configuration, credential helpers, prompts, and native source credentials are
not forwarded. The built-in command does **not** provision authenticated hosting
access. An explicitly selected `--git` executable is trusted code and may be a
narrow operator-managed authentication wrapper outside this repository. Such a
wrapper must preserve the configured arguments, owned process session, inherited
ownership descriptors, and environment isolation; it must not daemonize or close
those descriptors before its descendants exit. Credentials must not be embedded
in the remote URL. Status and errors omit the URL and raw transport diagnostics.

The fixed-size, checksummed, fsynced outbox holds at most a latest desired
position, one immutable pending position, and the last verified delivery.
New local deliveries coalesce without changing the pending Git OID. A private
local retention ref protects that exact pending commit and its verified complete
closure. This ref is never pushed. After a remote attempt, an independent exact
branch readback must match the selected OID before delivery is acknowledged;
ambiguous success is reconciled after restart. The acknowledgement is persisted
before compare-and-swap removal of the private pin. Missing, divergent, or
unavailable remote state preserves pending evidence. A previously acknowledged
but unfinished cleanup is also read back before pin removal.

The remote must initially have no selected branch, or already match the selected
pending OID. Later pushes require the last acknowledged OID observed at that
branch. The actual push is an ordinary non-force update: the receiver enforces
fast-forward ancestry, not a lease spanning the preceding observation. Concurrent
foreign updates that are ancestors of the selected commit are not fenced by an
atomic expected-old lease. Unrelated branches are never selected.

`desired`, `pending`, and `delivered` are separate facts. Lag is timestamped against
the last delivered exporter checkpoint, not a fresh native-server query; it is
unknown when observation fails, the actor stops, its fence is not held, or the
clock moves backwards. Status records bounded error categories, attempt counts,
retry deadlines, last remote-check time and physical writer-fence ownership.
Defaults are a five-second poll, sixty-second operation budget and capped
exponential retry up to five minutes. The corresponding millisecond options are
`--poll-interval-ms`, `--operation-timeout-ms`, and `--max-backoff-ms`.

Transport work owns an independent fence, never the exporter writer fence.
Supported Git processes and transport descendants inherit that fence and an
ownership endpoint. Logical expiry or cancellation initiates targeted session
cleanup, but only endpoint EOF permits physical ownership release. Linux pidfds
avoid signaling a reused numeric process identity; zombies are not treated as
live work. Uncertain cleanup retains the fence instead of admitting replacement
work. This contract covers the configured trusted executable and supported Git
helpers, not hostile same-user programs escaping the session or closing the
ownership descriptors. No service or credentials are installed by these commands.
