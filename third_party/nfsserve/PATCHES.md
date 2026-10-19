# Local NFS protocol hardening

Based on crates.io `nfsserve` **0.11.0**, published BSD-3-Clause sources
(<https://github.com/huggingface/nfsserve>). LICENSE is preserved. Cargo package
archive SHA-256 is
`ef1424b6d88c60a091931392970999ea3ceab132d968e6a5545c770fad5b97d7`;
this copy is a path crate.
Only library sources are included; upstream optional demo binaries are omitted.

Local changes:

- READDIR forwards the client's continuation cookie to the filesystem; the
  default adapter and Git frontend honor it without fetching file contents.
- Small directory buffers return NFS3ERR_TOOSMALL before arithmetic: READDIR
  requires 512 bytes; READDIRPLUS requires dircount >=512 and maxcount >=1024.
  This conservative floor guarantees room for a maximum-length first name.
- REMOVE and RMDIR use distinct callbacks, preventing wrong-type deletion.
- EXCLUSIVE CREATE decodes and forwards its 8-byte verifier. The Git frontend
  durably stores verifier + stable inode before replying; acknowledged creates
  retain same-verifier replay across restart. A different verifier or replaced
  inode returns EXIST. Removal/rename retire obsolete verifier records.
  The namespace and verifier sidecar are not one atomic transaction: a crash
  after creation but before verifier persistence leaves an unacknowledged empty
  file whose retry returns EXIST (fail closed; no false successful replay).
- Completed RPC retransmissions replay the original reply instead of being
  silently dropped. The 60-second replay table is capped at 4096 entries and
  16 MiB of response bytes. Admission evicts the oldest completed entry at
  capacity, so fresh requests continue to run. In-progress records are never
  evicted: if all entries are in progress, overload closes the client connection
  explicitly for retry instead of silently classifying new XIDs as duplicates.
  Parse errors release their in-progress tracking entry. NFSv3 JUKEBOX replies
  are sent but not retained, allowing the same XID to retry after admission
  recovers. This exception requires a matching NFSv3 procedure and a strictly
  validated accepted-success RPC envelope; successful mutations still replay.
- Guarded SETATTR compares ctime and applies attributes under one backend lock;
  guarded CREATE likewise performs existence checking and creation atomically.
- The listener admits 16 connections and defaults to 16 in-flight RPCs globally.
  Backends can select a bounded request budget (capped at 1024); lazy outputs
  use 128 slots with 64 separately admitted payload operations, reserving room
  for metadata while cold content waits. There are two
  queued replies per connection. Separate socket forwarding/writing prevents
  reply starvation when request queues apply backpressure.
- XDR byte arrays and integer arrays enforce a 2 MiB allocation ceiling before
  resize, including nested auth arrays and malformed truncated records.
- Fragmented RPC records are limited to 2 MiB before allocation; Git read/write
  operations are separately limited to 1 MiB.

Still not a general authenticated NFS server: single-user loopback only, no NLM,
hard links or remote identity enforcement. No native macOS kernel acceptance
has been claimed. Future upstream upgrades must preserve the real-wire tests.
