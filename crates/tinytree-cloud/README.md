# tinytree-cloud · library

Verified native jj operation/view recovery objects, using the pinned jj 0.46
SimpleOpStore codec. Semantic OP/VIEW IDs are 64-byte BLAKE2b identities;
transfer descriptors separately name raw bytes with BLAKE3 and length. Intrinsic
zero roots have no payload. Native commit references must be 32 bytes.

Export and verification bound protobuf bytes, fields and decode work, retain
operation views/rewrite maps and verify semantic identities. Import uses real
OpStore writers and syncs files/directories before success; an unsupported or
failed sync returns an error. Parentless legacy non-root operations are rejected
rather than normalized into a different identity. Absent local bookmarks must be
represented by absent map keys, matching jj's View API. Existing corrupt/nonregular
import slots fail without being overwritten.

This is not an automatic backup service. Callers must fence captures, authorize
access, retain complete operation/view/native dependencies and atomically install
restored workspace/head state. Import alone does not validate dependency residency
or authorize offloading sole local copies. Payload metadata is private; do not log it.
