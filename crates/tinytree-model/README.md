# tinytree-model · library

Native-only canonical objects, typed IDs, bounded codecs and exact byte/hash
verification. Storage, RPCs, jj Backend translation, cryptographic signature
verification and Git conversion are not implemented here.

## Format v1

Files are exact raw bytes: FileId is ordinary unkeyed BLAKE3. Metadata begins with
8 bytes `54 54 52 45 45 00 00 00`, version `u16` 1, then kind `u8`: tree=2,
commit=3, symlink=4, copy-history=5. Its ID is unkeyed BLAKE3 over that entire
kind/version-tagged envelope. Object keys include kind; equal raw file/envelope
bytes may reuse CAS storage without confusing their logical object kinds.
Descriptors contain kind, 32-byte ID, exact stored size and format version.
ChangeId is 16 bytes. Integers are fixed-width big-endian; lengths/counts are
`u32`; strings are exact UTF-8, without normalization; booleans/option tags are
exactly 0/1. Unknown versions/kinds, bad tags, truncation and trailing bytes fail.

Bodies, in order:

- **Tree:** entry count, entries sorted strictly by UTF-8 name bytes. Each entry
  has a length-prefixed name, then tag 1=file (ID, executable, `u64` file length,
  optional CopyId), 2=symlink (ID, `u64` target byte length), or 3=tree (ID).
- **Commit:** ordered parent IDs, ordered legacy predecessor IDs, ordered root
  tree merge IDs, ordered labels, ChangeId, description, author, committer,
  optional secure signature. ID arrays and label arrays have counts. Signatures
  contain name, email, `i64` epoch milliseconds and `i32` timezone minutes.
  Secure signature contains length-prefixed signed data and signature bytes.
- **Symlink:** length-prefixed UTF-8 target, without NUL.
- **Copy history:** length-prefixed relative current path, ordered parent CopyIds,
  length-prefixed salt bytes. Missing file CopyId explicitly means jj's empty
  placeholder, not an invented object reference.

Root merge terms preserve jj's alternating add/remove/add order, including every
negative term, duplicate and label. Native labels are expanded to one per term;
a resolved merge has one empty label. Backend translation must expand jj's
unlabeled forms and reconstruct through its ConflictLabels factories, not copy
an internal Vec representation. Legacy predecessors are retained but are not
complete evolution history; per-agent Operation.commit_predecessors remains in
jj's separate operation store.

CommitId zero is the intrinsic synthetic root, never a stored commit. Its ChangeId
is zero and its tree is the real canonical empty-tree object. Ordinary commits
must have parents. SecureSignature.data must equal `unsigned_commit_bytes()`:
the same complete canonical commit envelope with signature absent. The signed
envelope determines final CommitId. This binds the representation, not a signer
or cryptographic validity; future native Backend/CLI code must verify signatures.

Default budgets: 16 MiB complete object, 100,000 items per array, 1 MiB per field,
64 KiB per path/component. Callers can tighten budgets. Decode validates lengths
before allocation, checks semantic invariants and rejects rather than normalizes
bad ordering. Tree names reject empty, `.`, `..`, slash and NUL; relative paths
apply that to every component. Provider-specific filesystem limits are separate.

`Object::references()` retains every direct merge/copy/legacy predecessor edge,
excluding intrinsic root commits. Reference sizes, copy-path consistency, graph
acyclicity and complete object closure still require store-level validation.
GitSubmodule has no native v1 representation: adapters must explicitly return
`Error::UnsupportedGitSubmodule`; they must not silently drop it.

## Validation

`//crates/tinytree-model:unit_tests` covers public byte/hash vectors, field-complete
round trips, signature binding, closure edges, malformed inputs and budgets.
The format is an initial implementation requiring independent format/security
review before persistent compatibility or publication promises.
