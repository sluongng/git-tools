# native-workspace · library

Compiles verified native trees into `output-core` source manifests using directory
and symlink metadata only. Regular files use exact raw-BLAKE3 CAS artifacts and
hydrate through a scoped, pinned `NativeObjectIo` source when read. Per-path
executable state and canonical copy IDs remain available to snapshot adapters.

Construct the scope from trusted repository configuration plus its effective
provider security identity. Compile first, then install the manifest into the
matching output tree; compilation never mutates the namespace. One compiler
keeps a bounded append-only file source across root switches, retaining old IDs
for existing read pins. Failed compiles authorize no new files. Unresolved root
merges return an explicit error with their original terms, not a chosen side.
This is not a commit-closure validator, retention proof or native mount frontend.
