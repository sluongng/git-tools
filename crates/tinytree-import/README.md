# tinytree-import · library and executable

Seed one immutable Git revision into a native Tinytree reference. The importer
reads Git objects, never a checkout: dirty files, untracked credentials and local
configuration are not source content. Regular files, executable bits and UTF-8
symlink targets are preserved; unsupported entries fail before publication.
This is a one-way snapshot seed, not Git history import or a Git storage backend.

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
