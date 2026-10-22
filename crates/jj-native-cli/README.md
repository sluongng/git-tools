# jj-native-cli · local prototype executable

`tinytree-jj` runs jj's ordinary commands with the native Tinytree object backend.
It keeps each workspace's normal local operation, index and working-copy stores.

```sh
bazel build --config=rbe //crates/jj-native-cli:tinytree-jj
bazel-bin/crates/jj-native-cli/tinytree-jj tinytree init workspace
cd workspace
# Run the same executable here for status, new, describe, restore and undo.
```

This is a local, materialized-workspace prototype. Remote clone/fetch/push, lazy
mounts and multi-process broker integration are not implemented. The backend
currently allows one process owner and conservatively retains all local objects.
It does not open a Git database or need a Git executable; other workspace crates
can still unify Git features at compilation. Native publication is separate from
future bidirectional Git transformation.

`//crates/jj-native-cli:cli_tests` runs the actual declared binary across separate
processes with Git unavailable on `PATH`; it is not remote or mounted acceptance.
