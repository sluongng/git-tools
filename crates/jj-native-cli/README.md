# jj-native-cli · experimental executable

`tinytree-jj` runs ordinary jj commands with native objects, keeping each
workspace's operation, index and working-copy state independent. Use a configured
native remote alias in the [workspace daemon](../workspace-daemon); the CLI only
receives its private socket and alias, never upstream credentials.

```sh
bazel build --config=rbe //crates/jj-native-cli:tinytree-jj
JJ="$PWD/bazel-bin/crates/jj-native-cli/tinytree-jj"
"$JJ" tinytree clone project --broker /path/to/rbe.sock --store example \
  --reference main --bookmark main
cd project
"$JJ" status
"$JJ" tinytree fetch --reference main --bookmark main
# Make changes, then select the bookmark to publish.
"$JJ" bookmark set main -r @
"$JJ" tinytree push --reference main --bookmark main
```

Clone uses a normal **materialized** working copy, not a lazy mount. Native `init`
remains available locally or with `--broker SOCKET --store ALIAS`. Fetch an absent
reference explicitly before its first push. Native publication is not Git push.

Push uses this workspace's durable observed generation, which jj undo cannot
roll back. Interrupted pushes retain their original replay intent. Retry that
push to reconcile it; `fetch --abandon-pending-push` explicitly archives the local
intent before fetching. This does **not** cancel or roll back upstream work.
Observations and replay archives are bounded; failures never silently discard them.

Source mounts, client GC/offloading and standalone crash acceptance remain
separate work. Tests use actual jj subprocesses without Git and real loopback
native services; they do not establish mounted or full-stack acceptance.
