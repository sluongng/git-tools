# jj-native-cli · local prototype executable

`tinytree-jj` runs ordinary jj commands with native Tinytree objects and each
workspace's own operation, index and working-copy state. Git is not required.

```sh
bazel build --config=rbe //crates/jj-native-cli:tinytree-jj
jj="$PWD/bazel-bin/crates/jj-native-cli/tinytree-jj"
"$jj" tinytree init workspace
cd workspace
"$jj" status
```

To share objects through `workspace`, enable `[native.repositories.example]` in
[the daemon configuration](../../examples/workspace.toml) and start the daemon.
Then use its private socket and configured store alias:

```sh
"$jj" tinytree init agent --broker /path/to/private/rbe.sock --store example
```

Broker mode lets independent processes share immutable objects, not edits or jj
operation state. The workspace saves a locator; later commands reconnect. Direct
mode remains single-owner. Both modes conservatively retain authoritative data.

Remote clone/fetch/push and lazy native mounts are not implemented yet. Native
publication remains separate from future bidirectional Git transformation.
`//crates/jj-native-cli:cli_tests` runs actual commands without Git, including
concurrent broker-backed workspaces; it is not remote or mounted acceptance.
