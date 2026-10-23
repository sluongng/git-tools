# jj-vfs

Jujutsu working-copy library with Git and native projection adapters. Stock `jj`
cannot load these backends. Native workspaces use [`tinytree-jj`](../jj-native-cli)
and external private administration; the adapter never scans a materialized
checkout or falls back to one.

The custom `git-jj` executable integrates an active writable Linux `git-vfs` mount:

```sh
# Start git-vfs with --overlay /state/agent first.
cd /workspace/agent
git-jj init-vfs --overlay /state/agent
git-jj status
git-jj describe -m 'Implement change'
git-jj new
```

The backend adopts the mounted base without checkout, reads the change journal,
and stores changed contents in jj's Git backend before committing a snapshot.
Keep `.jj` state and the overlay: neither is disposable cache.
For native Git origins, `--size-source=git` warms size metadata before mounting.

Supports UTF-8 regular files, executable bits and symlinks. Submodules, sparse
checkout, `workspace add`, and snapshotting conflicts are unsupported; this is
not a general merge/conflict-resolution backend. Journal overflow falls back to
a metadata walk. See [platform support](../../docs/platforms.md) and
[build/test instructions](../../docs/development.md).
