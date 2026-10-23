# private-fs · library

Owner-private directories and small durable state files for native repository
clients. `PrivateDir` retains inspected ancestor handles while jobs use its path.

- Unix: current-user private leaf, trusted root/user-owned ancestors, no symlinks.
- Windows: current-user protected DACL, trusted ancestor ACLs, no reparse points;
  retained handles prevent directory replacement.
- Existing insecure state is rejected, never repaired automatically.

The library also provides bounded reads, private lock files and atomic replacement.
Unix replacement fsyncs the file and parent; Windows requests write-through rename.
These are OS durability operations, not proof of machine power-loss behavior.
