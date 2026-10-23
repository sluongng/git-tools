# native-workspace-admin · library

Owner-private registration and external jj administration for lazy native
workspaces. The mounted `.jj/native-workspace` marker contains only an opaque ID;
repository, operation, index and working-copy state live outside the projection.

Initialization requires a live native controller. Incomplete initialization stays
journaled; it is never reset or replaced with a materialized checkout.
