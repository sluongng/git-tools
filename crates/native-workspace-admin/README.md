# native-workspace-admin · library

Owner-private registration and external jj administration for lazy native
workspaces. The mounted `.jj/native-workspace` marker contains only an opaque ID;
repository, operation, index and working-copy state live outside the projection.

Initialization and exact retained-capture restore require a live native
controller. Restore stages verified operation/index metadata under a journal,
then finalizes through a sealed receipt before the host opens admission. A staged
receipt alone is not a ready mount. Committed resume preserves current local
state; it does not replay the original capture.

Incomplete or mismatched state remains available for explicit recovery. It is
never reset, adopted as another origin, or replaced with a materialized checkout.
