# tinytree-index

Library for a persistent, repository-scoped derived commit DAG. Canonical commit
verification and parent readiness precede atomic indexing. Frozen root scopes
provide exact graph and prefix queries with explicit work budgets.

The index stores graph facts, not canonical payloads. It is rebuildable and does
not establish object residency or retention; hosts must retain scoped roots.
Budget exhaustion and incomplete coverage are errors, never negative answers.
Scope-bound node tokens let operators follow verified parent edges without
repeating root-reachability checks or retaining parent vectors in a frontier.

Content search uses a separate keyspace for versioned FILE filters and complete
TREE manifests, with independent byte/record quotas and FIFO eviction. These
records do not retain canonical content. Missing records require rebuilding,
never a negative search answer.

Storage schema 3 adds a canonically derived root-tree conflict flag and rejects
older schemas explicitly. Rebuild canonical inputs
into a new private directory; existing logical records are never automatically
converted or discarded.
