# tinytree-index

Library for a persistent, repository-scoped derived commit DAG. Canonical commit
verification and parent readiness precede atomic indexing. Frozen root scopes
provide exact graph and prefix queries with explicit work budgets.

The index stores graph facts, not canonical payloads. It is rebuildable and does
not establish object residency or retention; hosts must retain scoped roots.
Budget exhaustion and incomplete coverage are errors, never negative answers.
Scope-bound node tokens let operators follow verified parent edges without
repeating root-reachability checks or retaining parent vectors in a frontier.

Storage schema 2 rejects legacy schema 1 explicitly. Rebuild canonical inputs
into a new private directory; existing logical records are never automatically
converted or discarded.
