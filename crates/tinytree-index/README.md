# tinytree-index

Library for a persistent, repository-scoped derived commit DAG. Canonical commit
verification and parent readiness precede atomic indexing. Frozen root scopes
provide exact graph and prefix queries with explicit work budgets.

The index stores graph facts, not canonical payloads. It is rebuildable and does
not establish object residency or retention; hosts must retain scoped roots.
Budget exhaustion and incomplete coverage are errors, never negative answers.
