# tinytree-landing · library

Construct deterministic candidates for bounded, resolved linear native stacks.
Disjoint tree-entry edits rebase without reading file bodies; divergent edits to
the same file conflict. Change IDs, author metadata and existing copy/predecessor
metadata are preserved. Already-present source changes are rejected. Supplied committer metadata is frozen by the caller.

This is **candidate construction only**: no reference publication, durable job,
check runner, authorization or jj evolution integration. Callers must retain and
strictly validate the complete input and output closures, including skipped
unchanged subtrees, before treating a candidate as publishable. Logical metadata
and work limits are not a physical process-memory quota. The output budget also
reserves conservative space for unfinished parent-tree entries.
