# native-workspace · library

Projects verified native trees into `output-core`. Lazy baselines fetch one
complete canonical directory at a time; unopened directories remain tree IDs,
and symlink targets load on demand. Regular files use scoped raw-BLAKE3 CAS
artifacts and hydrate only when read. Complete manifest compilation remains
available for existing callers. Unresolved root merges fail explicitly.

Construct the scope from trusted repository configuration and its effective
provider security identity. One compiler retains bounded file authorization
across root switches and existing read pins. Lazy metadata accounting covers
one active adapter and restarts on reattachment; persisted namespace and provider
limits separately bound durable state. This library is not a retention proof,
a native mount frontend or a complete commit-closure validator.
