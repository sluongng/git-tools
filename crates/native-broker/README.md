# native-broker · library

Private daemon-owned native object I/O. `BrokerService` maps configured opaque
store IDs to shared `NativeObjectIo` providers; clients never submit store paths.
`BrokerObjectIo` implements the same provider interface for independent jj
processes, including callers without a Tokio runtime.

Embed `rpc_service()` directly: it includes pre-decode admission and independent
response-body deadlines. `serve_local` takes an explicit `LocalEndpoint`: Unix
sockets require a trusted owner-private directory; Windows named pipes require
an owner-only protected DACL and same-user peers. Remote pipe names are rejected.
Existing endpoints are never taken over or removed. `connect_local` selects the
same transport for both clients; existing Unix convenience APIs remain available.
All methods negotiate zstd and bound frames, metadata, catalogs and temporary
bytes. File reads verify the complete stream in an anonymous private spool
before exposing a pinned reader. Catalog streams carry a checked completion
record and come from one server-side snapshot.

`max_temporary_bytes` bounds client verified spools and server write staging
across all store aliases. File writes reserve both temporary copies; metadata
writes reserve their canonical byte length until provider work finishes, even
after cancellation. Permanent store objects and external CAS are not covered.

This is a local-I/O component, not a remote repository service, jj operation
store, retention implementation or garbage collector. Use [the native CLI](../jj-native-cli) with a configured private host.
The direct provider still retains local objects.

Optional `NativeRepositoryControl` adapters add observation, admission and public
publication for an alias. `BrokerControlClient` uses explicit workspace-observed
CAS generations; the host never substitutes its latest value. Control shares
ObjectIo's pools and conservatively reserves the whole memory/staging budget,
serializing host control work. Adapter limits are checked before registration.

`//crates/native-broker:rpc_tests` uses actual independent client processes with
Git unavailable, shared-store concurrency, host restart and cancellation tests.

`:endpoint_tests` checks endpoint validation. `:windows_ipc_tests` is a Windows
native suite covering independent clients, protected-pipe connection, existing
instance refusal, and insecure-server rejection. Cross-compilation alone is not
native transport acceptance.

Optional `NativeQueryIo` adapters and `BrokerQueryClient` provide bounded graph
queries, path history and literal search through the same local endpoint.
Handles are broker-local; upstream
credentials and cursors stay in the configured adapter. Distinct repository or
trust scopes require distinct adapters; aliases explicitly sharing one adapter
are trusted synonyms for its handles. Partial-query close is
explicit cancellation, not proof of exhaustion. Returned pages retain byte/root
owners independently of transport slots.
Both clients support clone-local, shrink-only absolute operation deadlines.

`max_query_bytes` defaults to 512 KiB. The separate
`max_query_persistent_bytes` defaults to 512 MiB and bounds the sum of declared
registry/replay/SDK pools across distinct adapters, not aliases sharing one
adapter. These are worst-case limits, not preallocated memory. Registration
fails explicitly when the aggregate cap is too small; lower per-repository
limits or configure a larger aggregate cap.
