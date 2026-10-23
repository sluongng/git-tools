# native-broker · library

Private daemon-owned native object I/O. `BrokerService` maps configured opaque
store IDs to shared `NativeObjectIo` providers; clients never submit store paths.
`BrokerObjectIo` implements the same provider interface for independent jj
processes, including callers without a Tokio runtime.

Embed `rpc_service()` directly: it includes pre-decode admission and independent
response-body deadlines. `serve_unix` binds only in a trusted owner-private
directory and verifies peer ownership. Existing endpoints are never removed.
All methods negotiate zstd and bound frames, metadata, catalogs and temporary
bytes. File reads verify the complete stream in an anonymous private spool
before exposing a pinned reader. Catalog streams carry a checked completion
record and come from one server-side snapshot.

`max_temporary_bytes` bounds client verified spools and server write staging
across all store aliases. File writes reserve both temporary copies; metadata
writes reserve their canonical byte length until provider work finishes, even
after cancellation. Permanent store objects and external CAS are not covered.

This is a Unix local-I/O component, not a remote repository service, jj operation
store, retention implementation or garbage collector. Use [the native CLI](../jj-native-cli) with a configured private host.
The direct provider still retains local objects.

`//crates/native-broker:rpc_tests` uses actual independent client processes with
Git unavailable, shared-store concurrency, host restart and cancellation tests.
