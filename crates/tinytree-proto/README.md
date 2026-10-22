# tinytree-proto · library

Generated Rust clients, servers and descriptors for the native Tinytree
repository gRPC protocol. Git storage and transformation are separate services.
Canonical object encoding lives in [tinytree-model](../tinytree-model), not in
protobuf serialization.

Builders support zstd negotiation. Authentication, admission, timeout policy and
durable storage must be supplied by a service implementation.

Build and check the contract with
`bazel test --config=rbe //crates/tinytree-proto:unit_tests`.
