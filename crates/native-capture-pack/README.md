# native-capture-pack · library

Shared framing and bounded verification for immutable local workspace captures.
Checks section hashes, offsets, the canonical table, exact EOF and whole-pack
identity without loading the complete pack into memory.

Portable leases expose only OP/VIEW, index and capsule sections. Private
working-copy and sidecar sections are verified but have no portable read API.
Run blocking reads on an admitted, owned worker; leases and returned buffers
retain the private file, directory and resource reservations.

Structural verification is not semantic object proof, frontend quiescence,
remote backup acknowledgement, restore readiness or GC permission.
