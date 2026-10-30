# Experimental Cloudflare repository backend

This is an opt-in native repository storage service built with Workers,
SQLite-backed Durable Objects, and R2. It does not proxy to a Linux server.
The existing `tinytree-server` and its filesystem/Fjall Store remain unchanged
and remain the implementation for the complete Forge API.

## Scope

This backend supports one raw repository per configured account, verified
immutable native objects, reference generation/head compare-and-set, durable
request replay, and explicit commit pins. `GET /v1/capabilities` reports the
supported operations and resource limits and requires authentication.

It does **not** implement the Forge review, filtered-view, query, transform,
workspace-backup, or checked-landing APIs. The normal `tt` Forge workflow is
not yet supported by this target. Unsupported operations fail explicitly;
there is no fallback to the local backend. Do not expose it as an equivalent
public Forge service or switch an existing account between the two authorities.

There is no object-deletion API or garbage collector in this first backend.
Pending uploads and their byte reservations count toward finite account limits.
Pin release does not delete data, and old replay receipts are not silently
discarded to make room for new requests. Reaching a limit fails closed.

## Build and verify

Use the repository's Bazel workflow; Cargo is dependency metadata only.
JavaScript tools, Node, workerd, and package dependencies are pinned and declared.

```sh
bazel build --config=rbe //cloudflare:worker
bazel test --config=rbe \
  //crates/tinytree-cloudflare:unit_tests \
  //cloudflare:typecheck \
  //cloudflare:runtime_tests
```

The bundle contains `worker.mjs` and the Bazel-built `core.wasm`. The Wasm
module uses `tinytree-model` for canonical hashing, decoding, and verification.
It also validates complete commit closures, leaf lengths, and contextual
CopyHistory paths. JavaScript does not implement a separate native object codec.

Runtime tests use local workerd/Miniflare with SQLite Durable Objects and R2
bindings. A declared Rust fixture generates canonical objects and expected
reference/replay results using the existing local Store. These tests do not
establish deployed provider durability, availability, latency, or billing.

## Authority and authentication

One Durable Object owns each account's metadata and reference transactions.
R2 keys include the account and native object kind and digest. Objects must be
canonically verified before upload. A durable pending metadata reservation
precedes the R2 write; logical visibility and the completed request receipt
follow confirmation of the immutable bytes. Reference changes and their replay
receipts commit in the same SQLite transaction.

Every request is authenticated against the operator-configured
`PRINCIPALS_JSON` secret. Each record contains `principal`, `token_sha256`,
`accounts`, and `operator`; `token_sha256` is the lowercase SHA-256 digest of a
separately distributed high-entropy bearer token. Account grants authorize raw
repository reads and writes; `operator` adds provisioning authority only. They are not Forge view permissions. Provisioning requires
an operator grant, and neither request bodies nor object digests grant access.

Requests are disabled unless `EXPERIMENTAL_RAW_BACKEND` is explicitly set to
`enabled`. Never put bearer tokens or principal secrets in source, Wrangler
configuration, or a public bucket. Cloudflare TLS does not replace application
authorization.

## Deployment selection

Choose the local server deployment for complete Forge behavior, or this Worker
deployment for the supported experimental raw repository operations. The
Worker's `wrangler.jsonc` describes a new SQLite Durable Object namespace and
an R2 binding; it selects no account, zone, hostname, or credentials and disables
automatic public preview hostnames.

Deployment is a separate operator action. Provision the intended resources,
review bindings and access policy, configure secrets and the explicit opt-in,
then validate the real endpoint before relying on acknowledged writes. The
manual `//cloudflare:wrangler` tool is not run by build or test targets.
Do not point the local Store's filesystem path at an R2 bucket.
