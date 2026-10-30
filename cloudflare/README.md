# Experimental Cloudflare repository backend

This is an opt-in native repository storage service built with Workers,
SQLite-backed Durable Objects, and R2. It does not proxy to a Linux server.
The existing `tinytree-server` and its filesystem/Fjall Store remain the
implementation for the complete Forge API.

## Scope

This backend supports one raw repository per configured account, verified
immutable native objects, reference generation/head compare-and-set, durable
request replay, and explicit commit pins. `GET /v1/capabilities` reports the
supported raw operations and resource limits and requires authentication.

An independently gated `POST /api/v1/forge/call` endpoint supports account
provisioning, account snapshots and membership changes (operations 1, 2 and 3),
plus view publication, discovery and retirement (operations 4, 5 and 11).
Prepared snapshots, README/file previews and scoped object ranges are available
through operations 6, 7 and 9. Operation 8 provides bounded, policy-scoped
code search, and operation 10 supports actor-owned scoped object uploads.
The bundled browser starts with the pinned README and code search.
Set `FORGE_BACKEND=enabled` and use a `kind=forge` identity for that endpoint.
Account ownership, membership, policy epochs and replay receipts are SQLite
authority; ordinary Forge operations cannot use raw identities. Provisioning
requires an operator identity, and an existing unowned raw repository cannot
be silently adopted. Once Forge owns an account, all raw object/ref/pin routes
for it are fenced, including operator calls.

View definitions use the native version identity, current membership policy and
durable replay rules. Publication requires an already validated canonical root;
this is not an arbitrary-digest import path. Anonymous reads are admitted only
for configured Forge accounts and still require current public-view policy.
Invalid supplied credentials never become anonymous access. Public reads expose
only the exact sanitized projection, not canonical objects, ancestry or private
copy origins.

Cold snapshots return bounded pending progress, pinned to an immutable view and
current policy epoch. The SDK and `tt view show` continue that same preparation;
there is no unnamed/latest restart or local fallback. Use the explicit
`forge-https-v1` transport when selecting a CLI context. Account/view publication
does not register a local workspace provider automatically.

Review operations 20–23 provide immutable patchset admission, server-side candidate
preparation, exact-candidate decisions and comments. Large source admission returns
bounded `admission_pending` progress before reserving a patchset; the SDK and CLI
continue the original saved intent. Candidate construction uses the shared native
rebase engine, and published result reads use a separate sanitized projection.
Preparation does not execute checks: successful candidates remain `checks_pending`
until a separately authenticated execution-only checker returns the exact
physical receipt. Operation 24 atomically publishes only a current candidate
with a fresh accepted passing check, required approvals, verified closure and
matching main head and generation. Dependent candidates must satisfy the same
obligations. Approval votes alone never publish a change.

General queries, transforms and workspace backup remain unsupported. The
complete `tt` Forge workflow is not yet supported by this target. Unsupported
operations fail explicitly; there is no fallback to the local backend. Do not
expose it as an equivalent public Forge service or switch an existing account
between the two authorities.

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
The declared `tt` binary also exercises account commands against the actual
workerd endpoint and resumes an exact completed intent after SQLite/R2 restart;
it drives cold preparation of a native-generated 701-path view and verifies the
native snapshot. Separate storage/projection cases cover revocation, scoped
ranges, closure progress and rollback. These do not establish the full native
workspace, deployed browser or checked-landing workflow.

## Authority and authentication

One Durable Object owns each account's metadata and reference transactions.
R2 keys include the account and native object kind and digest. Objects must be
canonically verified before upload. A durable pending metadata reservation
precedes the R2 write; logical visibility and the completed request receipt
follow confirmation of the immutable bytes. Reference changes and their replay
receipts commit in the same SQLite transaction.

Every request is authenticated against the operator-configured
`PRINCIPALS_JSON` secret. Each record contains `principal`, `token_sha256`,
`accounts`, `operator`, and `kind` (`raw` or `forge`); `token_sha256` is the lowercase SHA-256 digest of a
separately distributed high-entropy bearer token. For raw identities, account
grants authorize raw repository reads and writes; `operator` adds provisioning
authority only. For Forge identities, this list is an admission ceiling, not
membership or view authority: every operation checks the current SQLite policy.
They are not Forge view permissions. Provisioning requires
an operator grant, and neither request bodies nor object digests grant access.

Review submission additionally requires an operator-selected
`FORGE_REVIEW_CONFIG_JSON` secret containing `profile` (`name` and a 64-character
lowercase hexadecimal `version`) and `committer` (`name` and `email`). Public
requests cannot select these trusted fields. The version must identify the
intended immutable check profile; configuring it does not run a checker or
provide a check receipt. Missing configuration refuses submission.

The separate `POST /api/v1/checker/call` endpoint uses `CHECKERS_JSON`, not Forge
membership or raw identities. Each record contains `principal`, `token_sha256`,
`accounts` and exact `profiles` (`name` and `version`). The checker can claim only
server-selected candidates for its granted account/profile, read their current
snapshot, renew custody and deliver results. It cannot approve or land changes,
read arbitrary objects, or access canonical ancestry and copy-origin payloads.
`FORGE_CHECK_POLICY_JSON` freezes `profile`, exact `targets` and the `test`
boolean; it must agree with the selected review profile. Missing or changed
policy cannot turn an old receipt into a passing current check.

The Linux [check runner](../crates/tinytree-check-runner/README.md) downloads and
independently verifies the candidate, executes pinned Bazel against a readonly
snapshot, and delivers a result only after cleanup. Expiry is not cleanup:
uncertain execution custody and retained roots require explicit recovery.
Result acceptance does not publish main; operation 24 performs that separate
transaction. This is operator-trusted execution, not hostile-tenant isolation.

Raw requests are disabled unless `EXPERIMENTAL_RAW_BACKEND` is explicitly set to
`enabled`. Never put bearer tokens or principal secrets in source, Wrangler
configuration, or a public bucket. Cloudflare TLS does not replace application
authorization.

## Deployment selection

Choose the local server deployment for complete Forge behavior, or this Worker
deployment for the supported experimental repository and Forge operations. The
Worker's `wrangler.jsonc` describes a new SQLite Durable Object namespace and
an R2 binding; it selects no account, zone, hostname, or credentials and disables
automatic public preview hostnames.

Deployment is a separate operator action. Provision the intended resources,
review bindings and access policy, configure secrets and the explicit opt-in,
then validate the real endpoint before relying on acknowledged writes. The
manual `//cloudflare:wrangler` tool is not run by build or test targets.
Do not point the local Store's filesystem path at an R2 bucket.
