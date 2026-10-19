# Protocol provenance

`bazel_output_service*.proto` are unmodified from
[Bazel 9.2.0](https://github.com/bazelbuild/bazel/tree/9.2.0/src/main/protobuf),
Apache-2.0; see `LICENSE.bazel` and each file's copyright header.
Imports use the pinned `git-reapi/proto` definitions; generated REAPI and
`google.rpc` Rust types are reused from `git-reapi` (not duplicated).
