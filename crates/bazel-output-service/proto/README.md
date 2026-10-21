# Bazel output-service protocols

Pinned protocol inputs for the [`bazel-output-service`](../README.md) library
and `git-output-service` executable, not a separate crate or service.

`bazel_output_service*.proto` come from
[Bazel 9.2.0](https://github.com/bazelbuild/bazel/tree/9.2.0/src/main/protobuf),
under Apache-2.0; see [LICENSE.bazel](LICENSE.bazel) and the copyright headers.
Build-time generation uses the pinned [`reapi` protocols](../../reapi/proto/)
and reuses that crate's REAPI and `google.rpc` Rust types.
