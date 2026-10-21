# Pinned protocol sources

Unmodified Apache-2.0 sources from:

- bazelbuild/remote-apis: 6def1c5d27a527c400875c24ae8b1a160145d7e1
- googleapis/googleapis: e7513c828b8db536fa12fc5493d541f323aedf90

Generated at build time with vendored protoc; no network access is needed.

The Build Event Service files under `google/devtools/build/v1` are pinned to
buildbuddy-io/buildbuddy `15882ddf1aee6ff08cda2ce603b3a344c1d3950f`
(`proto/{publish_build_event,build_events,build_status}.proto`, Apache-2.0).
Their imports use the canonical Google paths. The streaming request additionally
includes `stream_metadata` (tag 8), as carried by Bazel's `googleapis.patch`;
its internal-only visibility annotation is omitted. BuildBuddy's experimental
tool-event field (tag 1000) is retained. Bazel event and stream-metadata `Any`
payload bytes remain opaque.
