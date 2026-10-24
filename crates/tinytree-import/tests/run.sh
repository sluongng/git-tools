#!/bin/sh
set -eu
# Use the execution image's trusted sticky temporary root. Bazel scratch ancestors
# may be writable; production private-directory checks must not be weakened.
export TMPDIR=/tmp
export TINYTREE_IMPORT_TEST_TOKEN=public-import-loopback-token-not-secret
exec "$TINYTREE_IMPORT_FIXTURE" "$@"
