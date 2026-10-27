#!/bin/sh
set -eu
export TMPDIR=/tmp
export TINYTREE_GIT_TEST_TOKEN=public-git-export-loopback-token
export TINYTREE_HELPER_FIXTURE_ENV=neutral-helper-only-secret
exec "$TINYTREE_GIT_FIXTURE" "$@"
