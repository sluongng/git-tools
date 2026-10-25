#!/bin/sh
set -eu
export TMPDIR=/tmp
export TINYTREE_GIT_TEST_TOKEN=public-git-export-loopback-token
exec "$TINYTREE_GIT_FIXTURE" "$@"
