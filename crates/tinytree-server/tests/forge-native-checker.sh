#!/bin/sh
# Genuine host-kernel acceptance; binaries are declared runfiles, not host PATH.
set -eu
umask 077
: "${TINYTREE_ACCEPTANCE_BAZEL:?supply an explicit pinned Bazel ELF}"
: "${TINYTREE_ACCEPTANCE_RBE_SOCKET:?supply an explicit trusted RBE Unix socket}"
: "${TINYTREE_ACCEPTANCE_RECEIPTS:?supply an ignored owner-private evidence directory}"
: "${TINYTREE_ACCEPTANCE_TEST_BINARY:?use the manual fixture target}"
if [ ! -c /dev/fuse ] || [ ! -r /dev/fuse ] || [ ! -w /dev/fuse ]; then
  echo 'Real Linux/FUSE capability is unavailable; native acceptance was not executed.' >&2
  exit 1
fi
export TINYTREE_ACCEPTANCE_CHECKER_TOKEN='native-acceptance-token-not-a-secret'
exec "$TINYTREE_ACCEPTANCE_TEST_BINARY" --ignored --exact forge_native_checker_acceptance::actual_account_candidate_is_physically_remote_checked_then_approved_and_landed --nocapture
