#!/bin/sh
# Only disposable fixture services and data are used. Never inherit a daemon RC.
set -eu
umask 077
: "${TINYTREE_ACCEPTANCE_BAZEL:?explicit pinned Bazel ELF required}"
: "${TINYTREE_ACCEPTANCE_RBE_SOCKET:?explicit trusted RBE Unix socket required}"
: "${TINYTREE_ACCEPTANCE_RECEIPTS:?existing owner-private absolute receipt directory required}"
: "${TINYTREE_ACCEPTANCE_TEST_BINARY:?use the declared manual fixture target}"
if [ ! -c /dev/fuse ] || [ ! -r /dev/fuse ] || [ ! -w /dev/fuse ]; then
  echo 'Real Linux/FUSE unavailable; native self-hosted acceptance was not executed.' >&2
  exit 1
fi
exec "$TINYTREE_ACCEPTANCE_TEST_BINARY" --ignored --exact native_self_hosted_backend_backup_restore_checks_and_lands_from_fresh_client --nocapture --test-threads=1
