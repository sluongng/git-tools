#!/bin/sh
# Real loopback provider for production embedded UI browser acceptance.
set -eu
umask 077
: "${TINYTREE_BROWSER_READY:?supply a fresh readiness filename in an owner-private ignored directory}"
: "${TINYTREE_BROWSER_TEST_BINARY:?use the manual browser provider fixture target}"
exec "$TINYTREE_BROWSER_TEST_BINARY" --ignored --exact service::web::tests::forge_browser_provider_acceptance --nocapture
