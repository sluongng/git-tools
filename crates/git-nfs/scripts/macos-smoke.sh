#!/bin/sh
# Actual macOS kernel NFS acceptance, not a Linux protocol substitute.
# Prerequisite: PATH git must support cat-file remote-object-info on both client
# and file:// upload-pack server. No content-fetch fallback is allowed for sizes.
set -eu
[ "$(uname -s)" = Darwin ] || { echo 'This verifier requires macOS.' >&2; exit 1; }
: "${GIT_NFS_BIN:?absolute path to git-nfs}"
case "$GIT_NFS_BIN" in /*) ;; *) echo 'GIT_NFS_BIN must be absolute.' >&2; exit 1 ;; esac
[ -x "$GIT_NFS_BIN" ] || { echo 'GIT_NFS_BIN is not executable.' >&2; exit 1; }
TASK_TMP=$(mktemp -d "${TMPDIR:-/tmp}/git-nfs-smoke.XXXXXX")
TASK_PID=
TASK_MOUNTED=0
TASK_GENERATION=0
privileged() {
  if [ "$(id -u)" = 0 ]; then "$@"; else sudo -n "$@"; fi
}
await_exit() {
  kill -s "$1" "$TASK_PID" 2>/dev/null || true
  TASK_STOP_ATTEMPTS=0
  while kill -0 "$TASK_PID" 2>/dev/null; do
    TASK_STOP_ATTEMPTS=$((TASK_STOP_ATTEMPTS + 1))
    [ "$TASK_STOP_ATTEMPTS" -le "$2" ] || return 1
    sleep 1
  done
}
stop() {
  if [ "$TASK_MOUNTED" = 1 ]; then
    privileged umount "$TASK_TMP/mount" || return 1
    TASK_MOUNTED=0
  fi
  if [ -n "$TASK_PID" ]; then
    # Escalation is safe only after the mount has successfully gone away.
    # Do not enter an unbounded wait for a child stuck flushing/fetching.
    if ! await_exit INT 10; then
      echo "Server $TASK_PID did not stop after SIGINT; escalating to SIGTERM." >&2
      if ! await_exit TERM 5; then
        echo "Server $TASK_PID did not stop after SIGTERM; escalating to SIGKILL." >&2
        await_exit KILL 3 || return 1
      fi
    fi
    wait "$TASK_PID" 2>/dev/null || true
    TASK_PID=
  fi
}
cleanup() {
  TASK_RESULT=$?
  trap - EXIT INT TERM
  # Do not kill a server underneath a mount whose unmount failed.
  if ! stop; then
    echo "Cleanup failed; inspect mounted export and server PID $TASK_PID." >&2
    TASK_RESULT=1
  fi
  echo "Evidence retained in $TASK_TMP"
  exit "$TASK_RESULT"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir "$TASK_TMP/source" "$TASK_TMP/mount"
git -C "$TASK_TMP/source" init --template=
printf 'tracked\n' > "$TASK_TMP/source/tracked"
# Distinct blob deliberately left untouched during the initial lazy-read check.
printf 'unrelated cold content\n' > "$TASK_TMP/source/unrelated"
ln -s tracked "$TASK_TMP/source/link"
git -C "$TASK_TMP/source" add .
git -C "$TASK_TMP/source" -c user.name=Test -c user.email=test@example.invalid commit -qm fixture
git -C "$TASK_TMP/source" config uploadpack.allowFilter true
# These capabilities are scoped to this disposable local acceptance origin.
git -C "$TASK_TMP/source" config transfer.advertiseObjectInfo true
git -C "$TASK_TMP/source" config uploadpack.allowAnySHA1InWant true
# file:// forces upload-pack; an ordinary local clone ignores blob filtering.
git clone --bare --no-local --filter=blob:none "file://$TASK_TMP/source" "$TASK_TMP/repo.git"
[ "$(git -C "$TASK_TMP/repo.git" config remote.origin.promisor)" = true ]
[ "$(git -C "$TASK_TMP/repo.git" config remote.origin.partialclonefilter)" = blob:none ]
TASK_TRACKED_OID=$(git -C "$TASK_TMP/source" rev-parse HEAD:tracked)
TASK_UNRELATED_OID=$(git -C "$TASK_TMP/source" rev-parse HEAD:unrelated)
object_state() {
  # --missing=print does not fetch missing objects, unlike a plain cat-file probe.
  git -C "$TASK_TMP/repo.git" rev-list --objects --missing=print HEAD > "$TASK_TMP/objects.$1"
}
missing() { grep -Fqx "?$2" "$TASK_TMP/objects.$1"; }
present() { grep -Eq "^$2( |$)" "$TASK_TMP/objects.$1"; }
object_state before-start
missing before-start "$TASK_TRACKED_OID"
missing before-start "$TASK_UNRELATED_OID"
git --version > "$TASK_TMP/git-version"
command -v git > "$TASK_TMP/git-path"
if ! printf 'remote-object-info origin %s %s\n' "$TASK_TRACKED_OID" "$TASK_UNRELATED_OID" | \
  git -C "$TASK_TMP/repo.git" -c protocol.version=2 cat-file \
    '--batch-command=%(objectname) %(objectsize)' > "$TASK_TMP/remote-sizes" 2> "$TASK_TMP/remote-sizes.stderr"; then
  echo 'PATH git must support remote-object-info; this verifier does not fetch contents to discover sizes.' >&2
  cat "$TASK_TMP/remote-sizes.stderr" >&2
  exit 1
fi
grep -Fxq "$TASK_TRACKED_OID $(git -C "$TASK_TMP/source" cat-file -s "$TASK_TRACKED_OID")" "$TASK_TMP/remote-sizes"
grep -Fxq "$TASK_UNRELATED_OID $(git -C "$TASK_TMP/source" cat-file -s "$TASK_UNRELATED_OID")" "$TASK_TMP/remote-sizes"
object_state after-size-query
missing after-size-query "$TASK_TRACKED_OID"
missing after-size-query "$TASK_UNRELATED_OID"
start() {
  TASK_GENERATION=$((TASK_GENERATION + 1))
  TASK_LOG="$TASK_TMP/server.$TASK_GENERATION.log"
  "$GIT_NFS_BIN" --repo "$TASK_TMP/repo.git" --overlay "$TASK_TMP/state" \
    --mountpoint "$TASK_TMP/mount" --listen 127.0.0.1:0 > "$TASK_LOG" 2>&1 &
  TASK_PID=$!
  TASK_ATTEMPTS=0
  until grep -q '^NFS export ready on 127.0.0.1:' "$TASK_LOG"; do
    if ! kill -0 "$TASK_PID" 2>/dev/null; then cat "$TASK_LOG" >&2; return 1; fi
    TASK_ATTEMPTS=$((TASK_ATTEMPTS + 1))
    [ "$TASK_ATTEMPTS" -lt 30 ] || { echo 'Server readiness exceeded 30 seconds.' >&2; return 1; }
    sleep 1
  done
  TASK_PORT=$(sed -n 's/^NFS export ready on 127\.0\.0\.1:\([0-9][0-9]*\);.*/\1/p' "$TASK_LOG")
  [ -n "$TASK_PORT" ] || { echo 'Could not determine NFS listener port.' >&2; return 1; }
  # No interactive password prompts in a remote verifier. Obtain sudo authority
  # beforehand, or run this script from a root shell on the native Mac.
  privileged mount_nfs -o "vers=3,tcp,nolocks,resvport,port=$TASK_PORT,mountport=$TASK_PORT,nobrowse,nosuid,nodev" \
    127.0.0.1:/ "$TASK_TMP/mount"
  TASK_MOUNTED=1
}
start
object_state after-mount
missing after-mount "$TASK_TRACKED_OID"
missing after-mount "$TASK_UNRELATED_OID"
[ "$(cat "$TASK_TMP/mount/tracked")" = tracked ]
object_state after-read
present after-read "$TASK_TRACKED_OID"
missing after-read "$TASK_UNRELATED_OID"
printf 'Lazy hydration PASS: accessed blob fetched; unrelated blob remains missing.\n'
# Subsequent Git status/commit may legitimately read additional indexed files;
# lazy-read assertions intentionally precede those operations.
[ "$(readlink "$TASK_TMP/mount/link")" = tracked ]
printf 'edited\n' > "$TASK_TMP/mount/tracked"
mkdir "$TASK_TMP/mount/dir"
printf 'new\n' > "$TASK_TMP/mount/dir/file"
mv "$TASK_TMP/mount/dir/file" "$TASK_TMP/mount/new"
rmdir "$TASK_TMP/mount/dir"
git -C "$TASK_TMP/mount" status --short > "$TASK_TMP/status.before"
git -C "$TASK_TMP/mount" add tracked new
git -C "$TASK_TMP/mount" -c user.name=Test -c user.email=test@example.invalid commit -qm edit
git -C "$TASK_TMP/mount" log -1 --format=%s | grep '^edit$'
stop
start
[ "$(cat "$TASK_TMP/mount/tracked")" = edited ]
[ "$(cat "$TASK_TMP/mount/new")" = new ]
[ ! -e "$TASK_TMP/mount/dir" ]
git -C "$TASK_TMP/mount" status --porcelain > "$TASK_TMP/status.after-restart"
[ ! -s "$TASK_TMP/status.after-restart" ]
rm "$TASK_TMP/mount/new"
stop
start
[ ! -e "$TASK_TMP/mount/new" ]
[ ! -L "$TASK_TMP/mount/new" ]
[ ! -e "$TASK_TMP/mount/dir" ]
[ "$(cat "$TASK_TMP/mount/tracked")" = edited ]
git -C "$TASK_TMP/mount" status --porcelain > "$TASK_TMP/status.after-delete-restart"
grep -Fxq ' D new' "$TASK_TMP/status.after-delete-restart"
printf '\nmacOS native mount, lazy read, writes, Git commit and deletion across restart PASS\n'
