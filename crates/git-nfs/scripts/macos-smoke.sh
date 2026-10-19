#!/bin/sh
# Actual macOS kernel NFS acceptance, not a Linux protocol substitute.
set -eu
[ "$(uname -s)" = Darwin ] || { echo 'This verifier requires macOS.' >&2; exit 1; }
: "${GIT_NFS_BIN:?absolute path to git-nfs}"
TASK_TMP=$(mktemp -d "${TMPDIR:-/tmp}/git-nfs-smoke.XXXXXX")
TASK_PID=
TASK_MOUNTED=0
cleanup() {
  [ "$TASK_MOUNTED" = 0 ] || sudo umount "$TASK_TMP/mount"
  [ -z "$TASK_PID" ] || { kill "$TASK_PID" 2>/dev/null || true; wait "$TASK_PID" 2>/dev/null || true; }
  echo "Evidence retained in $TASK_TMP"
}
trap cleanup EXIT INT TERM
mkdir "$TASK_TMP/source" "$TASK_TMP/mount"
git -C "$TASK_TMP/source" init --template=
printf 'tracked\n' > "$TASK_TMP/source/tracked"
ln -s tracked "$TASK_TMP/source/link"
git -C "$TASK_TMP/source" add .
git -C "$TASK_TMP/source" -c user.name=Test -c user.email=test@example.invalid commit -qm fixture
git clone --bare "$TASK_TMP/source" "$TASK_TMP/repo.git"
start() {
  "$GIT_NFS_BIN" --repo "$TASK_TMP/repo.git" --overlay "$TASK_TMP/state" \
    --mountpoint "$TASK_TMP/mount" --listen 127.0.0.1:11111 > "$TASK_TMP/server.log" 2>&1 &
  TASK_PID=$!
  # The native mount command retries while the user-mode listener starts.
  sudo mount_nfs -o vers=3,tcp,nolocks,resvport,port=11111,mountport=11111,nobrowse,nosuid,nodev \
    127.0.0.1:/ "$TASK_TMP/mount"
  TASK_MOUNTED=1
}
start
[ "$(cat "$TASK_TMP/mount/tracked")" = tracked ]
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
sudo umount "$TASK_TMP/mount"; TASK_MOUNTED=0
kill "$TASK_PID"; wait "$TASK_PID" 2>/dev/null || true; TASK_PID=
start
[ "$(cat "$TASK_TMP/mount/tracked")" = edited ]
[ "$(cat "$TASK_TMP/mount/new")" = new ]
[ -z "$(git -C "$TASK_TMP/mount" status --porcelain)" ]
rm "$TASK_TMP/mount/new"
printf '\nmacOS kernel mount, writes, Git commit, restart and status PASS\n'
