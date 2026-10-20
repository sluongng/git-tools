use nfsserve::{
    nfs::*,
    tcp::{NFSTcp, NFSTcpListener},
    vfs::NFSFileSystem,
};
use std::path::Path;
use std::process::Command;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use workspace_core::{
    backend::{GitRepo, SizeFallback},
    filesystem::Snapshot,
};
use workspace_nfs::GitNfs;
use workspace_overlay::{overlay::Overlay, worktree::WorkingTree};

fn git(path: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn fixture() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    git(dir.path(), &["init", "--template=", "source"]);
    let source = dir.path().join("source");
    std::fs::write(source.join("hello.txt"), b"hello from Git\n").unwrap();
    std::fs::write(source.join("cold.bin"), vec![23u8; 128 * 1024]).unwrap();
    std::os::unix::fs::symlink("hello.txt", source.join("link")).unwrap();
    git(&source, &["add", "."]);
    git(
        &source,
        &[
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "-qm",
            "fixture",
        ],
    );
    git(&source, &["config", "uploadpack.allowFilter", "true"]);
    let url = format!("file://{}", source.display());
    git(
        dir.path(),
        &["clone", "--bare", "--filter=blob:none", &url, "repo.git"],
    );
    dir
}
fn open(dir: &Path) -> GitNfs {
    open_with_git(dir, Path::new("git"))
}
fn open_with_git(dir: &Path, git: &Path) -> GitNfs {
    let repo = GitRepo::open(dir.join("repo.git"), git, "origin", SizeFallback::Fetch).unwrap();
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let working = WorkingTree::lock(&dir.join("state")).unwrap();
    let overlay = Overlay::open(&working.directory, &snapshot, &working.gitdir).unwrap();
    GitNfs::new(repo, overlay, working).unwrap()
}
fn word(out: &mut Vec<u8>, n: u32) {
    out.extend_from_slice(&n.to_be_bytes());
}
fn opaque(out: &mut Vec<u8>, bytes: &[u8]) {
    word(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
    while !out.len().is_multiple_of(4) {
        out.push(0);
    }
}
async fn rpc(port: u16, procedure: u32, args: Vec<u8>) -> Vec<u8> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    rpc_stream(&mut stream, procedure, args).await
}
async fn rpc_stream(stream: &mut tokio::net::TcpStream, procedure: u32, args: Vec<u8>) -> Vec<u8> {
    rpc_stream_xid(stream, 42, procedure, args).await
}
async fn rpc_stream_xid(
    stream: &mut tokio::net::TcpStream,
    xid: u32,
    procedure: u32,
    args: Vec<u8>,
) -> Vec<u8> {
    let mut call = Vec::new();
    for n in [xid, 0, 2, 100003, 3, procedure, 0, 0, 0, 0] {
        word(&mut call, n);
    }
    call.extend(args);
    stream
        .write_u32(0x8000_0000 | call.len() as u32)
        .await
        .unwrap();
    stream.write_all(&call).await.unwrap();
    let mut reply = Vec::new();
    loop {
        let marker = stream.read_u32().await.unwrap();
        let len = (marker & 0x7fff_ffff) as usize;
        assert!(len < 2 * 1024 * 1024);
        let start = reply.len();
        reply.resize(start + len, 0);
        stream.read_exact(&mut reply[start..]).await.unwrap();
        if marker & 0x8000_0000 != 0 {
            break;
        }
    }
    assert_eq!(
        &reply[..24],
        [xid, 1, 0, 0, 0, 0].map(u32::to_be_bytes).as_flattened()
    );
    reply[24..].to_vec()
}

fn start_server(
    fs: GitNfs,
) -> (
    u16,
    tokio::sync::oneshot::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let (port_tx, port_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let server_fs = fs;
    let server_thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let server = NFSTcpListener::bind("127.0.0.1:0", server_fs).await.unwrap();
            port_tx.send(server.get_listen_port()).unwrap();
            tokio::select! { result = server.handle_forever() => result.unwrap(), _ = stop_rx => {} }
        });
        // Dropping the runtime terminates accepted-connection tasks too.
    });
    let port = port_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    (port, stop_tx, server_thread)
}

#[tokio::test]
async fn real_tcp_nfs_reads_and_file_sync_writes_persist() {
    let dir = fixture();
    let fs = open(dir.path());
    let cold = Command::new("git")
        .current_dir(dir.path().join("source"))
        .args(["rev-parse", "HEAD:cold.bin"])
        .output()
        .unwrap();
    let cold = String::from_utf8(cold.stdout).unwrap();
    let missing = || {
        let output = Command::new("git")
            .current_dir(dir.path().join("repo.git"))
            .args(["rev-list", "--objects", "--missing=print", "HEAD"])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains(&format!("?{}", cold.trim()))
        );
    };
    missing();
    let id = fs.lookup(1, &b"hello.txt"[..].into()).await.unwrap();
    let fh = fs.id_to_fh(id).data;
    let (port, stop_tx, server_thread) = start_server(fs.clone());
    let mut args = Vec::new();
    opaque(&mut args, &fh);
    let attr = rpc(port, 1, args).await;
    assert_eq!(&attr[..8], &[0, 0, 0, 0, 0, 0, 0, 1]); // success, regular file
    let mut args = Vec::new();
    opaque(&mut args, &fh);
    word(&mut args, 0);
    word(&mut args, 0);
    word(&mut args, 100);
    let read = rpc(port, 6, args).await;
    assert_eq!(&read[..4], &[0; 4]);
    assert!(read.windows(15).any(|w| w == b"hello from Git\n"));
    let mut args = Vec::new();
    opaque(&mut args, &fh);
    word(&mut args, 0);
    word(&mut args, 0);
    word(&mut args, 5);
    word(&mut args, 2);
    opaque(&mut args, b"HELLO");
    let write = rpc(port, 7, args).await;
    assert_eq!(&write[..4], &[0; 4]);
    assert_eq!(
        &write[write.len() - 12..write.len() - 8],
        &2u32.to_be_bytes()
    ); // FILE_SYNC
    missing(); // The unaccessed 128 KiB blob was never hydrated.
    stop_tx.send(()).unwrap();
    server_thread.join().unwrap();
    drop(fs);
    let reopened = open(dir.path());
    assert_eq!(
        reopened.read(id, 0, 100).await.unwrap(),
        (b"HELLO from Git\n".to_vec(), true)
    );
}

#[tokio::test]
async fn writable_namespace_pagination_symlink_and_restart() {
    let dir = fixture();
    let fs = open(dir.path());
    let (folder, _) = fs.mkdir(1, &b"folder"[..].into()).await.unwrap();
    let (file, _) = fs
        .create(folder, &b"new"[..].into(), sattr3::default())
        .await
        .unwrap();
    fs.write(file, 3, b"tail").await.unwrap();
    assert_eq!(fs.read(file, 0, 100).await.unwrap().0, b"\0\0\0tail");
    fs.setattr(
        file,
        sattr3 {
            size: set_size3::size(4),
            ..sattr3::default()
        },
    )
    .await
    .unwrap();
    fs.rename(folder, &b"new"[..].into(), 1, &b"renamed"[..].into())
        .await
        .unwrap();
    let (link, _) = fs
        .symlink(
            1,
            &b"newlink"[..].into(),
            &b"renamed"[..].into(),
            &sattr3::default(),
        )
        .await
        .unwrap();
    assert_eq!(fs.readlink(link).await.unwrap().0, b"renamed");
    let first = fs.readdir(1, 0, 1).await.unwrap();
    assert_eq!(first.entries.len(), 1);
    assert!(!first.end);
    let next = fs.readdir(1, first.entries[0].fileid, 100).await.unwrap();
    assert!(next.end);
    assert!(
        next.entries
            .iter()
            .all(|e| e.fileid != first.entries[0].fileid)
    );
    assert!(
        fs.create_exclusive(1, &b"renamed"[..].into(), [1; 8])
            .await
            .is_err()
    );
    assert!(
        fs.create(1, &b"../escape"[..].into(), sattr3::default())
            .await
            .is_err()
    );
    fs.rmdir(1, &b"folder"[..].into()).await.unwrap();
    let hook = Command::new(env!("CARGO_BIN_EXE_git-nfs"))
        .args(["fsmonitor", "--overlay"])
        .arg(dir.path().join("state"))
        .args(["2", "initial"])
        .output()
        .unwrap();
    assert!(
        hook.status.success(),
        "{}",
        String::from_utf8_lossy(&hook.stderr)
    );
    assert!(hook.stdout.ends_with(&[0]));
    assert!(
        hook.stdout
            .split(|byte| *byte == 0)
            .any(|path| path == b"/")
    );
    let exclusive = fs
        .create_exclusive(1, &b"exclusive"[..].into(), [7; 8])
        .await
        .unwrap();
    drop(fs);
    let reopened = open(dir.path());
    assert_eq!(
        reopened
            .create_exclusive(1, &b"exclusive"[..].into(), [7; 8])
            .await
            .unwrap(),
        exclusive
    );
    assert!(
        reopened
            .create_exclusive(1, &b"exclusive"[..].into(), [8; 8])
            .await
            .is_err()
    );
    assert_eq!(
        reopened.lookup(1, &b"renamed"[..].into()).await.unwrap(),
        file
    );
    assert_eq!(reopened.read(file, 0, 100).await.unwrap().0, b"\0\0\0t");
    assert_eq!(reopened.readlink(link).await.unwrap().0, b"renamed");
    reopened.remove(1, &b"exclusive"[..].into()).await.unwrap();
    assert_eq!(
        std::fs::read_dir(dir.path().join("state/nfs-exclusive"))
            .unwrap()
            .count(),
        0
    );
    reopened.remove(1, &b"renamed"[..].into()).await.unwrap();
    assert!(reopened.getattr(file).await.is_err());
}

#[tokio::test]
async fn real_wire_directory_boundaries_exclusive_verifier_and_retransmission() {
    let dir = fixture();
    let fs = open(dir.path());
    let (folder, _) = fs.mkdir(1, &b"folder"[..].into()).await.unwrap();
    for index in 0..40 {
        fs.create(
            1,
            &format!("file-{index:03}").as_bytes().into(),
            sattr3::default(),
        )
        .await
        .unwrap();
    }
    let (port, stop, thread) = start_server(fs.clone());
    let root = fs.id_to_fh(1).data;
    let mut remove = Vec::new();
    opaque(&mut remove, &root);
    opaque(&mut remove, b"folder");
    assert_eq!(
        &rpc(port, 12, remove.clone()).await[..4],
        &21u32.to_be_bytes()
    );
    assert_eq!(&rpc(port, 13, remove).await[..4], &0u32.to_be_bytes());
    assert!(fs.getattr(folder).await.is_err());
    let mut small = Vec::new();
    opaque(&mut small, &root);
    small.extend([0; 16]);
    word(&mut small, 0);
    assert_eq!(&rpc(port, 16, small).await[..4], &10005u32.to_be_bytes());
    let first = fs.readdir_simple(1, 0, 1).await.unwrap().entries[0].fileid;
    let mut page = Vec::new();
    opaque(&mut page, &root);
    page.extend(first.to_be_bytes());
    page.extend([0; 8]);
    word(&mut page, 512);
    let result = rpc(port, 16, page).await;
    assert_eq!(&result[..4], &0u32.to_be_bytes());
    // status, post-op attr, cookie verifier, entry present, first file ID.
    assert_eq!(&result[100..104], &1u32.to_be_bytes());
    assert_ne!(
        u64::from_be_bytes(result[104..112].try_into().unwrap()),
        first
    );
    let mut exclusive = Vec::new();
    opaque(&mut exclusive, &root);
    opaque(&mut exclusive, b"exclusive-wire");
    word(&mut exclusive, 2);
    exclusive.extend([19; 8]);
    assert_eq!(
        &rpc(port, 8, exclusive.clone()).await[..4],
        &0u32.to_be_bytes()
    );
    assert_eq!(
        &rpc(port, 8, exclusive.clone()).await[..4],
        &0u32.to_be_bytes()
    );
    *exclusive.last_mut().unwrap() = 20;
    assert_eq!(&rpc(port, 8, exclusive).await[..4], &17u32.to_be_bytes());
    let mut guarded = Vec::new();
    opaque(&mut guarded, &root);
    opaque(&mut guarded, b"guarded");
    word(&mut guarded, 1);
    guarded.extend([0; 24]);
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let first_reply = rpc_stream(&mut stream, 8, guarded.clone()).await;
    assert_eq!(&first_reply[..4], &0u32.to_be_bytes());
    let replay = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        rpc_stream(&mut stream, 8, guarded),
    )
    .await
    .unwrap();
    assert_eq!(replay, first_reply);
    // A fresh guarded CREATE must not apply attributes to an existing file.
    let guarded_id = fs.lookup(1, &b"guarded"[..].into()).await.unwrap();
    fs.write(guarded_id, 0, b"keep").await.unwrap();
    let mut again = Vec::new();
    opaque(&mut again, &root);
    opaque(&mut again, b"guarded");
    word(&mut again, 1);
    for n in [0, 0, 0, 1, 0, 0, 0, 0] {
        word(&mut again, n);
    } // size = 0
    assert_eq!(&rpc(port, 8, again).await[..4], &17u32.to_be_bytes());
    assert_eq!(fs.read(guarded_id, 0, 100).await.unwrap().0, b"keep");
    // A rejected guarded truncate must neither mutate nor append a success reply.
    let mut truncate = Vec::new();
    opaque(&mut truncate, &fs.id_to_fh(guarded_id).data);
    for n in [0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0] {
        word(&mut truncate, n);
    }
    let rejected = rpc(port, 2, truncate).await;
    assert_eq!(&rejected[..4], &10002u32.to_be_bytes());
    assert_eq!(rejected.len(), 12); // status + empty weak-cache-consistency fields
    assert_eq!(fs.read(guarded_id, 0, 100).await.unwrap().0, b"keep");
    drop(stream);
    stop.send(()).unwrap();
    thread.join().unwrap();
}

#[tokio::test]
async fn guarded_operations_compare_and_mutate_atomically() {
    let dir = fixture();
    let fs = open(dir.path());
    let mut creates = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let fs = fs.clone();
        creates.spawn(async move {
            fs.create_guarded(1, &b"race"[..].into(), sattr3::default())
                .await
        });
    }
    let mut successes = Vec::new();
    while let Some(result) = creates.join_next().await {
        match result.unwrap() {
            Ok((id, _)) => successes.push(id),
            Err(nfsstat3::NFS3ERR_EXIST) => {}
            Err(error) => panic!("{error:?}"),
        }
    }
    assert_eq!(successes.len(), 1);
    let id = successes[0];
    fs.write(id, 0, b"retain").await.unwrap();
    let guard = fs.getattr(id).await.unwrap().ctime;
    let mut setters = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let fs = fs.clone();
        setters.spawn(async move {
            fs.setattr_guarded(
                id,
                sattr3 {
                    mode: set_mode3::mode(0o600),
                    ..sattr3::default()
                },
                Some(guard),
            )
            .await
        });
    }
    let mut successes = 0;
    while let Some(result) = setters.join_next().await {
        match result.unwrap() {
            Ok(_) => successes += 1,
            Err(nfsstat3::NFS3ERR_NOT_SYNC) => {}
            Err(error) => panic!("{error:?}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(fs.read(id, 0, 100).await.unwrap().0, b"retain");
}

#[tokio::test]
async fn malformed_nested_xdr_lengths_are_rejected_without_large_allocations() {
    let dir = fixture();
    let fs = open(dir.path());
    let (port, stop, thread) = start_server(fs.clone());
    let mut bad_handle = Vec::new();
    for n in [43, 0, 2, 100003, 3, 1, 0, 0, 0, 0, u32::MAX] {
        word(&mut bad_handle, n);
    }
    let mut bad_auth = Vec::new();
    for n in [44, 0, 2, 100003, 3, 1, 0, u32::MAX] {
        word(&mut bad_auth, n);
    }
    let mut bad_groups = Vec::new();
    for n in [45, 0, 2, 100003, 3, 1, 1, 20, 0, 0, 0, 0, u32::MAX, 0, 0] {
        word(&mut bad_groups, n);
    }
    for call in [bad_handle, bad_auth, bad_groups] {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        stream
            .write_u32(0x8000_0000 | call.len() as u32)
            .await
            .unwrap();
        stream.write_all(&call).await.unwrap();
        let read = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read_u8())
            .await
            .unwrap();
        assert!(read.is_err(), "malformed record must close connection");
    }
    let mut valid = Vec::new();
    opaque(&mut valid, &fs.id_to_fh(1).data);
    assert_eq!(&rpc(port, 1, valid).await[..4], &0u32.to_be_bytes());
    stop.send(()).unwrap();
    thread.join().unwrap();
}

#[tokio::test]
async fn pipelined_writes_make_progress_under_bounded_rpc_backpressure() {
    let dir = fixture();
    let fs = open(dir.path());
    let (id, _) = fs
        .create(1, &b"pipeline"[..].into(), sattr3::default())
        .await
        .unwrap();
    let (port, stop, thread) = start_server(fs.clone());
    let stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let (mut reader, mut writer) = stream.into_split();
    let mut requests = Vec::new();
    for xid in 1000..1032 {
        let mut call = Vec::new();
        for n in [xid, 0, 2, 100003, 3, 7, 0, 0, 0, 0] {
            word(&mut call, n);
        }
        opaque(&mut call, &fs.id_to_fh(id).data);
        for n in [0, 0, 65536, 2] {
            word(&mut call, n);
        }
        opaque(&mut call, &vec![xid as u8; 65536]);
        word(&mut requests, 0x8000_0000 | call.len() as u32);
        requests.extend(call);
    }
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let (sent, received) = tokio::join!(
            async {
                writer.write_all(&requests).await.unwrap();
            },
            async {
                let mut ids = std::collections::HashSet::new();
                for _ in 0..32 {
                    let marker = reader.read_u32().await.unwrap();
                    assert_ne!(marker & 0x8000_0000, 0);
                    let size = (marker & 0x7fff_ffff) as usize;
                    assert!(size < 4096);
                    let mut bytes = vec![0; size];
                    reader.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(&bytes[24..28], &[0; 4]);
                    ids.insert(u32::from_be_bytes(bytes[..4].try_into().unwrap()));
                }
                assert_eq!(ids.len(), 32);
            }
        );
        let _ = (sent, received);
    })
    .await
    .unwrap();
    drop(reader);
    drop(writer);
    stop.send(()).unwrap();
    thread.join().unwrap();
}

#[tokio::test]
async fn completed_rpc_capacity_turns_over_and_recent_replay_survives() {
    let dir = fixture();
    let fs = open(dir.path());
    let (port, stop, thread) = start_server(fs.clone());
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream.set_nodelay(true).unwrap();
    let mut args = Vec::new();
    opaque(&mut args, &fs.id_to_fh(1).data);
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut recent = Vec::new();
        for xid in 0..4100 {
            recent = rpc_stream_xid(&mut stream, xid, 1, args.clone()).await;
            assert_eq!(&recent[..4], &[0; 4]);
        }
        // The newest completed reply remains replayable despite old-entry eviction.
        assert_eq!(
            rpc_stream_xid(&mut stream, 4099, 1, args.clone()).await,
            recent
        );
        // A fresh request after turnover is still admitted.
        assert_eq!(
            &rpc_stream_xid(&mut stream, 4100, 1, args).await[..4],
            &[0; 4]
        );
    })
    .await
    .unwrap();
    drop(stream);
    stop.send(()).unwrap();
    thread.join().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_hydration_does_not_lock_namespace_and_revalidates_mutations() {
    use std::os::unix::fs::PermissionsExt;
    use std::time::Duration;
    let dir = fixture();
    let wrapper = dir.path().join("controlled-git");
    // The fixture wrapper gates only network fetch, not local object extraction.
    std::fs::write(
        &wrapper,
        r#"#!/bin/sh
root=$(dirname "$0")
case " $* " in
  *" fetch "*)
    touch "$root/started"
    while test ! -e "$root/release"; do sleep .01; done
    ;;
esac
exec git "$@"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let fs = open_with_git(dir.path(), &wrapper);
    let cold = fs.lookup(1, &b"cold.bin"[..].into()).await.unwrap();
    // Warm a separate local mutable file, without fetching an immutable blob.
    let (warm, _) = fs
        .create(1, &b"warm"[..].into(), sattr3::default())
        .await
        .unwrap();
    fs.write(warm, 0, b"warm").await.unwrap();
    let reader = fs.clone();
    let pending = tokio::spawn(async move { reader.read(cold, 0, 4096).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !dir.path().join("started").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cold fetch did not start");
    tokio::time::timeout(Duration::from_secs(2), async {
        assert!(matches!(fs.getattr(1).await.unwrap().ftype, ftype3::NF3DIR));
        assert_eq!(fs.read(warm, 0, 4).await.unwrap().0, b"warm");
        fs.write(warm, 0, b"WARM").await.unwrap();
        // The same inode is renamed while its immutable read is in flight.
        fs.rename(1, &b"cold.bin"[..].into(), 1, &b"renamed"[..].into())
            .await
            .unwrap();
        // Truncate-to-zero does not need old contents. The stale reply must not
        // resurrect the original bytes after this authoritative local edit.
        let mut attr = sattr3::default();
        attr.size = set_size3::size(0);
        fs.setattr(cold, attr).await.unwrap();
        fs.write(cold, 0, b"new").await.unwrap();
    })
    .await
    .expect("namespace operation blocked behind a cold fetch");
    std::fs::write(dir.path().join("release"), b"").unwrap();
    assert_eq!(pending.await.unwrap().unwrap(), (b"new".to_vec(), true));
    assert_eq!(fs.lookup(1, &b"renamed"[..].into()).await.unwrap(), cold);
}

#[tokio::test]
async fn repeated_immutable_ranges_reuse_the_verified_spool() {
    let dir = fixture();
    let fs = open(dir.path());
    let cold = fs.lookup(1, &b"cold.bin"[..].into()).await.unwrap();
    assert_eq!(
        fs.read(cold, 0, 4096).await.unwrap(),
        (vec![23; 4096], false)
    );
    let extracted = fs.content_stats().local_bytes;
    assert_eq!(extracted, 128 * 1024);
    for offset in [4096, 8192, 120 * 1024] {
        assert_eq!(fs.read(cold, offset, 4096).await.unwrap().0, vec![23; 4096]);
    }
    assert_eq!(fs.content_stats().local_bytes, extracted);
    assert!(fs.content_stats().cache_hits >= 3);
}

#[tokio::test]
async fn export_host_closes_admission_and_preserves_writes_on_restart() {
    use workspace_nfs::host::{ExportConfig, ExportHost};
    let dir = fixture();
    let repo = GitRepo::open(
        dir.path().join("repo.git"),
        "git",
        "origin",
        SizeFallback::Fetch,
    )
    .unwrap();
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let mut host = ExportHost::start(
        repo,
        snapshot,
        ExportConfig {
            mountpoint: dir.path().join("mount"),
            overlay: dir.path().join("state"),
            listen: "127.0.0.1:0".parse().unwrap(),
            git_vfs_executable: std::path::PathBuf::from(env!("CARGO_BIN_EXE_git-nfs")),
        },
    )
    .await
    .unwrap();
    let fs = host.filesystem();
    let id = fs
        .lookup(fs.root_dir(), &b"hello.txt"[..].into())
        .await
        .unwrap();
    fs.write(id, 0, b"HELLO").await.unwrap();
    let mut args = Vec::new();
    opaque(&mut args, &fs.id_to_fh(id).data);
    let attrs = rpc(host.port(), 1, args).await;
    assert_eq!(&attrs[..4], &[0; 4]);
    host.shutdown().await.unwrap();
    host.shutdown().await.unwrap();
    assert!(host.is_finished());
    assert!(matches!(
        fs.write(id, 0, b"wrong").await,
        Err(nfsstat3::NFS3ERR_STALE)
    ));
    drop(fs);
    drop(host);
    let reopened = open(dir.path());
    assert_eq!(
        reopened.read(id, 0, 100).await.unwrap().0,
        b"HELLO from Git\n"
    );
}
