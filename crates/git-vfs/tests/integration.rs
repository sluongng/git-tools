use git_vfs::backend::{GitRepo, SizeFallback};
use git_vfs::filesystem::Snapshot;
use git_vfs::storage::open_fjall;
use std::ffi::{CString, OsString};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct Fixture {
    _temp: TempDir,
    source: PathBuf,
    clone: PathBuf,
    mount: PathBuf,
    odd_name: PathBuf,
}

fn git(path: &Path, args: &[&str]) -> Output {
    let output = Command::new("git")
        .arg("-C")
        .arg(path)
        .args(args)
        .env("GIT_NO_LAZY_FETCH", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

fn cli(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_git-vfs"))
        .args(args)
        .output()
        .unwrap()
}

fn ok_cli(args: &[&str]) -> Output {
    let output = cli(args);
    assert!(
        output.status.success(),
        "git-vfs {args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}

impl Fixture {
    fn new() -> Self {
        Self::with_blob_count(135)
    }

    fn with_blob_count(blob_count: usize) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        let clone = temp.path().join("partial.git");
        let mount = temp.path().join("mount");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&mount).unwrap();
        git(&source, &["init", "--initial-branch=main"]);
        git(&source, &["config", "user.name", "Fixture"]);
        git(
            &source,
            &["config", "user.email", "fixture@example.invalid"],
        );
        git(&source, &["config", "uploadpack.allowFilter", "true"]);
        git(
            &source,
            &["config", "uploadpack.allowAnySHA1InWant", "true"],
        );
        git(&source, &["config", "transfer.advertiseObjectInfo", "true"]);
        fs::write(source.join("hello.txt"), b"hello from git-vfs\n").unwrap();
        fs::write(source.join("duplicate.txt"), b"hello from git-vfs\n").unwrap();
        fs::write(source.join("empty"), b"").unwrap();
        fs::write(source.join("binary"), [0, 255, 1, 0, 2]).unwrap();
        fs::write(source.join("run.sh"), b"#!/bin/sh\nprintf 'ok\\n'\n").unwrap();
        fs::set_permissions(source.join("run.sh"), fs::Permissions::from_mode(0o755)).unwrap();
        fs::write(source.join("large"), vec![b'x'; 256 * 1024]).unwrap();
        symlink("hello.txt", source.join("link")).unwrap();
        fs::create_dir(source.join("many")).unwrap();
        for i in 0..blob_count {
            fs::write(
                source.join(format!("many/file-{i}")),
                format!("unique content {i}\n"),
            )
            .unwrap();
        }
        let odd_name = PathBuf::from(OsString::from_vec(b"odd \n\xff name".to_vec()));
        fs::write(source.join(&odd_name), b"non-UTF8 path\n").unwrap();
        git(&source, &["add", "."]);
        git(&source, &["commit", "-m", "Fixture snapshot"]);
        ok_cli(&[
            "clone",
            &format!("file://{}", source.display()),
            clone.to_str().unwrap(),
        ]);
        Self {
            _temp: temp,
            source,
            clone,
            mount,
            odd_name,
        }
    }

    fn repo(&self, fallback: SizeFallback) -> GitRepo {
        GitRepo::open(&self.clone, "git", "origin", fallback).unwrap()
    }

    fn missing_blobs(&self, oids: &[String]) -> usize {
        let mut child = Command::new("git")
            .arg("--git-dir")
            .arg(&self.clone)
            .arg("cat-file")
            .arg("--batch-check=%(objectname)")
            .env("GIT_NO_LAZY_FETCH", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut stdin = child.stdin.take().unwrap();
        let input = format!("{}\n", oids.join("\n"));
        let writer = std::thread::spawn(move || stdin.write_all(input.as_bytes()));
        let output = child.wait_with_output().unwrap();
        writer.join().unwrap().unwrap();
        assert!(output.status.success());
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .filter(|line| line.ends_with(" missing"))
            .count()
    }
}

#[test]
fn native_size_warming_batches_large_snapshots_without_hydrating_or_mutating_refs() {
    use git_vfs::backend::{SizeSource, SizeWarmOptions};
    let fixture = Fixture::with_blob_count(4105);
    let wrapper = fixture.clone.with_file_name("counting-size-git");
    fs::write(
        &wrapper,
        r#"#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
if '--batch-command=%(objectname) %(objectsize)' in sys.argv:
    payload = sys.stdin.buffer.read()
    fields = payload.decode('ascii').split()
    assert fields[0] == 'remote-object-info'
    with pathlib.Path(__file__).with_suffix('.requests').open('a') as log:
        log.write(json.dumps({'oids': len(fields) - 2, 'bytes': len(payload)}) + '\n')
    if pathlib.Path(__file__).with_suffix('.fail-tail').exists() and len(fields) - 2 < 4096:
        sys.exit(1)
    sys.exit(subprocess.run(['git', *sys.argv[1:]], input=payload).returncode)
os.execvp('git', ['git', *sys.argv[1:]])
"#,
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let mut repo = GitRepo::open(&fixture.clone, &wrapper, "origin", SizeFallback::Error).unwrap();
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let mut oids: Vec<_> = snapshot
        .entries()
        .into_iter()
        .filter(|entry| matches!(entry.kind.as_str(), "file" | "symlink"))
        .filter_map(|entry| entry.oid)
        .collect();
    oids.sort();
    oids.dedup();
    assert!(oids.len() > 4096);
    assert_eq!(fixture.missing_blobs(&oids), oids.len());
    let refs = git(&fixture.clone, &["show-ref"]).stdout;
    let head = fs::read(fixture.clone.join("HEAD")).unwrap();
    let fetch_head = fs::read(fixture.clone.join("FETCH_HEAD")).ok();
    fs::write(
        wrapper.with_extension("fail-tail"),
        b"fail final metadata batch",
    )
    .unwrap();
    assert!(
        repo.warm_snapshot_sizes(
            &snapshot,
            &SizeWarmOptions {
                source: SizeSource::Git,
                github_token_env: None,
            }
        )
        .is_err()
    );
    assert_eq!(
        repo.cache_stats().unwrap().entries,
        0,
        "failed final batch must not publish earlier sizes"
    );
    assert_eq!(
        fixture.missing_blobs(&oids),
        oids.len(),
        "metadata errors must not hydrate contents"
    );
    fs::remove_file(wrapper.with_extension("fail-tail")).unwrap();
    fs::remove_file(wrapper.with_extension("requests")).unwrap();
    let report = repo
        .warm_snapshot_sizes(
            &snapshot,
            &SizeWarmOptions {
                source: SizeSource::Git,
                github_token_env: None,
            },
        )
        .unwrap();
    assert_eq!(report.source, "git");
    assert_eq!(report.learned_blobs, oids.len());
    assert_eq!(report.cached_blobs, 0);
    let sizes = repo.blob_sizes(&oids).unwrap();
    let listing = git(&fixture.source, &["ls-tree", "-r", "-l", "-z", "HEAD"]).stdout;
    let mut expected = std::collections::HashMap::new();
    for record in listing
        .split(|b| *b == 0)
        .filter(|record| !record.is_empty())
    {
        let tab = record.iter().position(|b| *b == b'\t').unwrap();
        let header = std::str::from_utf8(&record[..tab]).unwrap();
        let fields: Vec<_> = header.split_whitespace().collect();
        expected.insert(fields[2].to_owned(), fields[3].parse::<u64>().unwrap());
    }
    assert_eq!(sizes, expected);
    let records = fs::read_to_string(wrapper.with_extension("requests")).unwrap();
    let requests: Vec<serde_json::Value> = records
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0]["oids"], 4096);
    assert_eq!(requests[1]["oids"], oids.len() - 4096);
    assert!(
        requests
            .iter()
            .all(|request| request["bytes"].as_u64().unwrap() < 200_000)
    );
    assert_eq!(fixture.missing_blobs(&oids), oids.len());
    assert_eq!(git(&fixture.clone, &["show-ref"]).stdout, refs);
    assert_eq!(fs::read(fixture.clone.join("HEAD")).unwrap(), head);
    assert_eq!(fs::read(fixture.clone.join("FETCH_HEAD")).ok(), fetch_head);
    drop(repo);
    git(&fixture.clone, &["remote", "remove", "origin"]);
    let mut repo = GitRepo::open(&fixture.clone, &wrapper, "origin", SizeFallback::Error).unwrap();
    let report = repo
        .warm_snapshot_sizes(&snapshot, &SizeWarmOptions::default())
        .unwrap();
    assert_eq!(report.source, "cache");
    assert_eq!(report.cached_blobs, oids.len());
    assert_eq!(
        fs::read_to_string(wrapper.with_extension("requests")).unwrap(),
        records
    );
    assert_eq!(fixture.missing_blobs(&oids), oids.len());
}

#[test]
fn blobless_metadata_digest_and_persistent_sizes_do_not_fetch_content() {
    let fixture = Fixture::new();
    let mut repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let mut oids: Vec<_> = snapshot
        .entries()
        .into_iter()
        .filter(|e| matches!(e.kind.as_str(), "file" | "symlink"))
        .filter_map(|e| e.oid)
        .collect();
    oids.sort();
    oids.dedup();
    assert_eq!(
        fixture.missing_blobs(&oids),
        oids.len(),
        "clone must omit every blob"
    );
    assert_eq!(repo.cache_stats().unwrap().entries, 0);
    let digest = snapshot.digest_path(Path::new("hello.txt")).unwrap();
    assert_eq!(digest.len(), 20);
    let expected = git(&fixture.source, &["rev-parse", "HEAD:hello.txt"]);
    assert_eq!(
        hex::encode(&digest),
        String::from_utf8(expected.stdout).unwrap().trim()
    );
    assert_eq!(fixture.missing_blobs(&oids), oids.len());
    let sizes = repo.blob_sizes(&oids).unwrap();
    assert_eq!(sizes.len(), oids.len());
    assert_eq!(repo.cache_stats().unwrap().entries, oids.len() as u64);
    assert_eq!(
        fixture.missing_blobs(&oids),
        oids.len(),
        "remote size queries must not fetch blobs"
    );
    assert_eq!(
        snapshot
            .stat_path(&mut repo, Path::new("empty"))
            .unwrap()
            .size,
        0
    );
    assert_eq!(
        snapshot
            .stat_path(&mut repo, Path::new("large"))
            .unwrap()
            .size,
        256 * 1024
    );
    assert_eq!(
        snapshot
            .stat_path(&mut repo, Path::new("run.sh"))
            .unwrap()
            .mode,
        0o100755
    );
    assert!(snapshot.digest_path(Path::new("many")).is_err());
    assert!(snapshot.digest_path(Path::new("../hello.txt")).is_err());
    drop(repo);
    // A new process can stat solely from the persistent cache with no remote.
    git(
        &fixture.clone,
        &["remote", "set-url", "origin", "/does/not/exist"],
    );
    let mut repo = fixture.repo(SizeFallback::Error);
    assert_eq!(repo.blob_sizes(&oids).unwrap(), sizes);
    drop(repo);
    let stat = ok_cli(&[
        "stat",
        "--repo",
        fixture.clone.to_str().unwrap(),
        "hello.txt",
    ]);
    let stat: serde_json::Value = serde_json::from_slice(&stat.stdout).unwrap();
    assert_eq!(stat["size"], 19);
    // A fully warm snapshot is provider-independent: even a selected GitHub
    // source with no API credentials must reuse Fjall without making HTTP calls.
    git(
        &fixture.clone,
        &[
            "remote",
            "set-url",
            "origin",
            "https://github.com/fixture/example.git",
        ],
    );
    let warm = ok_cli(&[
        "warm-sizes",
        "--repo",
        fixture.clone.to_str().unwrap(),
        "--size-source=github",
        "--github-token-env",
        "GIT_VFS_TEST_UNUSED_TOKEN_SOURCE",
    ]);
    let warm: serde_json::Value = serde_json::from_slice(&warm.stdout).unwrap();
    assert_eq!(warm["entries"], oids.len());
    assert_eq!(warm["warm"]["source"], "cache");
    assert_eq!(warm["warm"]["http_requests"], 0);
    assert_eq!(warm["warm"]["http_bytes"], 0);
    assert_eq!(warm["warm"]["cached_blobs"], oids.len());
    assert_eq!(fixture.missing_blobs(&oids), oids.len());
    let mut repo = fixture.repo(SizeFallback::Error);
    assert_eq!(fixture.missing_blobs(&oids), oids.len());
    git(
        &fixture.clone,
        &[
            "remote",
            "set-url",
            "origin",
            fixture.source.to_str().unwrap(),
        ],
    );
    assert_eq!(
        snapshot
            .read_path(&mut repo, Path::new("hello.txt"))
            .unwrap(),
        b"hello from git-vfs\n"
    );
    assert_eq!(
        fixture.missing_blobs(&oids),
        oids.len() - 1,
        "reading one blob must not fetch unrelated blobs"
    );
    assert_eq!(
        snapshot.read_path(&mut repo, &fixture.odd_name).unwrap(),
        b"non-UTF8 path\n"
    );
    assert_eq!(
        snapshot.read_path(&mut repo, Path::new("binary")).unwrap(),
        [0, 255, 1, 0, 2]
    );
}

#[test]
fn writable_namespace_lookup_counts_and_tombstones_survive_mutation_and_reopen() {
    use git_vfs::overlay::{Kind, Overlay};
    use std::ffi::OsStr;

    let fixture = Fixture::new();
    let mut repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let directory = fixture._temp.path().join("namespace-overlay");
    let gitdir = fixture._temp.path().join("private-git");
    fs::create_dir(&gitdir).unwrap();
    let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
    let initial_directories = overlay.directory_children_count(1).unwrap();
    assert!(
        overlay
            .entry(overlay.lookup(1, OsStr::new(".git")).unwrap())
            .unwrap()
            .control
    );
    assert!(overlay.lookup(1, fixture.odd_name.as_os_str()).is_ok());
    let a = overlay
        .create(1, OsStr::new("a"), Kind::Directory, 0o755, None)
        .unwrap();
    let b = overlay
        .create(1, OsStr::new("b"), Kind::Directory, 0o755, None)
        .unwrap();
    assert_eq!(
        overlay.directory_children_count(1).unwrap(),
        initial_directories + 2
    );
    let source = overlay
        .create(
            a,
            OsStr::new("source"),
            Kind::File,
            0o644,
            Some(b"source bytes".to_vec()),
        )
        .unwrap();
    let overwritten = overlay
        .create(
            b,
            OsStr::new("target"),
            Kind::File,
            0o644,
            Some(b"old target bytes".to_vec()),
        )
        .unwrap();
    overlay
        .rename(a, OsStr::new("source"), a, OsStr::new("renamed"), 0)
        .unwrap();
    assert!(overlay.lookup(a, OsStr::new("source")).is_err());
    assert_eq!(overlay.lookup(a, OsStr::new("renamed")).unwrap(), source);
    overlay
        .rename(a, OsStr::new("renamed"), b, OsStr::new("target"), 0)
        .unwrap();
    assert!(overlay.children(a).unwrap().is_empty());
    assert_eq!(overlay.lookup(b, OsStr::new("target")).unwrap(), source);
    assert!(!overlay.entry(overwritten).unwrap().linked);
    assert_eq!(
        overlay.read(overwritten, &mut repo).unwrap(),
        b"old target bytes"
    );
    overlay.unlink(b, OsStr::new("target"), false).unwrap();
    let replacement = overlay
        .create(
            b,
            OsStr::new("target"),
            Kind::File,
            0o644,
            Some(b"new bytes".to_vec()),
        )
        .unwrap();
    assert_ne!(replacement, source);
    assert!(!overlay.entry(source).unwrap().linked);
    assert_eq!(overlay.read(source, &mut repo).unwrap(), b"source bytes");
    let raw_name = OsString::from_vec(b"raw-\xff".to_vec());
    let raw_inode = overlay
        .create(b, &raw_name, Kind::File, 0o644, Some(vec![1, 2]))
        .unwrap();
    let sub = overlay
        .create(b, OsStr::new("sub"), Kind::Directory, 0o755, None)
        .unwrap();
    let empty = overlay
        .create(a, OsStr::new("empty"), Kind::Directory, 0o755, None)
        .unwrap();
    assert_eq!(overlay.directory_children_count(a).unwrap(), 1);
    assert_eq!(overlay.directory_children_count(b).unwrap(), 1);
    overlay
        .rename(a, OsStr::new("empty"), b, OsStr::new("sub"), 0)
        .unwrap();
    assert!(!overlay.entry(sub).unwrap().linked);
    assert_eq!(overlay.lookup(b, OsStr::new("sub")).unwrap(), empty);
    assert_eq!(overlay.directory_children_count(a).unwrap(), 0);
    assert_eq!(overlay.directory_children_count(b).unwrap(), 1);
    assert!(
        overlay
            .rename(
                b,
                &raw_name,
                b,
                OsStr::new("target"),
                libc::RENAME_NOREPLACE
            )
            .is_err()
    );
    assert!(
        overlay
            .rename(1, OsStr::new("b"), empty, OsStr::new("cycle"), 0)
            .is_err()
    );
    assert!(overlay.unlink(1, OsStr::new("b"), true).is_err());
    assert_eq!(overlay.lookup(b, &raw_name).unwrap(), raw_inode);
    assert_eq!(
        overlay.lookup(b, OsStr::new("target")).unwrap(),
        replacement
    );
    overlay.unlink(b, OsStr::new("sub"), true).unwrap();
    assert_eq!(overlay.directory_children_count(b).unwrap(), 0);
    let children = overlay.children(b).unwrap();
    assert_eq!(children, vec![replacement, raw_inode]);
    // Live-session unlinked files still support descriptor-style content access.
    overlay.write(source, 0, b"SOURCE", &mut repo).unwrap();
    assert_eq!(overlay.read(source, &mut repo).unwrap(), b"SOURCE bytes");
    let base = overlay.lookup(1, OsStr::new("hello.txt")).unwrap();
    overlay.write(base, 0, b"edited base", &mut repo).unwrap();
    overlay.unlink(1, OsStr::new("hello.txt"), false).unwrap();
    let unmodified = overlay.lookup(1, OsStr::new("binary")).unwrap();
    overlay.unlink(1, OsStr::new("binary"), false).unwrap();
    // Cross the maintenance batch boundary; all records/inode high-water remain.
    let mut highest = 0;
    for n in 0..130 {
        let name = OsString::from(format!("discard-{n}"));
        highest = overlay
            .create(1, &name, Kind::File, 0o600, Some(vec![7; n]))
            .unwrap();
        overlay.unlink(1, &name, false).unwrap();
    }
    let before = overlay.stats();
    assert!(before.dead_payload_bytes > 0);
    let (token, _) = git_vfs::overlay::changes_since(&directory, "").unwrap();
    let dirty = git_vfs::overlay::dirty_paths(&directory).unwrap();
    assert_eq!(
        overlay.stats().rename_visited_entries,
        3,
        "two file moves and one empty-directory move visit only their sources"
    );
    overlay.sync().unwrap();
    drop(overlay);

    let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
    assert_eq!(overlay.lookup(1, OsStr::new(".")).unwrap(), 1);
    assert_eq!(overlay.lookup(b, OsStr::new("..")).unwrap(), 1);
    assert_eq!(overlay.lookup(b, &raw_name).unwrap(), raw_inode);
    assert_eq!(
        overlay.lookup(b, OsStr::new("target")).unwrap(),
        replacement
    );
    assert_eq!(overlay.children(b).unwrap(), children);
    assert_eq!(
        overlay.directory_children_count(1).unwrap(),
        initial_directories + 2
    );
    assert_eq!(overlay.directory_children_count(b).unwrap(), 0);
    assert!(overlay.entry(source).is_err());
    assert!(overlay.entry(overwritten).is_err());
    assert!(overlay.lookup(1, OsStr::new(".git")).is_ok());
    let after = overlay.stats();
    assert_eq!(after.entry_count, before.linked_entries + 2);
    assert_eq!(after.linked_entries, before.linked_entries);
    assert_eq!(after.tombstone_entries, 2);
    assert_eq!(after.live_payload_bytes, before.live_payload_bytes);
    assert_eq!(after.dead_payload_bytes, 0);
    assert_eq!(after.reclaimed_payload_bytes, before.dead_payload_bytes);
    assert_eq!(overlay.read(replacement, &mut repo).unwrap(), b"new bytes");
    assert_eq!(overlay.read(raw_inode, &mut repo).unwrap(), [1, 2]);
    assert!(overlay.lookup(1, OsStr::new("hello.txt")).is_err());
    for inode in [source, overwritten, base, unmodified, highest] {
        if inode == base || inode == unmodified {
            let entry = overlay.entry(inode).unwrap();
            assert!(!entry.linked);
            assert!(matches!(entry.payload, git_vfs::overlay::Payload::None));
        } else {
            assert!(overlay.entry(inode).is_err());
        }
        assert!(overlay.read(inode, &mut repo).is_err());
        assert!(overlay.size(inode, &mut repo).is_err());
        assert!(overlay.digest(inode).is_err());
        assert!(overlay.content_entry(inode).is_err());
        assert!(
            overlay
                .write(inode, 0, b"no resurrection", &mut repo)
                .is_err()
        );
        assert!(
            overlay
                .setattr(inode, None, Some(0), None, &mut repo)
                .is_err()
        );
    }
    assert!(overlay.entry(base).unwrap().base_oid.is_some());
    assert_eq!(
        git_vfs::overlay::changes_since(&directory, &token).unwrap(),
        (token.clone(), vec![])
    );
    assert_eq!(git_vfs::overlay::dirty_paths(&directory).unwrap(), dirty);
    drop(overlay);
    let mut overlay = Overlay::open(&directory, &snapshot, &gitdir).unwrap();
    assert_eq!(overlay.stats().reclaimed_payload_bytes, 0);
    assert_eq!(overlay.stats().dead_payload_bytes, 0);
    assert_eq!(overlay.stats().entry_count, after.entry_count);
    assert_eq!(
        git_vfs::overlay::changes_since(&directory, &token).unwrap(),
        (token, vec![])
    );
    let next = overlay
        .create(1, OsStr::new("next"), Kind::File, 0o600, None)
        .unwrap();
    assert!(next > highest);
    let moving = overlay
        .create(1, OsStr::new("moving"), Kind::Directory, 0o700, None)
        .unwrap();
    overlay
        .create(moving, OsStr::new("alpha"), Kind::File, 0o600, None)
        .unwrap();
    let deep = overlay
        .create(moving, OsStr::new("deep"), Kind::Directory, 0o700, None)
        .unwrap();
    let native_child = OsString::from_vec(b"bravo-\xff".to_vec());
    overlay
        .create(deep, &native_child, Kind::File, 0o600, None)
        .unwrap();
    let (token, _) = git_vfs::overlay::changes_since(&directory, "").unwrap();
    overlay
        .rename(1, OsStr::new("moving"), 1, OsStr::new("moved"), 0)
        .unwrap();
    assert_eq!(overlay.stats().rename_visited_entries, 4);
    let mut expected = vec![
        b"moving/".to_vec(),
        b"moving/alpha".to_vec(),
        b"moving/deep/".to_vec(),
        b"moving/deep/bravo-\xff".to_vec(),
        b"moved/".to_vec(),
        b"moved/alpha".to_vec(),
        b"moved/deep/".to_vec(),
        b"moved/deep/bravo-\xff".to_vec(),
    ];
    expected.sort();
    assert_eq!(
        git_vfs::overlay::changes_since(&directory, &token)
            .unwrap()
            .1,
        expected
    );
}

#[test]
fn edited_digest_pairs_cache_by_generation_and_retire_with_payloads() {
    use git_vfs::overlay::{Kind, Overlay};
    use std::ffi::OsStr;
    let fixture = Fixture::with_blob_count(0);
    let mut repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let directory = fixture._temp.path().join("digest-generations");
    let mut overlay = Overlay::open(&directory, &snapshot, &fixture.clone).unwrap();
    let inode = overlay.lookup(1, OsStr::new("hello.txt")).unwrap();
    assert_eq!(
        overlay.digest(inode).unwrap(),
        snapshot.digest_path(Path::new("hello.txt")).unwrap()
    );
    assert_eq!(overlay.stats().digest_hash_operations, 0);
    overlay.write(inode, 0, b"H", &mut repo).unwrap();
    for _ in 0..10 {
        overlay.digest_available(inode).unwrap();
    }
    assert_eq!(
        overlay.stats().digest_hash_operations,
        0,
        "availability probes do not hash"
    );
    let first = overlay.digest(inode).unwrap();
    assert_eq!(
        overlay.blake3_digest(inode).unwrap(),
        *blake3::hash(b"Hello from git-vfs\n").as_bytes()
    );
    assert_eq!(overlay.stats().digest_hash_operations, 1);
    assert_eq!(overlay.stats().digest_hash_bytes, 19);
    assert_eq!(
        overlay.stats().payload_hash_bytes,
        19,
        "both algorithms use one backing stream"
    );
    overlay
        .setattr(inode, Some(0o600), None, None, &mut repo)
        .unwrap();
    assert_eq!(overlay.digest(inode).unwrap(), first);
    assert_eq!(
        overlay.stats().digest_hash_operations,
        1,
        "metadata changes retain cached content"
    );
    overlay.write(inode, 0, b"h", &mut repo).unwrap();
    assert_ne!(
        overlay.digest(inode).unwrap(),
        first,
        "same-size edits invalidate the cached pair"
    );
    assert_eq!(
        overlay.blake3_digest(inode).unwrap(),
        *blake3::hash(b"hello from git-vfs\n").as_bytes()
    );
    assert_eq!(overlay.stats().digest_hash_operations, 2);
    overlay
        .setattr(inode, None, Some(19), None, &mut repo)
        .unwrap();
    assert!(
        overlay.blake3_digest_with_status(inode).unwrap().1,
        "even same-size truncate advances content generation"
    );
    assert_eq!(overlay.stats().digest_hash_operations, 3);
    overlay
        .setattr(inode, None, Some(0), None, &mut repo)
        .unwrap();
    assert_eq!(
        overlay.blake3_digest(inode).unwrap(),
        *blake3::hash(b"").as_bytes()
    );
    assert_eq!(overlay.stats().digest_hash_operations, 4);
    assert_eq!(overlay.stats().digest_hash_bytes, 57);
    overlay.unlink(1, OsStr::new("hello.txt"), false).unwrap();
    overlay.reclaim_unreferenced().unwrap();
    overlay.drain_maintenance().unwrap();
    assert_eq!(
        overlay.stats().digest_cache_entries,
        0,
        "retired base tombstones retain no digest cache"
    );
    assert!(overlay.digest(inode).is_err());
    for _ in 0..8 {
        let inode = overlay
            .create(1, OsStr::new("temporary"), Kind::File, 0o600, None)
            .unwrap();
        overlay.digest(inode).unwrap();
        assert_eq!(overlay.stats().digest_cache_entries, 1);
        overlay.unlink(1, OsStr::new("temporary"), false).unwrap();
        overlay.reclaim_unreferenced().unwrap();
        assert_eq!(
            overlay.stats().digest_cache_entries,
            0,
            "created inode collection retires cached digests"
        );
    }
}

#[test]
fn directory_rename_overflow_uses_bounded_subtree_invalidation() {
    use git_vfs::overlay::{Kind, Overlay};
    use std::ffi::OsStr;
    let fixture = Fixture::with_blob_count(0);
    let repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let directory = fixture._temp.path().join("bounded-rename");
    let mut overlay = Overlay::open(&directory, &snapshot, &fixture.clone).unwrap();
    let source = overlay
        .create(1, OsStr::new("source"), Kind::Directory, 0o700, None)
        .unwrap();
    for n in 0..4096 {
        overlay
            .create(
                source,
                OsStr::new(&format!("child-{n}")),
                Kind::File,
                0o600,
                None,
            )
            .unwrap();
    }
    let (token, _) = git_vfs::overlay::changes_since(&directory, "").unwrap();
    let before = overlay.stats().rename_visited_entries;
    overlay
        .rename(1, OsStr::new("source"), 1, OsStr::new("moved"), 0)
        .unwrap();
    assert_eq!(
        overlay.stats().rename_visited_entries - before,
        2049,
        "stop walking after the full-invalidation fallback"
    );
    assert_eq!(
        git_vfs::overlay::changes_since(&directory, &token)
            .unwrap()
            .1,
        [b"/".to_vec()]
    );
    assert_eq!(overlay.lookup(1, OsStr::new("moved")).unwrap(), source);
    assert!(overlay.lookup(source, OsStr::new("child-4095")).is_ok());
}

#[test]
fn bazel_prefetch_unions_scoped_metadata_and_symlinks_in_one_fetch_and_reuses_offline() {
    let fixture = Fixture::new();
    fs::create_dir(fixture.source.join("metadata")).unwrap();
    let mut expected_paths = Vec::new();
    for i in 0..2000 {
        let path = format!("metadata/rules-{i}.bzl");
        fs::write(fixture.source.join(&path), format!("# rule {i}\n")).unwrap();
        expected_paths.push(path);
    }
    for name in [
        "BUILD",
        "BUILD.bazel",
        "MODULE.bazel",
        "nested.MODULE.bazel",
        ".bazelrc",
    ] {
        fs::write(fixture.source.join(name), b"# identical metadata\n").unwrap();
        expected_paths.push(name.to_string());
    }
    fs::write(
        fixture.source.join("not-starlark.txt"),
        b"must stay absent\n",
    )
    .unwrap();
    fs::create_dir(fixture.source.join("metadata-sibling")).unwrap();
    fs::write(
        fixture.source.join("metadata-sibling/BUILD"),
        b"# sibling metadata\n",
    )
    .unwrap();
    expected_paths.push("metadata-sibling/BUILD".to_string());
    for (name, target) in [
        ("arbitrary-link", "hello.txt"),
        ("dangling-link", "does-not-exist"),
        ("outside-link", "/outside/repository/never-follow"),
        ("metadata/arbitrary-link", "not-starlark.txt"),
    ] {
        symlink(target, fixture.source.join(name)).unwrap();
        expected_paths.push(name.to_string());
    }
    expected_paths.push("link".to_string());
    let native_root = PathBuf::from(OsString::from_vec(b"bad\xff".to_vec()));
    let unicode_root = PathBuf::from("bad\u{fffd}");
    for (root, target) in [
        (&native_root, "native-target"),
        (&unicode_root, "unicode-target"),
    ] {
        fs::create_dir(fixture.source.join(root)).unwrap();
        symlink(target, fixture.source.join(root).join("arbitrary-link")).unwrap();
        expected_paths.push(root.join("arbitrary-link").to_string_lossy().into_owned());
    }
    git(&fixture.source, &["add", "."]);
    git(&fixture.source, &["commit", "-m", "Bazel metadata fixture"]);
    let clone = fixture.clone.with_file_name("prefetch.git");
    ok_cli(&[
        "clone",
        &format!("file://{}", fixture.source.display()),
        clone.to_str().unwrap(),
        "--depth",
        "1",
    ]);
    assert_eq!(git(&clone, &["rev-list", "--count", "HEAD"]).stdout, b"1\n");
    assert!(clone.join("shallow").is_file());
    let repo = GitRepo::open(&clone, "git", "origin", SizeFallback::Error).unwrap();
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let ordinary_oid = hex::encode(snapshot.digest_path(Path::new("not-starlark.txt")).unwrap());
    let native_oid = hex::encode(
        snapshot
            .digest_path(&native_root.join("arbitrary-link"))
            .unwrap(),
    );
    let unicode_oid = hex::encode(
        snapshot
            .digest_path(&unicode_root.join("arbitrary-link"))
            .unwrap(),
    );
    // Two distinct native paths have the same lossy display string. Scope each
    // through OsStr CLI arguments and verify only the intended link blob loads.
    for (index, root, wanted, excluded) in [
        (0, &native_root, &native_oid, &unicode_oid),
        (1, &unicode_root, &unicode_oid, &native_oid),
    ] {
        let scoped_clone = fixture
            .clone
            .with_file_name(format!("native-scope-{index}.git"));
        ok_cli(&[
            "clone",
            &format!("file://{}", fixture.source.display()),
            scoped_clone.to_str().unwrap(),
        ]);
        let scoped = Command::new(env!("CARGO_BIN_EXE_git-vfs"))
            .args(["prefetch-bazel", "--repo"])
            .arg(&scoped_clone)
            .arg("--path")
            .arg(root)
            .output()
            .unwrap();
        assert!(
            scoped.status.success(),
            "{}",
            String::from_utf8_lossy(&scoped.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&scoped.stdout).unwrap();
        assert_eq!(report["blobs"]["requested_blobs"], 1);
        let scoped_repo =
            GitRepo::open(&scoped_clone, "git", "origin", SizeFallback::Error).unwrap();
        assert!(
            scoped_repo
                .git_output(&["cat-file", "-e", wanted], false)
                .is_ok()
        );
        assert!(
            scoped_repo
                .git_output(&["cat-file", "-e", excluded], false)
                .is_err()
        );
    }
    let metadata_oid = hex::encode(snapshot.digest_path(Path::new("BUILD")).unwrap());
    // Explicit blob wants must ignore custom storage mappings and pruning policy.
    git(
        &clone,
        &[
            "config",
            "--add",
            "remote.origin.fetch",
            &format!("{metadata_oid}:refs/tags/unwanted-prefetch"),
        ],
    );
    git(&clone, &["config", "fetch.prune", "true"]);
    git(&clone, &["config", "fetch.pruneTags", "true"]);
    git(&clone, &["update-ref", "refs/tags/local-only", "HEAD"]);
    let head_before = git(&clone, &["rev-parse", "HEAD"]).stdout;
    let refs_before = git(&clone, &["show-ref"]).stdout;
    // Invalid roots must fail before fetching even if another root is valid.
    for invalid in ["../metadata", "/metadata", "metadata/../metadata", ""] {
        let output = cli(&[
            "prefetch-bazel",
            "--repo",
            clone.to_str().unwrap(),
            "--path",
            "metadata",
            "--path",
            invalid,
        ]);
        assert!(!output.status.success());
        assert!(
            repo.git_output(&["cat-file", "-e", &metadata_oid], false)
                .is_err()
        );
    }
    drop(repo);
    // Overlapping file/subtree roots deduplicate the union; a component boundary
    // must exclude metadata-sibling. More than 128 unique objects use one fetch.
    let trace = clone.with_file_name("prefetch.trace");
    let wrapper = clone.with_file_name("noisy-git");
    fs::write(&wrapper, b"#!/usr/bin/env python3\nimport os, sys\nif 'fetch' in sys.argv and '--stdin' in sys.argv:\n    os.write(2, b'x' * (128 * 1024))\nos.execvp('git', ['git', *sys.argv[1:]])\n").unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let output_path = clone.with_file_name("scoped-output.json");
    let error_path = clone.with_file_name("scoped-error.txt");
    let mut scoped = Command::new(env!("CARGO_BIN_EXE_git-vfs"))
        .args(["--git", wrapper.to_str().unwrap()])
        .args([
            "prefetch-bazel",
            "--repo",
            clone.to_str().unwrap(),
            "--path",
            "metadata",
            "--path",
            "BUILD",
            "--path",
            "./metadata/rules-0.bzl",
        ])
        .env("GIT_TRACE", &trace)
        .stdout(fs::File::create(&output_path).unwrap())
        .stderr(fs::File::create(&error_path).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = scoped.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = scoped.kill();
            let _ = scoped.wait();
            panic!("prefetch deadlocked while supplying stdin and draining noisy Git stderr");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "{}",
        fs::read_to_string(error_path).unwrap()
    );
    let native_fetches = fs::read_to_string(trace)
        .unwrap()
        .lines()
        .filter(|line| line.contains("built-in: git ") && line.contains(" fetch "))
        .count();
    assert_eq!(
        native_fetches, 1,
        "the full union must use one native fetch process"
    );
    let scoped: serde_json::Value =
        serde_json::from_slice(&fs::read(output_path).unwrap()).unwrap();
    let mut scoped_paths: Vec<_> = expected_paths
        .iter()
        .filter(|path| path.starts_with("metadata/") || *path == "BUILD")
        .cloned()
        .collect();
    scoped_paths.sort();
    assert_eq!(scoped["selected_paths"], serde_json::json!(scoped_paths));
    assert_eq!(scoped["blobs"]["requested_blobs"], 2002);
    assert_eq!(scoped["blobs"]["fetched_blobs"], 2002);
    assert_eq!(scoped["blobs"]["fetch_batches"], 1);
    let report = ok_cli(&["prefetch-bazel", "--repo", clone.to_str().unwrap()]);
    let repo = GitRepo::open(&clone, "git", "origin", SizeFallback::Error).unwrap();
    let report: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    expected_paths.sort();
    assert_eq!(report["selected_paths"], serde_json::json!(expected_paths));
    assert_eq!(report["blobs"]["requested_blobs"], 2008);
    assert_eq!(report["blobs"]["already_local_blobs"], 2002);
    assert_eq!(report["blobs"]["fetched_blobs"], 6);
    assert_eq!(report["blobs"]["fetch_batches"], 1);
    assert_eq!(repo.cache_stats().unwrap().entries, 2008);
    assert_eq!(git(&clone, &["rev-parse", "HEAD"]).stdout, head_before);
    assert_eq!(git(&clone, &["show-ref"]).stdout, refs_before);
    assert!(!clone.join("FETCH_HEAD").exists());
    // Fetching metadata must not hydrate ordinary sources or the existing fixture.
    assert!(
        repo.git_output(&["cat-file", "-e", &ordinary_oid], false)
            .is_err()
    );
    assert!(
        repo.git_output(
            &[
                "cat-file",
                "-e",
                &hex::encode(snapshot.digest_path(Path::new("hello.txt")).unwrap())
            ],
            false
        )
        .is_err()
    );
    git(&clone, &["remote", "set-url", "origin", "/does/not/exist"]);
    drop(repo);
    let report = ok_cli(&["prefetch-bazel", "--repo", clone.to_str().unwrap()]);
    let mut repo = GitRepo::open(&clone, "git", "origin", SizeFallback::Error).unwrap();
    let report: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert_eq!(report["blobs"]["already_local_blobs"], 2008);
    assert_eq!(report["blobs"]["fetched_blobs"], 0);
    assert_eq!(report["blobs"]["fetch_batches"], 0);
    assert!(
        repo.prefetch_blobs(&["not-an-object-id".to_string()])
            .is_err()
    );
    drop(repo);

    // Repository loading needs more than Starlark: discover only supported
    // literal file references, including an imported rc's downloader config.
    fs::create_dir(fixture.source.join("support")).unwrap();
    fs::write(
        fixture.source.join("MODULE.bazel"),
        b"# patches = ['//support:unused.json']\nsingle_version_override(module_name='example', patches=['//support:fix.patch', '//support:fix.patch'])\npip.parse(requirements_lock='//support:requirements.txt')\n",
    )
    .unwrap();
    fs::write(
        fixture.source.join(".bazelrc"),
        b"common --downloader_config=support/downloader.cfg\nimport %workspace%/support/local.rc\ntry-import %workspace%/support/missing.rc\n",
    )
    .unwrap();
    let discovered = [
        ("support/fix.patch", "patch content\n"),
        ("support/requirements.txt", "dependency==1\n"),
        ("support/downloader.cfg", "rewrite .* example.invalid\n"),
        (
            "support/local.rc",
            "common --downloader_config=support/second.cfg\n",
        ),
        ("support/second.cfg", "block example.invalid\n"),
    ];
    for (path, data) in discovered {
        fs::write(fixture.source.join(path), data).unwrap();
    }
    fs::write(
        fixture.source.join("support/unused.json"),
        b"not a loader input\n",
    )
    .unwrap();
    git(&fixture.source, &["add", "."]);
    git(
        &fixture.source,
        &["commit", "-m", "Repository loading fixture"],
    );
    let discovery_clone = fixture.clone.with_file_name("discovery.git");
    ok_cli(&[
        "clone",
        &format!("file://{}", fixture.source.display()),
        discovery_clone.to_str().unwrap(),
    ]);
    let report = ok_cli(&[
        "prefetch-bazel",
        "--repo",
        discovery_clone.to_str().unwrap(),
    ]);
    let report: serde_json::Value = serde_json::from_slice(&report.stdout).unwrap();
    assert_eq!(report["blobs"]["fetch_batches"], 3);
    assert_eq!(report["discovery_waves"], 2);
    let discovery_repo =
        GitRepo::open(&discovery_clone, "git", "origin", SizeFallback::Error).unwrap();
    let discovery_snapshot = Snapshot::load(&discovery_repo, "HEAD").unwrap();
    for (path, _) in discovered {
        assert!(
            report["selected_paths"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!(path))
        );
        let oid = hex::encode(discovery_snapshot.digest_path(Path::new(path)).unwrap());
        assert!(
            discovery_repo
                .git_output(&["cat-file", "-e", &oid], false)
                .is_ok()
        );
    }
    let unused_oid = hex::encode(
        discovery_snapshot
            .digest_path(Path::new("support/unused.json"))
            .unwrap(),
    );
    assert!(
        discovery_repo
            .git_output(&["cat-file", "-e", &unused_oid], false)
            .is_err()
    );
    assert!(!discovery_clone.join("FETCH_HEAD").exists());
    drop(discovery_repo);

    // Known input manifests can be expressed as additive includes. They join the
    // original metadata/symlink union in one fetch with discovery disabled.
    let included_clone = fixture.clone.with_file_name("included.git");
    ok_cli(&[
        "clone",
        &format!("file://{}", fixture.source.display()),
        included_clone.to_str().unwrap(),
    ]);
    let rejected = cli(&[
        "prefetch-bazel",
        "--repo",
        included_clone.to_str().unwrap(),
        "--include",
        "support/fix.patch",
        "--include",
        "support/does-not-exist",
    ]);
    assert!(!rejected.status.success());
    let included_repo =
        GitRepo::open(&included_clone, "git", "origin", SizeFallback::Error).unwrap();
    let patch_oid = hex::encode(
        discovery_snapshot
            .digest_path(Path::new("support/fix.patch"))
            .unwrap(),
    );
    assert!(
        included_repo
            .git_output(&["cat-file", "-e", &patch_oid], false)
            .is_err()
    );
    drop(included_repo);
    let mut command = Command::new(env!("CARGO_BIN_EXE_git-vfs"));
    command.args([
        "prefetch-bazel",
        "--repo",
        included_clone.to_str().unwrap(),
        "--no-discover",
    ]);
    for (path, _) in discovered {
        command.args(["--include", path]);
    }
    let included = command.output().unwrap();
    assert!(
        included.status.success(),
        "{}",
        String::from_utf8_lossy(&included.stderr)
    );
    let included: serde_json::Value = serde_json::from_slice(&included.stdout).unwrap();
    assert_eq!(included["blobs"]["fetch_batches"], 1);
    assert_eq!(included["discovery_waves"], 0);
    let included_repo =
        GitRepo::open(&included_clone, "git", "origin", SizeFallback::Error).unwrap();
    for (path, _) in discovered {
        let oid = hex::encode(discovery_snapshot.digest_path(Path::new(path)).unwrap());
        assert!(
            included_repo
                .git_output(&["cat-file", "-e", &oid], false)
                .is_ok()
        );
    }
    assert!(
        included_repo
            .git_output(&["cat-file", "-e", &unused_oid], false)
            .is_err()
    );
}

#[test]
fn unsupported_size_metadata_is_strict_unless_fetch_is_explicit() {
    let fixture = Fixture::new();
    git(
        &fixture.source,
        &["config", "transfer.advertiseObjectInfo", "false"],
    );
    let mut repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let oid = hex::encode(snapshot.digest_path(Path::new("hello.txt")).unwrap());
    let error = repo.blob_sizes(std::slice::from_ref(&oid)).unwrap_err();
    assert!(format!("{error:#}").contains("blob-size metadata unavailable"));
    assert_eq!(fixture.missing_blobs(std::slice::from_ref(&oid)), 1);
    assert_eq!(repo.cache_stats().unwrap().entries, 0);
    drop(repo);
    let mut repo = fixture.repo(SizeFallback::Fetch);
    assert_eq!(
        repo.blob_sizes(std::slice::from_ref(&oid)).unwrap()[&oid],
        19
    );
    assert_eq!(fixture.missing_blobs(std::slice::from_ref(&oid)), 0);
    assert_eq!(repo.cache_stats().unwrap().entries, 1);
}

#[test]
fn compatibility_signed_commits_and_replace_refs_preserve_blob_identity() {
    let fixture = Fixture::new();
    let original = git(&fixture.source, &["cat-file", "commit", "HEAD"]).stdout;
    let boundary = original
        .windows(2)
        .position(|bytes| bytes == b"\n\n")
        .unwrap();
    let mut signed = original[..boundary + 1].to_vec();
    signed.extend_from_slice(b"gpgsig -----BEGIN PGP SIGNATURE-----\n harmless-invalid-test-signature\n -----END PGP SIGNATURE-----\n");
    signed.extend_from_slice(&original[boundary + 1..]);
    let mut child = Command::new("git")
        .arg("-C")
        .arg(&fixture.source)
        .args(["hash-object", "-t", "commit", "-w", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(&signed).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let signed_oid = String::from_utf8(output.stdout).unwrap().trim().to_owned();
    git(
        &fixture.source,
        &["update-ref", "refs/heads/main", &signed_oid],
    );
    git(
        &fixture.clone,
        &["fetch", "--filter=blob:none", "origin", "main"],
    );
    git(&fixture.clone, &["config", "log.showSignature", "true"]);
    git(&fixture.clone, &["config", "log.showNotes", "true"]);
    let mut repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, &signed_oid).unwrap();
    assert_eq!(snapshot.commit, signed_oid);
    let hello = hex::encode(snapshot.digest_path(Path::new("hello.txt")).unwrap());
    let binary = hex::encode(snapshot.digest_path(Path::new("binary")).unwrap());
    repo.read_blob(&hello).unwrap();
    repo.read_blob(&binary).unwrap();
    // Replacement refs are legitimate user config, but must not change advertised blob bytes.
    git(&fixture.clone, &["replace", "-f", &hello, &binary]);
    assert_eq!(
        snapshot
            .read_path(&mut repo, Path::new("hello.txt"))
            .unwrap(),
        b"hello from git-vfs\n"
    );
    assert_eq!(
        repo.blob_sizes(std::slice::from_ref(&hello)).unwrap()[&hello],
        19
    );
}

#[test]
fn persistent_blob_reader_reuses_native_git_and_recovers_after_failed_reads() {
    let fixture = Fixture::new();
    let wrapper = fixture.clone.with_file_name("counting-git");
    let calls = fixture.clone.with_file_name("blob-reader-calls");
    fs::write(
        &wrapper,
        format!(
            r#"#!/usr/bin/env python3
import os, sys
if sys.argv[-2:] == ['cat-file', '--batch']:
    assert 'GIT_NO_LAZY_FETCH' not in os.environ
    with open({calls:?}, 'a') as f:
        f.write(str(os.getpid()) + '\n')
    os.write(2, b'x' * (128 * 1024))
os.execvp('git', ['git', *sys.argv[1:]])
"#
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    let mut repo = GitRepo::open(&fixture.clone, &wrapper, "origin", SizeFallback::Error).unwrap();
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    assert!(
        !calls.exists(),
        "metadata must not start the content reader"
    );
    let cases: &[(&str, &[u8])] = &[
        ("binary", &[0, 255, 1, 0, 2]),
        ("empty", b""),
        ("large", &vec![b'x'; 256 * 1024]),
        ("hello.txt", b"hello from git-vfs\n"),
    ];
    for _ in 0..2 {
        for (path, expected) in cases {
            assert_eq!(
                snapshot.read_path(&mut repo, Path::new(path)).unwrap(),
                *expected
            );
        }
    }
    let first_pid: i32 = fs::read_to_string(&calls).unwrap().trim().parse().unwrap();
    assert_eq!(unsafe { libc::kill(first_pid, 0) }, 0);
    let old_oid = hex::encode(snapshot.digest_path(Path::new("hello.txt")).unwrap());
    fs::write(fixture.source.join("hello.txt"), b"changed source\n").unwrap();
    git(&fixture.source, &["add", "hello.txt"]);
    git(&fixture.source, &["commit", "-m", "Change source"]);
    assert_eq!(repo.read_blob(&old_oid).unwrap(), b"hello from git-vfs\n");
    // Wrong-type and missing objects must discard the reader, not leave unread
    // protocol bytes that corrupt the next valid request. No automatic retry.
    assert!(repo.read_blob(&snapshot.commit).is_err());
    assert_eq!(
        unsafe { libc::kill(first_pid, 0) },
        -1,
        "failed reader must be reaped"
    );
    assert_eq!(repo.read_blob(&old_oid).unwrap(), b"hello from git-vfs\n");
    assert_eq!(fs::read_to_string(&calls).unwrap().lines().count(), 2);
    assert!(
        repo.read_blob("0000000000000000000000000000000000000000")
            .is_err()
    );
    assert_eq!(fs::read_to_string(&calls).unwrap().lines().count(), 2);
    assert_eq!(repo.read_blob(&old_oid).unwrap(), b"hello from git-vfs\n");
    let pids: Vec<i32> = fs::read_to_string(&calls)
        .unwrap()
        .lines()
        .map(|line| line.parse().unwrap())
        .collect();
    assert_eq!(pids.len(), 3);
    drop(repo);
    for pid in pids {
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            -1,
            "owned Git child must be reaped"
        );
    }
}

#[test]
fn persistent_blob_reader_rejects_invalid_framing_without_poisoning_later_reads() {
    let fixture = Fixture::new();
    let wrapper = fixture.clone.with_file_name("invalid-reader-git");
    let fault = fixture.clone.with_file_name("reader-fault");
    fs::write(
        &wrapper,
        format!(
            r#"#!/usr/bin/env python3
import os, sys
if sys.argv[-2:] == ['cat-file', '--batch'] and os.path.exists({fault:?}):
    with open({fault:?}) as f:
        mode = f.read()
    os.unlink({fault:?})
    oid = sys.stdin.readline().strip()
    payloads = {{
        'wrong-oid': '0' * 40 + ' blob 1\nx\n',
        'invalid-size': oid + ' blob +1\nx\n',
        'oversized-header': 'x' * 129 + '\n',
        'truncated-body': oid + ' blob 5\nx',
        'invalid-terminator': oid + ' blob 1\nx!',
    }}
    sys.stdout.write(payloads[mode])
    sys.stdout.flush()
    sys.exit(0)
os.execvp('git', ['git', *sys.argv[1:]])
"#
        ),
    )
    .unwrap();
    fs::set_permissions(&wrapper, fs::Permissions::from_mode(0o755)).unwrap();
    for mode in [
        "wrong-oid",
        "invalid-size",
        "oversized-header",
        "truncated-body",
        "invalid-terminator",
    ] {
        fs::write(&fault, mode).unwrap();
        let mut repo =
            GitRepo::open(&fixture.clone, &wrapper, "origin", SizeFallback::Error).unwrap();
        let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
        let oid = hex::encode(snapshot.digest_path(Path::new("binary")).unwrap());
        assert!(repo.read_blob(&oid).is_err(), "{mode}");
        assert_eq!(repo.read_blob(&oid).unwrap(), [0, 255, 1, 0, 2], "{mode}");
    }
}

#[test]
fn concurrent_size_caches_share_entries_and_reject_conflicting_sizes() {
    let fixture = Fixture::new();
    let repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let oid = hex::encode(snapshot.digest_path(Path::new("hello.txt")).unwrap());
    drop(repo);
    std::thread::scope(|scope| {
        let mut workers = Vec::new();
        for _ in 0..4 {
            let oid = &oid;
            let fixture = &fixture;
            workers.push(scope.spawn(move || {
                let mut repo = fixture.repo(SizeFallback::Error);
                assert_eq!(repo.blob_sizes(std::slice::from_ref(oid)).unwrap()[oid], 19);
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
    });
    let mut repo = fixture.repo(SizeFallback::Error);
    assert_eq!(repo.cache_stats().unwrap().entries, 1);
    {
        let database = open_fjall(&fixture.clone.join("git-vfs-sizes.fjall")).unwrap();
        let sizes = database
            .keyspace("blob_sizes", fjall::KeyspaceCreateOptions::default)
            .unwrap();
        let mut tx = database
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        tx.insert(&sizes, hex::decode(&oid).unwrap(), 999u64.to_be_bytes());
        tx.commit().unwrap();
    }
    let error = repo.read_blob(&oid).unwrap_err();
    assert!(format!("{error:#}").contains("conflicting size"));
    let size = repo.blob_sizes(std::slice::from_ref(&oid)).unwrap()[&oid];
    assert_eq!(
        size, 999,
        "conflicts must not silently overwrite immutable metadata"
    );
}

fn xattr(path: &Path, name: &str) -> Vec<u8> {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let name = CString::new(name).unwrap();
    let size = unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
    assert!(size >= 0, "getxattr: {}", std::io::Error::last_os_error());
    let mut value = vec![0; size as usize];
    let actual = unsafe {
        libc::getxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            value.len(),
        )
    };
    assert_eq!(actual, size);
    value
}

struct Mounted {
    child: Child,
    path: PathBuf,
}

fn writable_mount(fixture: &Fixture, overlay: &Path) -> Mounted {
    writable_mount_with_env(fixture, overlay, &[])
}

fn writable_mount_with_env(
    fixture: &Fixture,
    overlay: &Path,
    environment: &[(&str, &str)],
) -> Mounted {
    writable_mount_options(fixture, overlay, environment, &[])
}

fn writable_mount_options(
    fixture: &Fixture,
    overlay: &Path,
    environment: &[(&str, &str)],
    extra: &[&str],
) -> Mounted {
    writable_mount_options_with_stderr(fixture, overlay, environment, extra, Stdio::inherit())
}
fn writable_mount_options_with_stderr(
    fixture: &Fixture,
    overlay: &Path,
    environment: &[(&str, &str)],
    extra: &[&str],
    stderr: Stdio,
) -> Mounted {
    let child = Command::new(env!("CARGO_BIN_EXE_git-vfs"))
        .envs(environment.iter().copied())
        .arg("mount")
        .args(extra)
        .arg("--repo")
        .arg(&fixture.clone)
        .arg("--overlay")
        .arg(overlay)
        .arg(&fixture.mount)
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .unwrap();
    let mut mounted = Mounted {
        child,
        path: fixture.mount.clone(),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            mounted.child.try_wait().unwrap().is_none(),
            "writable mount daemon exited early"
        );
        if fs::read_to_string("/proc/self/mountinfo")
            .unwrap()
            .lines()
            .any(|line| line.split_whitespace().nth(4) == fixture.mount.to_str())
        {
            return mounted;
        }
        assert!(
            Instant::now() < deadline,
            "writable mount startup timed out"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

// The wrapper normally pauses only explicit speculative fetches. A marker can
// instead pause the demand-only blob reader, without blocking metadata commands.
struct ControlledPrefetch {
    wrapper: PathBuf,
    marker: PathBuf,
    release: PathBuf,
    requests: PathBuf,
    failure: PathBuf,
    stderr: PathBuf,
    trace: PathBuf,
}

impl ControlledPrefetch {
    fn new(fixture: &Fixture) -> Self {
        let root = fixture._temp.path();
        let control = Self {
            wrapper: root.join("controlled-git"),
            marker: root.join("fetch-started"),
            release: root.join("fetch-release"),
            requests: root.join("fetch-requests"),
            failure: root.join("fetch-fail-once"),
            stderr: root.join("mount-stderr"),
            trace: root.join("native-git.trace"),
        };
        fs::write(
            &control.wrapper,
            r#"#!/usr/bin/env python3
import os, pathlib, sys, time
root = pathlib.Path(__file__).parent
is_fetch = 'fetch' in sys.argv and '--stdin' in sys.argv
is_demand = (root / 'pause-demand').exists() and sys.argv[-2:] == ['cat-file', '--batch'] and os.environ.get('GIT_NO_LAZY_FETCH') != '1'
if is_fetch or is_demand:
    with (root / 'fetch-requests').open('a') as log:
        log.write('fetch\n' if is_fetch else 'demand\n')
    (root / 'fetch-started').write_text(f'{os.getpid()} {os.getpgrp()}\n')
    while not (root / 'fetch-release').exists():
        time.sleep(0.01)
    failure = root / 'fetch-fail-once'
    if failure.exists():
        failure.unlink()
        sys.exit(1)
os.execvp('git', ['git', *sys.argv[1:]])
"#,
        )
        .unwrap();
        fs::set_permissions(&control.wrapper, fs::Permissions::from_mode(0o755)).unwrap();
        control
    }

    fn mount(&self, fixture: &Fixture, args: &[&str]) -> Mounted {
        self.mount_with_prefetch(fixture, args, true)
    }

    fn mount_with_prefetch(&self, fixture: &Fixture, args: &[&str], prefetch: bool) -> Mounted {
        let mut command = Command::new(env!("CARGO_BIN_EXE_git-vfs"));
        command
            .arg("--git")
            .arg(&self.wrapper)
            .env("GIT_TRACE", &self.trace)
            .args(["mount", "--repo"])
            .arg(&fixture.clone)
            .arg("--overlay")
            .arg(fixture._temp.path().join("async-overlay"));
        if prefetch {
            command.arg("--prefetch-bazel");
        }
        let child = command
            .args(args)
            .arg(&fixture.mount)
            .stdout(Stdio::null())
            .stderr(fs::File::create(&self.stderr).unwrap())
            .spawn()
            .unwrap();
        let mut mounted = Mounted {
            child,
            path: fixture.mount.clone(),
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            assert!(
                mounted.child.try_wait().unwrap().is_none(),
                "mount exited: {}",
                fs::read_to_string(&self.stderr).unwrap()
            );
            if fs::read_to_string("/proc/self/mountinfo")
                .unwrap()
                .lines()
                .any(|line| line.split_whitespace().nth(4) == fixture.mount.to_str())
            {
                return mounted;
            }
            assert!(Instant::now() < deadline, "async mount startup timed out");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn wait_for(&self, predicate: impl Fn() -> bool, message: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !predicate() {
            assert!(
                Instant::now() < deadline,
                "{message}: {}",
                fs::read_to_string(&self.stderr).unwrap_or_default()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn release(&self) {
        fs::write(&self.release, b"continue").unwrap();
    }
}

fn reader_is_reading_mount(child: &Child, path: &Path) -> bool {
    let syscall = fs::read_to_string(format!("/proc/{}/syscall", child.id())).unwrap_or_default();
    let fields: Vec<_> = syscall.split_whitespace().collect();
    if fields.first().and_then(|nr| nr.parse::<i64>().ok()) != Some(libc::SYS_read) {
        return false;
    }
    fields
        .get(1)
        .and_then(|fd| usize::from_str_radix(fd.trim_start_matches("0x"), 16).ok())
        .and_then(|fd| fs::read_link(format!("/proc/{}/fd/{fd}", child.id())).ok())
        .is_some_and(|actual| actual == path)
}

fn wait_for_command(mut child: Child, message: &str) -> Output {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if child.try_wait().unwrap().is_some() {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{message}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            return output;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("{message} timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
#[ignore = "requires Linux /dev/fuse and permission to mount"]
fn mounted_background_prefetch_is_live_scoped_and_shares_hydration_with_reads() {
    let mut fixture = Fixture::with_blob_count(2);
    fs::create_dir(fixture.source.join("scoped")).unwrap();
    fs::create_dir(fixture.source.join("excluded")).unwrap();
    fs::write(fixture.source.join("scoped/BUILD"), b"# scoped metadata\n").unwrap();
    fs::write(
        fixture.source.join("excluded/BUILD"),
        b"# excluded metadata\n",
    )
    .unwrap();
    symlink("../hello.txt", fixture.source.join("scoped/link")).unwrap();
    git(&fixture.source, &["add", "."]);
    git(
        &fixture.source,
        &["commit", "-m", "Scoped async prefetch fixture"],
    );
    fixture.clone = fixture.clone.with_file_name("async.git");
    ok_cli(&[
        "clone",
        &format!("file://{}", fixture.source.display()),
        fixture.clone.to_str().unwrap(),
    ]);
    let repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let oid = |path| hex::encode(snapshot.digest_path(Path::new(path)).unwrap());
    let selected = [oid("scoped/BUILD"), oid("scoped/link"), oid("hello.txt")];
    let excluded = [oid("excluded/BUILD"), oid("link"), oid("binary")];
    drop(repo);
    let refs_before = git(&fixture.clone, &["show-ref"]).stdout;
    let head_before = git(&fixture.clone, &["rev-parse", "HEAD"]).stdout;
    let control = ControlledPrefetch::new(&fixture);
    let mounted = control.mount(
        &fixture,
        &[
            "--prefetch-path",
            "scoped",
            "--prefetch-include",
            "hello.txt",
            "--prefetch-no-discover",
        ],
    );
    control.wait_for(|| control.marker.exists(), "background fetch never started");
    let stat = wait_for_command(
        Command::new("stat")
            .arg("--format=%s")
            .arg(fixture.mount.join("hello.txt"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        "metadata during prefetch",
    );
    assert_eq!(stat.stdout, b"19\n");
    assert_eq!(fixture.missing_blobs(&selected), selected.len());
    let mut reader = Command::new("cat")
        .arg(fixture.mount.join("hello.txt"))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Observe read(2) on the mounted file before releasing the transfer.
    control.wait_for(
        || reader_is_reading_mount(&reader, &fixture.mount.join("hello.txt")),
        "concurrent reader never entered read on the mounted file",
    );
    assert!(reader.try_wait().unwrap().is_none());
    control.release();
    let output = wait_for_command(reader, "read concurrent with prefetch");
    assert_eq!(output.stdout, b"hello from git-vfs\n");
    control.wait_for(
        || {
            fs::read_to_string(&control.stderr)
                .unwrap()
                .contains("Background prefetch:")
        },
        "background prefetch never completed",
    );
    assert_eq!(fixture.missing_blobs(&selected), 0);
    assert_eq!(fixture.missing_blobs(&excluded), excluded.len());
    assert_eq!(
        fs::read_link(fixture.mount.join("scoped/link")).unwrap(),
        Path::new("../hello.txt")
    );
    assert_eq!(
        fs::read_to_string(&control.requests).unwrap(),
        "fetch\n",
        "read must not duplicate the batch fetch"
    );
    let native_fetches = fs::read_to_string(&control.trace)
        .unwrap()
        .lines()
        .filter(|line| line.contains("built-in: git ") && line.contains(" fetch "))
        .count();
    assert_eq!(
        native_fetches, 1,
        "a concurrent demand read must not trigger an implicit native fetch"
    );
    assert_eq!(git(&fixture.clone, &["show-ref"]).stdout, refs_before);
    assert_eq!(
        git(&fixture.clone, &["rev-parse", "HEAD"]).stdout,
        head_before
    );
    drop(mounted);
}

#[test]
#[ignore = "requires Linux /dev/fuse and permission to mount"]
fn mounted_background_prefetch_cancels_stalled_fetch_on_unmount() {
    struct Case {
        name: &'static str,
        prefetch: bool,
        expected_requests: &'static str,
    }
    for case in [
        Case {
            name: "stalled speculative prefetch",
            prefetch: true,
            expected_requests: "fetch\n",
        },
        Case {
            name: "stalled explicit demand without prefetch",
            prefetch: false,
            expected_requests: "fetch\n",
        },
    ] {
        let fixture = Fixture::with_blob_count(2);
        let control = ControlledPrefetch::new(&fixture);
        if !case.prefetch {
            fs::write(fixture._temp.path().join("pause-demand"), b"pause").unwrap();
        }
        let args: &[&str] = if case.prefetch {
            &["--prefetch-include", "hello.txt"]
        } else {
            &[]
        };
        let mut mounted = control.mount_with_prefetch(&fixture, args, case.prefetch);
        if case.prefetch {
            control.wait_for(|| control.marker.exists(), "background fetch never started");
        }
        let mut reader = Command::new("cat")
            .arg(fixture.mount.join("hello.txt"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        control.wait_for(
            || control.marker.exists(),
            "controlled content reader never started",
        );
        let ids: Vec<i32> = fs::read_to_string(&control.marker)
            .unwrap()
            .split_whitespace()
            .map(|id| id.parse().unwrap())
            .collect();
        assert_eq!(ids.len(), 2, "{}", case.name);
        assert_ne!(
            ids[1],
            unsafe { libc::getpgrp() },
            "{} must have an independently cancellable process group",
            case.name
        );
        control.wait_for(
            || reader_is_reading_mount(&reader, &fixture.mount.join("hello.txt")),
            "reader never blocked behind controlled content operation",
        );
        let started = Instant::now();
        unsafe {
            libc::kill(mounted.child.id() as i32, libc::SIGINT);
        }
        loop {
            let daemon_exited = mounted.child.try_wait().unwrap().is_some();
            let reader_exited = reader.try_wait().unwrap().is_some();
            if daemon_exited && reader_exited {
                break;
            }
            assert!(
                started.elapsed() < Duration::from_secs(5),
                "shutdown exceeded deadline: daemon_exited={daemon_exited}, reader_exited={reader_exited}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = reader.wait_with_output().unwrap();
        assert!(
            !output.status.success(),
            "pending read unexpectedly succeeded during shutdown"
        );
        assert!(output.stdout.is_empty());
        let native_fetches = fs::read_to_string(&control.trace)
            .unwrap()
            .lines()
            .filter(|line| line.contains("built-in: git ") && line.contains(" fetch "))
            .count();
        assert_eq!(
            native_fetches, 0,
            "shutdown must not start an implicit lazy fetch after cancelling the paused batch"
        );
        assert!(!control.release.exists());
        assert_eq!(
            fs::read_to_string(&control.requests).unwrap(),
            case.expected_requests,
            "{}: cancelled content workers must not start another batch",
            case.name
        );
        assert_eq!(
            unsafe { libc::kill(ids[0], 0) },
            -1,
            "fetch wrapper survived unmount"
        );
        assert_eq!(
            unsafe { libc::kill(-ids[1], 0) },
            -1,
            "fetch process group survived unmount"
        );
        let shutdown = fs::read_to_string(&control.stderr).unwrap();
        let reports: Vec<_> = shutdown
            .lines()
            .filter_map(|line| line.strip_prefix("Overlay state: "))
            .collect();
        assert_eq!(reports.len(), 1, "{}: {shutdown}", case.name);
        let state: serde_json::Value = serde_json::from_str(reports[0]).unwrap();
        assert!(state["entry_count"].as_u64().unwrap() > 0);
        assert_eq!(state["dead_payload_bytes"], 0);
    }
}

#[test]
#[ignore = "requires Linux /dev/fuse and permission to mount"]
fn mounted_background_prefetch_failure_does_not_break_later_demand_reads() {
    let fixture = Fixture::with_blob_count(2);
    let control = ControlledPrefetch::new(&fixture);
    fs::write(&control.failure, b"fail once").unwrap();
    let mounted = control.mount(
        &fixture,
        &["--prefetch-include", "hello.txt", "--prefetch-no-discover"],
    );
    control.wait_for(|| control.marker.exists(), "background fetch never started");
    control.release();
    control.wait_for(
        || {
            fs::read_to_string(&control.stderr)
                .unwrap()
                .contains("Background prefetch failed:")
        },
        "background failure was not reported",
    );
    let output = wait_for_command(
        Command::new("cat")
            .arg(fixture.mount.join("hello.txt"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
        "read after speculative fetch failure",
    );
    assert_eq!(output.stdout, b"hello from git-vfs\n");
    drop(mounted);
}

#[test]
fn mount_rejects_invalid_background_prefetch_includes_before_mounting() {
    let fixture = Fixture::with_blob_count(2);
    let refs_before = git(&fixture.clone, &["show-ref"]).stdout;
    for invalid in ["../hello.txt", "missing.txt"] {
        let output = cli(&[
            "mount",
            "--repo",
            fixture.clone.to_str().unwrap(),
            "--prefetch-bazel",
            "--prefetch-include",
            invalid,
            fixture.mount.to_str().unwrap(),
        ]);
        assert!(!output.status.success());
        assert!(
            String::from_utf8_lossy(&output.stderr).contains(if invalid.starts_with("..") {
                "repository-relative"
            } else {
                "absent from the pinned snapshot"
            }),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(fs::read_dir(&fixture.mount).unwrap().count(), 0);
    }
    assert_eq!(git(&fixture.clone, &["show-ref"]).stdout, refs_before);
}

fn status(path: &Path) -> Vec<Vec<u8>> {
    git(
        path,
        &["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    )
    .stdout
    .split(|&b| b == 0)
    .filter(|entry| !entry.is_empty())
    .map(Vec::from)
    .collect()
}

#[test]
#[ignore = "requires Linux /dev/fuse and permission to mount"]
fn writable_mount_native_git_status_uses_journal_and_survives_remount() {
    let mut fixture = Fixture::new();
    fs::write(fixture.source.join("second-commit.txt"), b"second commit\n").unwrap();
    git(&fixture.source, &["add", "."]);
    git(&fixture.source, &["commit", "-m", "Second fixture commit"]);
    let shallow_clone = fixture.clone.with_file_name("shallow.git");
    ok_cli(&[
        "clone",
        &format!("file://{}", fixture.source.display()),
        shallow_clone.to_str().unwrap(),
        "--depth",
        "1",
    ]);
    fixture.clone = shallow_clone;
    let overlay = fixture._temp.path().join("overlay state 'quoted'");
    let repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let mut oids: Vec<_> = snapshot
        .entries()
        .into_iter()
        .filter(|e| matches!(e.kind.as_str(), "file" | "symlink"))
        .filter_map(|e| e.oid)
        .collect();
    oids.sort();
    oids.dedup();
    drop(repo);
    let mut mounted = writable_mount(&fixture, &overlay);
    assert_eq!(
        git(&fixture.mount, &["rev-parse", "--is-shallow-repository"]).stdout,
        b"true\n"
    );
    assert_eq!(
        git(&fixture.mount, &["rev-list", "--count", "HEAD"]).stdout,
        b"1\n"
    );
    assert_eq!(status(&fixture.mount), Vec::<Vec<u8>>::new());
    git(
        &fixture.mount,
        &[
            "fetch",
            "--unshallow",
            "--no-tags",
            "--filter=blob:none",
            "origin",
            "main",
        ],
    );
    assert_eq!(
        git(&fixture.mount, &["rev-list", "--count", "HEAD"]).stdout,
        b"2\n"
    );
    assert!(
        fixture.clone.join("shallow").exists(),
        "native Git history stays isolated from the base clone"
    );
    assert_eq!(
        fixture.missing_blobs(&oids),
        oids.len(),
        "clean native status must not hydrate the baseline"
    );
    fs::write(fixture.mount.join("empty"), b"temporary change").unwrap();
    assert!(
        status(&fixture.mount)
            .iter()
            .any(|entry| entry == b" M empty")
    );
    fs::write(fixture.mount.join("empty"), b"").unwrap();
    assert!(
        status(&fixture.mount).is_empty(),
        "journal entries are candidates, not an assertion that contents differ"
    );
    fs::write(
        fixture.mount.join("hello.txt"),
        b"edited from writable vfs\n",
    )
    .unwrap();
    fs::write(
        fixture.mount.join(&fixture.odd_name),
        b"updated native filename",
    )
    .unwrap();
    fs::remove_file(fixture.mount.join("binary")).unwrap();
    fs::rename(
        fixture.mount.join("run.sh"),
        fixture.mount.join("run-renamed.sh"),
    )
    .unwrap();
    fs::create_dir(fixture.mount.join("new-dir")).unwrap();
    fs::write(fixture.mount.join("new-dir/new.txt"), b"new file\n").unwrap();
    symlink("hello.txt", fixture.mount.join("new-link")).unwrap();
    let actual = status(&fixture.mount);
    assert!(actual.iter().any(|entry| entry == b" M odd \n\xff name"));
    for expected in [
        b" M hello.txt".as_slice(),
        b" D binary",
        b" D run.sh",
        b"?? run-renamed.sh",
        b"?? new-dir/new.txt",
        b"?? new-link",
    ] {
        assert!(
            actual.iter().any(|entry| entry == expected),
            "missing {expected:?} in {actual:?}"
        );
    }
    assert_eq!(
        fixture.missing_blobs(&oids),
        oids.len(),
        "status must not fetch unrelated clean files"
    );
    let calculated = git(
        &fixture.mount,
        &["hash-object", "--no-filters", "hello.txt"],
    );
    assert_eq!(
        xattr(&fixture.mount.join("hello.txt"), "user.gitsha1.hex"),
        String::from_utf8(calculated.stdout)
            .unwrap()
            .trim()
            .as_bytes()
    );
    // Staging changes the isolated index; a subsequent same-size edit must still be detected.
    git(&fixture.mount, &["add", "hello.txt"]);
    assert!(
        status(&fixture.mount)
            .iter()
            .any(|entry| entry == b"M  hello.txt")
    );
    fs::write(
        fixture.mount.join("hello.txt"),
        b"edited from writable VFS\n",
    )
    .unwrap();
    assert!(
        status(&fixture.mount)
            .iter()
            .any(|entry| entry == b"MM hello.txt")
    );
    // Exercise native Git's large-file hashing/private mmap workflow.
    fs::write(fixture.mount.join("large"), vec![b'y'; 256 * 1024]).unwrap();
    git(&fixture.mount, &["add", "large"]);
    assert!(
        status(&fixture.mount)
            .iter()
            .any(|entry| entry == b"M  large")
    );
    let base_head = git(&fixture.clone, &["rev-parse", "HEAD"]).stdout;
    let committed = Command::new("git")
        .arg("-C")
        .arg(&fixture.mount)
        .args([
            "-c",
            "user.name=VFS Fixture",
            "-c",
            "user.email=fixture@example.test",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-m",
            "Commit staged fixture edits",
        ])
        .env_remove("GIT_NO_LAZY_FETCH")
        .output()
        .unwrap();
    assert!(
        committed.status.success(),
        "{}",
        String::from_utf8_lossy(&committed.stderr)
    );
    assert_eq!(
        git(&fixture.clone, &["rev-parse", "HEAD"]).stdout,
        base_head
    );
    assert_eq!(
        git(&fixture.mount, &["log", "-1", "--format=%s"]).stdout,
        b"Commit staged fixture edits\n"
    );
    // Native commit is functional, but stock Git hydrates missing indexed objects
    // into this private admin. Base-only absence is isolation, NOT lazy-commit proof.
    git(
        &overlay.join("git"),
        &["cat-file", "-e", "HEAD:many/file-134"],
    );
    let before = status(&fixture.mount);
    let missing = fixture.missing_blobs(&oids);
    assert_eq!(missing, oids.len(), "fixture must begin blobless");
    // Idle descriptors must not keep the session thread or overlay owner alive.
    // Keep both descriptors open until after the same overlay has remounted.
    let idle_file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.mount.join("hello.txt"))
        .unwrap();
    idle_file.sync_all().unwrap();
    let idle_directory = fs::read_dir(fixture.mount.join("new-dir")).unwrap();
    let (journal_token, _) = git_vfs::overlay::changes_since(&overlay, "").unwrap();
    let shutdown_started = Instant::now();
    assert_eq!(
        unsafe { libc::kill(mounted.child.id() as i32, libc::SIGINT) },
        0
    );
    loop {
        if let Some(exit) = mounted.child.try_wait().unwrap() {
            assert!(exit.success(), "idle-descriptor shutdown failed: {exit}");
            break;
        }
        assert!(
            shutdown_started.elapsed() < Duration::from_secs(5),
            "daemon did not exit while idle file and directory descriptors remained open"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    drop(mounted);
    // Immediate reacquisition proves the daemon joined its filesystem/database
    // owner before returning, rather than leaving a detached session behind.
    let mounted = writable_mount(&fixture, &overlay);
    assert_eq!(
        git_vfs::overlay::changes_since(&overlay, &journal_token).unwrap(),
        (journal_token, vec![])
    );
    drop(idle_directory);
    drop(idle_file);
    assert_eq!(
        git(&fixture.mount, &["rev-parse", "--is-shallow-repository"]).stdout,
        b"false\n"
    );
    assert_eq!(
        git(&fixture.mount, &["rev-list", "--count", "HEAD"]).stdout,
        b"3\n"
    );
    assert_eq!(
        fs::read(fixture.mount.join("hello.txt")).unwrap(),
        b"edited from writable VFS\n"
    );
    assert_eq!(status(&fixture.mount), before);
    assert_eq!(fixture.missing_blobs(&oids), missing);
    assert!(!fixture.mount.join("binary").exists());
    assert_eq!(
        fs::read_link(fixture.mount.join("new-link")).unwrap(),
        Path::new("hello.txt")
    );
    drop(mounted);
}

#[test]
#[ignore = "requires Linux /dev/fuse and permission to mount"]
fn writable_trimmed_history_bootstrap_still_reports_all_edits_after_crash() {
    let fixture = Fixture::with_blob_count(2);
    let overlay = fixture._temp.path().join("trimmed-bootstrap");
    let mut mounted = writable_mount(&fixture, &overlay);
    assert!(status(&fixture.mount).is_empty());
    fs::write(fixture.mount.join("hello.txt"), b"preserved edit\n").unwrap();
    fs::remove_file(fixture.mount.join("binary")).unwrap();
    fs::rename(
        fixture.mount.join("run.sh"),
        fixture.mount.join("renamed.sh"),
    )
    .unwrap();
    let (old_token, _) = git_vfs::overlay::changes_since(&overlay, "").unwrap();
    for n in 0..4097 {
        fs::set_permissions(
            fixture.mount.join("hello.txt"),
            fs::Permissions::from_mode(if n % 2 == 0 { 0o644 } else { 0o600 }),
        )
        .unwrap();
    }
    assert_eq!(
        git_vfs::overlay::changes_since(&overlay, &old_token)
            .unwrap()
            .1,
        [b"/".to_vec()]
    );
    let expected = status(&fixture.mount);
    for path in [
        b" M hello.txt".as_slice(),
        b" D binary",
        b" D run.sh",
        b"?? renamed.sh",
    ] {
        assert!(
            expected.iter().any(|entry| entry == path),
            "missing {path:?}: {expected:?}"
        );
    }
    // Model an interrupted first index setup, preserving every other state file.
    mounted.child.kill().unwrap();
    assert!(
        Command::new("fusermount3")
            .arg("-uz")
            .arg(&fixture.mount)
            .status()
            .unwrap()
            .success()
    );
    drop(mounted);
    fs::remove_file(overlay.join("git-ready")).unwrap();
    let mounted = writable_mount(&fixture, &overlay);
    assert_eq!(
        fs::read(fixture.mount.join("hello.txt")).unwrap(),
        b"preserved edit\n"
    );
    assert!(!fixture.mount.join("binary").exists());
    assert_eq!(
        status(&fixture.mount),
        expected,
        "bounded history must not erase index-bootstrap dirty candidates"
    );
    drop(mounted);
}

#[test]
#[ignore = "requires Linux /dev/fuse and permission to mount"]
fn writable_file_handles_truncation_and_directory_rename_preserve_identity() {
    let fixture = Fixture::new();
    let overlay = fixture._temp.path().join("overlay");
    let mounted = writable_mount(&fixture, &overlay);
    assert!(fs::metadata(fixture.mount.join("first")).is_err());
    fs::write(fixture.mount.join("first"), b"first content").unwrap();
    fs::write(fixture.mount.join("second"), b"second content").unwrap();
    assert_eq!(fs::metadata(fixture.mount.join("first")).unwrap().len(), 13);
    assert_eq!(
        fs::metadata(fixture.mount.join("second")).unwrap().len(),
        14
    );
    let mut first = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(fixture.mount.join("first"))
        .unwrap();
    let mut second = fs::File::open(fixture.mount.join("second")).unwrap();
    // Read-only close needs no sync, but explicit fsync remains supported.
    second.sync_all().unwrap();
    fs::rename(fixture.mount.join("first"), fixture.mount.join("second")).unwrap();
    assert!(fs::metadata(fixture.mount.join("first")).is_err());
    assert_eq!(
        fs::metadata(fixture.mount.join("second")).unwrap().len(),
        13
    );
    assert_eq!(second.metadata().unwrap().nlink(), 0);
    let mut old = String::new();
    second.read_to_string(&mut old).unwrap();
    assert_eq!(
        old, "second content",
        "overwritten inode must remain available through its open handle"
    );
    assert_eq!(
        fs::read(fixture.mount.join("second")).unwrap(),
        b"first content"
    );
    fs::remove_file(fixture.mount.join("second")).unwrap();
    assert!(fs::metadata(fixture.mount.join("second")).is_err());
    assert_eq!(first.metadata().unwrap().nlink(), 0);
    first.seek(SeekFrom::Start(0)).unwrap();
    first.write_all(b"still").unwrap();
    first.sync_data().unwrap();
    first.seek(SeekFrom::Start(0)).unwrap();
    let mut bytes = [0; 5];
    first.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes, b"still");
    drop(first);
    drop(second);
    let large = String::from_utf8(git(&fixture.clone, &["rev-parse", "HEAD:large"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert_eq!(fixture.missing_blobs(std::slice::from_ref(&large)), 1);
    assert_eq!(
        fs::metadata(fixture.mount.join("large")).unwrap().len(),
        256 * 1024
    );
    let empty = fs::File::create(fixture.mount.join("large")).unwrap();
    drop(empty);
    assert_eq!(fs::metadata(fixture.mount.join("large")).unwrap().len(), 0);
    assert_eq!(
        fixture.missing_blobs(std::slice::from_ref(&large)),
        1,
        "truncating to zero must not fetch old content"
    );
    assert_eq!(fs::read(fixture.mount.join("large")).unwrap(), b"");
    assert_eq!(
        xattr(&fixture.mount.join("large"), "user.gitsha1.hex"),
        b"e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
    );
    let root_links = fs::metadata(&fixture.mount).unwrap().nlink();
    fs::create_dir(fixture.mount.join("directory")).unwrap();
    assert_eq!(
        fs::metadata(&fixture.mount).unwrap().nlink(),
        root_links + 1
    );
    fs::write(fixture.mount.join("directory/child"), b"child").unwrap();
    fs::rename(
        fixture.mount.join("directory"),
        fixture.mount.join("renamed-directory"),
    )
    .unwrap();
    assert_eq!(
        fs::metadata(&fixture.mount).unwrap().nlink(),
        root_links + 1
    );
    assert_eq!(
        fs::read(fixture.mount.join("renamed-directory/child")).unwrap(),
        b"child"
    );
    assert!(
        status(&fixture.mount)
            .iter()
            .any(|entry| entry == b"?? renamed-directory/child")
    );
    let mut append = fs::OpenOptions::new()
        .append(true)
        .open(fixture.mount.join("renamed-directory/child"))
        .unwrap();
    assert_eq!(append.metadata().unwrap().len(), 5);
    append.write_all(b" appended").unwrap();
    assert_eq!(append.metadata().unwrap().len(), 14);
    drop(append);
    assert_eq!(
        fs::read(fixture.mount.join("renamed-directory/child")).unwrap(),
        b"child appended"
    );
    // Warm attributes, then mutate immediately without truncating or waiting for
    // the positive cache TTL. In-band writes must invalidate even same-size data.
    let child_path = fixture.mount.join("renamed-directory/child");
    let mut child = fs::OpenOptions::new()
        .write(true)
        .open(&child_path)
        .unwrap();
    let old_mtime = std::time::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    child
        .set_times(fs::FileTimes::new().set_modified(old_mtime))
        .unwrap();
    assert_eq!(
        fs::metadata(&child_path).unwrap().modified().unwrap(),
        old_mtime
    );
    child.write_all(b"CHILD appended").unwrap();
    let modified = fs::metadata(&child_path).unwrap();
    assert_eq!(modified.len(), 14);
    assert_ne!(modified.modified().unwrap(), old_mtime);
    assert_eq!(fs::read(&child_path).unwrap(), b"CHILD appended");
    let calculated = git(
        &fixture.mount,
        &["hash-object", "--no-filters", "renamed-directory/child"],
    );
    assert_eq!(
        xattr(&child_path, "user.gitsha1.hex"),
        String::from_utf8(calculated.stdout)
            .unwrap()
            .trim()
            .as_bytes()
    );
    drop(child);
    assert_eq!(
        fs::metadata(fixture.mount.join("run.sh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    fs::set_permissions(
        fixture.mount.join("run.sh"),
        fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    assert_eq!(
        fs::metadata(fixture.mount.join("run.sh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o644
    );
    assert!(
        status(&fixture.mount)
            .iter()
            .any(|entry| entry == b" M run.sh")
    );
    assert!(fs::remove_dir(fixture.mount.join("renamed-directory")).is_err());
    assert!(
        fs::rename(
            fixture.mount.join("renamed-directory"),
            fixture.mount.join("renamed-directory/nested")
        )
        .is_err()
    );
    assert!(fs::write(fixture.mount.join(".git"), b"broken").is_err());
    assert!(fs::remove_file(fixture.mount.join(".git")).is_err());
    assert!(
        fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(fixture.mount.join("hello.txt"))
            .is_err()
    );
    fs::create_dir(fixture.mount.join("new-parent")).unwrap();
    assert_eq!(
        fs::metadata(&fixture.mount).unwrap().nlink(),
        root_links + 2
    );
    fs::rename(
        fixture.mount.join("renamed-directory"),
        fixture.mount.join("new-parent/moved"),
    )
    .unwrap();
    assert_eq!(
        fs::metadata(&fixture.mount).unwrap().nlink(),
        root_links + 1
    );
    assert_eq!(
        fs::metadata(fixture.mount.join("new-parent"))
            .unwrap()
            .nlink(),
        3
    );
    fs::remove_file(fixture.mount.join("new-parent/moved/child")).unwrap();
    fs::remove_dir(fixture.mount.join("new-parent/moved")).unwrap();
    assert_eq!(
        fs::metadata(fixture.mount.join("new-parent"))
            .unwrap()
            .nlink(),
        2
    );
    fs::remove_dir(fixture.mount.join("new-parent")).unwrap();
    assert_eq!(fs::metadata(&fixture.mount).unwrap().nlink(), root_links);
    drop(mounted);
}

#[test]
#[ignore = "requires Linux /dev/fuse and permission to mount"]
fn compatibility_writable_git_admin_recovers_and_ignores_hash_defaults() {
    let fixture = Fixture::new();
    let overlay = fixture._temp.path().join("explicit-sha1-overlay");
    let mounted = writable_mount_with_env(&fixture, &overlay, &[("GIT_DEFAULT_HASH", "sha256")]);
    assert!(status(&fixture.mount).is_empty());
    assert_eq!(
        git(&fixture.mount, &["rev-parse", "--show-object-format"]).stdout,
        b"sha1\n"
    );
    drop(mounted);
    let recovery = fixture._temp.path().join("interrupted-overlay");
    fs::create_dir_all(recovery.join("git")).unwrap();
    git(
        &recovery,
        &["init", "--bare", "--object-format=sha1", "git"],
    );
    assert!(!recovery.join("git/objects/info/alternates").exists());
    let mut mounted = writable_mount(&fixture, &recovery);
    assert!(status(&fixture.mount).is_empty());
    assert!(recovery.join("git/objects/info/alternates").is_file());
    // A second writer cannot mount the same state while the first daemon holds its lock.
    let second_mount = fixture._temp.path().join("second-mount");
    fs::create_dir(&second_mount).unwrap();
    let rejected = cli(&[
        "mount",
        "--repo",
        fixture.clone.to_str().unwrap(),
        "--overlay",
        recovery.to_str().unwrap(),
        second_mount.to_str().unwrap(),
    ]);
    assert!(!rejected.status.success());
    let reason = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        reason.contains("already mounted or locked")
            || reason.contains("Fjall requires exclusive process ownership"),
        "unexpected refusal: {reason}"
    );
    let (token, paths) = git_vfs::overlay::changes_since(&recovery, "foreign-token").unwrap();
    assert_eq!(paths, vec![b"/".to_vec()]);
    assert!(
        git_vfs::overlay::changes_since(&recovery, &token)
            .unwrap()
            .1
            .is_empty()
    );
    fs::write(fixture.mount.join("new-file"), b"new").unwrap();
    assert!(
        git_vfs::overlay::changes_since(&recovery, &token)
            .unwrap()
            .1
            .contains(&b"new-file".to_vec())
    );
    // Acknowledged mutations and their fsmonitor paths survive a killed daemon,
    // not just the graceful shutdown exercised by the other remount test.
    mounted.child.kill().unwrap();
    assert!(
        Command::new("fusermount3")
            .arg("-uz")
            .arg(&fixture.mount)
            .status()
            .unwrap()
            .success()
    );
    drop(mounted);
    let mounted = writable_mount(&fixture, &recovery);
    assert_eq!(fs::read(fixture.mount.join("new-file")).unwrap(), b"new");
    assert!(
        git_vfs::overlay::changes_since(&recovery, &token)
            .unwrap()
            .1
            .contains(&b"new-file".to_vec())
    );
    drop(mounted);
    // Removed backends must not be mistaken for empty Fjall overlays.
    for legacy in ["overlay.sqlite", "overlay.lmdb/data.mdb"] {
        let directory = fixture._temp.path().join(if legacy.ends_with("sqlite") {
            "legacy-sqlite"
        } else {
            "legacy-lmdb"
        });
        let marker = directory.join(legacy);
        fs::create_dir_all(marker.parent().unwrap()).unwrap();
        fs::write(&marker, b"preserve legacy user data").unwrap();
        let rejected = Command::new(env!("CARGO_BIN_EXE_git-vfs"))
            .args(["mount", "--repo"])
            .arg(&fixture.clone)
            .arg("--overlay")
            .arg(&directory)
            .arg(&second_mount)
            .output()
            .unwrap();
        assert!(!rejected.status.success());
        assert!(
            String::from_utf8_lossy(&rejected.stderr)
                .contains("automatic migration is not supported")
        );
        assert_eq!(fs::read(&marker).unwrap(), b"preserve legacy user data");
        assert!(!directory.join("overlay.fjall").exists());
    }
    let mounted = writable_mount(&fixture, &recovery);
    assert_eq!(fs::read(fixture.mount.join("new-file")).unwrap(), b"new");
    drop(mounted);
}

impl Drop for Mounted {
    fn drop(&mut self) {
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGINT);
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.child.try_wait().unwrap().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        if self.child.try_wait().unwrap().is_none() {
            let _ = Command::new("fusermount3")
                .arg("-uz")
                .arg(&self.path)
                .status();
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
#[ignore = "requires Linux /dev/fuse and permission to mount"]
fn mounted_filesystem_exposes_native_reads_and_raw_digest_xattrs() {
    let fixture = Fixture::new();
    let repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let digest = snapshot.digest_path(Path::new("hello.txt")).unwrap();
    let oids: Vec<_> = snapshot
        .entries()
        .into_iter()
        .filter_map(|e| match e.kind.as_str() {
            "file" | "symlink" => e.oid,
            _ => None,
        })
        .collect();
    drop(repo);
    // Separate daemon and reader processes reflect the supported user workflow.
    let child = Command::new(env!("CARGO_BIN_EXE_git-vfs"))
        .arg("mount")
        .arg("--read-only")
        .arg("--repo")
        .arg(&fixture.clone)
        .arg(&fixture.mount)
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut session = Mounted {
        child,
        path: fixture.mount.clone(),
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            session.child.try_wait().unwrap().is_none(),
            "mount daemon exited early"
        );
        if fs::read_to_string("/proc/self/mountinfo")
            .unwrap()
            .lines()
            .any(|line| line.split_whitespace().nth(4) == fixture.mount.to_str())
        {
            break;
        }
        assert!(Instant::now() < deadline, "mount startup timed out");
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(fs::read_dir(&fixture.mount).unwrap().count() >= 9);
    assert_eq!(
        fs::metadata(fixture.mount.join("hello.txt")).unwrap().len(),
        19
    );
    assert_eq!(
        xattr(&fixture.mount.join("hello.txt"), "user.gitsha1"),
        digest
    );
    assert_eq!(
        xattr(&fixture.mount.join("hello.txt"), "user.gitsha1.hex"),
        hex::encode(&digest).as_bytes()
    );
    assert_eq!(fixture.missing_blobs(&oids), oids.len());
    assert_eq!(
        fs::read(fixture.mount.join("hello.txt")).unwrap(),
        b"hello from git-vfs\n"
    );
    assert_eq!(fs::read(fixture.mount.join("empty")).unwrap(), b"");
    assert_eq!(
        fs::read(fixture.mount.join(&fixture.odd_name)).unwrap(),
        b"non-UTF8 path\n"
    );
    assert_eq!(
        fs::read_link(fixture.mount.join("link")).unwrap(),
        Path::new("hello.txt")
    );
    assert_eq!(
        fs::metadata(fixture.mount.join("run.sh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o555
    );
    let mut file = fs::File::open(fixture.mount.join("large")).unwrap();
    file.seek(SeekFrom::Start(1024)).unwrap();
    let mut bytes = [0; 5];
    file.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, [b'x'; 5]);
    file.seek(SeekFrom::End(0)).unwrap();
    assert_eq!(file.read(&mut bytes).unwrap(), 0);
    assert!(fs::write(fixture.mount.join("hello.txt"), b"changed").is_err());
    assert!(fs::write(fixture.mount.join("new-file"), b"new").is_err());
    drop(file);
    drop(session);
    assert_eq!(
        fs::read_dir(&fixture.mount).unwrap().count(),
        0,
        "session drop must unmount"
    );
}

/// One-request HTTP fixture; no credential helpers, shell commands, or external network.
fn digest_proxy_response(
    status: u16,
    body: String,
) -> (String, std::thread::JoinHandle<serde_json::Value>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let handle = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "CLI never contacted proxy");
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accepting proxy request: {error}"),
            }
        };
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            assert!(header.len() < 8192);
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            header.push(byte[0]);
        }
        let header = String::from_utf8(header).unwrap();
        assert!(header.starts_with("POST /api/v1/repos/fixture_repo/objects HTTP/1.1\r\n"));
        assert!(!header.to_ascii_lowercase().contains("authorization:"));
        let length: usize = header
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().unwrap())
            })
            .unwrap();
        assert!(length < 1024);
        let mut request = vec![0; length];
        stream.read_exact(&mut request).unwrap();
        write!(stream, "HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
        serde_json::from_slice(&request).unwrap()
    });
    (origin, handle)
}

#[test]
fn proxy_cas_digest_uses_snapshot_blob_without_local_hydration() {
    let fixture = Fixture::new();
    let entries = Snapshot::load(&fixture.repo(SizeFallback::Error), "HEAD")
        .unwrap()
        .entries();
    let oids: Vec<_> = entries.into_iter().filter_map(|entry| entry.oid).collect();
    let missing = fixture.missing_blobs(&oids);
    for (path, size) in [("hello.txt", 19), ("link", 9), ("empty", 0)] {
        let oid =
            String::from_utf8(git(&fixture.source, &["rev-parse", &format!("HEAD:{path}")]).stdout)
                .unwrap()
                .trim()
                .to_owned();
        let hash = "ab".repeat(32);
        let (origin, server) = digest_proxy_response(
            200,
            serde_json::json!({"objects": [{
                "oid": oid, "object_type": "blob", "size_bytes": size,
                "blake3": {"hash": hash.to_uppercase(), "size_bytes": size}
            }]})
            .to_string(),
        );
        let result = ok_cli(&[
            "cas-digest",
            "--repo",
            fixture.clone.to_str().unwrap(),
            "--proxy-url",
            &origin,
            "--proxy-repo",
            "fixture_repo",
            path,
        ]);
        assert_eq!(
            server.join().unwrap(),
            serde_json::json!({"oids": [oid], "blake3": true})
        );
        let result: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert_eq!(
            result,
            serde_json::json!({"hash": hash, "size_bytes": size, "digest_function": "BLAKE3", "git_sha1": oid})
        );
    }
    assert_eq!(fixture.missing_blobs(&oids), missing);
    // A directory must fail before a network request, regardless of proxy configuration.
    let result = cli(&[
        "cas-digest",
        "--repo",
        fixture.clone.to_str().unwrap(),
        "--proxy-url",
        "http://127.0.0.1:1",
        "--proxy-repo",
        "fixture_repo",
        "many",
    ]);
    assert!(!result.status.success());
    assert!(!String::from_utf8_lossy(&result.stderr).contains("querying proxy"));
}

#[test]
fn proxy_cas_digest_rejects_invalid_mappings_and_http_errors() {
    let fixture = Fixture::new();
    let oid = String::from_utf8(git(&fixture.source, &["rev-parse", "HEAD:hello.txt"]).stdout)
        .unwrap()
        .trim()
        .to_owned();
    let valid = serde_json::json!({"oid": oid, "object_type": "blob", "size_bytes": 19, "blake3": {"hash": "ab".repeat(32), "size_bytes": 19}});
    let mut cases = Vec::new();
    for (name, field, value) in [
        ("different object", "oid", serde_json::json!("0".repeat(40))),
        ("tree", "object_type", serde_json::json!("tree")),
        (
            "REAPI size overflow",
            "size_bytes",
            serde_json::json!(u64::MAX),
        ),
        ("negative size", "size_bytes", serde_json::json!(-1)),
        ("disabled BLAKE3", "blake3", serde_json::Value::Null),
        (
            "wrong digest size",
            "blake3",
            serde_json::json!({"hash": "ab".repeat(32), "size_bytes": 20}),
        ),
        (
            "short hash",
            "blake3",
            serde_json::json!({"hash": "abcd", "size_bytes": 19}),
        ),
        (
            "nonhex hash",
            "blake3",
            serde_json::json!({"hash": "gg".repeat(32), "size_bytes": 19}),
        ),
    ] {
        let mut object = valid.clone();
        object[field] = value;
        cases.push((
            name,
            200,
            serde_json::json!({"objects": [object]}).to_string(),
        ));
    }
    cases.extend([
        ("missing object", 200, "{\"objects\":[]}".into()),
        (
            "duplicate object",
            200,
            serde_json::json!({"objects": [valid.clone(), valid.clone()]}).to_string(),
        ),
        ("malformed JSON", 200, "not json".into()),
        ("response bound", 200, " ".repeat(65 * 1024)),
        ("HTTP failure", 503, "unavailable".into()),
        ("redirect", 302, "redirect".into()),
    ]);
    for (name, status, body) in cases {
        let (origin, server) = digest_proxy_response(status, body);
        let result = cli(&[
            "cas-digest",
            "--repo",
            fixture.clone.to_str().unwrap(),
            "--proxy-url",
            &origin,
            "--proxy-repo",
            "fixture_repo",
            "hello.txt",
        ]);
        server.join().unwrap();
        assert!(!result.status.success(), "accepted {name}");
        assert!(result.stdout.is_empty(), "printed digest for {name}");
    }
    assert_eq!(fixture.missing_blobs(&[oid]), 1);
}

#[test]
fn proxy_cas_digest_requires_explicit_origin_and_safe_alias() {
    let fixture = Fixture::new();
    for (origin, alias) in [
        ("file:///tmp/proxy", "fixture_repo"),
        ("http://user:secret@127.0.0.1:1", "fixture_repo"),
        ("http://127.0.0.1:1/prefix", "fixture_repo"),
        ("http://127.0.0.1:1/prefix/..", "fixture_repo"),
        ("http://127.0.0.1:1?token=secret", "fixture_repo"),
        ("http://127.0.0.1:1#fragment", "fixture_repo"),
        ("http://127.0.0.1:1", "../other"),
        ("http://127.0.0.1:1", ""),
    ] {
        let result = cli(&[
            "cas-digest",
            "--repo",
            fixture.clone.to_str().unwrap(),
            "--proxy-url",
            origin,
            "--proxy-repo",
            alias,
            "hello.txt",
        ]);
        assert!(!result.status.success(), "accepted {origin} / {alias}");
        assert!(!String::from_utf8_lossy(&result.stderr).contains("querying proxy"));
    }
}

fn snapshot_mapping_response(fixture: &Fixture) -> (Vec<String>, serde_json::Value) {
    let repo = fixture.repo(SizeFallback::Error);
    let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
    let mut oids: Vec<_> = snapshot
        .entries()
        .into_iter()
        .filter(|entry| matches!(entry.kind.as_str(), "file" | "symlink"))
        .filter_map(|entry| entry.oid)
        .collect();
    oids.sort();
    oids.dedup();
    let objects: Vec<_> = oids
        .iter()
        .map(|oid| {
            let data = git(&fixture.source, &["cat-file", "blob", oid]).stdout;
            serde_json::json!({"oid": oid, "object_type": "blob", "size_bytes": data.len(),
            "blake3": {"hash": blake3::hash(&data).to_hex().to_string(), "size_bytes": data.len()}})
        })
        .collect();
    (oids, serde_json::json!({"objects": objects}))
}

#[test]
fn proxy_blake3_warming_is_batched_persistent_and_atomic_on_invalid_response() {
    let fixture = Fixture::with_blob_count(0);
    let (oids, body) = snapshot_mapping_response(&fixture);
    let mut invalid = body.clone();
    invalid["objects"][1] = invalid["objects"][0].clone();
    let (origin, server) = digest_proxy_response(200, invalid.to_string());
    {
        let mut repo = fixture.repo(SizeFallback::Error);
        let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
        assert!(
            repo.warm_blake3_snapshot(&snapshot, &origin, "fixture_repo")
                .is_err()
        );
        for oid in &oids {
            assert!(repo.cached_blake3(oid).unwrap().is_none());
        }
    }
    server.join().unwrap();
    let mut overflow = body.clone();
    for object in overflow["objects"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .take(3)
    {
        object["size_bytes"] = serde_json::json!(i64::MAX as u64);
        object["blake3"]["size_bytes"] = serde_json::json!(i64::MAX as u64);
    }
    let (origin, server) = digest_proxy_response(200, overflow.to_string());
    {
        let mut repo = fixture.repo(SizeFallback::Error);
        let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
        let error = repo
            .warm_blake3_snapshot(&snapshot, &origin, "fixture_repo")
            .unwrap_err();
        assert!(error.to_string().contains("snapshot sizes exceed u64"));
        for oid in &oids {
            assert!(repo.cached_blake3(oid).unwrap().is_none());
        }
        assert_eq!(
            repo.cache_stats().unwrap().entries,
            0,
            "overflow must not persist sizes or hashes"
        );
    }
    server.join().unwrap();
    let (origin, server) = digest_proxy_response(200, body.to_string());
    let output = ok_cli(&[
        "warm-blake3",
        "--repo",
        fixture.clone.to_str().unwrap(),
        "--proxy-url",
        &origin,
        "--proxy-repo",
        "fixture_repo",
    ]);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["requests"], 1);
    assert_eq!(report["proxy_objects"], oids.len());
    let request = server.join().unwrap();
    assert_eq!(request["oids"].as_array().unwrap().len(), oids.len());
    assert_eq!(request["blake3"], true);
    // No listener now: a reopened cache must answer entirely offline.
    let output = ok_cli(&[
        "warm-blake3",
        "--repo",
        fixture.clone.to_str().unwrap(),
        "--proxy-url",
        &origin,
        "--proxy-repo",
        "fixture_repo",
    ]);
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["requests"], 0);
    assert_eq!(report["cache_hits"], oids.len());
    assert_eq!(fixture.missing_blobs(&oids), oids.len());
    // Existing cas-digest response tests cover type, size, hash and HTTP errors;
    // this batch case specifically verifies duplicate identity and atomicity.
}

#[test]
#[ignore = "requires Linux /dev/fuse and fusermount3"]
fn mounted_blake3_xattrs_use_cached_base_and_current_same_size_overlay_bytes() {
    let fixture = Fixture::with_blob_count(0);
    let (oids, body) = snapshot_mapping_response(&fixture);
    let (origin, server) = digest_proxy_response(200, body.to_string());
    ok_cli(&[
        "warm-blake3",
        "--repo",
        fixture.clone.to_str().unwrap(),
        "--proxy-url",
        &origin,
        "--proxy-repo",
        "fixture_repo",
    ]);
    server.join().unwrap();
    let overlay = fixture._temp.path().join("blake3-overlay");
    let writable_log = fixture._temp.path().join("digest-writable.log");
    let mounted = writable_mount_options_with_stderr(
        &fixture,
        &overlay,
        &[],
        &[
            "--blake3-proxy-url",
            &origin,
            "--blake3-proxy-repo",
            "fixture_repo",
        ],
        Stdio::from(fs::File::create(&writable_log).unwrap()),
    );
    let path = fixture.mount.join("hello.txt");
    for (name, length) in [
        ("user.gitsha1", 20),
        ("user.gitsha1.hex", 40),
        ("user.blake3", 32),
        ("user.blake3.hex", 64),
    ] {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let name = CString::new(name).unwrap();
        for _ in 0..3 {
            assert_eq!(
                unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) },
                length
            );
        }
    }
    assert_eq!(
        xattr(&path, "user.blake3"),
        blake3::hash(b"hello from git-vfs\n").as_bytes()
    );
    assert_eq!(
        fixture.missing_blobs(&oids),
        oids.len(),
        "base xattr must not hydrate"
    );
    fs::write(&path, b"HELLO from git-vfs\n").unwrap();
    for (name, length) in [
        ("user.gitsha1", 20),
        ("user.gitsha1.hex", 40),
        ("user.blake3", 32),
        ("user.blake3.hex", 64),
    ] {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let name = CString::new(name).unwrap();
        for _ in 0..3 {
            assert_eq!(
                unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) },
                length
            );
        }
    }

    assert_eq!(
        xattr(&path, "user.blake3"),
        blake3::hash(b"HELLO from git-vfs\n").as_bytes()
    );
    assert_eq!(
        xattr(&path, "user.blake3.hex"),
        blake3::hash(b"HELLO from git-vfs\n").to_hex().as_bytes()
    );
    let sha1 = xattr(&path, "user.gitsha1");
    assert_eq!(
        xattr(&path, "user.gitsha1.hex"),
        hex::encode(&sha1).as_bytes()
    );
    fs::write(fixture.mount.join("new.txt"), b"new bytes").unwrap();
    assert_eq!(
        xattr(&fixture.mount.join("new.txt"), "user.blake3"),
        blake3::hash(b"new bytes").as_bytes()
    );
    for path in [fixture.mount.clone(), fixture.mount.join(".git")] {
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let name = CString::new("user.blake3").unwrap();
        assert_eq!(
            unsafe { libc::getxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) },
            -1
        );
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ENODATA)
        );
    }
    drop(mounted);
    fn report(log: &Path, prefix: &str) -> serde_json::Value {
        let log = fs::read_to_string(log).unwrap();
        let matches: Vec<_> = log
            .lines()
            .filter_map(|line| line.strip_prefix(prefix))
            .collect();
        assert_eq!(matches.len(), 1, "{log}");
        serde_json::from_str(matches[0]).unwrap()
    }
    let work = report(&writable_log, "Overlay state: ");
    assert_eq!(work["digest_hash_operations"], 2);
    assert_eq!(work["digest_hash_bytes"], 28);
    assert_eq!(
        work["digest_cache_hits"], 3,
        "size probes must not touch the content cache"
    );
    let consumption = report(&writable_log, "BLAKE3 xattrs: ");
    assert_eq!(consumption["base_hits"], 1);
    assert_eq!(consumption["local_hashes"], 2);
    assert_eq!(consumption["local_cache_hits"], 1);
    let readonly_log = fixture._temp.path().join("digest-readonly.log");
    let readonly = writable_mount_options_with_stderr(
        &fixture,
        &overlay,
        &[],
        &[
            "--read-only",
            "--blake3-proxy-url",
            &origin,
            "--blake3-proxy-repo",
            "fixture_repo",
        ],
        Stdio::from(fs::File::create(&readonly_log).unwrap()),
    );
    for (name, length) in [
        ("user.gitsha1", 20),
        ("user.gitsha1.hex", 40),
        ("user.blake3", 32),
        ("user.blake3.hex", 64),
    ] {
        let native = CString::new(path.as_os_str().as_bytes()).unwrap();
        let name = CString::new(name).unwrap();
        assert_eq!(
            unsafe { libc::getxattr(native.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) },
            length
        );
    }
    assert_eq!(
        xattr(&path, "user.blake3"),
        blake3::hash(b"hello from git-vfs\n").as_bytes()
    );
    drop(readonly);
    let consumption = report(&readonly_log, "BLAKE3 xattrs: ");
    assert_eq!(
        consumption["base_hits"], 1,
        "readonly size probes must not count as digest consumption"
    );
    assert_eq!(consumption["local_hashes"], 0);
}
