use git_proxy::{Config, ObjectResponse};
use serde_json::json;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{io::AsyncWriteExt, process::Command};
fn git() -> PathBuf {
    std::env::var_os("GIT_PROXY_TEST_GIT")
        .map(PathBuf::from)
        .unwrap_or_else(|| "git".into())
}
async fn run(args: &[&str]) -> String {
    let output = Command::new(git())
        .args(args)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .output()
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{:?}: {}",
        args,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
async fn upstream(root: &Path) -> (PathBuf, String, String) {
    let path = root.join("upstream");
    run(&["init", "-b", "main", path.to_str().unwrap()]).await;
    run(&["-C", path.to_str().unwrap(), "config", "user.name", "Test"]).await;
    run(&[
        "-C",
        path.to_str().unwrap(),
        "config",
        "user.email",
        "test@example.invalid",
    ])
    .await;
    std::fs::write(path.join("hello"), b"hello world\n").unwrap();
    run(&["-C", path.to_str().unwrap(), "add", "."]).await;
    run(&["-C", path.to_str().unwrap(), "commit", "-m", "initial"]).await;
    let blob = run(&["-C", path.to_str().unwrap(), "rev-parse", "HEAD:hello"]).await;
    let head = run(&["-C", path.to_str().unwrap(), "rev-parse", "HEAD"]).await;
    (path, blob, head)
}
struct Server {
    shutdown: tokio::sync::oneshot::Sender<()>,
    thread: std::thread::JoinHandle<()>,
}
impl Server {
    async fn stop(self) {
        let _ = self.shutdown.send(());
        // Axum can signal graceful completion just before a connection task
        // drops its router. Joining the runtime thread is a real destruction
        // barrier before a persistence test reopens the cache, like process exit.
        tokio::task::spawn_blocking(move || self.thread.join().unwrap())
            .await
            .unwrap();
    }
}
async fn serve(config: Config) -> (String, Server) {
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let (ready, started) = tokio::sync::oneshot::channel();
    let thread = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let app = git_proxy::router(config).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            ready
                .send(format!("http://{}", listener.local_addr().unwrap()))
                .unwrap();
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = receiver.await;
                })
                .await
                .unwrap();
        });
    });
    let url = started.await.unwrap();
    (url, Server { shutdown, thread })
}
fn config(root: &Path, upstream: &Path, ttl: u64) -> Config {
    Config {
        cache_dir: root.join("cache"),
        git: git(),
        repositories: vec![("test".into(), upstream.to_str().unwrap().into())],
        refresh_ttl: Duration::from_secs(ttl),
        enable_blake3: true,
        pack_cache: git_proxy::PackCacheConfig::default(),
        cas_sync: None,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn smart_http_partial_clone_object_info_and_offline_cache() {
    let temp = tempfile::tempdir().unwrap();
    let (upstream, blob, head) = upstream(temp.path()).await;
    let cfg = config(temp.path(), &upstream, 3600);
    let (url, task) = serve(cfg.clone()).await;
    // Simultaneous initial advertisements must share one atomic mirror initialization.
    let remote = format!("{url}/git/test.git");
    let args = ["ls-remote", &remote, "HEAD"];
    let (a, b) = tokio::join!(run(&args), run(&args));
    assert!(a.starts_with(&head));
    assert_eq!(a, b);
    assert!(
        git_proxy::router(cfg).is_err(),
        "a second process owner must be rejected"
    );
    let clone = temp.path().join("clone");
    run(&[
        "-c",
        "protocol.version=2",
        "clone",
        "--filter=blob:none",
        "--depth=1",
        "--no-checkout",
        &remote,
        clone.to_str().unwrap(),
    ])
    .await;
    let local_count = run(&[
        "-C",
        clone.to_str().unwrap(),
        "rev-list",
        "--objects",
        "--all",
        "--missing=print",
    ])
    .await;
    assert!(
        local_count.lines().any(|line| line == format!("?{blob}")),
        "blob must remain absent: {local_count}"
    );
    // Exercise the same native Git remote-object-info command used by git-vfs.
    let mut child = Command::new(git())
        .args([
            "-C",
            clone.to_str().unwrap(),
            "-c",
            "protocol.version=2",
            "cat-file",
            "--batch-command=%(objectname) %(objectsize)",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(format!("remote-object-info origin {blob}\n").as_bytes())
        .await
        .unwrap();
    let output = child.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!("{blob} 12")
    );
    // Git's curl transport gzips requests larger than 1 KiB. A large native
    // object-info batch exercises Content-Encoding forwarding/decompression.
    let mut child = Command::new(git())
        .args([
            "-C",
            clone.to_str().unwrap(),
            "-c",
            "protocol.version=2",
            "cat-file",
            "--batch-command=%(objectname) %(objectsize)",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            format!(
                "remote-object-info origin {}\n",
                vec![blob.clone(); 128].join(" ")
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    let output = child.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "large native request: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().lines().count(),
        128
    );
    // A pinned object introduced after mirror creation is fetched on want,
    // without updating cached refs inside the freshness TTL.
    std::fs::write(upstream.join("later"), b"later content\n").unwrap();
    run(&["-C", upstream.to_str().unwrap(), "add", "later"]).await;
    run(&["-C", upstream.to_str().unwrap(), "commit", "-m", "later"]).await;
    let pinned = run(&["-C", upstream.to_str().unwrap(), "rev-parse", "HEAD"]).await;
    run(&["-C", clone.to_str().unwrap(), "fetch", "origin", &pinned]).await;
    assert!(
        run(&["ls-remote", &remote, "refs/heads/main"])
            .await
            .starts_with(&head)
    );
    std::fs::rename(&upstream, temp.path().join("offline")).unwrap();
    assert!(
        run(&["ls-remote", &remote, "HEAD"])
            .await
            .starts_with(&head)
    );
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{url}/api/v1/repos/test/objects"))
        .json(&json!({"oids":[blob], "blake3":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    task.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_blake3_persistence_validation_and_ref_refresh() {
    let temp = tempfile::tempdir().unwrap();
    let (upstream, blob, head) = upstream(temp.path()).await;
    // Replacements must never change content-addressed metadata or poison the
    // globally shared Git-SHA1 to BLAKE3 map under the original object's ID.
    let replacement_file = temp.path().join("replacement");
    std::fs::write(&replacement_file, b"different replacement content\n").unwrap();
    let replacement = run(&[
        "-C",
        upstream.to_str().unwrap(),
        "hash-object",
        "-w",
        replacement_file.to_str().unwrap(),
    ])
    .await;
    run(&[
        "-C",
        upstream.to_str().unwrap(),
        "replace",
        &blob,
        &replacement,
    ])
    .await;
    let cfg = config(temp.path(), &upstream, 0);
    let (url, task) = serve(cfg.clone()).await;
    let client = reqwest::Client::new();
    let endpoint = format!("{url}/api/v1/repos/test/objects");
    let response = client
        .post(&endpoint)
        .json(&json!({"oids":[blob, head, blob.to_uppercase()], "blake3":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let response: ObjectResponse = response.json().await.unwrap();
    assert_eq!(response.objects.len(), 2);
    assert_eq!(response.objects[0].size_bytes, 12);
    assert_eq!(response.objects[0].object_type, "blob");
    assert_eq!(
        response.objects[0].blake3.as_ref().unwrap().hash,
        blake3::hash(b"hello world\n").to_hex().to_string()
    );
    assert!(response.objects[1].blake3.is_none());
    for body in [
        json!({"oids":["--help"]}),
        json!({"oids":["a".repeat(39)]}),
        json!({"oids":[], "unknown":true}),
    ] {
        assert_eq!(
            client
                .post(&endpoint)
                .json(&body)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
    }
    assert_eq!(
        client
            .post(&endpoint)
            .json(&json!({"oids":["0".repeat(40)]}))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client
            .post(format!("{url}/git/test.git/git-receive-pack"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client
            .get(format!("{url}/git/test.git/config"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(
        client
            .get(format!("{url}/api/v1/repos/other/objects"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    std::fs::write(upstream.join("hello"), b"updated\n").unwrap();
    run(&["-C", upstream.to_str().unwrap(), "commit", "-am", "updated"]).await;
    run(&[
        "-C",
        upstream.to_str().unwrap(),
        "update-ref",
        "refs/changes/01",
        "HEAD",
    ])
    .await;
    run(&["-C", upstream.to_str().unwrap(), "branch", "next"]).await;
    run(&[
        "-C",
        upstream.to_str().unwrap(),
        "symbolic-ref",
        "HEAD",
        "refs/heads/next",
    ])
    .await;
    let updated = run(&["-C", upstream.to_str().unwrap(), "rev-parse", "HEAD"]).await;
    assert!(
        run(&[
            "ls-remote",
            &format!("{url}/git/test.git"),
            "refs/heads/main"
        ])
        .await
        .starts_with(&updated)
    );
    assert!(
        run(&[
            "ls-remote",
            &format!("{url}/git/test.git"),
            "refs/changes/01"
        ])
        .await
        .starts_with(&updated)
    );
    assert!(
        run(&[
            "ls-remote",
            "--symref",
            &format!("{url}/git/test.git"),
            "HEAD"
        ])
        .await
        .starts_with("ref: refs/heads/next")
    );
    run(&[
        "-C",
        upstream.to_str().unwrap(),
        "update-ref",
        "-d",
        "refs/changes/01",
    ])
    .await;
    assert!(
        run(&[
            "ls-remote",
            &format!("{url}/git/test.git"),
            "refs/changes/01"
        ])
        .await
        .is_empty()
    );
    drop(client);
    task.stop().await;
    // Force cache DB to reopen; no persisted SHA1/BLAKE3 mapping is lost.
    let (url, task) = serve(cfg).await;
    let client = reqwest::Client::new();
    let response: ObjectResponse = client
        .post(format!("{url}/api/v1/repos/test/objects"))
        .json(&json!({"oids":[blob], "blake3":true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        response.objects[0].blake3.as_ref().unwrap().hash,
        blake3::hash(b"hello world\n").to_hex().to_string()
    );
    task.stop().await;
}
#[test]
fn rejects_alias_path_traversal() {
    let temp = tempfile::tempdir().unwrap();
    let mut cfg = config(temp.path(), temp.path(), 1);
    cfg.repositories[0].0 = "../outside".into();
    assert!(git_proxy::router(cfg).is_err());
}

async fn cache_stats(client: &reqwest::Client, url: &str) -> serde_json::Value {
    client
        .get(format!("{url}/api/v1/cache/stats"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}
fn fetch_body(oid: &str, extra: &[&str]) -> Vec<u8> {
    let mut body = Vec::new();
    let mut packet = |s: &str| {
        body.extend_from_slice(format!("{:04x}{s}", s.len() + 4).as_bytes());
    };
    packet("command=fetch\n");
    packet("object-format=sha1\n");
    body.extend_from_slice(b"0001");
    for line in ["thin-pack\n", "ofs-delta\n", "no-progress\n"]
        .into_iter()
        .chain(extra.iter().copied())
    {
        body.extend_from_slice(format!("{:04x}{line}", line.len() + 4).as_bytes());
    }
    for line in [format!("want {oid}\n"), "done\n".to_owned()] {
        body.extend_from_slice(format!("{:04x}{line}", line.len() + 4).as_bytes());
    }
    body.extend_from_slice(b"0000");
    body
}
async fn fetch_response(client: &reqwest::Client, url: &str, body: Vec<u8>) -> (String, Vec<u8>) {
    let response = client
        .post(format!("{url}/git/test.git/git-upload-pack"))
        .header("Git-Protocol", "version=2")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let disposition = response.headers()["x-git-proxy-pack-cache"]
        .to_str()
        .unwrap()
        .to_owned();
    (disposition, response.bytes().await.unwrap().to_vec())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_cache_reuses_native_clones_and_distinguishes_fetch_inputs() {
    let temp = tempfile::tempdir().unwrap();
    let (upstream, _, head) = upstream(temp.path()).await;
    let (url, task) = serve(config(temp.path(), &upstream, 3600)).await;
    let client = reqwest::Client::new();
    let remote = format!("{url}/git/test.git");
    for name in ["first", "second"] {
        let destination = temp.path().join(name);
        run(&[
            "-c",
            "protocol.version=2",
            "clone",
            "--filter=blob:none",
            "--depth=1",
            "--no-checkout",
            &remote,
            destination.to_str().unwrap(),
        ])
        .await;
        assert_eq!(
            run(&["-C", destination.to_str().unwrap(), "rev-parse", "HEAD"]).await,
            head
        );
    }
    let stats = cache_stats(&client, &url).await;
    assert_eq!(stats["fetch_generations"], 1, "{stats}");
    assert_eq!(stats["pack_cache"]["misses"], 1, "{stats}");
    assert_eq!(stats["pack_cache"]["hits"], 1, "{stats}");
    // Distinct depth/filter inputs must not reuse the preceding blobless response.
    let destination = temp.path().join("unfiltered");
    run(&[
        "-c",
        "protocol.version=2",
        "clone",
        "--depth=1",
        "--no-checkout",
        &remote,
        destination.to_str().unwrap(),
    ])
    .await;
    assert_eq!(cache_stats(&client, &url).await["fetch_generations"], 2);
    let plain = fetch_body(&head, &["deepen 1\n", "filter blob:none\n"]);
    let (miss, first) = fetch_response(&client, &url, plain.clone()).await;
    assert_eq!(miss, "MISS");
    assert!(!first.is_empty());
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gzip, &plain).unwrap();
    let compressed = gzip.finish().unwrap();
    let response = client
        .post(format!("{url}/git/test.git/git-upload-pack"))
        .header("Git-Protocol", "version=2")
        .header("Content-Type", "application/x-git-upload-pack-request")
        .header("Content-Encoding", "gzip")
        .body(compressed)
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["x-git-proxy-pack-cache"], "HIT");
    assert_eq!(response.bytes().await.unwrap().as_ref(), first);
    let metrics = client
        .get(format!("{url}/metrics"))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("git_proxy_pack_cache_hits 2\n"));
    task.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generated_responses_survive_restart_but_changed_tags_get_new_identity() {
    let temp = tempfile::tempdir().unwrap();
    let (upstream, _, head) = upstream(temp.path()).await;
    let cfg = config(temp.path(), &upstream, 0);
    let client = reqwest::Client::new();
    let body = fetch_body(&head, &["include-tag\n"]);
    let (url, task) = serve(cfg.clone()).await;
    run(&["ls-remote", &format!("{url}/git/test.git"), "HEAD"]).await;
    let (disposition, original) = fetch_response(&client, &url, body.clone()).await;
    assert_eq!(disposition, "MISS");
    task.stop().await;
    let (url, task) = serve(cfg.clone()).await;
    // A restarted process starts epoch at zero, but the actual ref view is stable.
    assert_eq!(
        fetch_response(&client, &url, body.clone()).await,
        ("HIT".to_owned(), original.clone())
    );
    assert_eq!(cache_stats(&client, &url).await["fetch_generations"], 0);
    task.stop().await;
    run(&[
        "-C",
        upstream.to_str().unwrap(),
        "tag",
        "-a",
        "restart-tag",
        "-m",
        "annotated after restart",
    ])
    .await;
    let (url, task) = serve(cfg).await;
    run(&["ls-remote", &format!("{url}/git/test.git"), "HEAD"]).await;
    let (disposition, changed) = fetch_response(&client, &url, body.clone()).await;
    assert_eq!(disposition, "MISS");
    assert_ne!(
        changed, original,
        "include-tag must include the new annotated tag"
    );
    assert_eq!(fetch_response(&client, &url, body).await.0, "HIT");
    task.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_ref_inventory_is_cacheable_and_survives_restart() {
    let temp = tempfile::tempdir().unwrap();
    let (upstream, _, head) = upstream(temp.path()).await;
    let mut command = Command::new(git());
    command
        .args(["-C", upstream.to_str().unwrap(), "update-ref", "--stdin"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = command.spawn().unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut updates = String::new();
    for index in 0..20_000 {
        updates.push_str(&format!(
            "create refs/heads/cache-fixture/{index:05}-{} {head}\n",
            "x".repeat(50)
        ));
    }
    input.write_all(updates.as_bytes()).await.unwrap();
    drop(input);
    let output = child.wait_with_output().await.unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    run(&[
        "-C",
        upstream.to_str().unwrap(),
        "pack-refs",
        "--all",
        "--prune",
    ])
    .await;
    let inventory = run(&[
        "-C",
        upstream.to_str().unwrap(),
        "for-each-ref",
        "--sort=refname",
        "--format=%(refname)%00%(objectname)%00%(symref)",
    ])
    .await;
    assert!(
        inventory.len() > 2 * 1024 * 1024,
        "fixture must exceed both the old 1 MiB capture bound and 2 MiB"
    );
    let cfg = config(temp.path(), &upstream, 3600);
    let client = reqwest::Client::new();
    let body = fetch_body(&head, &["include-tag\n"]);
    let (url, task) = serve(cfg.clone()).await;
    let original = fetch_response(&client, &url, body.clone()).await;
    assert_eq!(original.0, "MISS");
    assert_eq!(fetch_response(&client, &url, body.clone()).await.0, "HIT");
    assert_eq!(
        cache_stats(&client, &url).await["pack_identity_failures"],
        0
    );
    task.stop().await;
    let (url, task) = serve(cfg).await;
    assert_eq!(
        fetch_response(&client, &url, body).await,
        ("HIT".to_owned(), original.1)
    );
    let stats = cache_stats(&client, &url).await;
    assert_eq!(stats["fetch_generations"], 0);
    assert_eq!(stats["pack_identity_failures"], 0);
    task.stop().await;
}

fn gated_git(root: &Path) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    fn quote(s: &str) -> String {
        format!("'{}'", s.replace('\'', "'\\''"))
    }
    let path = root.join("git-wrapper");
    let script = format!(
        r#"#!/bin/sh
case " $* " in
  *" ls-remote "*)
    if [ -f {fail} ]; then exit 1; fi ;;
  *" fetch "*)
    if [ -f {block_refresh} ]; then
      touch {refresh_started}
      while [ ! -f {release_refresh} ]; do sleep 0.01; done
    fi ;;
  *" http-backend "*)
    if [ "$REQUEST_METHOD" = POST ] && [ -f {block} ]; then
      request=$(mktemp {request}) || exit 1
      cat > "$request"
      case "$(cat "$request")" in
        *command=fetch*)
          touch {started}
          while [ ! -f {release} ]; do sleep 0.01; done ;;
      esac
      {git} "$@" < "$request"
      result=$?
      rm -f "$request"
      exit "$result"
    fi ;;
esac
exec {git} "$@"
"#,
        fail = quote(root.join("fail-head").to_str().unwrap()),
        block_refresh = quote(root.join("block-refresh").to_str().unwrap()),
        refresh_started = quote(root.join("refresh-started").to_str().unwrap()),
        release_refresh = quote(root.join("release-refresh").to_str().unwrap()),
        block = quote(root.join("block-fetch").to_str().unwrap()),
        request = quote(root.join("request.XXXXXX").to_str().unwrap()),
        started = quote(root.join("fetch-started").to_str().unwrap()),
        release = quote(root.join("release-fetch").to_str().unwrap()),
        git = quote(git().to_str().unwrap())
    );
    std::fs::write(&path, script).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
    path
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_waits_for_generated_response_ref_view_to_finish() {
    let temp = tempfile::tempdir().unwrap();
    let (upstream, _, head) = upstream(temp.path()).await;
    let mut cfg = config(temp.path(), &upstream, 0);
    cfg.git = gated_git(temp.path());
    let (url, task) = serve(cfg).await;
    let client = reqwest::Client::new();
    run(&["ls-remote", &format!("{url}/git/test.git"), "HEAD"]).await;
    run(&[
        "-C",
        upstream.to_str().unwrap(),
        "tag",
        "-a",
        "concurrent-tag",
        "-m",
        "new upstream tag",
    ])
    .await;
    std::fs::write(temp.path().join("block-fetch"), "").unwrap();
    let body = fetch_body(&head, &["include-tag\n"]);
    let first = {
        let c = client.clone();
        let u = url.clone();
        let b = body.clone();
        tokio::spawn(async move { fetch_response(&c, &u, b).await })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !temp.path().join("fetch-started").exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let advertisement = {
        let c = client.clone();
        let u = url.clone();
        tokio::spawn(async move {
            c.get(format!(
                "{u}/git/test.git/info/refs?service=git-upload-pack"
            ))
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap()
        })
    };
    // The native fetch body is paused before reading refs. The refresh may start
    // concurrently, but its writer must not alter the producer's pinned view.
    std::fs::write(temp.path().join("release-fetch"), "").unwrap();
    let old = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), advertisement)
        .await
        .unwrap()
        .unwrap();
    let new = fetch_response(&client, &url, body).await;
    assert_eq!((old.0.as_str(), new.0.as_str()), ("MISS", "MISS"));
    assert_ne!(old.1, new.1, "the new view must include the annotated tag");
    task.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_fetches_share_generation_and_oversize_followers_bypass_cleanly() {
    for (limit, expected_status, expected_generations, expected_entries) in
        [(1024 * 1024, "COALESCED", 1, 1), (16, "BYPASS", 2, 0)]
    {
        let temp = tempfile::tempdir().unwrap();
        let (upstream, _, head) = upstream(temp.path()).await;
        let mut cfg = config(temp.path(), &upstream, 3600);
        cfg.git = gated_git(temp.path());
        cfg.pack_cache.max_entry_bytes = limit;
        let (url, task) = serve(cfg).await;
        let client = reqwest::Client::new();
        run(&["ls-remote", &format!("{url}/git/test.git"), "HEAD"]).await;
        std::fs::write(temp.path().join("block-fetch"), "").unwrap();
        let body = fetch_body(&head, &["deepen 1\n"]);
        let first = {
            let c = client.clone();
            let u = url.clone();
            let b = body.clone();
            tokio::spawn(async move { fetch_response(&c, &u, b).await })
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while !temp.path().join("fetch-started").exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        let second = {
            let c = client.clone();
            let u = url.clone();
            tokio::spawn(async move { fetch_response(&c, &u, body).await })
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            while cache_stats(&client, &url).await["pack_cache"]["coalesced"]
                .as_u64()
                .unwrap()
                == 0
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        std::fs::write(temp.path().join("release-fetch"), "").unwrap();
        let (first, second) = (first.await.unwrap(), second.await.unwrap());
        assert_eq!(first.0, "MISS");
        assert_eq!(first.1, second.1);
        let stats = cache_stats(&client, &url).await;
        assert_eq!(second.0, expected_status);
        assert_eq!(stats["fetch_generations"], expected_generations);
        assert_eq!(stats["pack_cache"]["entries"], expected_entries);
        task.stop().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetched_ref_changes_invalidate_cache_even_when_head_refresh_fails() {
    let temp = tempfile::tempdir().unwrap();
    let (upstream, _, head) = upstream(temp.path()).await;
    let mut cfg = config(temp.path(), &upstream, 0);
    cfg.git = gated_git(temp.path());
    let (url, task) = serve(cfg).await;
    let client = reqwest::Client::new();
    run(&["ls-remote", &format!("{url}/git/test.git"), "HEAD"]).await;
    let body = fetch_body(&head, &["include-tag\n"]);
    assert_eq!(fetch_response(&client, &url, body.clone()).await.0, "MISS");
    assert_eq!(fetch_response(&client, &url, body.clone()).await.0, "HIT");
    run(&[
        "-C",
        upstream.to_str().unwrap(),
        "tag",
        "-a",
        "cache-tag",
        "-m",
        "new annotated tag",
    ])
    .await;
    std::fs::write(temp.path().join("fail-head"), "").unwrap();
    let failed = client
        .get(format!(
            "{url}/git/test.git/info/refs?service=git-upload-pack"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(failed.status(), 502);
    assert_eq!(fetch_response(&client, &url, body).await.0, "MISS");
    assert_eq!(cache_stats(&client, &url).await["fetch_generations"], 2);
    task.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn cas_credentials_do_not_reach_git_subprocesses() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let (upstream, blob, _) = upstream(root.path()).await;
    let wrapper = root.path().join("credential-checking-git");
    let observed = root.path().join("git-without-cas-credential");
    let native_git = git().to_string_lossy().replace('\'', "'\"'\"'");
    std::fs::write(
        &wrapper,
        format!(
            r#"#!/bin/sh
if [ "${{GIT_TOOLS_FIXTURE_CAS_CREDENTIAL+x}}" = x ]; then exit 42; fi
printf 'clean\n' >> '{}'
exec '{}' "$@"
"#,
            observed.display(),
            native_git
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    // Only the spawned server receives this synthetic credential; the test
    // process/global environment and concurrent tests remain untouched.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let process = std::process::Command::new(env!("CARGO_BIN_EXE_git-proxy"))
        .args([
            "--repo",
            &format!("fixture={}", upstream.display()),
            "--cache-dir",
        ])
        .arg(root.path().join("cas-cache"))
        .arg("--git")
        .arg(&wrapper)
        .args([
            "--listen",
            &address.to_string(),
            "--enable-blake3",
            "--cas-endpoint",
            "http://127.0.0.1:1",
            "--cas-header-env",
            "x-buildbuddy-api-key=GIT_TOOLS_FIXTURE_CAS_CREDENTIAL",
        ])
        .env(
            "GIT_TOOLS_FIXTURE_CAS_CREDENTIAL",
            "synthetic-cas-only-secret",
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    struct Owned(std::process::Child);
    impl Drop for Owned {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let _owned = Owned(process);
    let origin = format!("http://{address}");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if reqwest::get(format!("{origin}/api/v1/cache/stats"))
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("{origin}/api/v1/repos/fixture/objects"))
        .json(&serde_json::json!({"oids":[blob],"blake3":true}))
        .send()
        .await
        .unwrap();
    assert!(
        response.status().is_success(),
        "Git must work without inheriting the CAS credential"
    );
    assert!(!std::fs::read_to_string(observed).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_preparation_coalesces_and_keeps_cached_metadata_observable_during_refresh() {
    let temp = tempfile::tempdir().unwrap();
    let (upstream, blob, _) = upstream(temp.path()).await;
    let mut cfg = config(temp.path(), &upstream, 0);
    cfg.git = gated_git(temp.path());
    let (url, server) = serve(cfg.clone()).await;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let status_url = format!("{url}/api/v1/repos/test/status");
    let prepare_url = format!("{url}/api/v1/repos/test/prepare");
    let initial: serde_json::Value = client
        .get(&status_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(initial["ready"], false);
    assert!(initial["ref_age_seconds"].is_null());
    assert_eq!(client.get(&prepare_url).send().await.unwrap().status(), 405);
    assert_eq!(
        client.post(&prepare_url).send().await.unwrap().status(),
        200
    );
    std::fs::write(temp.path().join("block-refresh"), "").unwrap();
    let first = {
        let client = client.clone();
        let url = prepare_url.clone();
        tokio::spawn(async move { client.post(url).send().await.unwrap() })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !temp.path().join("refresh-started").exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let second = {
        let client = client.clone();
        let url = prepare_url.clone();
        tokio::spawn(async move { client.post(url).send().await.unwrap() })
    };
    let during: serde_json::Value = client
        .get(&status_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(during["ready"], true);
    assert_eq!(during["preparation_in_progress"], true);
    assert_eq!(during["completed_attempts"], 1);
    // A warm SHA1 lookup must not wait for the mutex held by network refresh.
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        client
            .post(format!("{url}/api/v1/repos/test/objects"))
            .json(&json!({"oids":[blob]}))
            .send(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.json::<ObjectResponse>().await.unwrap().objects[0].size_bytes,
        12
    );
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    // Observe the second request actually waiting, not a timing guess about
    // when its HTTP task might reach the held operation.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status: serde_json::Value = client
                .get(&status_url)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if status["preparation_waiters"] == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    std::fs::write(temp.path().join("release-refresh"), "").unwrap();
    assert_eq!(first.await.unwrap().status(), 200);
    assert_eq!(second.await.unwrap().status(), 200);
    let complete: serde_json::Value = client
        .get(&status_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(complete["completed_attempts"], 2);
    assert_eq!(complete["last_outcome"], "success");
    std::fs::write(temp.path().join("fail-head"), "").unwrap();
    std::fs::remove_file(temp.path().join("release-refresh")).unwrap();
    std::fs::remove_file(temp.path().join("refresh-started")).unwrap();
    let failed_first = {
        let client = client.clone();
        let url = prepare_url.clone();
        tokio::spawn(async move { client.post(url).send().await.unwrap() })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !temp.path().join("refresh-started").exists() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    let failed_second = {
        let client = client.clone();
        let url = prepare_url.clone();
        tokio::spawn(async move { client.post(url).send().await.unwrap() })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status: serde_json::Value = client
                .get(&status_url)
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if status["preparation_waiters"] == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    std::fs::write(temp.path().join("release-refresh"), "").unwrap();
    assert_eq!(failed_first.await.unwrap().status(), 502);
    assert_eq!(failed_second.await.unwrap().status(), 502);
    let failed: serde_json::Value = client
        .get(&status_url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(failed["ready"], true);
    assert_eq!(failed["failed_attempts"], 1);
    assert_eq!(failed["last_outcome"], "failed");
    assert!(
        !failed.to_string().contains(upstream.to_str().unwrap()),
        "status must not reveal origin credentials or stderr"
    );
    server.stop().await;
    let (url, server) = serve(cfg).await;
    let restarted: serde_json::Value = client
        .get(format!("{url}/api/v1/repos/test/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        restarted["ready"], false,
        "restart must validate the persisted origin binding"
    );
    // A metadata-only validation must not mark persisted refs fresh. The next
    // advertisement/preparation still contacts origin and observes its error.
    assert_eq!(
        client
            .post(format!("{url}/api/v1/repos/test/objects"))
            .json(&json!({"oids":[blob]}))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let validated: serde_json::Value = client
        .get(format!("{url}/api/v1/repos/test/status"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(validated["ready"], true);
    assert!(validated["ref_age_seconds"].is_null());
    assert_eq!(
        client
            .post(format!("{url}/api/v1/repos/test/prepare"))
            .send()
            .await
            .unwrap()
            .status(),
        502
    );
    server.stop().await;
}
