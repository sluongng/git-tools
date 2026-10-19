//! GitHub tree metadata is trusted only after matching the local pinned tree manifest.
use std::collections::{HashMap, HashSet};
use std::io::Read;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Deserialize;

use crate::filesystem::{Kind, Snapshot};

const BODY_LIMIT: u64 = 8 * 1024 * 1024;
const TOTAL_LIMIT: u64 = 64 * 1024 * 1024;
const REQUEST_LIMIT: usize = 1024;
const ENTRY_LIMIT: usize = 500_000;

pub(crate) struct Metadata {
    pub sizes: HashMap<String, u64>,
    pub requests: usize,
    pub bytes: u64,
}

/// Only recognized public GitHub URLs can select the fixed API origin.
pub(crate) fn github_repository(remote: &[u8]) -> Option<String> {
    let remote = std::str::from_utf8(remote).ok()?;
    let remote = remote.strip_suffix('\n').unwrap_or(remote);
    if remote
        .bytes()
        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return None;
    }
    let path = if let Some(path) = remote.strip_prefix("git@github.com:") {
        path.to_owned()
    } else {
        let url = reqwest::Url::parse(remote).ok()?;
        if url.host_str()? != "github.com"
            || url.port().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.password().is_some()
            || !((url.scheme() == "https" && url.username().is_empty())
                || (url.scheme() == "ssh" && url.username() == "git"))
        {
            return None;
        }
        url.path().strip_prefix('/')?.to_owned()
    };
    let path = path.strip_suffix(".git").unwrap_or(&path);
    let (owner, repo) = path.split_once('/')?;
    if owner.is_empty()
        || repo.is_empty()
        || owner == "."
        || owner == ".."
        || repo == "."
        || repo == ".."
        || !owner
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        || !repo
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return None;
    }
    Some(format!("{owner}/{repo}"))
}

fn authorization(token: Option<&str>) -> Result<Option<HeaderValue>> {
    token
        .map(|token| {
            ensure!(
                !token.is_empty(),
                "GitHub token environment variable is empty"
            );
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| anyhow::anyhow!("GitHub token has invalid header characters"))?;
            value.set_sensitive(true);
            Ok(value)
        })
        .transpose()
}

struct Http {
    client: Client,
    repository: String,
}
trait Transport {
    fn get(&mut self, tree: &str, recursive: bool) -> Result<Vec<u8>>;
}
impl Transport for Http {
    fn get(&mut self, tree: &str, recursive: bool) -> Result<Vec<u8>> {
        let url = format!(
            "https://api.github.com/repos/{}/git/trees/{tree}",
            self.repository
        );
        let mut request = self.client.get(url);
        // GitHub treats *any* recursive query value as true: omit the key on subtree requests.
        if recursive {
            request = request.query(&[("recursive", "1")]);
        }
        let response = request
            .send()
            .map_err(|_| anyhow::anyhow!("GitHub tree HTTP request failed"))?;
        ensure!(
            response.status().is_success(),
            "GitHub tree API returned HTTP {}",
            response.status().as_u16()
        );
        ensure!(
            response.content_length().is_none_or(|n| n <= BODY_LIMIT),
            "GitHub tree response exceeds body limit"
        );
        let mut body = Vec::new();
        response
            .take(BODY_LIMIT + 1)
            .read_to_end(&mut body)
            .map_err(|_| anyhow::anyhow!("GitHub tree response read failed"))?;
        ensure!(
            body.len() as u64 <= BODY_LIMIT,
            "GitHub tree response exceeds body limit"
        );
        Ok(body)
    }
}

pub(crate) fn fetch_sizes(
    snapshot: &Snapshot,
    tree: &str,
    repository: &str,
    token_env: Option<&str>,
) -> Result<Metadata> {
    let token = token_env
        .map(|name| {
            ensure!(
                !name.is_empty() && !name.contains('='),
                "invalid GitHub token environment variable name"
            );
            std::env::var(name).map_err(|_| {
                anyhow::anyhow!("GitHub token environment variable is missing or not UTF-8")
            })
        })
        .transpose()?;
    let client = http_client(token.as_deref())?;
    load(
        snapshot,
        tree,
        &mut Http {
            client,
            repository: repository.into(),
        },
    )
}

fn http_client(token: Option<&str>) -> Result<Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "accept",
        HeaderValue::from_static("application/vnd.github+json"),
    );
    headers.insert(
        "x-github-api-version",
        HeaderValue::from_static("2026-03-10"),
    );
    if let Some(value) = authorization(token)? {
        headers.insert(AUTHORIZATION, value);
    }
    Client::builder()
        .user_agent("git-vfs/0.1")
        .default_headers(headers)
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| anyhow::anyhow!("create GitHub metadata HTTP client"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Expected {
    mode: u32,
    oid: String,
    kind: &'static str,
}
type Manifest = HashMap<Vec<u8>, Expected>;
#[derive(Deserialize)]
struct Tree {
    sha: String,
    truncated: bool,
    tree: Vec<Entry>,
}
#[derive(Deserialize)]
struct Entry {
    path: String,
    mode: String,
    #[serde(rename = "type")]
    kind: String,
    sha: String,
    size: Option<i64>,
}
fn oid(value: &str) -> Result<()> {
    ensure!(
        value.len() == 40
            && value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid GitHub SHA1 metadata"
    );
    Ok(())
}
fn manifest(snapshot: &Snapshot) -> Result<Manifest> {
    let mut expected = Manifest::new();
    ensure!(
        snapshot.nodes.len() <= ENTRY_LIMIT,
        "snapshot exceeds GitHub metadata entry limit"
    );
    for node in snapshot.nodes.iter().skip(1) {
        let kind = match node.kind {
            Kind::Directory => "tree",
            Kind::Gitlink => "commit",
            _ => "blob",
        };
        let id = node
            .oid
            .as_ref()
            .context("snapshot entry has no object ID")?
            .clone();
        oid(&id)?;
        ensure!(
            expected
                .insert(
                    git_path_bytes(&node.path)?,
                    Expected {
                        mode: node.mode,
                        oid: id,
                        kind
                    }
                )
                .is_none(),
            "duplicate snapshot entry"
        );
    }
    Ok(expected)
}
fn validate(tree: Tree, expected: &Manifest, sizes: &mut HashMap<String, u64>) -> Result<()> {
    ensure!(!tree.truncated, "GitHub subtree response is truncated");
    ensure!(
        tree.tree.len() == expected.len(),
        "GitHub tree manifest entry count mismatch"
    );
    let mut seen = HashSet::new();
    for entry in tree.tree {
        let path = entry.path.as_bytes();
        ensure!(
            !path.is_empty()
                && path
                    .split(|b| *b == b'/')
                    .all(|p| !p.is_empty() && p != b"." && p != b"..")
                && !path.contains(&0),
            "unsafe GitHub tree path"
        );
        ensure!(seen.insert(path.to_vec()), "duplicate GitHub tree path");
        let item = expected
            .get(path)
            .context("GitHub tree path does not match pinned local snapshot")?;
        oid(&entry.sha)?;
        ensure!(
            entry.mode.len() == 6 && entry.mode.bytes().all(|b| (b'0'..=b'7').contains(&b)),
            "invalid GitHub tree mode"
        );
        let mode = u32::from_str_radix(&entry.mode, 8)?;
        ensure!(
            item.mode == mode && item.oid == entry.sha && item.kind == entry.kind,
            "GitHub tree metadata does not match pinned local snapshot"
        );
        if item.kind == "blob" {
            let size = entry.size.context("GitHub blob size is missing")?;
            ensure!(size >= 0, "GitHub blob size is negative");
            if let Some(old) = sizes.insert(entry.sha, size as u64) {
                ensure!(
                    old == size as u64,
                    "conflicting GitHub sizes for the same blob"
                );
            }
        }
    }
    Ok(())
}
fn request(
    transport: &mut impl Transport,
    tree: &str,
    recursive: bool,
    metadata: &mut Metadata,
    started: Instant,
) -> Result<Tree> {
    ensure!(
        metadata.requests < REQUEST_LIMIT && started.elapsed() < Duration::from_secs(120),
        "GitHub size discovery request/time limit exceeded"
    );
    metadata.requests += 1;
    let bytes = transport.get(tree, recursive)?;
    ensure!(
        bytes.len() as u64 <= BODY_LIMIT,
        "GitHub tree response exceeds body limit"
    );
    metadata.bytes = metadata
        .bytes
        .checked_add(bytes.len() as u64)
        .context("GitHub metadata byte overflow")?;
    ensure!(
        metadata.bytes <= TOTAL_LIMIT,
        "GitHub size discovery aggregate byte limit exceeded"
    );
    let response: Tree = serde_json::from_slice(&bytes)
        .map_err(|_| anyhow::anyhow!("invalid GitHub tree JSON schema"))?;
    ensure!(
        response.tree.len() <= ENTRY_LIMIT,
        "GitHub tree exceeds entry limit"
    );
    ensure!(
        response.sha == tree,
        "GitHub response tree does not match pinned tree"
    );
    Ok(response)
}
fn load(snapshot: &Snapshot, root: &str, transport: &mut impl Transport) -> Result<Metadata> {
    oid(root)?;
    let started = Instant::now();
    let expected = manifest(snapshot)?;
    let mut metadata = Metadata {
        sizes: HashMap::new(),
        requests: 0,
        bytes: 0,
    };
    let response = request(transport, root, true, &mut metadata, started)?;
    if !response.truncated {
        validate(response, &expected, &mut metadata.sizes)?;
        return Ok(metadata);
    }
    // Discard the incomplete recursive response. Fetch only known local subtrees,
    // deduplicated by tree OID, and validate their immediate children independently.
    let mut by_path = HashMap::from([(Vec::new(), Manifest::new())]);
    for (path, item) in &expected {
        if item.kind == "tree" {
            by_path.insert(path.clone(), Manifest::new());
        }
    }
    for (path, item) in &expected {
        let (parent, name) = match path.iter().rposition(|b| *b == b'/') {
            Some(index) => (&path[..index], &path[index + 1..]),
            None => (&b""[..], path.as_slice()),
        };
        by_path
            .get_mut(parent)
            .context("local snapshot parent tree is missing")?
            .insert(name.to_vec(), item.clone());
    }
    let mut trees: HashMap<String, Manifest> = HashMap::new();
    for (path, children) in by_path {
        let id = if path.is_empty() {
            root.to_owned()
        } else {
            expected
                .get(&path)
                .context("local snapshot tree is missing")?
                .oid
                .clone()
        };
        if let Some(old) = trees.insert(id, children.clone()) {
            ensure!(old == children, "local tree OID has conflicting manifests");
        }
    }
    let mut trees: Vec<_> = trees.into_iter().collect();
    trees.sort_by(|a, b| a.0.cmp(&b.0));
    for (tree, children) in trees {
        let response = request(transport, &tree, false, &mut metadata, started)?;
        validate(response, &children, &mut metadata.sizes)?;
    }
    Ok(metadata)
}

fn git_path_bytes(path: &std::path::Path) -> Result<Vec<u8>> {
    #[cfg(unix)]
    {
        Ok(path.as_os_str().as_bytes().to_vec())
    }
    #[cfg(windows)]
    {
        Ok(path
            .to_str()
            .context("Windows requires UTF-8 Git paths")?
            .replace('\\', "/")
            .into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{GitRepo, SizeFallback, SizeSource, SizeWarmOptions};
    use serde_json::{Value, json};
    use std::process::Command;

    fn fixture() -> (tempfile::TempDir, GitRepo, Snapshot, String) {
        let dir = tempfile::tempdir().unwrap();
        let git = |args: &[&str]| {
            let output = Command::new("git")
                .arg("-C")
                .arg(dir.path())
                .args(args)
                .output()
                .unwrap();
            assert!(output.status.success(), "Git fixture command failed");
            String::from_utf8(output.stdout).unwrap().trim().to_owned()
        };
        git(&["init", "-q"]);
        git(&["config", "user.name", "Fixture"]);
        git(&["config", "user.email", "fixture@example.invalid"]);
        for name in ["one", "two"] {
            std::fs::create_dir(dir.path().join(name)).unwrap();
            std::fs::write(dir.path().join(name).join("shared"), b"same").unwrap();
        }
        std::fs::write(dir.path().join("BUILD"), b"build\n").unwrap();
        git(&["add", "."]);
        git(&["commit", "-qm", "fixture"]);
        let root = git(&["rev-parse", "HEAD^{tree}"]);
        git(&["clone", "-q", "--bare", ".", "bare.git"]);
        let repo = GitRepo::open(
            dir.path().join("bare.git"),
            "git",
            "origin",
            SizeFallback::Error,
        )
        .unwrap();
        let snapshot = Snapshot::load(&repo, "HEAD").unwrap();
        (dir, repo, snapshot, root)
    }

    struct Fake {
        responses: HashMap<(String, bool), Vec<u8>>,
        calls: Vec<(String, bool)>,
    }
    impl Transport for Fake {
        fn get(&mut self, tree: &str, recursive: bool) -> Result<Vec<u8>> {
            self.calls.push((tree.into(), recursive));
            self.responses
                .get(&(tree.into(), recursive))
                .cloned()
                .context("unexpected HTTP request")
        }
    }
    fn entry(path: &str, mode: &str, kind: &str, oid: &str, size: Option<u64>) -> Value {
        let mut value = json!({"path":path,"mode":mode,"type":kind,"sha":oid});
        if let Some(size) = size {
            value["size"] = size.into();
        }
        value
    }
    fn recursive(snapshot: &Snapshot, root: &str) -> Value {
        let entries: Vec<_> = snapshot
            .nodes
            .iter()
            .skip(1)
            .map(|node| {
                let size = match node.path.to_str().unwrap() {
                    "BUILD" => Some(6),
                    "one/shared" | "two/shared" => Some(4),
                    _ => None,
                };
                entry(
                    node.path.to_str().unwrap(),
                    &format!("{:06o}", node.mode),
                    if node.kind == Kind::Directory {
                        "tree"
                    } else {
                        "blob"
                    },
                    node.oid.as_deref().unwrap(),
                    size,
                )
            })
            .collect();
        json!({"sha":root,"truncated":false,"tree":entries})
    }
    fn fake(root: &str, response: Value) -> Fake {
        Fake {
            responses: HashMap::from([(
                (root.into(), true),
                serde_json::to_vec(&response).unwrap(),
            )]),
            calls: vec![],
        }
    }

    #[test]
    fn complete_tree_returns_unique_blob_sizes_in_one_request() {
        let (_dir, _repo, snapshot, root) = fixture();
        let mut transport = fake(&root, recursive(&snapshot, &root));
        let metadata = load(&snapshot, &root, &mut transport).unwrap();
        assert_eq!(metadata.requests, 1);
        assert_eq!(metadata.sizes.len(), 2);
        let mut sizes: Vec<_> = metadata.sizes.values().copied().collect();
        sizes.sort();
        assert_eq!(sizes, [4, 6]);
        assert!(metadata.bytes > 0);
    }

    #[test]
    fn truncated_recursive_is_discarded_and_known_subtrees_are_deduplicated() {
        let (_dir, _repo, snapshot, root) = fixture();
        let complete = recursive(&snapshot, &root);
        let mut transport = fake(
            &root,
            json!({"sha":root,"truncated":true,"tree":[{"path":"untrusted","mode":"100644","type":"blob","sha":"0000000000000000000000000000000000000000","size":99}]}),
        );
        let mut root_entries = vec![];
        let mut child = None;
        for value in complete["tree"].as_array().unwrap() {
            let path = value["path"].as_str().unwrap();
            if !path.contains('/') {
                root_entries.push(value.clone());
            }
            if path == "one" {
                child = Some(value["sha"].as_str().unwrap().to_owned());
            }
        }
        let child = child.unwrap();
        let shared = snapshot
            .nodes
            .iter()
            .find(|n| n.path.to_str() == Some("one/shared"))
            .unwrap();
        transport.responses.insert(
            (root.clone(), false),
            serde_json::to_vec(&json!({"sha":root,"truncated":false,"tree":root_entries})).unwrap(),
        );
        transport.responses.insert((child.clone(), false), serde_json::to_vec(&json!({"sha":child,"truncated":false,"tree":[entry("shared","100644","blob",shared.oid.as_deref().unwrap(),Some(4))]})).unwrap());
        let metadata = load(&snapshot, &root, &mut transport).unwrap();
        assert_eq!(metadata.requests, 3);
        assert_eq!(metadata.sizes.len(), 2);
        assert_eq!(
            transport
                .calls
                .iter()
                .filter(|(_, recursive)| !recursive)
                .count(),
            2
        );
    }

    #[test]
    fn malformed_and_inconsistent_metadata_never_returns_sizes() {
        let (_dir, _repo, snapshot, root) = fixture();
        let good = recursive(&snapshot, &root);
        let mut cases = vec![];
        let mut wrong_root = good.clone();
        wrong_root["sha"] = "0000000000000000000000000000000000000000".into();
        cases.push(("wrong root", wrong_root));
        let mut absent = good.clone();
        absent["tree"].as_array_mut().unwrap().pop();
        cases.push(("missing path", absent));
        let mut duplicate = good.clone();
        duplicate["tree"][1] = duplicate["tree"][0].clone();
        cases.push(("duplicate path", duplicate));
        let mut wrong_mode = good.clone();
        wrong_mode["tree"][0]["mode"] = "100755".into();
        cases.push(("wrong mode", wrong_mode));
        let mut wrong_oid = good.clone();
        wrong_oid["tree"][0]["sha"] = "0000000000000000000000000000000000000000".into();
        cases.push(("wrong blob", wrong_oid));
        let mut negative = good.clone();
        negative["tree"][0]["size"] = (-1).into();
        cases.push(("negative size", negative));
        let mut oversized = good.clone();
        oversized["tree"][0]["size"] = u64::MAX.into();
        cases.push(("signed overflow", oversized));
        let mut conflict = good.clone();
        for value in conflict["tree"].as_array_mut().unwrap() {
            if value["path"] == "two/shared" {
                value["size"] = 5.into();
            }
        }
        cases.push(("conflicting blob size", conflict));
        for (name, response) in cases {
            assert!(
                load(&snapshot, &root, &mut fake(&root, response)).is_err(),
                "{name}"
            );
        }
        let mut transport = fake(&root, good);
        transport
            .responses
            .insert((root.clone(), true), vec![b' '; BODY_LIMIT as usize + 1]);
        assert!(load(&snapshot, &root, &mut transport).is_err());
        let mut transport = fake(&root, json!({"sha":root,"tree":[]}));
        assert!(load(&snapshot, &root, &mut transport).is_err());
    }

    #[test]
    fn github_origin_recognition_rejects_credentials_and_other_hosts() {
        for remote in [
            "https://github.com/owner/repo.git\n",
            "git@github.com:owner/repo.git",
            "ssh://git@github.com/owner/repo.git",
        ] {
            assert_eq!(
                github_repository(remote.as_bytes()).as_deref(),
                Some("owner/repo")
            );
        }
        for remote in [
            "https://github.com.evil.test/owner/repo",
            "https://github.com@evil.test/owner/repo",
            "https://token@github.com/owner/repo",
            "https://github.com:8443/owner/repo",
            "http://github.com/owner/repo",
            "https://github.com/owner/repo?token=x",
            "git@github.com:owner/repo/extra",
            "https://github.com/owner/%72epo",
            "ssh://other@github.com/owner/repo",
            "../repo",
            "https://github.com/owner/repo\n\n",
        ] {
            assert!(github_repository(remote.as_bytes()).is_none(), "{remote}");
        }
        let value = authorization(Some("secret")).unwrap().unwrap();
        assert!(value.is_sensitive());
        assert!(authorization(Some("secret\r\nInjected: x")).is_err());
        assert!(authorization(Some("")).is_err());
    }

    #[test]
    fn http_client_returns_redirect_without_following_it() {
        use std::io::Write;
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut request = [0; 4096];
            assert!(stream.read(&mut request).unwrap() > 0);
            stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: https://invalid.example/credential-sink\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
        });
        let response = http_client(None)
            .unwrap()
            .get(format!("http://{address}/tree"))
            .send()
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        server.join().unwrap();
    }

    #[test]
    fn aggregate_and_request_budgets_stop_before_unbounded_discovery() {
        let root = "0000000000000000000000000000000000000000";
        let response = json!({"sha":root,"truncated":false,"tree":[]});
        let mut transport = fake(root, response);
        let mut metadata = Metadata {
            sizes: HashMap::new(),
            requests: REQUEST_LIMIT,
            bytes: 0,
        };
        assert!(request(&mut transport, root, false, &mut metadata, Instant::now()).is_err());
        assert!(transport.calls.is_empty());
        metadata.requests = 0;
        metadata.bytes = TOTAL_LIMIT;
        // This fake responds only to recursive requests; register the subtree response.
        let body = transport
            .responses
            .get(&(root.to_owned(), true))
            .unwrap()
            .clone();
        transport.responses.insert((root.to_owned(), false), body);
        assert!(request(&mut transport, root, false, &mut metadata, Instant::now()).is_err());
    }

    fn set_remote(dir: &std::path::Path, args: &[&str]) {
        let output = Command::new("git")
            .arg("--git-dir")
            .arg(dir.join("bare.git"))
            .arg("remote")
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success());
    }

    #[test]
    fn full_local_snapshot_without_origin_warms_offline() {
        let (dir, mut repo, snapshot, _) = fixture();
        set_remote(dir.path(), &["remove", "origin"]);
        assert!(!repo.is_github_remote().unwrap());
        let report = repo
            .warm_snapshot_sizes(&snapshot, &SizeWarmOptions::default())
            .unwrap();
        assert_eq!(report.source, "local");
        assert_eq!(report.learned_blobs, 2);
        assert_eq!(report.http_requests, 0);
        assert_eq!(repo.cache_stats().unwrap().entries, 2);
    }

    #[test]
    fn full_local_github_snapshot_does_not_require_token_or_http() {
        let (dir, mut repo, snapshot, _) = fixture();
        set_remote(
            dir.path(),
            &["set-url", "origin", "https://github.com/owner/repo.git"],
        );
        assert!(repo.is_github_remote().unwrap());
        let report = repo
            .warm_snapshot_sizes(
                &snapshot,
                &SizeWarmOptions {
                    source: SizeSource::GitHub,
                    github_token_env: Some("GIT_VFS_TEST_MISSING_TOKEN_ENV_613b8e".into()),
                },
            )
            .unwrap();
        assert_eq!(report.source, "local");
        assert_eq!(report.cached_blobs, 0);
        assert_eq!(report.learned_blobs, 2);
        assert_eq!(report.http_requests, 0);
        assert_eq!(repo.cache_stats().unwrap().entries, 2);
    }

    #[test]
    fn failed_provider_does_not_persist_staged_local_sizes_or_hydrate_missing_blob() {
        let (dir, mut repo, snapshot, _) = fixture();
        set_remote(
            dir.path(),
            &["set-url", "origin", "https://github.com/owner/repo.git"],
        );
        let blob = snapshot
            .nodes
            .iter()
            .find(|node| node.path.to_str() == Some("BUILD"))
            .unwrap()
            .oid
            .as_ref()
            .unwrap();
        let object = dir
            .path()
            .join("bare.git/objects")
            .join(&blob[..2])
            .join(&blob[2..]);
        std::fs::remove_file(&object).unwrap();
        assert!(
            repo.warm_snapshot_sizes(
                &snapshot,
                &SizeWarmOptions {
                    source: SizeSource::GitHub,
                    github_token_env: Some("GIT_VFS_TEST_MISSING_TOKEN_ENV_613b8e".into()),
                }
            )
            .is_err()
        );
        assert!(!object.exists());
        assert_eq!(repo.cache_stats().unwrap().entries, 0);
    }

    #[test]
    fn durable_cache_makes_github_warming_offline_and_ignores_unused_token_env() {
        let (dir, mut repo, snapshot, _root) = fixture();
        let report = repo
            .warm_snapshot_sizes(
                &snapshot,
                &SizeWarmOptions {
                    source: SizeSource::Git,
                    github_token_env: None,
                },
            )
            .unwrap();
        assert_eq!(report.learned_blobs, 2);
        assert_eq!(report.source, "local");
        let output = Command::new("git")
            .arg("--git-dir")
            .arg(dir.path().join("bare.git"))
            .args([
                "remote",
                "set-url",
                "origin",
                "https://github.com/owner/repo.git",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(repo.is_github_remote().unwrap());
        let report = repo
            .warm_snapshot_sizes(
                &snapshot,
                &SizeWarmOptions {
                    source: SizeSource::GitHub,
                    github_token_env: Some("GIT_VFS_TEST_MISSING_TOKEN_ENV_613b8e".into()),
                },
            )
            .unwrap();
        assert_eq!(report.source, "cache");
        assert_eq!(report.cached_blobs, 2);
        assert_eq!(report.learned_blobs, 0);
        assert_eq!(report.http_requests, 0);
    }
}
