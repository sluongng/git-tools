//! Dedicated execution-lease transport. Never a customer view or raw repository client.
use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, Url, header};
use std::time::Duration;
use tinytree_forge_core::checker::{
    self, Call, CheckerLease, Command, Key, ManifestPage, ObjectChunk, Reply,
};
use tokio::{sync::watch, time::Instant};

const FRAME_LIMIT: usize = checker::MAX_FRAME_BYTES;
const REPLY_LIMIT: usize = checker::MAX_REPLY_BYTES;

#[derive(Clone)]
pub struct CandidateClient {
    client: Client,
    endpoint: Url,
    credential_env: String,
}

// Deliberately excludes the reqwest client (which retains the bearer header).
impl std::fmt::Debug for CandidateClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CandidateClient").finish_non_exhaustive()
    }
}

pub fn validate_origin(origin: &str) -> Result<()> {
    let url = Url::parse(origin).map_err(|_| anyhow::anyhow!("invalid checker origin"))?;
    let loopback = url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    ensure!(
        url.scheme() == "https" || (url.scheme() == "http" && loopback),
        "checker requires HTTPS or explicit loopback HTTP"
    );
    ensure!(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && matches!(url.path(), "" | "/"),
        "checker requires an origin without credentials or a path"
    );
    Ok(())
}

impl CandidateClient {
    pub fn from_config(origin: &str, credential_env: &str) -> Result<Self> {
        validate_origin(origin)?;
        ensure!(
            !credential_env.is_empty()
                && credential_env.len() <= 128
                && credential_env
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_'),
            "invalid checker credential reference"
        );
        let token = std::env::var(credential_env)
            .map_err(|_| anyhow::anyhow!("checker credential unavailable"))?;
        Self::with_token(origin, credential_env, &token)
    }

    fn with_token(origin: &str, credential_env: &str, token: &str) -> Result<Self> {
        validate_origin(origin)?;
        ensure!(
            !token.is_empty() && token.len() <= 4096,
            "invalid checker credential"
        );
        let mut authorization = header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| anyhow::anyhow!("invalid checker credential"))?;
        authorization.set_sensitive(true);
        let mut headers = header::HeaderMap::new();
        headers.insert(header::AUTHORIZATION, authorization);
        let client = Client::builder()
            .default_headers(headers)
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| anyhow::anyhow!("checker HTTP initialization failed"))?;
        let endpoint = Url::parse(origin)
            .expect("validated checker origin")
            .join("/api/v1/checker/call")?;
        Ok(Self {
            client,
            endpoint,
            credential_env: credential_env.to_owned(),
        })
    }

    pub fn matches_config(&self, origin: &str, credential_env: &str) -> bool {
        validate_origin(origin).is_ok()
            && credential_env == self.credential_env
            && Url::parse(origin)
                .ok()
                .and_then(|url| url.join("/api/v1/checker/call").ok())
                .as_ref()
                == Some(&self.endpoint)
    }

    /// No implicit claim/reselection or result retry. The controller owns saved
    /// mutation identities; a failed call never changes their normalized input.
    pub async fn call(
        &self,
        call: &Call,
        cancel: &mut watch::Receiver<bool>,
        deadline: Instant,
    ) -> Result<Reply> {
        ensure!(!*cancel.borrow(), "checker cancelled");
        call.validate()?;
        let payload = serde_json::to_vec(call)?;
        ensure!(
            payload.len() <= FRAME_LIMIT,
            "checker request exceeds limit"
        );
        let deadline = deadline.min(Instant::now() + Duration::from_secs(30));
        let operation = async {
            let mut response = self
                .client
                .post(self.endpoint.clone())
                .header(header::CONTENT_TYPE, "application/json")
                .body(payload)
                .send()
                .await
                .map_err(|_| anyhow::anyhow!("checker transport unavailable"))?;
            let status = response.status();
            ensure!(
                response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|h| h.to_str().ok())
                    .is_some_and(|h| h
                        .split(';')
                        .next()
                        .is_some_and(|s| s.trim().eq_ignore_ascii_case("application/json"))),
                "invalid checker response content type"
            );
            ensure!(
                response
                    .content_length()
                    .is_none_or(|n| n <= REPLY_LIMIT as u64),
                "checker response exceeds limit"
            );
            let mut bytes = Vec::new();
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| anyhow::anyhow!("checker transport unavailable"))?
            {
                ensure!(
                    chunk.len() <= REPLY_LIMIT.saturating_sub(bytes.len()),
                    "checker response exceeds limit"
                );
                bytes.extend_from_slice(&chunk);
            }
            if !status.is_success() {
                return Err(decode_error(status.as_u16(), &bytes)?.into());
            }
            ensure!(status.as_u16() == 200, "invalid checker success status");
            let response = checker::Response::decode(&bytes, call)
                .map_err(|_| anyhow::anyhow!("invalid checker response"))?;
            Ok(response.result)
        };
        tokio::select! {
            biased;
            _ = cancelled(cancel) => bail!("checker cancelled"),
            result = tokio::time::timeout_at(deadline, operation) => result.context("checker operation deadline")?,
        }
    }

    pub async fn manifest(
        &self,
        lease: &CheckerLease,
        cursor: u64,
        cancel: &mut watch::Receiver<bool>,
        deadline: Instant,
    ) -> Result<ManifestPage> {
        let call = Call {
            schema: 1,
            account_id: lease.scope.account_id.clone(),
            request_id: format!("manifest-{}-{cursor}", lease.lease_id),
            command: Command::Manifest {
                lease: lease.reference(),
                cursor,
                limit: 128,
            },
        };
        match self.call(&call, cancel, deadline).await? {
            Reply::Manifest { page } => {
                ensure!(
                    page.lease == lease.reference()
                        && page.manifest_id == lease.manifest.id
                        && page.digest == lease.manifest.digest
                        && page.start == cursor
                        && page.entries.len() <= 128,
                    "checker manifest binding differs"
                );
                Ok(page)
            }
            _ => bail!("unexpected checker manifest response"),
        }
    }

    pub async fn object(
        &self,
        lease: &CheckerLease,
        key: &Key,
        offset: u64,
        max_bytes: u32,
        cancel: &mut watch::Receiver<bool>,
        deadline: Instant,
    ) -> Result<ObjectChunk> {
        ensure!(max_bytes <= 65536, "checker object chunk exceeds limit");
        let call = Call {
            schema: 1,
            account_id: lease.scope.account_id.clone(),
            request_id: format!(
                "object-{}",
                blake3::hash(&serde_json::to_vec(&(
                    &lease.lease_id,
                    key,
                    offset,
                    max_bytes
                ))?)
                .to_hex()
            ),
            command: Command::Object {
                lease: lease.reference(),
                key: key.clone(),
                offset,
                max_bytes,
            },
        };
        match self.call(&call, cancel, deadline).await? {
            Reply::Object { chunk } => {
                ensure!(
                    chunk.lease == lease.reference()
                        && chunk.descriptor.kind == key.kind
                        && chunk.descriptor.id == key.id
                        && chunk.offset == offset
                        && chunk.data.len() <= max_bytes as usize,
                    "checker object binding differs"
                );
                Ok(chunk)
            }
            _ => bail!("unexpected checker object response"),
        }
    }
}

async fn cancelled(cancel: &mut watch::Receiver<bool>) {
    loop {
        if *cancel.borrow() || cancel.changed().await.is_err() {
            return;
        }
    }
}

/// Stable authenticated protocol refusal, without reflecting arbitrary bodies.
#[derive(Debug)]
pub struct CheckerHttpError {
    pub status: u16,
    pub code: String,
}
impl std::fmt::Display for CheckerHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "checker refused: {} ({})", self.code, self.status)
    }
}
impl std::error::Error for CheckerHttpError {}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorReply {
    schema: u32,
    error: ErrorCode,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorCode {
    code: String,
}
fn decode_error(status: u16, bytes: &[u8]) -> Result<CheckerHttpError> {
    ensure!(bytes.len() <= 4096, "invalid checker error response");
    let reply: ErrorReply = serde_json::from_slice(bytes)
        .map_err(|_| anyhow::anyhow!("invalid checker error response"))?;
    let codes: &[&str] = match status {
        400 => &["invalid_argument", "failed_precondition", "out_of_range"],
        401 => &["unauthenticated"],
        403 => &["permission_denied"],
        404 => &["not_found"],
        409 => &["aborted", "already_exists"],
        429 => &["resource_exhausted"],
        500 => &["internal", "unknown", "data_loss"],
        501 => &["unimplemented"],
        503 => &["unavailable"],
        504 => &["deadline_exceeded"],
        _ => &[],
    };
    ensure!(
        reply.schema == 1 && codes.contains(&reply.error.code.as_str()),
        "invalid checker error response"
    );
    Ok(CheckerHttpError {
        status,
        code: reply.error.code,
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::source;
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU8, Ordering},
        },
    };
    use tinytree_forge_core::{check, checker::*};
    use tinytree_model as model;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    fn fixture() -> (
        CheckerLease,
        Vec<ManifestEntry>,
        BTreeMap<(u8, String), (Descriptor, Vec<u8>)>,
        Vec<u8>,
    ) {
        let file = vec![42; 832134];
        let file_descriptor = Descriptor::from_native(&model::describe_file(&file));
        let empty = model::empty_tree();
        let link = model::encode(
            &model::Object::Symlink("source.txt".into()),
            model::Limits::default(),
        )
        .unwrap();
        let tree = model::encode(
            &model::Object::Tree(model::Tree {
                entries: vec![
                    model::TreeEntry {
                        name: "empty".into(),
                        value: model::TreeValue::Tree(model::TreeId(empty.descriptor.key.id.0)),
                    },
                    model::TreeEntry {
                        name: "link".into(),
                        value: model::TreeValue::Symlink {
                            id: model::SymlinkId(link.descriptor.key.id.0),
                            target_bytes: 10,
                        },
                    },
                    model::TreeEntry {
                        name: "source.txt".into(),
                        value: model::TreeValue::File {
                            id: model::FileId(file_descriptor.native().unwrap().key.id.0),
                            size_bytes: file.len() as u64,
                            executable: true,
                            copy_id: None,
                        },
                    },
                ],
            }),
            model::Limits::default(),
        )
        .unwrap();
        let mut commit = model::synthetic_root();
        commit.parents = vec![model::CommitId::ROOT];
        commit.root_tree.terms = vec![model::TreeId(tree.descriptor.key.id.0)];
        commit.change_id = model::ChangeId([7; 16]);
        let commit =
            model::encode(&model::Object::Commit(commit), model::Limits::default()).unwrap();
        let tree_descriptor = Descriptor::from_native(&tree.descriptor);
        let head = Descriptor::from_native(&commit.descriptor).id;
        let entries = vec![
            ManifestEntry {
                path: "".into(),
                descriptor: tree_descriptor.clone(),
                value: EntryValue::Directory {},
            },
            ManifestEntry {
                path: "empty".into(),
                descriptor: Descriptor::from_native(&empty.descriptor),
                value: EntryValue::Directory {},
            },
            ManifestEntry {
                path: "link".into(),
                descriptor: Descriptor::from_native(&link.descriptor),
                value: EntryValue::Symlink { target_bytes: 10 },
            },
            ManifestEntry {
                path: "source.txt".into(),
                descriptor: file_descriptor.clone(),
                value: EntryValue::File {
                    executable: true,
                    copy_id: None,
                },
            },
        ];
        let limits = SnapshotLimits::default();
        let mut hash = ManifestHasher::new(&head, &tree_descriptor.id).unwrap();
        for entry in &entries {
            hash.push(entry).unwrap();
        }
        let totals = hash.finish(&limits).unwrap();
        let lease = CheckerLease {
            schema: 1,
            lease_id: "11".repeat(32),
            generation: 0,
            request: check::Request {
                schema: 1,
                job_id: "22".repeat(32),
                attempt_id: "33".repeat(32),
                repository_id: "forge-example".into(),
                candidate_commit: head,
                root_tree: tree_descriptor.id,
                profile_name: "checks".into(),
                profile_version: "44".repeat(32),
            },
            scope: CandidateScope {
                account_id: "example".into(),
                view_version: "55".repeat(32),
                policy_epoch: 1,
                change_id: "66".repeat(16),
                patchset_id: "77".repeat(32),
                candidate_id: "22".repeat(32),
                target_head: "88".repeat(32),
                target_generation: 2,
                publication_generation: 3,
            },
            manifest: ManifestBinding {
                id: "99".repeat(32),
                digest: totals.digest,
                entries: totals.entries,
                file_bytes: totals.file_bytes,
                path_bytes: totals.path_bytes,
                metadata_bytes: totals.metadata_bytes,
            },
            expires_at: 4_000_000_000,
            limits,
        };
        let mut objects = BTreeMap::new();
        for object in [empty, link, tree, commit] {
            let descriptor = Descriptor::from_native(&object.descriptor);
            objects.insert(
                (descriptor.kind, descriptor.id.clone()),
                (descriptor, object.bytes),
            );
        }
        objects.insert(
            (1, file_descriptor.id.clone()),
            (file_descriptor, file.clone()),
        );
        (lease, entries, objects, file)
    }

    struct Peer {
        task: tokio::task::JoinHandle<()>,
    }
    impl Drop for Peer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[tokio::test]
    async fn leased_http_snapshot_verifies_native_tree_streams_files_and_refuses_corruption_and_rebinding()
     {
        use std::os::unix::fs::PermissionsExt;
        let (lease, entries, objects, file) = fixture();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let mode = Arc::new(AtomicU8::new(0));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let server_mode = mode.clone();
        let server_calls = calls.clone();
        let server_lease = lease.clone();
        let peer = Peer {
            task: tokio::spawn(async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut bytes = Vec::new();
                    let end = loop {
                        let mut block = [0; 1024];
                        let n = socket.read(&mut block).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&block[..n]);
                        if let Some(end) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
                            break end + 4;
                        }
                        assert!(bytes.len() <= 65536);
                    };
                    let headers = String::from_utf8_lossy(&bytes[..end]);
                    assert!(
                        headers
                            .to_ascii_lowercase()
                            .contains("authorization: bearer test-only-token")
                    );
                    let size: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length: ")
                                .map(str::to_owned)
                        })
                        .unwrap()
                        .trim()
                        .parse()
                        .unwrap();
                    while bytes.len() < end + size {
                        let mut block = [0; 1024];
                        let n = socket.read(&mut block).await.unwrap();
                        assert!(n > 0);
                        bytes.extend_from_slice(&block[..n]);
                    }
                    let call = Call::decode(&bytes[end..end + size]).unwrap();
                    server_calls.lock().unwrap().push(call.clone());
                    let result = match &call.command {
                        Command::Manifest { lease, cursor, .. } => Reply::Manifest {
                            page: ManifestPage {
                                lease: lease.clone(),
                                manifest_id: server_lease.manifest.id.clone(),
                                digest: server_lease.manifest.digest.clone(),
                                start: *cursor,
                                entries: entries.clone(),
                                next: None,
                            },
                        },
                        Command::Object {
                            lease,
                            key,
                            offset,
                            max_bytes,
                        } => {
                            let (descriptor, bytes) =
                                objects.get(&(key.kind, key.id.clone())).unwrap();
                            let end = (*offset as usize + *max_bytes as usize).min(bytes.len());
                            let mut data = bytes[*offset as usize..end].to_vec();
                            if server_mode.load(Ordering::SeqCst) == 1
                                && key.kind == 1
                                && !data.is_empty()
                            {
                                data[0] ^= 1;
                            }
                            let mut selected = lease.clone();
                            if server_mode.load(Ordering::SeqCst) == 2 {
                                selected.request.attempt_id = "aa".repeat(32);
                            }
                            Reply::Object {
                                chunk: ObjectChunk {
                                    lease: selected,
                                    descriptor: descriptor.clone(),
                                    offset: *offset,
                                    data,
                                    eof: end == bytes.len(),
                                },
                            }
                        }
                        _ => panic!("unexpected candidate operation"),
                    };
                    let response = serde_json::to_vec(&Response {
                        schema: 1,
                        account_id: call.account_id,
                        request_id: call.request_id,
                        result,
                    })
                    .unwrap();
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        response.len()
                    );
                    socket.write_all(headers.as_bytes()).await.unwrap();
                    socket.write_all(&response).await.unwrap();
                }
            }),
        };
        let client =
            CandidateClient::with_token(&origin, "CHECK_TOKEN", "test-only-token").unwrap();
        assert!(client.matches_config(&origin, "CHECK_TOKEN"));
        assert!(!client.matches_config(&origin, "OTHER_TOKEN"));
        let temporary = tempfile::tempdir().unwrap();
        let private = private_fs::PrivateDir::create(temporary.path().join("private")).unwrap();
        let (_sender, mut cancel) = watch::channel(false);
        let snapshot = source::materialize(
            &client,
            &lease,
            &private.path().join("source"),
            &private.path().join("snapshot.json"),
            &SnapshotLimits::default(),
            &mut cancel,
            Instant::now() + Duration::from_secs(30),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read(snapshot.root.join("source.txt")).unwrap(),
            file
        );
        assert_eq!(
            std::fs::metadata(snapshot.root.join("source.txt"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        assert_eq!(
            std::fs::read_link(snapshot.root.join("link")).unwrap(),
            std::path::Path::new("source.txt")
        );
        assert!(snapshot.root.join("empty").is_dir());
        let saved: SnapshotManifest =
            serde_json::from_slice(&std::fs::read(&snapshot.manifest_path).unwrap()).unwrap();
        assert_eq!(saved.immutable_binding().unwrap(), snapshot.binding);
        assert!(
            calls
                .lock()
                .unwrap()
                .iter()
                .filter(
                    |call| matches!(call.command, Command::Object { ref key, .. } if key.kind == 1)
                )
                .count()
                > 1
        );
        mode.store(1, Ordering::SeqCst);
        assert!(
            source::materialize(
                &client,
                &lease,
                &private.path().join("corrupt"),
                &private.path().join("corrupt.json"),
                &SnapshotLimits::default(),
                &mut cancel,
                Instant::now() + Duration::from_secs(30)
            )
            .await
            .is_err()
        );
        assert!(!private.path().join("corrupt.json").exists());
        mode.store(2, Ordering::SeqCst);
        assert!(
            source::materialize(
                &client,
                &lease,
                &private.path().join("rebound"),
                &private.path().join("rebound.json"),
                &SnapshotLimits::default(),
                &mut cancel,
                Instant::now() + Duration::from_secs(30)
            )
            .await
            .is_err()
        );
        assert!(!private.path().join("rebound.json").exists());
        drop(peer);
    }
}
