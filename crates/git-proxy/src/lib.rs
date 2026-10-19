//! A single-owner, configured-origin, read-only smart HTTP Git cache.
pub mod cas_client;
mod cas_sync;
mod storage_accounting;
pub use cas_sync::{CasShutdown, CasSyncConfig};
use git_pack_cache as pack_cache;
pub use pack_cache::PackCacheConfig;

use anyhow::{Context, Result, ensure};
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use fjall::Readable;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{Mutex, OwnedRwLockReadGuard, OwnedSemaphorePermit, RwLock, Semaphore},
};

const MAX_BODY: usize = 2 * 1024 * 1024;
const MAX_OBJECTS: usize = 16_384;
const GIT_TIMEOUT: Duration = Duration::from_secs(600);

#[derive(Clone, Debug)]
pub struct Config {
    pub cache_dir: PathBuf,
    pub git: PathBuf,
    pub repositories: Vec<(String, String)>,
    pub refresh_ttl: Duration,
    pub enable_blake3: bool,
    pub pack_cache: PackCacheConfig,
    pub cas_sync: Option<CasSyncConfig>,
}
struct Repository {
    upstream: String,
    path: PathBuf,
    refreshed: Mutex<Option<Instant>>,
    epoch: AtomicU64,
    refs_gate: Arc<RwLock<()>>,
    pack_identity: Mutex<Option<(u64, String)>>,
}
// A native fetch can update refs before a later error or cancellation. Invalidate
// cached responses on every exit path, not only after all refresh steps succeed.
struct RefMutation<'a>(&'a AtomicU64);
impl Drop for RefMutation<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Release);
    }
}
struct DigestCache {
    keyspace: fjall::SingleWriterTxKeyspace,
    db: fjall::SingleWriterTxDatabase,
}
struct App {
    config: Config,
    repositories: HashMap<String, Repository>,
    digests: std::sync::Mutex<DigestCache>,
    slots: Arc<Semaphore>,
    packs: Arc<pack_cache::PackCache>,
    storage_accounting: storage_accounting::StorageAccounting,
    http_backend_calls: AtomicU64,
    fetch_generations: AtomicU64,
    _lock: Arc<File>,
    cas_sync: Option<Arc<cas_sync::CasSync>>,
}

pub fn router(config: Config) -> Result<Router> {
    ensure!(
        config.cas_sync.is_none(),
        "CAS sync requires router_with_cas_shutdown and its service-owner handle"
    );
    Ok(router_with_cas_shutdown(config)?.0)
}

/// Join optional CAS work after stopping new HTTP traffic.
pub fn router_with_cas_shutdown(mut config: Config) -> Result<(Router, Option<CasShutdown>)> {
    std::fs::create_dir_all(&config.cache_dir)?;
    config.cache_dir = std::fs::canonicalize(&config.cache_dir)?;
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(config.cache_dir.join("owner.lock"))?;
    fs2::FileExt::try_lock_exclusive(&lock).context("cache directory already has an owner")?;
    let lock = Arc::new(lock);
    let mut repositories = HashMap::new();
    for (alias, upstream) in &config.repositories {
        ensure!(valid_alias(alias), "invalid repository alias {alias:?}");
        ensure!(
            !upstream.is_empty() && !upstream.starts_with('-'),
            "invalid upstream"
        );
        ensure!(
            !repositories.contains_key(alias),
            "duplicate repository alias {alias}"
        );
        repositories.insert(
            alias.clone(),
            Repository {
                upstream: upstream.clone(),
                path: config.cache_dir.join(format!("{alias}.git")),
                refreshed: Mutex::new(None),
                epoch: AtomicU64::new(0),
                refs_gate: Arc::new(RwLock::new(())),
                pack_identity: Mutex::new(None),
            },
        );
    }
    ensure!(!repositories.is_empty(), "at least one --repo is required");
    let db =
        fjall::SingleWriterTxDatabase::builder(config.cache_dir.join("digests.fjall")).open()?;
    let keyspace = db.keyspace("gitsha1_blake3", fjall::KeyspaceCreateOptions::default)?;
    let packs = pack_cache::PackCache::new(
        config.cache_dir.clone(),
        config.pack_cache.clone(),
        lock.clone(),
    )?;
    ensure!(
        config.cas_sync.is_none() || config.enable_blake3,
        "CAS sync requires BLAKE3 mapping support"
    );
    let sync = config
        .cas_sync
        .clone()
        .map(cas_sync::CasSync::new)
        .transpose()?;
    let cas_sync = sync.as_ref().map(|(sync, _, _)| sync.clone());
    let storage_accounting = storage_accounting::StorageAccounting::new(
        &config.cache_dir,
        repositories.values().map(|repo| repo.path.clone()),
    );
    let app = Arc::new(App {
        config,
        repositories,
        digests: std::sync::Mutex::new(DigestCache { keyspace, db }),
        slots: Arc::new(Semaphore::new(16)),
        packs,
        storage_accounting,
        http_backend_calls: AtomicU64::new(0),
        fetch_generations: AtomicU64::new(0),
        _lock: lock,
        cas_sync,
    });
    let shutdown =
        sync.map(|(sync, jobs, cancelled)| sync.start(Arc::downgrade(&app), jobs, cancelled));
    Ok((Router::new().fallback(handle).with_state(app), shutdown))
}
fn valid_alias(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}
fn valid_oid(s: &str) -> bool {
    s.len() == 40 && s.bytes().all(|b| b.is_ascii_hexdigit())
}
fn git(app: &App, path: Option<&Path>) -> Command {
    let mut command = Command::new(&app.config.git);
    if let Some(cas) = &app.config.cas_sync {
        for (_, variable) in &cas.header_env {
            command.env_remove(variable);
        }
    }
    command.process_group(0);
    command.arg("--no-replace-objects");
    command
        .kill_on_drop(true)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_GLOBAL", "/dev/null");
    // Do not inherit server CGI identity, protocol overrides, or client credentials.
    for key in [
        "GIT_COMMON_DIR",
        "GIT_SHALLOW_FILE",
        "GIT_NAMESPACE",
        "GIT_PREFIX",
        "GIT_EXEC_PATH",
        "GIT_CONFIG",
        "GIT_CONFIG_SYSTEM",
        "GIT_REPLACE_REF_BASE",
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_PARAMETERS",
        "GIT_PROTOCOL",
        "REMOTE_USER",
        "HTTP_AUTHORIZATION",
        "HTTP_CONTENT_ENCODING",
        "CONTENT_ENCODING",
    ] {
        command.env_remove(key);
    }
    if let Some(path) = path {
        command.arg("--git-dir").arg(path);
    }
    command
}
struct ProcessGroup(u32);
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        unsafe {
            libc::kill(-(self.0 as i32), libc::SIGKILL);
        }
    }
}
fn group(child: &Child) -> ProcessGroup {
    ProcessGroup(child.id().expect("newly spawned child has PID"))
}
async fn checked(mut command: Command) -> Result<Vec<u8>> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn()?;
    let _group = group(&child);
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let errors = tokio::spawn(async move {
        let mut kept = Vec::new();
        let mut buffer = [0; 8192];
        while let Ok(n) = stderr.read(&mut buffer).await {
            if n == 0 {
                break;
            }
            let keep = n.min(4096usize.saturating_sub(kept.len()));
            kept.extend_from_slice(&buffer[..keep]);
        }
        kept
    });
    let (output, status) = tokio::time::timeout(GIT_TIMEOUT, async {
        let mut output = Vec::new();
        (&mut stdout)
            .take(1024 * 1024 + 1)
            .read_to_end(&mut output)
            .await?;
        ensure!(
            output.len() <= 1024 * 1024,
            "Git command output exceeded limit"
        );
        Ok::<_, anyhow::Error>((output, child.wait().await?))
    })
    .await
    .context("Git timed out")??;
    let stderr = errors.await?;
    ensure!(
        status.success(),
        "Git command failed: {}",
        String::from_utf8_lossy(&stderr)
    );
    Ok(output)
}

async fn ensure_repo(app: &App, repo: &Repository, refresh: bool) -> Result<()> {
    {
        let refreshed = repo.refreshed.lock().await;
        if refreshed.is_some_and(|t| !refresh || t.elapsed() < app.config.refresh_ttl) {
            return Ok(());
        }
    }
    // Always acquire refs_gate before refreshed. A cache producer may hold a
    // read lease while fetching missing objects under refreshed; holding that
    // mutex while waiting for a write lease would deadlock those two requests.
    let _refs_write = repo.refs_gate.write().await;
    let mut refreshed = repo.refreshed.lock().await;
    if refreshed.is_some_and(|t| !refresh || t.elapsed() < app.config.refresh_ttl) {
        return Ok(());
    }
    if !repo.path.exists() {
        let temp = app.config.cache_dir.join(format!(
            ".clone-{}-{}",
            std::process::id(),
            repo.path.file_name().unwrap().to_string_lossy()
        ));
        if temp.exists() {
            std::fs::remove_dir_all(&temp)?;
        }
        let mut command = git(app, None);
        command
            .args(["clone", "--mirror", "--"])
            .arg(&repo.upstream)
            .arg(&temp);
        if let Err(error) = checked(command).await {
            let _ = std::fs::remove_dir_all(&temp);
            return Err(error);
        }
        let format = checked({
            let mut c = git(app, Some(&temp));
            c.args(["rev-parse", "--show-object-format"]);
            c
        })
        .await?;
        ensure!(format == b"sha1\n", "only SHA1 repositories are supported");
        // Pin cache identity: changing alias URL must never silently serve the old repository.
        checked({
            let mut c = git(app, Some(&temp));
            c.args(["config", "gitproxy.upstream"]).arg(&repo.upstream);
            c
        })
        .await?;
        for (key, value) in [
            ("uploadpack.allowFilter", "true"),
            ("uploadpack.allowAnySHA1InWant", "true"),
            ("transfer.advertiseObjectInfo", "true"),
            ("http.receivepack", "false"),
        ] {
            checked({
                let mut c = git(app, Some(&temp));
                c.args(["config", key, value]);
                c
            })
            .await?;
        }
        std::fs::rename(&temp, &repo.path)?;
        repo.epoch.fetch_add(1, Ordering::Release);
        *refreshed = Some(Instant::now());
    }
    let upstream = checked({
        let mut c = git(app, Some(&repo.path));
        c.args(["config", "--get", "gitproxy.upstream"]);
        c
    })
    .await?;
    ensure!(
        upstream.strip_suffix(b"\n").unwrap_or(&upstream) == repo.upstream.as_bytes(),
        "cached repository belongs to another upstream"
    );
    if refresh && refreshed.is_none_or(|t| t.elapsed() >= app.config.refresh_ttl) {
        let mutation = RefMutation(&repo.epoch);
        let fetched = checked({
            let mut c = git(app, Some(&repo.path));
            c.args([
                "fetch",
                "--atomic",
                "--prune",
                "--no-tags",
                "--no-write-fetch-head",
                "--recurse-submodules=no",
                "--refmap=",
            ])
            .arg(&repo.upstream)
            .args(["+refs/*:refs/*"]);
            c
        })
        .await;
        drop(mutation);
        fetched?;
        let remote_head = checked({
            let mut c = git(app, None);
            c.args(["ls-remote", "--symref"])
                .arg(&repo.upstream)
                .arg("HEAD");
            c
        })
        .await?;
        if let Some(target) = std::str::from_utf8(&remote_head)?.lines().find_map(|line| {
            line.strip_prefix("ref: ")
                .and_then(|s| s.strip_suffix("\tHEAD"))
        }) {
            ensure!(
                target.starts_with("refs/"),
                "invalid upstream symbolic HEAD"
            );
            checked({
                let mut c = git(app, Some(&repo.path));
                c.args(["check-ref-format", target]);
                c
            })
            .await?;
            checked({
                let mut c = git(app, Some(&repo.path));
                c.args(["symbolic-ref", "HEAD", target]);
                c
            })
            .await?;
        }
        *refreshed = Some(Instant::now());
    }
    Ok(())
}
// Called under refs_gate's read lease. The process-local epoch only invalidates
// this memo; the persisted key is a hash of the actual ref/config/Git view.
async fn pack_identity(app: &App, repo: &Repository, alias: &str) -> Result<String> {
    let epoch = repo.epoch.load(Ordering::Acquire);
    let mut memo = repo.pack_identity.lock().await;
    if let Some((generation, identity)) = &*memo
        && *generation == epoch
    {
        return Ok(identity.clone());
    }
    let mut hash = blake3::Hasher::new();
    for value in [
        b"git-proxy/ref-view/v2".as_slice(),
        alias.as_bytes(),
        repo.upstream.as_bytes(),
        app.config.git.as_os_str().as_encoded_bytes(),
    ] {
        hash.update(&(value.len() as u64).to_le_bytes());
        hash.update(value);
    }
    for args in [
        vec![
            "for-each-ref",
            "--sort=refname",
            "--format=%(refname)%00%(objectname)%00%(symref)",
        ],
        vec!["rev-parse", "--symbolic-full-name", "HEAD"],
        vec!["rev-parse", "--verify", "HEAD"],
        vec!["config", "--local", "--includes", "--null", "--list"],
        vec!["version"],
    ] {
        let value = checked({
            let mut c = git(app, Some(&repo.path));
            c.args(args);
            c
        })
        .await?;
        hash.update(&(value.len() as u64).to_le_bytes());
        hash.update(&value);
    }
    let identity = hash.finalize().to_hex().to_string();
    *memo = Some((epoch, identity.clone()));
    Ok(identity)
}
#[derive(Debug)]
struct HttpError(StatusCode, String);
impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}
fn internal(error: anyhow::Error) -> HttpError {
    eprintln!("git-proxy: {error:#}");
    HttpError(
        StatusCode::BAD_GATEWAY,
        "upstream or Git operation failed\n".into(),
    )
}
async fn read_body(request: Request) -> Result<Bytes, HttpError> {
    tokio::time::timeout(
        Duration::from_secs(30),
        to_bytes(request.into_body(), MAX_BODY),
    )
    .await
    .map_err(|_| HttpError(StatusCode::REQUEST_TIMEOUT, "request body timeout\n".into()))?
    .map_err(|_| HttpError(StatusCode::PAYLOAD_TOO_LARGE, "request too large\n".into()))
}
async fn handle(State(app): State<Arc<App>>, request: Request) -> Result<Response, HttpError> {
    if request.uri().path() == "/api/v1/storage" {
        if request.method() != "GET" {
            return Err(HttpError(
                StatusCode::METHOD_NOT_ALLOWED,
                "GET required\n".into(),
            ));
        }
        let snapshot = app
            .storage_accounting
            .snapshot()
            .await
            .map_err(|message| HttpError(StatusCode::SERVICE_UNAVAILABLE, message.into()))?;
        return Ok(axum::Json(&*snapshot).into_response());
    }
    if matches!(request.uri().path(), "/api/v1/cache/stats" | "/metrics") {
        if request.method() != "GET" {
            return Err(HttpError(
                StatusCode::METHOD_NOT_ALLOWED,
                "GET required\n".into(),
            ));
        }
        let stats = serde_json::json!({
            "pack_cache": app.packs.snapshot(),
            "http_backend_calls": app.http_backend_calls.load(Ordering::Relaxed),
            "fetch_generations": app.fetch_generations.load(Ordering::Relaxed),
            "cas_sync": app.cas_sync.as_ref().map(|sync| sync.snapshot()),
        });
        if request.uri().path() == "/metrics" {
            let mut text = String::new();
            for (name, value) in stats["pack_cache"].as_object().unwrap() {
                text.push_str(&format!("git_proxy_pack_cache_{name} {value}\n"));
            }
            text.push_str(&format!(
                "git_proxy_http_backend_calls {}\ngit_proxy_fetch_generations {}\n",
                stats["http_backend_calls"], stats["fetch_generations"]
            ));
            return Ok(
                ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], text).into_response(),
            );
        }
        return Ok(axum::Json(stats).into_response());
    }
    let permit = app.slots.clone().try_acquire_owned().map_err(|_| {
        HttpError(
            StatusCode::SERVICE_UNAVAILABLE,
            "request concurrency limit reached\n".into(),
        )
    })?;
    let path = request.uri().path().to_owned();
    if let Some(alias) = path
        .strip_prefix("/api/v1/repos/")
        .and_then(|s| s.strip_suffix("/cas-sync"))
    {
        if request.method() != "POST" {
            return Err(HttpError(
                StatusCode::METHOD_NOT_ALLOWED,
                "POST required\n".into(),
            ));
        }
        if !app.repositories.contains_key(alias) {
            return Err(HttpError(
                StatusCode::NOT_FOUND,
                "unknown repository\n".into(),
            ));
        }
        let sync = app.cas_sync.as_ref().ok_or_else(|| {
            HttpError(
                StatusCode::BAD_REQUEST,
                "CAS sync is not configured\n".into(),
            )
        })?;
        let query: SyncRequest = serde_json::from_slice(&read_body(request).await?)
            .map_err(|_| HttpError(StatusCode::BAD_REQUEST, "invalid JSON\n".into()))?;
        if query.oids.len() > MAX_OBJECTS || query.oids.iter().any(|oid| !valid_oid(oid)) {
            return Err(HttpError(
                StatusCode::BAD_REQUEST,
                "expected at most 16384 full SHA1 blob IDs\n".into(),
            ));
        }
        let accepted = sync.enqueue(alias, query.oids, query.force).map_err(|_| {
            HttpError(
                StatusCode::SERVICE_UNAVAILABLE,
                "CAS queue full, shutting down, or force overlaps pending work\n".into(),
            )
        })?;
        return Ok((StatusCode::ACCEPTED, axum::Json(accepted)).into_response());
    }
    if let Some(alias) = path
        .strip_prefix("/api/v1/repos/")
        .and_then(|s| s.strip_suffix("/objects"))
    {
        let repo = app
            .repositories
            .get(alias)
            .ok_or_else(|| HttpError(StatusCode::NOT_FOUND, "unknown repository\n".into()))?;
        if request.method() != "POST" {
            return Err(HttpError(
                StatusCode::METHOD_NOT_ALLOWED,
                "POST required\n".into(),
            ));
        }
        let body = read_body(request).await?;
        let query: ObjectRequest = serde_json::from_slice(&body)
            .map_err(|_| HttpError(StatusCode::BAD_REQUEST, "invalid JSON\n".into()))?;
        if query.oids.len() > MAX_OBJECTS || query.oids.iter().any(|s| !valid_oid(s)) {
            return Err(HttpError(
                StatusCode::BAD_REQUEST,
                "expected at most 16384 full SHA1 object IDs\n".into(),
            ));
        }
        if query.blake3 && !app.config.enable_blake3 {
            return Err(HttpError(
                StatusCode::BAD_REQUEST,
                "BLAKE3 is not enabled\n".into(),
            ));
        }
        ensure_repo(&app, repo, false).await.map_err(internal)?;
        let enqueue = query.blake3;
        let objects = metadata(&app, repo, query).await?;
        if enqueue && let Some(sync) = &app.cas_sync {
            // Publication is optional and never delays/fails metadata RPCs.
            let _ = sync.enqueue(
                alias,
                objects
                    .iter()
                    .filter(|obj| obj.object_type == "blob")
                    .map(|obj| obj.oid.clone())
                    .collect(),
                false,
            );
        }
        drop(permit);
        return Ok(axum::Json(ObjectResponse { objects }).into_response());
    }
    let Some((alias, endpoint)) = path
        .strip_prefix("/git/")
        .and_then(|s| s.split_once(".git/"))
    else {
        return Err(HttpError(StatusCode::NOT_FOUND, "not found\n".into()));
    };
    let repo = app
        .repositories
        .get(alias)
        .ok_or_else(|| HttpError(StatusCode::NOT_FOUND, "unknown repository\n".into()))?;
    let advertisement = endpoint == "info/refs"
        && request.method() == "GET"
        && request.uri().query() == Some("service=git-upload-pack");
    let upload = endpoint == "git-upload-pack"
        && request.method() == "POST"
        && request.uri().query().is_none();
    if !advertisement && !upload {
        return Err(HttpError(
            StatusCode::NOT_FOUND,
            "only upload-pack smart HTTP is supported\n".into(),
        ));
    }
    let protocol = request
        .headers()
        .get("git-protocol")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    if protocol.len() > 1024 {
        return Err(HttpError(
            StatusCode::BAD_REQUEST,
            "invalid protocol header\n".into(),
        ));
    }
    let encoding = request
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("identity")
        .to_owned();
    if !["identity", "gzip"].contains(&encoding.as_str()) {
        return Err(HttpError(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported content encoding\n".into(),
        ));
    }
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_owned();
    if upload && content_type != "application/x-git-upload-pack-request" {
        return Err(HttpError(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "invalid Git content type\n".into(),
        ));
    }
    let body = read_body(request).await?;
    let body = if encoding == "gzip" {
        let mut decoded = Vec::new();
        let decoder = flate2::read::GzDecoder::new(body.as_ref());
        std::io::Read::read_to_end(
            &mut std::io::Read::take(decoder, (MAX_BODY + 1) as u64),
            &mut decoded,
        )
        .map_err(|_| HttpError(StatusCode::BAD_REQUEST, "invalid gzip request\n".into()))?;
        if decoded.len() > MAX_BODY {
            return Err(HttpError(
                StatusCode::PAYLOAD_TOO_LARGE,
                "decoded request too large\n".into(),
            ));
        }
        Bytes::from(decoded)
    } else {
        body
    };
    ensure_repo(&app, repo, advertisement)
        .await
        .map_err(internal)?;
    let refs_lease = repo.refs_gate.clone().read_owned().await;
    // Unknown/oversized ref views bypass caching, not native Git service.
    let identity = if upload {
        pack_identity(&app, repo, alias).await.ok()
    } else {
        None
    };
    let key = identity.and_then(|identity| pack_cache::cache_key(&protocol, &body, &identity, 0));
    if let Some(key) = key {
        let lookup = app.packs.lookup(&key).await;
        let producer = match lookup {
            pack_cache::Lookup::Hit(entry) => {
                return Ok(hold_permit(entry.response("HIT"), permit));
            }
            pack_cache::Lookup::Wait(flight) => {
                if let Some(entry) = flight.wait().await {
                    return Ok(hold_permit(entry.response("COALESCED"), permit));
                }
                None
            }
            pack_cache::Lookup::Produce(producer) => Some(producer),
            pack_cache::Lookup::Bypass => None,
        };
        fetch_missing_wants(&app, repo, &body)
            .await
            .map_err(internal)?;
        app.fetch_generations.fetch_add(1, Ordering::Relaxed);
        if let Some(producer) = producer {
            let response = git_http(
                app.clone(),
                alias,
                endpoint,
                advertisement,
                protocol,
                content_type,
                body,
                None,
                true,
            )
            .await?;
            if response.status() != StatusCode::OK
                || response
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .is_none_or(|v| v != "application/x-git-upload-pack-result")
            {
                drop(producer);
                return Ok(hold_permit(response, permit));
            }
            return Ok(producer.stream_with_deadline(
                hold_refs(response, refs_lease),
                permit,
                tokio::time::Instant::now() + GIT_TIMEOUT,
            ));
        }
        let mut response = git_http(
            app.clone(),
            alias,
            endpoint,
            advertisement,
            protocol,
            content_type,
            body,
            Some(permit),
            false,
        )
        .await?;
        response
            .headers_mut()
            .insert("x-git-proxy-pack-cache", "BYPASS".parse().unwrap());
        return Ok(response);
    }
    if upload {
        fetch_missing_wants(&app, repo, &body)
            .await
            .map_err(internal)?;
    }
    let mut response = git_http(
        app.clone(),
        alias,
        endpoint,
        advertisement,
        protocol,
        content_type,
        body,
        Some(permit),
        false,
    )
    .await?;
    response
        .headers_mut()
        .insert("x-git-proxy-pack-cache", "BYPASS".parse().unwrap());
    Ok(response)
}
fn hold_refs(response: Response, lease: OwnedRwLockReadGuard<()>) -> Response {
    let (parts, body) = response.into_parts();
    let stream = async_stream::try_stream! {
        let _lease = lease;
        let mut data = Box::pin(body.into_data_stream());
        while let Some(chunk) = std::future::poll_fn(|cx| futures_core::Stream::poll_next(data.as_mut(), cx)).await {
            yield chunk?;
        }
    };
    Response::from_parts(
        parts,
        Body::from_stream(Box::pin(stream)
            as std::pin::Pin<
                Box<dyn futures_core::Stream<Item = Result<Bytes, axum::Error>> + Send>,
            >),
    )
}
fn hold_permit(response: Response, permit: OwnedSemaphorePermit) -> Response {
    let (parts, body) = response.into_parts();
    let stream = async_stream::try_stream! {
        let _permit = permit;
        let mut data = Box::pin(body.into_data_stream());
        while let Some(chunk) = std::future::poll_fn(|cx| futures_core::Stream::poll_next(data.as_mut(), cx)).await {
            yield chunk?;
        }
    };
    Response::from_parts(
        parts,
        Body::from_stream(Box::pin(stream)
            as std::pin::Pin<
                Box<dyn futures_core::Stream<Item = Result<Bytes, axum::Error>> + Send>,
            >),
    )
}
#[allow(clippy::too_many_arguments)]
async fn git_http(
    app: Arc<App>,
    alias: &str,
    endpoint: &str,
    advertisement: bool,
    protocol: String,
    content_type: String,
    body: Bytes,
    permit: Option<OwnedSemaphorePermit>,
    validate_fetch: bool,
) -> Result<Response, HttpError> {
    app.http_backend_calls.fetch_add(1, Ordering::Relaxed);
    let mut command = git(&app, None);
    command
        .arg("http-backend")
        .env("GIT_PROJECT_ROOT", &app.config.cache_dir)
        .env("GIT_HTTP_EXPORT_ALL", "1")
        .env("PATH_INFO", format!("/{alias}.git/{endpoint}"))
        .env("REQUEST_METHOD", if advertisement { "GET" } else { "POST" })
        .env(
            "QUERY_STRING",
            if advertisement {
                "service=git-upload-pack"
            } else {
                ""
            },
        )
        .env("CONTENT_TYPE", content_type)
        .env("CONTENT_LENGTH", body.len().to_string())
        .env("GIT_PROTOCOL", protocol)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().map_err(|e| internal(e.into()))?;
    let group_guard = group(&child);
    let mut stdin = child.stdin.take().unwrap();
    tokio::spawn(async move {
        let _ = stdin.write_all(&body).await;
    });
    let mut stderr = child.stderr.take().unwrap();
    tokio::spawn(async move {
        let mut buf = [0; 8192];
        while let Ok(n) = stderr.read(&mut buf).await {
            if n == 0 {
                break;
            }
        }
    });
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut headers = Vec::new();
    let mut header_bytes = 0;
    loop {
        let mut line = Vec::new();
        // Bounded read, including a malicious or broken Git binary's unterminated line.
        let n = tokio::time::timeout(
            Duration::from_secs(30),
            (&mut stdout).take(8193).read_until(b'\n', &mut line),
        )
        .await
        .map_err(|_| internal(anyhow::anyhow!("CGI header timeout")))?
        .map_err(|e| internal(e.into()))?;
        header_bytes += n;
        if n == 0 || header_bytes > 32768 || line.len() > 8192 {
            return Err(internal(anyhow::anyhow!("invalid CGI headers")));
        }
        if line == b"\r\n" || line == b"\n" {
            break;
        }
        let line = String::from_utf8(line).map_err(|e| internal(e.into()))?;
        let (key, value) = line
            .trim_end()
            .split_once(':')
            .ok_or_else(|| internal(anyhow::anyhow!("invalid CGI header")))?;
        headers.push((key.to_owned(), value.trim().to_owned()));
    }
    let mut response = Response::builder();
    let mut response_status = StatusCode::OK;
    for (key, value) in headers {
        if key.eq_ignore_ascii_case("status") {
            let status = value
                .split_whitespace()
                .next()
                .unwrap_or("500")
                .parse::<u16>()
                .unwrap_or(500);
            response_status = StatusCode::from_u16(status).map_err(|e| internal(e.into()))?;
            response = response.status(response_status);
        } else {
            response = response.header(key, value);
        }
    }
    let validate_fetch = validate_fetch
        && response_status == StatusCode::OK
        && response
            .headers_ref()
            .and_then(|h| h.get(header::CONTENT_TYPE))
            .is_some_and(|v| v == "application/x-git-upload-pack-result");
    let stream: std::pin::Pin<Box<dyn futures_core::Stream<Item = std::io::Result<Bytes>> + Send>> =
        Box::pin(async_stream::try_stream! {
            let _permit = permit;
            let _group_guard = group_guard;
            let deadline = tokio::time::Instant::now() + GIT_TIMEOUT;
            let mut chunk = vec![0u8; 64 * 1024];
            let mut validator = validate_fetch.then(pack_cache::FetchResponseValidator::new);
            loop {
                let n = tokio::time::timeout_at(deadline, stdout.read(&mut chunk)).await.map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Git response timed out"))??;
                if n == 0 { break; }
                if let Some(v) = &mut validator { v.feed(&chunk[..n]).map_err(std::io::Error::other)?; }
                yield Bytes::copy_from_slice(&chunk[..n]);
            }
            let status = tokio::time::timeout_at(deadline, child.wait()).await.map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Git exit timed out"))??;
            if !status.success() { Err(std::io::Error::other("Git HTTP backend failed"))?; }
            if let Some(v) = validator { v.finish().map_err(std::io::Error::other)?; }
        });
    response
        .body(Body::from_stream(stream))
        .map_err(|e| internal(e.into()))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SyncRequest {
    oids: Vec<String>,
    #[serde(default)]
    force: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectRequest {
    pub oids: Vec<String>,
    #[serde(default)]
    pub blake3: bool,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ObjectResponse {
    pub objects: Vec<ObjectMetadata>,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ObjectMetadata {
    pub oid: String,
    pub size_bytes: u64,
    pub object_type: String,
    pub blake3: Option<Blake3Digest>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Blake3Digest {
    pub hash: String,
    pub size_bytes: u64,
}

async fn fetch_missing_wants(app: &App, repo: &Repository, body: &[u8]) -> Result<()> {
    let mut wants = Vec::new();
    let mut cursor = 0;
    while cursor + 4 <= body.len() {
        let length = std::str::from_utf8(&body[cursor..cursor + 4])
            .ok()
            .and_then(|s| usize::from_str_radix(s, 16).ok());
        let Some(length) = length else {
            break;
        };
        cursor += 4;
        if length <= 2 {
            continue;
        }
        if length < 4 || cursor + length - 4 > body.len() {
            break;
        }
        let packet = &body[cursor..cursor + length - 4];
        cursor += length - 4;
        if let Some(value) = packet.strip_prefix(b"want ") {
            let oid = std::str::from_utf8(value.get(..40).context("short want")?)?;
            ensure!(valid_oid(oid), "invalid want");
            wants.push(oid.to_ascii_lowercase());
        }
    }
    ensure!(wants.len() <= MAX_OBJECTS, "too many wants");
    fetch_absent(app, repo, &wants).await
}
async fn fetch_absent(app: &App, repo: &Repository, oids: &[String]) -> Result<()> {
    if oids.is_empty() {
        return Ok(());
    }
    let info = batch_info(app, repo, oids).await?;
    let missing: Vec<_> = oids
        .iter()
        .zip(info)
        .filter_map(|(oid, row)| row.is_none().then_some(oid.clone()))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let _guard = repo.refreshed.lock().await;
    let info = batch_info(app, repo, &missing).await?;
    let missing: Vec<_> = missing
        .into_iter()
        .zip(info)
        .filter_map(|(oid, row)| row.is_none().then_some(oid))
        .collect();
    if missing.is_empty() {
        return Ok(());
    }
    let mut command = git(app, Some(&repo.path));
    command
        .args([
            "fetch",
            "--no-prune",
            "--no-prune-tags",
            "--no-tags",
            "--no-write-fetch-head",
            "--recurse-submodules=no",
            "--refmap=",
            "--stdin",
        ])
        .arg(&repo.upstream)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut child = command.spawn()?;
    let _group = group(&child);
    let mut stdin = child.stdin.take().unwrap();
    let input = missing.join("\n") + "\n";
    tokio::spawn(async move {
        let _ = stdin.write_all(input.as_bytes()).await;
    });
    let status = tokio::time::timeout(GIT_TIMEOUT, child.wait()).await??;
    // Missing object IDs are reported by the metadata endpoint as 404 below.
    if !status.success() {
        return Ok(());
    }
    Ok(())
}
async fn batch_info(
    app: &App,
    repo: &Repository,
    oids: &[String],
) -> Result<Vec<Option<(String, u64)>>> {
    let mut command = git(app, Some(&repo.path));
    command
        .args(["cat-file", "--batch-check=%(objecttype) %(objectsize)"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = command.spawn()?;
    let _group = group(&child);
    let mut stdin = child.stdin.take().unwrap();
    let input = oids.join("\n") + "\n";
    tokio::spawn(async move {
        let _ = stdin.write_all(input.as_bytes()).await;
    });
    let output = tokio::time::timeout(GIT_TIMEOUT, child.wait_with_output()).await??;
    ensure!(output.status.success(), "cat-file failed");
    let mut result = Vec::new();
    for line in std::str::from_utf8(&output.stdout)?.lines() {
        if line.ends_with(" missing") {
            result.push(None);
            continue;
        }
        let (kind, size) = line.split_once(' ').context("invalid cat-file metadata")?;
        ensure!(
            ["blob", "tree", "commit", "tag"].contains(&kind),
            "invalid object type"
        );
        result.push(Some((kind.to_owned(), size.parse()?)));
    }
    ensure!(result.len() == oids.len(), "incomplete cat-file response");
    Ok(result)
}
async fn metadata(
    app: &App,
    repo: &Repository,
    query: ObjectRequest,
) -> Result<Vec<ObjectMetadata>, HttpError> {
    let mut seen = HashSet::new();
    let oids: Vec<_> = query
        .oids
        .into_iter()
        .map(|s| s.to_ascii_lowercase())
        .filter(|s| seen.insert(s.clone()))
        .collect();
    if oids.is_empty() {
        return Ok(Vec::new());
    }
    fetch_absent(app, repo, &oids).await.map_err(internal)?;
    let info = batch_info(app, repo, &oids).await.map_err(internal)?;
    let mut objects = Vec::new();
    for (oid, info) in oids.into_iter().zip(info) {
        let (object_type, size_bytes) = info
            .ok_or_else(|| HttpError(StatusCode::NOT_FOUND, format!("object {oid} not found\n")))?;
        let blake3 = if query.blake3 && object_type == "blob" {
            Some(
                blob_digest(app, repo, &oid, size_bytes)
                    .await
                    .map_err(internal)?,
            )
        } else {
            None
        };
        objects.push(ObjectMetadata {
            oid,
            size_bytes,
            object_type,
            blake3,
        });
    }
    Ok(objects)
}
async fn blob_digest(app: &App, repo: &Repository, oid: &str, size: u64) -> Result<Blake3Digest> {
    let key = hex::decode(oid)?;
    {
        let cache = app
            .digests
            .lock()
            .map_err(|_| anyhow::anyhow!("digest cache lock poisoned"))?;
        if let Some(value) = cache.db.read_tx().get(&cache.keyspace, &key)? {
            ensure!(value.len() == 40, "invalid cached digest");
            ensure!(
                u64::from_be_bytes(value[32..].try_into()?) == size,
                "cached size mismatch"
            );
            return Ok(Blake3Digest {
                hash: hex::encode(&value[..32]),
                size_bytes: size,
            });
        }
    }
    let mut command = git(app, Some(&repo.path));
    command
        .args(["cat-file", "blob", oid])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child: Child = command.spawn()?;
    let _group = group(&child);
    let mut stdout = child.stdout.take().unwrap();
    let mut hasher = blake3::Hasher::new();
    let mut total = 0u64;
    let deadline = tokio::time::Instant::now() + GIT_TIMEOUT;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let n = tokio::time::timeout_at(deadline, stdout.read(&mut buffer)).await??;
        if n == 0 {
            break;
        }
        total += n as u64;
        ensure!(total <= size, "blob exceeded reported size");
        hasher.update(&buffer[..n]);
    }
    ensure!(
        tokio::time::timeout_at(deadline, child.wait())
            .await??
            .success()
            && total == size,
        "incomplete blob"
    );
    let hash = hasher.finalize();
    let mut value = hash.as_bytes().to_vec();
    value.extend_from_slice(&size.to_be_bytes());
    let cache = app
        .digests
        .lock()
        .map_err(|_| anyhow::anyhow!("digest cache lock poisoned"))?;
    let mut tx = cache
        .db
        .write_tx()
        .durability(Some(fjall::PersistMode::SyncAll));
    tx.insert(&cache.keyspace, key, value);
    tx.commit()?;
    Ok(Blake3Digest {
        hash: hash.to_hex().to_string(),
        size_bytes: size,
    })
}
