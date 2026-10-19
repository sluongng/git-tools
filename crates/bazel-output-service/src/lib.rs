//! Bazel 9.2 Remote Output Service v1, eagerly materializing a pinned CAS.
//!
//! Outputs are real local files. Every build requests a conservative rescan;
//! no remote-only metadata or unverified incremental state survives a build.
//! The service is intended for a single trusted local user on loopback.
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt, OpenOptionsSyncExt};
use cap_std::fs::{Dir, OpenOptions};
use prost::Message;
use reapi::{Digest, proto::google::bytestream as bs};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex;
use tonic::{
    Request, Response, Status,
    transport::{Channel, ClientTlsConfig, Endpoint},
};

pub mod proto {
    tonic::include_proto!("bazel_output_service");
}
pub mod rev2 {
    tonic::include_proto!("bazel_output_service_rev2");
}
use proto::bazel_output_service_server::BazelOutputService;
const ARGS_TYPE: &str = "bazel_output_service_rev2.StartBuildArgs";
const FILE_TYPE: &str = "bazel_output_service_rev2.FileArtifactLocator";
const MAX_ITEMS: usize = 16_384;

/// The backend origin is operator-configured, never chosen by an RPC caller.
/// Pass credentials via `api_key`, usually read from an environment variable.
/// Do not derive `Debug`: credentials must not appear in logs.
pub struct Config {
    pub root: PathBuf,
    pub cas_endpoint: String,
    pub instance_name: String,
    pub api_key: Option<String>,
    pub max_blob_bytes: u64,
    pub rpc_timeout: Duration,
}
#[derive(Clone)]
pub struct OutputService {
    root: Arc<Dir>,
    root_path: PathBuf,
    backend: Backend,
    // Serializes lifecycle changes with staging, including across network awaits.
    // This favors correctness over parallel staging in this initial implementation.
    state: Arc<Mutex<HashMap<String, Build>>>,
    metrics: Arc<Metrics>,
}
#[derive(Default)]
struct Metrics {
    stage_requests: AtomicU64,
    staged_files: AtomicU64,
    verified_bytes: AtomicU64,
}
#[derive(Debug)]
pub struct MetricsSnapshot {
    pub stage_requests: u64,
    pub staged_files: u64,
    pub verified_bytes: u64,
}
struct Build {
    id: String,
    dir: Dir,
    digest_function: i32,
    finalized: bool,
}
// Clean up even when a stage RPC future is cancelled during a CAS read.
struct TempOutput {
    parent: Dir,
    name: String,
}
impl Drop for TempOutput {
    fn drop(&mut self) {
        let _ = self.parent.remove_file(&self.name);
    }
}
#[derive(Clone)]
struct Backend {
    channel: Channel,
    endpoint: String,
    instance: String,
    api_key: Option<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>,
    max_blob_bytes: u64,
    timeout: Duration,
}

fn io_status(error: std::io::Error) -> Status {
    match error.kind() {
        std::io::ErrorKind::NotFound => Status::not_found("output path does not exist"),
        std::io::ErrorKind::PermissionDenied => {
            Status::permission_denied("output path is not accessible within the output tree")
        }
        _ => Status::failed_precondition("output filesystem operation failed"),
    }
}
fn relative(path: &str, allow_root: bool) -> Result<PathBuf, Status> {
    if path.len() > 16_384 || path.contains('\\') || path.as_bytes().contains(&0) {
        return Err(Status::invalid_argument("invalid output path"));
    }
    let path = Path::new(path);
    if path
        .components()
        .any(|c| !matches!(c, Component::Normal(_) | Component::CurDir))
        || (!allow_root && (path.as_os_str().is_empty() || path == Path::new(".")))
    {
        return Err(Status::invalid_argument(
            "path must be confined and relative",
        ));
    }
    // Reject Win32 aliases before capability resolution: ADS and device names
    // are not ordinary output files, even when the path itself is relative.
    #[cfg(windows)]
    for component in path.components() {
        if let Component::Normal(name) = component {
            let name = name
                .to_str()
                .ok_or_else(|| Status::invalid_argument("invalid Windows output name"))?;
            let stem = name
                .split('.')
                .next()
                .unwrap()
                .trim_end_matches(' ')
                .to_uppercase();
            if name.ends_with(['.', ' '])
                || name.chars().any(|ch| ch < ' ' || "<>:\"|?*".contains(ch))
                || name.encode_utf16().count() > 255
                || matches!(
                    stem.as_str(),
                    "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
                )
                || (stem.len() == 4
                    && (stem.starts_with("COM") || stem.starts_with("LPT"))
                    && matches!(stem.as_bytes()[3], b'1'..=b'9'))
                || ["COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³"].contains(&stem.as_str())
            {
                return Err(Status::invalid_argument("invalid Windows output name"));
            }
        }
    }
    Ok(if path.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        path.to_path_buf()
    })
}
fn origin(value: &str) -> Result<String, Status> {
    let value = if let Some(x) = value.strip_prefix("grpcs://") {
        format!("https://{x}")
    } else if let Some(x) = value.strip_prefix("grpc://") {
        format!("http://{x}")
    } else {
        value.to_owned()
    };
    let uri: tonic::codegen::http::Uri = value
        .parse()
        .map_err(|_| Status::invalid_argument("invalid CAS endpoint"))?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.authority().is_none()
        || uri.authority().is_some_and(|a| a.as_str().contains('@'))
        || uri.path() != "/"
        || uri.query().is_some()
    {
        return Err(Status::invalid_argument(
            "CAS endpoint must be an http(s)/grpc(s) origin",
        ));
    }
    Ok(format!(
        "{}://{}",
        uri.scheme_str().unwrap(),
        uri.authority().unwrap()
    ))
}
fn unpack<T: Message + Default>(any: Option<prost_types::Any>, name: &str) -> Result<T, Status> {
    let any = any.ok_or_else(|| Status::invalid_argument("missing protocol arguments"))?;
    if any.type_url.rsplit('/').next() != Some(name) {
        return Err(Status::invalid_argument(
            "unsupported protocol argument type",
        ));
    }
    T::decode(any.value.as_slice())
        .map_err(|_| Status::invalid_argument("invalid protocol argument encoding"))
}
pub fn file_locator(digest: Digest) -> prost_types::Any {
    prost_types::Any {
        type_url: format!("type.googleapis.com/{FILE_TYPE}"),
        value: rev2::FileArtifactLocator {
            digest: Some(digest),
        }
        .encode_to_vec(),
    }
}
fn digest_of(locator: Option<prost_types::Any>) -> Result<Digest, Status> {
    let d = unpack::<rev2::FileArtifactLocator>(locator, FILE_TYPE)?
        .digest
        .ok_or_else(|| Status::invalid_argument("missing digest"))?;
    if d.size_bytes < 0
        || d.hash.len() != 64
        || !d
            .hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(Status::invalid_argument("invalid SHA256/BLAKE3 digest"));
    }
    Ok(d)
}
enum Hasher {
    Sha256(Sha256),
    Blake3(Box<blake3::Hasher>),
}
impl Hasher {
    fn new(function: i32) -> Result<Self, Status> {
        match function {
            1 => Ok(Self::Sha256(Sha256::new())),
            9 => Ok(Self::Blake3(Box::new(blake3::Hasher::new()))),
            _ => Err(Status::invalid_argument(
                "only SHA256 and BLAKE3 are supported",
            )),
        }
    }
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha256(h) => h.update(bytes),
            Self::Blake3(h) => {
                h.update(bytes);
            }
        }
    }
    fn finish(self) -> String {
        match self {
            Self::Sha256(h) => format!("{:x}", h.finalize()),
            Self::Blake3(h) => h.finalize().to_hex().to_string(),
        }
    }
}
impl Backend {
    fn request<T>(&self, value: T) -> Request<T> {
        let mut req = Request::new(value);
        req.set_timeout(self.timeout);
        if let Some(key) = &self.api_key {
            req.metadata_mut()
                .insert("x-buildbuddy-api-key", key.clone());
        }
        req
    }
    async fn download(
        &self,
        digest: &Digest,
        function: i32,
        file: &mut cap_std::fs::File,
    ) -> Result<(), Status> {
        if digest.size_bytes as u64 > self.max_blob_bytes {
            return Err(Status::resource_exhausted(
                "blob exceeds configured size limit",
            ));
        }
        let resource = format!(
            "{}{}blobs/{}{}/{}",
            self.instance,
            if self.instance.is_empty() { "" } else { "/" },
            if function == 9 { "blake3/" } else { "" },
            digest.hash,
            digest.size_bytes
        );
        let mut client = bs::byte_stream_client::ByteStreamClient::new(self.channel.clone());
        let mut stream = client
            .read(self.request(bs::ReadRequest {
                resource_name: resource,
                read_offset: 0,
                read_limit: 0,
            }))
            .await?
            .into_inner();
        let mut hasher = Hasher::new(function)?;
        let mut size = 0u64;
        while let Some(chunk) = stream.message().await? {
            size = size
                .checked_add(chunk.data.len() as u64)
                .ok_or_else(|| Status::data_loss("CAS stream length overflow"))?;
            if size > digest.size_bytes as u64 {
                return Err(Status::data_loss("CAS stream exceeded declared length"));
            }
            file.write_all(&chunk.data).map_err(io_status)?;
            hasher.update(&chunk.data);
        }
        if size != digest.size_bytes as u64 || hasher.finish() != digest.hash {
            return Err(Status::data_loss("CAS content digest or length mismatch"));
        }
        Ok(())
    }
}
impl OutputService {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let endpoint = origin(&config.cas_endpoint)?;
        if config.instance_name.starts_with('/')
            || config.instance_name.ends_with('/')
            || config
                .instance_name
                .split('/')
                .any(|c| matches!(c, "." | ".."))
        {
            anyhow::bail!("invalid CAS instance name");
        }
        // Configure the root once, outside untrusted request processing.
        std::fs::create_dir_all(&config.root)?;
        let root_path = std::fs::canonicalize(&config.root)?;
        let root = Dir::open_ambient_dir(&root_path, cap_std::ambient_authority())?;
        let mut transport = Endpoint::from_shared(endpoint.clone())?
            .connect_timeout(config.rpc_timeout)
            .timeout(config.rpc_timeout);
        if endpoint.starts_with("https://") {
            transport = transport.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
        }
        let mut api_key = config
            .api_key
            .map(|key| key.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>())
            .transpose()
            .map_err(|_| anyhow::anyhow!("invalid backend credential encoding"))?;
        if let Some(value) = &mut api_key {
            value.set_sensitive(true);
        }
        Ok(Self {
            root: Arc::new(root),
            root_path,
            backend: Backend {
                channel: transport.connect_lazy(),
                endpoint,
                instance: config.instance_name,
                api_key,
                max_blob_bytes: config.max_blob_bytes,
                timeout: config.rpc_timeout,
            },
            state: Arc::new(Mutex::new(HashMap::new())),
            metrics: Arc::new(Metrics::default()),
        })
    }
    pub fn metrics(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            stage_requests: self.metrics.stage_requests.load(Ordering::Relaxed),
            staged_files: self.metrics.staged_files.load(Ordering::Relaxed),
            verified_bytes: self.metrics.verified_bytes.load(Ordering::Relaxed),
        }
    }
    fn key(id: &str) -> Result<String, Status> {
        if id.is_empty() || id.len() > 256 {
            return Err(Status::invalid_argument("invalid output_base_id"));
        }
        Ok(blake3::hash(id.as_bytes()).to_hex().to_string())
    }
    fn build<'a>(state: &'a HashMap<String, Build>, id: &str) -> Result<&'a Build, Status> {
        state
            .values()
            .find(|b| b.id == id && !b.finalized)
            .ok_or_else(|| Status::failed_precondition("unknown or finalized build_id"))
    }
    async fn stage(
        &self,
        build: &Build,
        path: &str,
        locator: Option<prost_types::Any>,
    ) -> Result<(), Status> {
        let path = relative(path, false)?;
        let digest = digest_of(locator)?;
        let parent_path = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        // Capability-relative resolution rejects symlink escapes and is race-safe.
        build.dir.create_dir_all(parent_path).map_err(io_status)?;
        let parent = build.dir.open_dir(parent_path).map_err(io_status)?;
        let name = path
            .file_name()
            .ok_or_else(|| Status::invalid_argument("missing file name"))?;
        let temp = format!(".git-output-service-{}.tmp", uuid::Uuid::new_v4());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = parent.open_with(&temp, &options).map_err(io_status)?;
        let temp = TempOutput { parent, name: temp };
        async {
            tokio::time::timeout(
                self.backend.timeout,
                self.backend
                    .download(&digest, build.digest_function, &mut file),
            )
            .await
            .map_err(|_| Status::deadline_exceeded("CAS read timed out"))??;
            file.sync_all().map_err(io_status)?;
            // The protocol carries no executable bit: all staged files may be
            // executed, matching remote-output-service filesystem semantics.
            #[cfg(unix)]
            {
                use cap_std::fs::PermissionsExt;
                file.set_permissions(cap_std::fs::Permissions::from_mode(0o755))
                    .map_err(io_status)?;
            }
            let renamed = temp.parent.rename(&temp.name, &temp.parent, name);
            #[cfg(windows)]
            let renamed = match renamed {
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    use cap_std::fs::OpenOptionsExt;
                    // Windows cannot replace a read-only file. Open only the
                    // destination's attributes without following a reparse point;
                    // never unlink the old output before verified atomic rename.
                    let mut options = OpenOptions::new();
                    options.access_mode(0x80 | 0x100).follow(FollowSymlinks::No);
                    match temp.parent.open_with(name, &options) {
                        Ok(destination) if destination.metadata().map_err(io_status)?.is_file() => {
                            let original = destination.metadata().map_err(io_status)?.permissions();
                            if original.readonly() {
                                let mut writable = original.clone();
                                writable.set_readonly(false);
                                destination.set_permissions(writable).map_err(io_status)?;
                                let retry = temp.parent.rename(&temp.name, &temp.parent, name);
                                if retry.is_err() {
                                    let _ = destination.set_permissions(original);
                                }
                                retry
                            } else {
                                Err(error)
                            }
                        }
                        _ => Err(error),
                    }
                }
                result => result,
            };
            renamed.map_err(io_status)?;
            self.metrics.staged_files.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .verified_bytes
                .fetch_add(digest.size_bytes as u64, Ordering::Relaxed);
            Ok(())
        }
        .await
    }
    async fn stat(
        &self,
        build: &Build,
        path: &str,
    ) -> Result<Option<proto::batch_stat_response::Stat>, Status> {
        use proto::batch_stat_response::{Stat, stat};
        let path = relative(path, true)?;
        let metadata = match build.dir.symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            // An unresolvable external symlink or special path is an unknown
            // type, NOT an assertion that the path was absent.
            Err(_) => return Ok(Some(Stat { r#type: None })),
        };
        let kind = if metadata.is_symlink() {
            match build.dir.read_link_contents(&path) {
                Ok(target) => Some(stat::Type::Symlink(stat::Symlink {
                    target: target.to_string_lossy().into_owned(),
                })),
                Err(_) => None, // The final component changed after lstat.
            }
        } else if metadata.is_dir() {
            Some(stat::Type::Directory(stat::Directory {}))
        } else if metadata.is_file() {
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No).nonblock(true);
            let mut file = match build.dir.open_with(&path, &options) {
                Ok(file) => file,
                // A concurrent type change cannot turn lstat into a following
                // open, or block on a FIFO while retaining the lifecycle lock.
                Err(_) => return Ok(Some(Stat { r#type: None })),
            };
            let current = file.metadata().map_err(io_status)?;
            if !current.is_file() || current.len() > self.backend.max_blob_bytes {
                return Ok(Some(Stat { r#type: None }));
            }
            let deadline = std::time::Instant::now() + self.backend.timeout;
            let mut hash = Hasher::new(build.digest_function)?;
            let mut buffer = [0u8; 64 * 1024];
            let mut size = 0i64;
            loop {
                let n = file.read(&mut buffer).map_err(io_status)?;
                if n == 0 {
                    break;
                }
                hash.update(&buffer[..n]);
                size += n as i64;
                if size as u64 > self.backend.max_blob_bytes
                    || std::time::Instant::now() >= deadline
                {
                    return Ok(Some(Stat { r#type: None }));
                }
                // Bound cancellation latency during large local output scans.
                tokio::task::yield_now().await;
            }
            Some(stat::Type::File(stat::File {
                locator: Some(file_locator(Digest {
                    hash: hash.finish(),
                    size_bytes: size,
                })),
            }))
        } else {
            None
        };
        Ok(Some(Stat { r#type: kind }))
    }
}
#[tonic::async_trait]
impl BazelOutputService for OutputService {
    async fn start_build(
        &self,
        request: Request<proto::StartBuildRequest>,
    ) -> Result<Response<proto::StartBuildResponse>, Status> {
        let req = request.into_inner();
        if req.version != 1 {
            return Err(Status::invalid_argument(
                "unsupported BOS protocol version (expected 1)",
            ));
        }
        if req.build_id.is_empty() || req.build_id.len() > 256 {
            return Err(Status::invalid_argument("invalid build_id"));
        }
        let args = unpack::<rev2::StartBuildArgs>(req.args, ARGS_TYPE)?;
        Hasher::new(args.digest_function)?;
        if origin(&args.remote_cache)? != self.backend.endpoint
            || args.instance_name != self.backend.instance
        {
            return Err(Status::permission_denied(
                "requested CAS does not match configured backend",
            ));
        }
        let key = Self::key(&req.output_base_id)?;
        let target = self.root_path.join(&key);
        let suffix = if req.output_path_prefix.is_empty() {
            target.clone()
        } else {
            // Both paths must use the same canonical spelling, particularly
            // Windows verbatim prefixes and macOS /var -> /private/var aliases.
            let prefix = Path::new(&req.output_path_prefix);
            if !prefix.is_absolute() {
                return Err(Status::invalid_argument(
                    "output_path_prefix must be absolute",
                ));
            }
            let prefix = std::fs::canonicalize(prefix).map_err(|_| {
                Status::invalid_argument("output_path_prefix must name an existing local directory")
            })?;
            target
                .strip_prefix(prefix)
                .map_err(|_| {
                    Status::invalid_argument(
                        "output_path_prefix must contain configured output root",
                    )
                })?
                .to_path_buf()
        };
        let mut state = self.state.lock().await;
        if state
            .iter()
            .any(|(other, b)| *other != key && b.id == req.build_id)
        {
            return Err(Status::already_exists(
                "build_id already used by another output base",
            ));
        }
        self.root.create_dir_all(&key).map_err(io_status)?;
        let dir = self.root.open_dir(&key).map_err(io_status)?;
        state.insert(
            key,
            Build {
                id: req.build_id,
                dir,
                digest_function: args.digest_function,
                finalized: false,
            },
        );
        // No incremental tracking promise. All bytes are already local, and
        // Bazel must revalidate outputs instead of trusting stale metadata.
        Ok(Response::new(proto::StartBuildResponse {
            initial_output_path_contents: None,
            output_path_suffix: suffix.to_string_lossy().into_owned(),
        }))
    }
    async fn stage_artifacts(
        &self,
        request: Request<proto::StageArtifactsRequest>,
    ) -> Result<Response<proto::StageArtifactsResponse>, Status> {
        let req = request.into_inner();
        if req.artifacts.len() > MAX_ITEMS {
            return Err(Status::resource_exhausted("too many artifacts"));
        }
        let state = self.state.lock().await;
        let build = Self::build(&state, &req.build_id)?;
        self.metrics.stage_requests.fetch_add(1, Ordering::Relaxed);
        let mut responses = Vec::with_capacity(req.artifacts.len());
        for artifact in req.artifacts {
            let status = match self.stage(build, &artifact.path, artifact.locator).await {
                Ok(()) => reapi::proto::google::rpc::Status::default(),
                Err(error) => reapi::proto::google::rpc::Status {
                    code: error.code() as i32,
                    message: error.message().to_owned(),
                    details: vec![],
                },
            };
            responses.push(proto::stage_artifacts_response::Response {
                status: Some(status),
            });
        }
        Ok(Response::new(proto::StageArtifactsResponse { responses }))
    }
    async fn finalize_artifacts(
        &self,
        request: Request<proto::FinalizeArtifactsRequest>,
    ) -> Result<Response<proto::FinalizeArtifactsResponse>, Status> {
        let req = request.into_inner();
        if req.artifacts.len() > MAX_ITEMS {
            return Err(Status::resource_exhausted("too many artifacts"));
        }
        let state = self.state.lock().await;
        let build = Self::build(&state, &req.build_id)?;
        // Deliberately no dirty-bit promise: the next build scans everything.
        // Still validate paths/locators so malformed callers fail immediately.
        for artifact in req.artifacts {
            relative(&artifact.path, false)?;
            if artifact.locator.is_some() {
                digest_of(artifact.locator)?;
            }
        }
        let _ = build;
        Ok(Response::new(proto::FinalizeArtifactsResponse {}))
    }
    async fn finalize_build(
        &self,
        request: Request<proto::FinalizeBuildRequest>,
    ) -> Result<Response<proto::FinalizeBuildResponse>, Status> {
        let req = request.into_inner();
        let mut state = self.state.lock().await;
        let build = state
            .values_mut()
            .find(|b| b.id == req.build_id)
            .ok_or_else(|| Status::failed_precondition("unknown build_id"))?;
        // Finalization is idempotent so a lost successful response is retryable.
        build.finalized = true;
        Ok(Response::new(proto::FinalizeBuildResponse {}))
    }
    async fn clean(
        &self,
        request: Request<proto::CleanRequest>,
    ) -> Result<Response<proto::CleanResponse>, Status> {
        let key = Self::key(&request.into_inner().output_base_id)?;
        let mut state = self.state.lock().await;
        // Close the retained directory handle first: on Windows directory
        // handles do not share delete access. A failed clean invalidates the
        // session but can safely be retried by output_base_id.
        state.remove(&key);
        match self.root.remove_dir_all(&key) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io_status(e)),
        }
        Ok(Response::new(proto::CleanResponse {}))
    }
    async fn batch_stat(
        &self,
        request: Request<proto::BatchStatRequest>,
    ) -> Result<Response<proto::BatchStatResponse>, Status> {
        let req = request.into_inner();
        if req.paths.len() > MAX_ITEMS {
            return Err(Status::resource_exhausted("too many paths"));
        }
        let state = self.state.lock().await;
        let build = Self::build(&state, &req.build_id)?;
        let mut responses = Vec::with_capacity(req.paths.len());
        for path in req.paths {
            responses.push(proto::batch_stat_response::StatResponse {
                stat: self.stat(build, &path).await?,
            });
        }
        Ok(Response::new(proto::BatchStatResponse { responses }))
    }
}
