//! Bazel 9.2 Remote Output Service v1 with eager and native lazy projections.
//!
//! Eager storage downloads before staging; native lazy projections publish CAS
//! metadata and verify content on first read. Every build requests a conservative
//! rescan and checks remote presence before retaining untouched lazy entries.
//! The service is intended for a single trusted local user on loopback.
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt, OpenOptionsSyncExt};
use cap_std::fs::{Dir, OpenOptions};
use output_core::{Artifact, BlobSource, Projection, TreeStat};
use prost::Message;
use reapi::{
    Digest,
    policy::{Policy, timeout},
    proto::google::bytestream as bs,
};
use sha2::{Digest as _, Sha256};
use std::{
    collections::HashMap,
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex, OnceCell};
use tonic::{
    Request, Response, Status,
    codec::CompressionEncoding,
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
    pub policy: Policy,
}
#[derive(Clone)]
pub struct OutputService {
    root: Arc<Dir>,
    root_path: PathBuf,
    backend: Backend,
    projection: Option<Arc<dyn Projection>>,
    projection_tree: Option<Arc<output_core::Tree>>,
    // Registry operations never await; each output base owns its lifecycle lock.
    state: Arc<StdMutex<Registry>>,
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
    pub lazy_fetches: u64,
    pub lazy_verified_bytes: u64,
    pub lazy_cache_hits: u64,
    pub zstd_read_requests: u64,
    pub cas_read_wire_bytes: u64,
}
struct Build {
    id: String,
    dir: Option<Dir>,
    key: String,
    digest_function: i32,
    finalized: bool,
}
type Base = Mutex<Option<Build>>;
#[derive(Default)]
struct Registry {
    bases: HashMap<String, Weak<Base>>,
    active: HashMap<String, Arc<Base>>,
    ids: HashMap<String, (String, u64)>,
    next_token: u64,
}
// A canceled or failed StartBuild must not retain its tentative build ID.
struct Reservation {
    registry: Arc<StdMutex<Registry>>,
    id: String,
    token: u64,
    committed: bool,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.committed {
            let mut registry = self.registry.lock().unwrap();
            if registry
                .ids
                .get(&self.id)
                .is_some_and(|(_, token)| *token == self.token)
            {
                registry.ids.remove(&self.id);
            }
        }
    }
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
    policy: Policy,
    zstd_supported: Arc<OnceCell<bool>>,
    grpc_compression: Arc<reapi::compression::GrpcCompression>,
    zstd_read_requests: Arc<AtomicU64>,
    cas_read_wire_bytes: Arc<AtomicU64>,
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
fn projection_status(error: anyhow::Error) -> Status {
    if error
        .downcast_ref::<output_core::CacheTimeoutError>()
        .is_some()
    {
        return Status::deadline_exceeded("output cache operation timed out");
    }
    if error
        .downcast_ref::<output_core::CacheLimitError>()
        .is_some()
    {
        return Status::resource_exhausted("artifact exceeds configured lazy cache limits");
    }
    if let Some(status) = error.downcast_ref::<Status>() {
        return status.clone();
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return match error.kind() {
            std::io::ErrorKind::NotFound => Status::not_found("output path does not exist"),
            std::io::ErrorKind::PermissionDenied => {
                Status::permission_denied("output path is not accessible")
            }
            std::io::ErrorKind::AlreadyExists => {
                Status::already_exists("output path already exists")
            }
            _ => Status::failed_precondition("projected output filesystem operation failed"),
        };
    }
    Status::failed_precondition("projected output operation failed")
}

fn projected_path(build: &Build, path: &Path) -> String {
    if path == Path::new(".") || path.as_os_str().is_empty() {
        build.key.clone()
    } else {
        format!(
            "{}/{}",
            build.key,
            path.to_string_lossy().replace('\\', "/")
        )
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
    fn identity(&self) -> String {
        blake3::hash(format!("{}\0{}", self.endpoint, self.instance).as_bytes())
            .to_hex()
            .to_string()
    }
    fn request_with_timeout<T>(&self, value: T, budget: Duration) -> Request<T> {
        let mut req = Request::new(value);
        req.set_timeout(budget);
        if let Some(key) = &self.api_key {
            req.metadata_mut()
                .insert("x-buildbuddy-api-key", key.clone());
        }
        req
    }
    async fn supports_zstd(&self, deadline: tokio::time::Instant) -> Result<bool, Status> {
        use reapi::reapi::{GetCapabilitiesRequest, capabilities_client::CapabilitiesClient};
        timeout(deadline, async {
            self.zstd_supported
                .get_or_try_init(|| async {
                    self.policy
                        .retry(deadline, self.policy.rpc_timeout(), |budget| async move {
                            let mut client = CapabilitiesClient::new(self.channel.clone())
                                .accept_compressed(CompressionEncoding::Gzip);
                            if let Some(encoding) = self.grpc_compression.request_encoding() {
                                client = client.send_compressed(encoding);
                            }
                            let request = GetCapabilitiesRequest {
                                instance_name: self.instance.clone(),
                            };
                            let attempted = self.grpc_compression.request_encoding();
                            let response = match client
                                .get_capabilities(
                                    self.request_with_timeout(request.clone(), budget),
                                )
                                .await
                            {
                                Err(error)
                                    if self.grpc_compression.retry_identity(&error, attempted) =>
                                {
                                    CapabilitiesClient::new(self.channel.clone())
                                        .accept_compressed(CompressionEncoding::Gzip)
                                        .get_capabilities(
                                            self.request_with_timeout(request, budget),
                                        )
                                        .await
                                }
                                result => result,
                            };
                            match response {
                                Ok(response) => {
                                    self.grpc_compression.observe(response.metadata());
                                    Ok(response.into_inner().cache_capabilities.is_some_and(
                                        |caps| caps.supported_compressors.contains(&1),
                                    ))
                                }
                                Err(error) if error.code() == tonic::Code::Unimplemented => {
                                    Ok(false)
                                }
                                Err(error) => Err(error),
                            }
                        })
                        .await
                })
                .await
                .copied()
        })
        .await
    }
    async fn download(
        &self,
        digest: &Digest,
        function: i32,
        file: &mut std::fs::File,
    ) -> Result<(), Status> {
        // REAPI's canonical empty blob exists without a CAS transfer. In
        // particular, compressed ByteStream reads may finish without emitting a
        // ZSTD frame; verify the digest locally instead of weakening EOF checks.
        if digest.size_bytes == 0 {
            if Hasher::new(function)?.finish() != digest.hash {
                return Err(Status::data_loss("CAS empty content digest mismatch"));
            }
            file.set_len(0).map_err(io_status)?;
            file.seek(SeekFrom::Start(0)).map_err(io_status)?;
            return Ok(());
        }
        let deadline = tokio::time::Instant::now() + self.policy.operation_timeout();
        let compressed = self.supports_zstd(deadline).await?;
        self.policy
            .retry(deadline, self.policy.operation_timeout(), |_| {
                let file = file.try_clone().map_err(io_status);
                async move {
                    let mut file = file?;
                    file.set_len(0).map_err(io_status)?;
                    file.seek(SeekFrom::Start(0)).map_err(io_status)?;
                    self.download_attempt(digest, function, &mut file, compressed, deadline)
                        .await
                }
            })
            .await
    }
    async fn download_attempt(
        &self,
        digest: &Digest,
        function: i32,
        file: &mut std::fs::File,
        compressed: bool,
        deadline: tokio::time::Instant,
    ) -> Result<(), Status> {
        if digest.size_bytes as u64 > self.max_blob_bytes {
            return Err(Status::resource_exhausted(
                "blob exceeds configured size limit",
            ));
        }
        let resource = format!(
            "{}{}{}/{}{}/{}",
            self.instance,
            if self.instance.is_empty() { "" } else { "/" },
            if compressed {
                "compressed-blobs/zstd"
            } else {
                "blobs"
            },
            if function == 9 { "blake3/" } else { "" },
            digest.hash,
            digest.size_bytes
        );
        let mut client = bs::byte_stream_client::ByteStreamClient::new(self.channel.clone())
            .accept_compressed(CompressionEncoding::Gzip);
        if compressed {
            self.zstd_read_requests.fetch_add(1, Ordering::Relaxed);
        }
        let mut stream = timeout(
            (tokio::time::Instant::now() + self.policy.rpc_timeout()).min(deadline),
            client.read(self.request_with_timeout(
                bs::ReadRequest {
                    resource_name: resource,
                    read_offset: 0,
                    read_limit: 0,
                },
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )),
        )
        .await?
        .into_inner();
        // This writer checks the uncompressed length before publishing each chunk.
        // The decoder uses a fixed-size buffer, never a whole-blob allocation.
        struct VerifiedWriter<'a, W> {
            file: &'a mut W,
            hasher: Hasher,
            size: u64,
            limit: u64,
        }
        impl<W: Write> Write for VerifiedWriter<'_, W> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let size = self
                    .size
                    .checked_add(bytes.len() as u64)
                    .filter(|size| *size <= self.limit)
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "CAS stream exceeded declared length",
                        )
                    })?;
                self.file.write_all(bytes)?;
                self.hasher.update(bytes);
                self.size = size;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.file.flush()
            }
        }
        let mut writer = VerifiedWriter {
            file,
            hasher: Hasher::new(function)?,
            size: 0,
            limit: digest.size_bytes as u64,
        };
        let mut decoder = compressed
            .then(reapi::compression::ZstdDecoder::new)
            .transpose()
            .map_err(|_| Status::internal("cannot initialize CAS decoder"))?;
        let mut wire_bytes = 0u64;
        let wire_limit = (digest.size_bytes as u64)
            .saturating_add((digest.size_bytes as u64 / 8).max(1024 * 1024));
        let mut empty_messages = 0u32;
        let mut progress_deadline =
            (tokio::time::Instant::now() + self.policy.stream_idle_timeout()).min(deadline);
        while let Some(chunk) = timeout(progress_deadline, stream.message()).await? {
            if !chunk.data.is_empty() {
                progress_deadline =
                    (tokio::time::Instant::now() + self.policy.stream_idle_timeout()).min(deadline);
            }
            self.cas_read_wire_bytes
                .fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
            wire_bytes = wire_bytes
                .checked_add(chunk.data.len() as u64)
                .filter(|size| *size <= wire_limit)
                .ok_or_else(|| Status::data_loss("CAS compressed stream exceeded wire limit"))?;
            empty_messages = if chunk.data.is_empty() {
                empty_messages + 1
            } else {
                0
            };
            if empty_messages > 1024 {
                return Err(Status::data_loss("CAS stream made no progress"));
            }
            if let Some(decoder) = &mut decoder {
                let mut input = chunk.data.as_slice();
                let mut drain = false;
                while !input.is_empty() || drain {
                    let step = decoder
                        .decode_step(input, &mut writer)
                        .map_err(|_| Status::data_loss("invalid CAS content stream"))?;
                    input = &input[step.consumed..];
                    drain = step.needs_drain;
                    // A tiny compressed message can expand into many output chunks.
                    // Yield after each bounded step so the total deadline can cancel it.
                    tokio::task::yield_now().await;
                }
            } else {
                writer
                    .write_all(&chunk.data)
                    .map_err(|_| Status::data_loss("invalid CAS content stream"))?;
            }
        }
        if let Some(decoder) = decoder {
            decoder
                .finish()
                .map_err(|_| Status::data_loss("truncated CAS compressed stream"))?;
        }
        if writer.size != digest.size_bytes as u64 || writer.hasher.finish() != digest.hash {
            return Err(Status::data_loss("CAS content digest or length mismatch"));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl BlobSource for Backend {
    async fn fetch(&self, artifact: &Artifact, file: &mut std::fs::File) -> anyhow::Result<()> {
        anyhow::ensure!(
            artifact.backend == self.identity() && artifact.instance == self.instance,
            "artifact does not belong to the configured CAS"
        );
        let size_bytes = i64::try_from(artifact.size)?;
        self.download(
            &Digest {
                hash: artifact.hash.clone(),
                size_bytes,
            },
            artifact.digest_function,
            file,
        )
        .await?;
        Ok(())
    }

    async fn missing(&self, artifacts: &[Artifact]) -> anyhow::Result<Vec<Artifact>> {
        use reapi::reapi::{
            FindMissingBlobsRequest,
            content_addressable_storage_client::ContentAddressableStorageClient,
        };
        let deadline = tokio::time::Instant::now() + self.policy.operation_timeout();
        let mut missing = Vec::new();
        for function in [1, 9] {
            let selected: Vec<_> = artifacts
                .iter()
                .filter(|a| a.digest_function == function)
                .collect();
            for batch in selected.chunks(512) {
                let mut digests = Vec::new();
                for artifact in batch {
                    anyhow::ensure!(
                        artifact.backend == self.identity() && artifact.instance == self.instance,
                        "artifact does not belong to the configured CAS"
                    );
                    digests.push(Digest {
                        hash: artifact.hash.clone(),
                        size_bytes: i64::try_from(artifact.size)?,
                    });
                }
                self.supports_zstd(deadline).await?;
                let request = FindMissingBlobsRequest {
                    instance_name: self.instance.clone(),
                    blob_digests: digests,
                    digest_function: function,
                };
                let response = self
                    .policy
                    .retry(deadline, self.policy.rpc_timeout(), |budget| {
                        let request = request.clone();
                        async move {
                            let attempted = self.grpc_compression.request_encoding();
                            let mut client =
                                ContentAddressableStorageClient::new(self.channel.clone())
                                    .accept_compressed(CompressionEncoding::Gzip);
                            if let Some(encoding) = attempted {
                                client = client.send_compressed(encoding);
                            }
                            match client
                                .find_missing_blobs(
                                    self.request_with_timeout(request.clone(), budget),
                                )
                                .await
                            {
                                Err(error)
                                    if self.grpc_compression.retry_identity(&error, attempted) =>
                                {
                                    ContentAddressableStorageClient::new(self.channel.clone())
                                        .accept_compressed(CompressionEncoding::Gzip)
                                        .find_missing_blobs(
                                            self.request_with_timeout(request, budget),
                                        )
                                        .await
                                }
                                result => result,
                            }
                        }
                    })
                    .await?
                    .into_inner();
                for digest in response.missing_blob_digests {
                    let artifact = batch
                        .iter()
                        .find(|a| {
                            a.hash == digest.hash
                                && i64::try_from(a.size).ok() == Some(digest.size_bytes)
                        })
                        .ok_or_else(|| {
                            anyhow::anyhow!("CAS returned an unrequested missing digest")
                        })?;
                    missing.push((*artifact).clone());
                }
            }
        }
        anyhow::ensure!(
            artifacts.iter().all(|a| matches!(a.digest_function, 1 | 9)),
            "unsupported digest function"
        );
        Ok(missing)
    }
}
impl OutputService {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        config.policy.validate()?;
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
            .connect_timeout(config.policy.connect_timeout());
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
                policy: config.policy,
                zstd_supported: Arc::new(OnceCell::new()),
                grpc_compression: Arc::new(reapi::compression::GrpcCompression::default()),
                zstd_read_requests: Arc::new(AtomicU64::new(0)),
                cas_read_wire_bytes: Arc::new(AtomicU64::new(0)),
            },
            projection: None,
            projection_tree: None,
            state: Arc::new(StdMutex::new(Registry::default())),
            metrics: Arc::new(Metrics::default()),
        })
    }
    pub fn policy(&self) -> &Policy {
        &self.backend.policy
    }
    /// A credential-bearing source restricted to this operator-selected backend.
    pub fn blob_source(&self) -> Arc<dyn BlobSource> {
        Arc::new(self.backend.clone())
    }
    pub fn backend_identity(&self) -> String {
        self.backend.identity()
    }
    /// Attach a mounted projection before accepting any build requests.
    /// The adapter must return the root canonicalized before mounting. Do not
    /// re-enter a native mount synchronously from the async service.
    pub fn with_projection(
        mut self,
        projection: Arc<dyn Projection>,
        tree: Arc<output_core::Tree>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            projection.root() == self.root_path,
            "projection root must match the configured output root"
        );
        self.projection = Some(projection);
        self.projection_tree = Some(tree);
        Ok(self)
    }
    pub fn metrics(&self) -> MetricsSnapshot {
        let cache = self
            .projection_tree
            .as_ref()
            .map(|t| t.cache_metrics())
            .unwrap_or_default();
        MetricsSnapshot {
            stage_requests: self.metrics.stage_requests.load(Ordering::Relaxed),
            staged_files: self.metrics.staged_files.load(Ordering::Relaxed),
            verified_bytes: self.metrics.verified_bytes.load(Ordering::Relaxed),
            lazy_fetches: cache.fetches,
            lazy_verified_bytes: cache.verified_bytes,
            lazy_cache_hits: cache.hits,
            zstd_read_requests: self.backend.zstd_read_requests.load(Ordering::Relaxed),
            cas_read_wire_bytes: self.backend.cas_read_wire_bytes.load(Ordering::Relaxed),
        }
    }
    fn key(id: &str) -> Result<String, Status> {
        if id.is_empty() || id.len() > 256 {
            return Err(Status::invalid_argument("invalid output_base_id"));
        }
        Ok(blake3::hash(id.as_bytes()).to_hex().to_string())
    }
    fn base(&self, key: &str) -> Result<Arc<Base>, Status> {
        let mut registry = self.state.lock().unwrap();
        registry.bases.retain(|_, base| base.strong_count() != 0);
        if let Some(base) = registry.bases.get(key).and_then(Weak::upgrade) {
            return Ok(base);
        }
        if registry.bases.len() >= MAX_ITEMS {
            return Err(Status::resource_exhausted("too many output bases"));
        }
        let base = Arc::new(Mutex::new(None));
        registry.bases.insert(key.to_owned(), Arc::downgrade(&base));
        Ok(base)
    }
    fn session(&self, id: &str) -> Result<Arc<Base>, Status> {
        let registry = self.state.lock().unwrap();
        let (key, _) = registry
            .ids
            .get(id)
            .ok_or_else(|| Status::failed_precondition("unknown build_id"))?;
        registry
            .bases
            .get(key)
            .and_then(Weak::upgrade)
            .ok_or_else(|| Status::failed_precondition("unknown build_id"))
    }
    fn build<'a>(state: &'a Option<Build>, id: &str) -> Result<&'a Build, Status> {
        state
            .as_ref()
            .filter(|b| b.id == id && !b.finalized)
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
        if digest.size_bytes as u64 > self.backend.max_blob_bytes {
            return Err(Status::resource_exhausted(
                "blob exceeds configured size limit",
            ));
        }
        if let Some(projection) = &self.projection {
            projection
                .stage(
                    &projected_path(build, &path),
                    Artifact {
                        backend: self.backend.identity(),
                        instance: self.backend.instance.clone(),
                        digest_function: build.digest_function,
                        hash: digest.hash,
                        size: digest.size_bytes as u64,
                    },
                )
                .await
                .map_err(projection_status)?;
            self.metrics.staged_files.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let dir = build
            .dir
            .as_ref()
            .ok_or_else(|| Status::internal("missing eager output directory"))?;
        let parent_path = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        // Capability-relative resolution rejects symlink escapes and is race-safe.
        dir.create_dir_all(parent_path).map_err(io_status)?;
        let parent = dir.open_dir(parent_path).map_err(io_status)?;
        let name = path
            .file_name()
            .ok_or_else(|| Status::invalid_argument("missing file name"))?;
        let temp = format!(".git-output-service-{}.tmp", uuid::Uuid::new_v4());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let file = parent.open_with(&temp, &options).map_err(io_status)?;
        let temp = TempOutput { parent, name: temp };
        async {
            let mut target = file.try_clone().map_err(io_status)?.into_std();
            self.backend
                .download(&digest, build.digest_function, &mut target)
                .await?;
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
        if let Some(projection) = &self.projection {
            return projection
                .stat(&projected_path(build, &path), build.digest_function)
                .await
                .map_err(projection_status)
                .map(|metadata| {
                    metadata.map(|metadata| Stat {
                        r#type: match metadata {
                            TreeStat::Directory => Some(stat::Type::Directory(stat::Directory {})),
                            TreeStat::Symlink { target } => {
                                Some(stat::Type::Symlink(stat::Symlink { target }))
                            }
                            TreeStat::File { artifact } => Some(stat::Type::File(stat::File {
                                locator: artifact.map(|a| {
                                    file_locator(Digest {
                                        hash: a.hash,
                                        size_bytes: a.size as i64,
                                    })
                                }),
                            })),
                            TreeStat::Unknown => None,
                        },
                    })
                });
        }
        let dir = build
            .dir
            .as_ref()
            .ok_or_else(|| Status::internal("missing eager output directory"))?;
        let metadata = match dir.symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            // An unresolvable external symlink or special path is an unknown
            // type, NOT an assertion that the path was absent.
            Err(_) => return Ok(Some(Stat { r#type: None })),
        };
        let kind = if metadata.is_symlink() {
            match dir.read_link_contents(&path) {
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
            let mut file = match dir.open_with(&path, &options) {
                Ok(file) => file,
                // A concurrent type change cannot turn lstat into a following
                // open, or block on a FIFO while retaining the lifecycle lock.
                Err(_) => return Ok(Some(Stat { r#type: None })),
            };
            let current = file.metadata().map_err(io_status)?;
            if !current.is_file() || current.len() > self.backend.max_blob_bytes {
                return Ok(Some(Stat { r#type: None }));
            }
            let deadline = std::time::Instant::now() + self.backend.policy.operation_timeout();
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
        let deadline = tokio::time::Instant::now() + self.policy().operation_timeout();
        timeout(deadline, async {
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
                // The configured root was canonicalized before mounting. Its exact
                // path and ancestors need no filesystem round trip. In particular,
                // realpath/statfs on our own NFS mount can block its Tokio I/O driver.
                let prefix = if self.root_path.starts_with(prefix) {
                    prefix.to_path_buf()
                } else {
                    let prefix = prefix.to_path_buf();
                    tokio::task::spawn_blocking(move || std::fs::canonicalize(prefix))
                        .await
                        .map_err(|_| Status::internal("output prefix resolution task failed"))?
                        .map_err(|_| {
                            Status::invalid_argument(
                                "output_path_prefix must name an existing local directory",
                            )
                        })?
                };
                target
                    .strip_prefix(prefix)
                    .map_err(|_| {
                        Status::invalid_argument(
                            "output_path_prefix must contain configured output root",
                        )
                    })?
                    .to_path_buf()
            };
            let base = self.base(&key)?;
            let mut state = base.lock().await;
            let mut reservation = {
                let mut registry = self.state.lock().unwrap();
                if registry
                    .ids
                    .get(&req.build_id)
                    .is_some_and(|(other, _)| *other != key)
                {
                    return Err(Status::already_exists(
                        "build_id already used by another output base",
                    ));
                }
                registry.next_token = registry.next_token.wrapping_add(1);
                let token = registry.next_token;
                // An existing same-base ID is already reserved by the previous build.
                // Restore it on cancellation rather than erasing that active session.
                let existing = registry.ids.get(&req.build_id).cloned();
                if existing.is_none() {
                    registry
                        .ids
                        .insert(req.build_id.clone(), (key.clone(), token));
                }
                Reservation {
                    registry: self.state.clone(),
                    id: req.build_id.clone(),
                    token,
                    committed: existing.is_some(),
                }
            };
            let dir = if let Some(projection) = &self.projection {
                projection
                    .ensure_dir(&key)
                    .await
                    .map_err(projection_status)?;
                // A remote-only tree must not claim objects that expired from CAS.
                // The projection invalidates pruned bindings before acknowledging.
                projection.refresh(&key).await.map_err(projection_status)?;
                None
            } else {
                self.root.create_dir_all(&key).map_err(io_status)?;
                Some(self.root.open_dir(&key).map_err(io_status)?)
            };
            {
                let mut registry = self.state.lock().unwrap();
                if let Some(old) = state.as_ref() {
                    if old.id != req.build_id {
                        registry.ids.remove(&old.id);
                    }
                }
                registry.active.insert(key.clone(), base.clone());
                *state = Some(Build {
                    id: req.build_id,
                    dir,
                    key,
                    digest_function: args.digest_function,
                    finalized: false,
                });
                reservation.committed = true;
            }
            // No incremental tracking promise. All bytes are already local, and
            // Bazel must revalidate outputs instead of trusting stale metadata.
            Ok(Response::new(proto::StartBuildResponse {
                initial_output_path_contents: None,
                output_path_suffix: suffix.to_string_lossy().into_owned(),
            }))
        })
        .await
    }
    async fn stage_artifacts(
        &self,
        request: Request<proto::StageArtifactsRequest>,
    ) -> Result<Response<proto::StageArtifactsResponse>, Status> {
        let deadline = tokio::time::Instant::now() + self.policy().operation_timeout();
        timeout(deadline, async {
            let req = request.into_inner();
            if req.artifacts.len() > MAX_ITEMS {
                return Err(Status::resource_exhausted("too many artifacts"));
            }
            let base = self.session(&req.build_id)?;
            let state = base.lock().await;
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
        })
        .await
    }
    async fn finalize_artifacts(
        &self,
        request: Request<proto::FinalizeArtifactsRequest>,
    ) -> Result<Response<proto::FinalizeArtifactsResponse>, Status> {
        let deadline = tokio::time::Instant::now() + self.policy().operation_timeout();
        timeout(deadline, async {
            let req = request.into_inner();
            if req.artifacts.len() > MAX_ITEMS {
                return Err(Status::resource_exhausted("too many artifacts"));
            }
            let base = self.session(&req.build_id)?;
            let state = base.lock().await;
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
        })
        .await
    }
    async fn finalize_build(
        &self,
        request: Request<proto::FinalizeBuildRequest>,
    ) -> Result<Response<proto::FinalizeBuildResponse>, Status> {
        let deadline = tokio::time::Instant::now() + self.policy().operation_timeout();
        timeout(deadline, async {
            let req = request.into_inner();
            let base = self.session(&req.build_id)?;
            let mut state = base.lock().await;
            let build = state
                .as_mut()
                .filter(|b| b.id == req.build_id)
                .ok_or_else(|| Status::failed_precondition("unknown build_id"))?;
            // Finalization is idempotent so a lost successful response is retryable.
            build.finalized = true;
            Ok(Response::new(proto::FinalizeBuildResponse {}))
        })
        .await
    }
    async fn clean(
        &self,
        request: Request<proto::CleanRequest>,
    ) -> Result<Response<proto::CleanResponse>, Status> {
        let deadline = tokio::time::Instant::now() + self.policy().operation_timeout();
        timeout(deadline, async {
            let key = Self::key(&request.into_inner().output_base_id)?;
            let base = self.base(&key)?;
            let mut state = base.lock().await;
            // Drop eager directory handles before delete. Queued users retain the same
            // base lock through the weak registry even after the session is removed.
            let old = state.take();
            {
                let mut registry = self.state.lock().unwrap();
                if let Some(old) = &old {
                    registry.ids.remove(&old.id);
                }
                registry.active.remove(&key);
            }
            drop(old);
            if let Some(projection) = &self.projection {
                projection.clean(&key).await.map_err(projection_status)?;
                return Ok(Response::new(proto::CleanResponse {}));
            }
            match self.root.remove_dir_all(&key) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_status(e)),
            }
            Ok(Response::new(proto::CleanResponse {}))
        })
        .await
    }
    async fn batch_stat(
        &self,
        request: Request<proto::BatchStatRequest>,
    ) -> Result<Response<proto::BatchStatResponse>, Status> {
        let deadline = tokio::time::Instant::now() + self.policy().operation_timeout();
        timeout(deadline, async {
            let req = request.into_inner();
            if req.paths.len() > MAX_ITEMS {
                return Err(Status::resource_exhausted("too many paths"));
            }
            let base = self.session(&req.build_id)?;
            let state = base.lock().await;
            let build = Self::build(&state, &req.build_id)?;
            let mut responses = Vec::with_capacity(req.paths.len());
            for path in req.paths {
                responses.push(proto::batch_stat_response::StatResponse {
                    stat: self.stat(build, &path).await?,
                });
            }
            Ok(Response::new(proto::BatchStatResponse { responses }))
        })
        .await
    }
}
