//! Owner-private snapshot client. No REAPI upstream, proxy, raw origin, Git,
//! capture, BES or BOS is constructed. The descriptor contains references only.
use crate::{
    config::{Config, NativeIndexMode, NativeRepository},
    control::{Request, Response},
};
use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use fs2::FileExt;
use jj_native_backend::NativeObjectIo;
use local_cas::{Artifact, BlobSource, CacheConfig, LocalCas};
use native_broker::{ForgeProjection, ForgeRemoteConfig};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tinytree_forge_core::session::{ForgeSession, ForgeSessionIdentity};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
};
const CAP: usize = 65536;
const ALIAS: &str = "forge";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionPin {
    pub view_version: [u8; 32],
    pub revision: [u8; 32],
    pub policy_epoch: u64,
    pub root_prefix: String,
    pub projected_commit: [u8; 32],
    pub projected_tree: [u8; 32],
}
impl From<&ForgeProjection> for ProjectionPin {
    fn from(p: &ForgeProjection) -> Self {
        Self {
            view_version: p.view_version,
            revision: p.revision,
            policy_epoch: p.policy_epoch,
            root_prefix: p.root_prefix.clone(),
            projected_commit: p.projected_commit,
            projected_tree: p.projected_tree,
        }
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientDescriptor {
    schema_version: u32,
    state_dir: PathBuf,
    destination: PathBuf,
    registry_path: PathBuf,
    source: ForgeRemoteConfig,
    session: ForgeSession,
    pin: ProjectionPin,
    trust_digest: Option<[u8; 32]>,
    binding_digest: [u8; 32],
}
fn absolute(p: &Path) -> Result<()> {
    ensure!(
        p.is_absolute()
            && !p
                .components()
                .any(|c| matches!(c, Component::ParentDir | Component::CurDir)),
        "Forge client path must be normalized and absolute"
    );
    Ok(())
}
pub fn validate_token_reference(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 128
            && name.bytes().enumerate().all(|(i, b)| b == b'_'
                || b.is_ascii_alphabetic()
                || (i > 0 && b.is_ascii_digit())),
        "invalid Forge credential reference"
    );
    ensure!(
        !matches!(
            name,
            "HOME"
                | "PATH"
                | "TMPDIR"
                | "XDG_RUNTIME_DIR"
                | "XDG_CONFIG_HOME"
                | "XDG_STATE_HOME"
                | "XDG_CACHE_HOME"
                | "SHELL"
                | "BASH_ENV"
                | "ENV"
                | "RUST_LOG"
                | "RUST_BACKTRACE"
                | "USER"
                | "LOGNAME"
                | "LANG"
                | "LC_ALL"
                | "SSH_AUTH_SOCK"
                | "DBUS_SESSION_BUS_ADDRESS"
                | "LISTEN_FDS"
                | "LISTEN_PID"
                | "LISTEN_FDNAMES"
        ) && !["LD_", "DYLD_", "TINYTREE_", "CODEX_"]
            .iter()
            .any(|p| name.starts_with(p)),
        "reserved Forge credential reference"
    );
    Ok(())
}
fn trust(source: &ForgeRemoteConfig) -> Result<Option<[u8; 32]>> {
    source
        .client
        .tls
        .as_ref()
        .map(|tls| {
            use std::io::Read;
            let mut bytes = Vec::new();
            File::open(&tls.ca_certificate)
                .context("cannot read Forge trust roots")?
                .take(1048577)
                .read_to_end(&mut bytes)?;
            ensure!(
                !bytes.is_empty() && bytes.len() <= 1048576,
                "invalid Forge trust root size"
            );
            Ok(*blake3::hash(&bytes).as_bytes())
        })
        .transpose()
}
impl ClientDescriptor {
    pub fn new(
        base: &Path,
        destination: &Path,
        source: ForgeRemoteConfig,
        session: ForgeSession,
        pin: ProjectionPin,
    ) -> Result<Self> {
        absolute(base)?;
        absolute(destination)?;
        let registry_path = native_workspace_admin::Registry::default_path()?;
        absolute(&registry_path)?;
        let trust_digest = trust(&source)?;
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-forge-client-location-v1\0");
        hash.update(&serde_json::to_vec(&(
            &source,
            &session,
            &pin,
            destination,
            &registry_path,
            trust_digest,
        ))?);
        let binding_digest = *hash.finalize().as_bytes();
        let state_dir = base.join(&hash.finalize().to_hex().as_str()[..24]);
        let result = Self {
            schema_version: 1,
            state_dir,
            destination: destination.into(),
            registry_path,
            source,
            session,
            pin,
            trust_digest,
            binding_digest,
        };
        result.validate()?;
        Ok(result)
    }
    fn validate_binding(&self) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unsupported Forge client descriptor"
        );
        absolute(&self.state_dir)?;
        absolute(&self.destination)?;
        absolute(&self.registry_path)?;
        self.source
            .validate()
            .map_err(|_| anyhow::anyhow!("invalid Forge client source"))?;
        self.session
            .validate()
            .map_err(|_| anyhow::anyhow!("invalid Forge client session"))?;
        ensure!(
            self.source.account_id == self.session.account_id
                && self.source.client.repository_id == format!("forge-{}", self.source.account_id)
                && self.source.view_version == Some(self.pin.view_version)
                && self.pin.policy_epoch > 0
                && self.pin.view_version != [0; 32]
                && self.pin.revision != [0; 32]
                && self.pin.projected_commit != [0; 32]
                && self.pin.projected_tree != [0; 32],
            "Forge client projection binding differs"
        );
        tinytree_forge_core::accounts::validate_path(&self.pin.root_prefix, true)
            .map_err(|_| anyhow::anyhow!("invalid Forge root prefix"))?;
        if let Some(name) = &self.source.client.token_env {
            validate_token_reference(name)?;
        }
        ensure!(
            matches!(
                (&self.session.identity, &self.source.client.token_env),
                (ForgeSessionIdentity::Anonymous {}, None)
                    | (ForgeSessionIdentity::Authenticated { .. }, Some(_))
            ),
            "Forge client credential/session identity differs"
        );
        let mut hash = blake3::Hasher::new();
        hash.update(b"tinytree-forge-client-location-v1\0");
        hash.update(&serde_json::to_vec(&(
            &self.source,
            &self.session,
            &self.pin,
            &self.destination,
            &self.registry_path,
            self.trust_digest,
        ))?);
        ensure!(
            *hash.finalize().as_bytes() == self.binding_digest
                && self.state_dir.file_name().and_then(|v| v.to_str())
                    == Some(&hash.finalize().to_hex().as_str()[..24]),
            "Forge client descriptor identity changed"
        );
        use std::os::unix::ffi::OsStrExt;
        ensure!(
            self.broker_socket().as_os_str().as_bytes().len() < 108
                && self
                    .state_dir
                    .join("control.sock")
                    .as_os_str()
                    .as_bytes()
                    .len()
                    < 108,
            "Forge client Unix socket path is too long"
        );
        ensure!(
            serde_json::to_vec(self)?.len() <= CAP,
            "Forge client descriptor exceeds limit"
        );
        Ok(())
    }
    pub fn validate(&self) -> Result<()> {
        self.validate_binding()?;
        ensure!(
            trust(&self.source)? == self.trust_digest,
            "Forge client trust roots changed"
        );
        Ok(())
    }
    pub fn digest(&self) -> Result<[u8; 32]> {
        self.validate_binding()?;
        let mut h = blake3::Hasher::new();
        h.update(b"tinytree-forge-client-descriptor-v1\0");
        h.update(&serde_json::to_vec(self)?);
        Ok(*h.finalize().as_bytes())
    }
    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }
    pub fn destination(&self) -> &Path {
        &self.destination
    }
    pub fn registry_path(&self) -> &Path {
        &self.registry_path
    }
    pub fn source(&self) -> &ForgeRemoteConfig {
        &self.source
    }
    pub fn session(&self) -> &ForgeSession {
        &self.session
    }
    pub fn pin(&self) -> &ProjectionPin {
        &self.pin
    }
    pub fn store_alias(&self) -> &str {
        ALIAS
    }
    pub fn broker_socket(&self) -> PathBuf {
        self.state_dir.join("rbe.sock")
    }
    pub fn descriptor_path(&self) -> PathBuf {
        self.state_dir.join("descriptor.json")
    }
    pub fn save(&self) -> Result<PathBuf> {
        self.validate()?;
        let directory = PrivateDir::create_all_durable(&self.state_dir)?;
        let lock = directory.open_lock("descriptor.lock")?;
        lock.lock_exclusive()?;
        match directory.read("descriptor.json", CAP) {
            Ok(bytes) => {
                let old: Self = serde_json::from_slice(&bytes)
                    .context("invalid existing Forge client descriptor")?;
                ensure!(
                    old.digest()? == self.digest()?,
                    "existing Forge client descriptor differs; preserve state"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                directory.replace("descriptor.json", &serde_json::to_vec(self)?)?
            }
            Err(e) => return Err(e.into()),
        }
        Ok(self.descriptor_path())
    }
    pub fn load(path: &Path) -> Result<Self> {
        absolute(path)?;
        ensure!(
            path.file_name().and_then(|s| s.to_str()) == Some("descriptor.json"),
            "invalid Forge descriptor filename"
        );
        let parent = path
            .parent()
            .context("missing Forge descriptor directory")?;
        let dir = PrivateDir::open_existing(parent)?;
        let result: Self = serde_json::from_slice(&dir.read("descriptor.json", CAP)?)
            .context("invalid Forge client descriptor")?;
        result.validate_binding()?;
        ensure!(
            result.state_dir == parent,
            "Forge descriptor location differs"
        );
        Ok(result)
    }
    pub fn config(&self) -> Config {
        let mut c = Config::default();
        c.state_dir = self.state_dir.clone();
        c.cache.api_key_env.clear();
        c.bes.enabled = false;
        c.bos.enabled = false;
        c.native.workspaces.index_mode = NativeIndexMode::Legacy;
        c.native.workspaces.registry_path = Some(self.registry_path.clone());
        c.native.workspaces.max_workspaces = 1;
        c.native.workspaces.capture.policy = crate::config::NativeCapturePolicy::Disabled;
        c.native.workspaces.upload.policy = crate::config::NativeCaptureUploadPolicy::Disabled;
        let mut repository = NativeRepository::default();
        repository.forge = Some(self.source.clone());
        c.native.repositories.insert(ALIAS.into(), repository);
        c
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ready {
    pub schema_version: u32,
    pub descriptor_digest: [u8; 32],
    pub store: String,
    pub broker_socket: PathBuf,
    pub provider_identity: [u8; 32],
    pub pin: ProjectionPin,
}
struct LocalOnly;
#[async_trait]
impl BlobSource for LocalOnly {
    async fn fetch(&self, _: &Artifact, _: &mut File) -> Result<()> {
        anyhow::bail!("Forge client has no CAS upstream")
    }
    async fn missing(&self, values: &[Artifact]) -> Result<Vec<Artifact>> {
        Ok(values.to_vec())
    }
}
async fn request(path: PathBuf, request: Request, timeout: Duration) -> Result<serde_json::Value> {
    tokio::time::timeout(timeout, async move {
        let mut stream = UnixStream::connect(path).await?;
        let mut bytes = serde_json::to_vec(&request)?;
        bytes.push(b'\n');
        stream.write_all(&bytes).await?;
        let mut reader = BufReader::new(stream);
        let mut bytes = Vec::new();
        reader.take(1048577).read_until(b'\n', &mut bytes).await?;
        ensure!(
            bytes.len() <= 1048576 && bytes.last() == Some(&b'\n'),
            "Forge control response limit"
        );
        let r: Response = serde_json::from_slice(&bytes)?;
        ensure!(
            r.ok,
            "{}",
            r.error
                .unwrap_or_else(|| "Forge client control failed".into())
        );
        Ok(r.result)
    })
    .await
    .context("Forge client control deadline exceeded")?
}
fn blocking_request(d: &ClientDescriptor, r: Request, t: Duration) -> Result<serde_json::Value> {
    d.validate_binding()?;
    let saved = ClientDescriptor::load(&d.descriptor_path())?;
    ensure!(
        saved.digest()? == d.digest()?,
        "Forge client saved descriptor differs"
    );
    let socket = d.state_dir.join("control.sock");
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?
            .block_on(request(socket, r, t))
    })
    .join()
    .map_err(|_| anyhow::anyhow!("Forge control worker failed"))?
}
pub fn ready(d: &ClientDescriptor) -> Result<Ready> {
    let r: Ready = serde_json::from_value(blocking_request(
        d,
        Request::ForgeClientReady {
            descriptor_digest: d.digest()?,
        },
        Duration::from_secs(2),
    )?)?;
    ensure!(
        r.schema_version == 1
            && r.descriptor_digest == d.digest()?
            && r.store == ALIAS
            && r.broker_socket == d.broker_socket()
            && r.pin == d.pin
            && r.provider_identity != [0; 32],
        "Forge client readiness differs"
    );
    Ok(r)
}
pub fn stop(d: &ClientDescriptor) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    blocking_request(
        d,
        Request::ForgeClientStop {
            descriptor_digest: d.digest()?,
        },
        Duration::from_secs(180),
    )?;
    let dir = PrivateDir::open_existing(d.state_dir())?;
    let lock = dir.open_lock("daemon.lock")?;
    loop {
        if lock.try_lock_exclusive().is_ok() {
            ensure!(
                !d.state_dir.join("control.sock").exists() && !d.broker_socket().exists(),
                "Forge client stopped with endpoints still present"
            );
            return Ok(());
        }
        ensure!(
            std::time::Instant::now() < deadline,
            "Forge client drain is still owned"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}
async fn clean_socket(path: &Path) -> Result<()> {
    use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
    match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
        Ok(m) => ensure!(
            m.file_type().is_socket()
                && m.uid() == unsafe { libc::geteuid() }
                && m.permissions().mode() & 0o077 == 0,
            "foreign Forge client socket; preserve state"
        ),
    }
    match tokio::time::timeout(Duration::from_secs(1), UnixStream::connect(path)).await {
        Ok(Err(e))
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
            ) => {}
        _ => anyhow::bail!("live or uncertain Forge client socket; preserve state"),
    }
    std::fs::remove_file(path)?;
    File::open(path.parent().context("socket parent")?)?.sync_all()?;
    Ok(())
}
async fn verify_session(d: &ClientDescriptor, ca: Option<Arc<[u8]>>) -> Result<()> {
    let client = match ca {
        Some(ca) => {
            tinytree_client::NativeClient::connect_with_ca_certificate(d.source.client.clone(), ca)
                .await
        }
        None => tinytree_client::NativeClient::connect(d.source.client.clone()).await,
    }
    .map_err(|_| anyhow::anyhow!("Forge client transport unavailable"))?;
    let session = client
        .forge_get_session(&d.source.account_id, &d.source.view)
        .await
        .map_err(|_| anyhow::anyhow!("Forge client session unavailable"))?;
    ensure!(
        session == d.session,
        "Forge client session changed; preserve workspace"
    );
    Ok(())
}
async fn verify_roots(io: &Arc<dyn NativeObjectIo>, pin: &ProjectionPin) -> Result<()> {
    use tinytree_model as m;
    let metadata = io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(pin.projected_commit),
        })
        .await
        .map_err(|_| anyhow::anyhow!("Forge projected COMMIT unavailable"))?;
    ensure!(
        metadata.descriptor().key.id.0 == pin.projected_commit,
        "Forge COMMIT digest differs"
    );
    let m::Object::Commit(commit) = metadata.into_object() else {
        anyhow::bail!("invalid Forge COMMIT kind")
    };
    ensure!(
        commit.parents == [m::CommitId::ROOT]
            && commit.predecessors.is_empty()
            && commit.secure_signature.is_none()
            && commit.root_tree.terms == [m::TreeId(pin.projected_tree)],
        "Forge baseline COMMIT is not a sanitized single-root snapshot"
    );
    let metadata = io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Tree,
            id: m::ObjectId(pin.projected_tree),
        })
        .await
        .map_err(|_| anyhow::anyhow!("Forge projected TREE unavailable"))?;
    ensure!(
        metadata.descriptor().key.id.0 == pin.projected_tree
            && matches!(metadata.into_object(), m::Object::Tree(_)),
        "Forge TREE digest or encoding differs"
    );
    Ok(())
}
/// Serve a descriptor already durably installed by the owner. This entry never
/// loads an operator TOML or creates a remote-cache/proxy source.
pub async fn serve_forge_client(path: &Path) -> Result<()> {
    let d = ClientDescriptor::load(path)?;
    d.validate()?;
    let directory = PrivateDir::open_existing(&d.state_dir)?;
    let owner = directory.open_lock("daemon.lock")?;
    owner
        .try_lock_exclusive()
        .context("Forge client state is already owned")?;
    clean_socket(&d.state_dir.join("control.sock")).await?;
    clean_socket(&d.broker_socket()).await?;
    let config = d.config();
    config.validate_forge_client()?;
    let credentials = crate::identity::Credentials::load(&config)?;
    let ca = credentials.native_ca.get(ALIAS).cloned();
    ensure!(
        ca.as_ref().map(|v| *blake3::hash(v).as_bytes()) == d.trust_digest,
        "Forge trust changed during startup"
    );
    verify_session(&d, ca.clone()).await?;
    let cache = LocalCas::open(
        d.state_dir.join("cas"),
        Arc::new(LocalOnly),
        CacheConfig {
            max_bytes: 256 * 1024 * 1024,
            max_inflight_bytes: 128 * 1024 * 1024,
            ..Default::default()
        },
    )?;
    let commands = Arc::new(
        crate::native_commands::NativeCommandHost::open_with_capture(
            &d.state_dir.join("native-commands"),
            config.native.workspaces.capture.clone(),
        )
        .await?,
    );
    let pending = commands
        .pending_provider_registrations(32768, 64 * 1024 * 1024)
        .await?;
    let cfg = config.clone();
    let cache_copy = cache.clone();
    let cmd = commands.clone();
    let native_ca = credentials.native_ca.clone();
    let native_credentials = credentials.native.clone();
    let expected = d.pin.clone();
    let (broker, repositories) = tokio::task::spawn_blocking(move || {
        crate::daemon::open_native_host(
            &cfg,
            &cache_copy,
            &native_ca,
            &native_credentials,
            Some(cmd),
            pending.as_slice(),
            Some(&expected),
        )
    })
    .await
    .context("Forge provider startup failed")??;
    let broker = broker.context("Forge broker missing")?;
    let repository = repositories.get(ALIAS).context("Forge provider missing")?;
    verify_roots(&repository.io, &d.pin).await?;
    verify_session(&d, ca).await?;
    let description = broker
        .describe_provider(ALIAS)
        .map_err(|_| anyhow::anyhow!("Forge provider identity unavailable"))?;
    let policy = description
        .creation_policy
        .context("Forge creation policy unavailable")?;
    ensure!(
        description.index_mode == native_broker::ProviderIndexMode::Legacy
            && !description.query_capable
            && !policy.capture_enabled
            && !policy.upload_enabled
            && !policy.auto_capture
            && !policy.auto_upload,
        "Forge provider must remain snapshot-only"
    );
    let ready = Ready {
        schema_version: 1,
        descriptor_digest: d.digest()?,
        store: ALIAS.into(),
        broker_socket: d.broker_socket(),
        provider_identity: description.identity,
        pin: d.pin.clone(),
    };
    let mut manager =
        crate::native_workspaces::Manager::open(config.clone(), cache.clone(), repositories)?
            .with_commands(commands)?;
    manager.require_forge_client_scope(&d.destination, ALIAS, d.pin.projected_commit)?;
    // The owner lock proves no cooperating old process owns these endpoints;
    // an unrelated live socket still refuses replacement rather than being killed.
    let listener = UnixListener::bind(d.state_dir.join("control.sock"))?;
    let rpc_listener = UnixListener::bind(d.broker_socket())?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        d.state_dir.join("control.sock"),
        std::fs::Permissions::from_mode(0o600),
    )?;
    std::fs::set_permissions(d.broker_socket(), std::fs::Permissions::from_mode(0o600))?;
    File::open(&d.state_dir)?.sync_all()?;
    let (shutdown, stopping) = tokio::sync::oneshot::channel::<()>();
    let rpc = manager.rpc();
    let mut server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(broker.rpc_service())
            .add_service(broker.control_rpc_service())
            // Description and Legacy creation preflight use this facade. No
            // query provider is registered, so general queries remain disabled.
            .add_service(broker.query_rpc_service())
            .add_service(rpc)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::UnixListenerStream::new(rpc_listener),
                async {
                    let _ = stopping.await;
                },
            )
            .await
    });
    manager.restore_ready().await;
    let accept = reapi::server_io::AcceptState::default();
    let loop_result:Result<()>=async {
 loop {
  let stream=tokio::select! {
   v=reapi::server_io::accept_unix(&listener,&accept)=>v.context("Forge client control accept failed")?,
   _=tokio::signal::ctrl_c()=>{if manager.active(){continue;}break;},
   v=&mut server=>{v.context("Forge broker worker failed")??;anyhow::bail!("Forge broker stopped unexpectedly");}
  };
  let (read,mut write)=stream.into_split();let mut reader=BufReader::new(read);let mut bytes=Vec::new();
  let read=tokio::time::timeout(Duration::from_secs(5),(&mut reader).take(CAP as u64+1).read_until(b'\n',&mut bytes)).await;
  let request=match read {Ok(Ok(_))if bytes.len()<=CAP=>serde_json::from_slice::<Request>(&bytes).ok(),_=>None};
  let mut end=false;
  let result:Result<serde_json::Value>=async {
   match request {
    Some(Request::ForgeClientReady{descriptor_digest})=>{ensure!(descriptor_digest==ready.descriptor_digest,"Forge client descriptor differs");Ok(serde_json::to_value(&ready)?)},
    Some(Request::ForgeClientStop{descriptor_digest})=>{ensure!(descriptor_digest==ready.descriptor_digest,"Forge client descriptor differs");manager.stop_all().await?;manager.drain().await?;end=true;Ok(serde_json::json!(true))},
    Some(Request::Status)=>Ok(serde_json::json!({"mode":"forge_client","descriptor_digest":ready.descriptor_digest,"workspaces":manager.status()})),
    Some(Request::NativeMount{root,store,commit,workspace_id})=>{ensure!(root==d.destination&&store==ALIAS&&commit==hex(&d.pin.projected_commit),"Forge workspace selection differs");Ok(serde_json::to_value(manager.create(root,store,commit,workspace_id).await?)?)},
    Some(Request::NativeMountPlanned{root,store,commit,workspace_id,creation_plan})=>{ensure!(root==d.destination&&store==ALIAS&&commit==hex(&d.pin.projected_commit),"Forge workspace selection differs");Ok(serde_json::to_value(manager.create_planned(root,store,commit,workspace_id,creation_plan).await?)?)},
    Some(Request::NativeResume{root})=>{ensure!(root==d.destination,"Forge resume destination differs");Ok(serde_json::to_value(manager.resume_root(root).await?)?)},
    Some(Request::NativeMounted{workspace_id})=>Ok(serde_json::json!(manager.mounted(&workspace_id)?)),
    Some(Request::NativeUnmount{workspace_id})=>{manager.stop(&workspace_id).await?;Ok(serde_json::json!(true))},
        _=>anyhow::bail!("unsupported Forge client control operation"),
   }
  }.await;
  let response=match result {Ok(result)=>Response{ok:true,result,error:None},Err(_)=>Response{ok:false,result:serde_json::Value::Null,error:Some("forge_client_operation_failed".into())}};
  let mut bytes=serde_json::to_vec(&response)?;bytes.push(b'\n');let _=tokio::time::timeout(Duration::from_secs(5),write.write_all(&bytes)).await;
  if end{break;}
 }Ok(())
 }.await;
    // Retain all mount/controller owners through cleanup even if the initiating
    // client disconnects. A failed drain must not drop a live provider.
    while manager.stop_all().await.is_err() || manager.drain().await.is_err() {
        // Keep the owner control endpoint useful during uncertain cleanup. Do not
        // drop a live provider, reset a namespace, or abandon a retained command.
        let stream = tokio::select! {v=reapi::server_io::accept_unix(&listener,&accept)=>match v{Ok(s)=>s,Err(_)=>{tokio::time::sleep(Duration::from_secs(1)).await;continue;}},_=tokio::time::sleep(Duration::from_secs(1))=>continue};
        let (read, mut write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let mut data = Vec::new();
        let read = tokio::time::timeout(
            Duration::from_secs(5),
            (&mut reader)
                .take(CAP as u64 + 1)
                .read_until(b'\n', &mut data),
        )
        .await;
        let request = match read {
            Ok(Ok(_)) if data.len() <= CAP => serde_json::from_slice::<Request>(&data).ok(),
            _ => None,
        };
        let result = match request {
            Some(Request::Status) => Ok(
                serde_json::json!({"mode":"forge_client_draining","workspaces":manager.status()}),
            ),
            Some(Request::NativeUnmount { workspace_id }) => manager
                .stop(&workspace_id)
                .await
                .map(|()| serde_json::json!(true)),
            Some(Request::ForgeClientStop { descriptor_digest })
                if descriptor_digest == ready.descriptor_digest =>
            {
                manager.stop_all().await.map(|()| serde_json::json!(true))
            }
            _ => Err(anyhow::anyhow!("Forge client is draining")),
        };
        let response = match result {
            Ok(result) => Response {
                ok: true,
                result,
                error: None,
            },
            Err(_) => Response {
                ok: false,
                result: serde_json::Value::Null,
                error: Some("forge_client_drain_pending".into()),
            },
        };
        if let Ok(mut data) = serde_json::to_vec(&response) {
            data.push(b'\n');
            let _ = tokio::time::timeout(Duration::from_secs(5), write.write_all(&data)).await;
        }
    }
    let _ = shutdown.send(());
    if !server.is_finished() {
        server.await.context("Forge broker drain failed")??;
    }
    cache.wait_for_idle().await?;
    drop(listener);
    for path in [d.state_dir.join("control.sock"), d.broker_socket()] {
        let _ = std::fs::remove_file(path);
    }
    File::open(&d.state_dir)?.sync_all()?;
    drop(owner);
    loop_result
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    fn descriptor(base: &Path, destination: &Path) -> Result<ClientDescriptor> {
        let mut client = tinytree_client::Config::default();
        client.endpoint = "https://example.invalid".into();
        client.repository_id = "forge-example".into();
        client.transport = tinytree_client::Transport::ForgeHttpsV1;
        ClientDescriptor::new(
            base,
            destination,
            ForgeRemoteConfig {
                account_id: "example".into(),
                view: "public".into(),
                view_version: Some([1; 32]),
                client,
            },
            ForgeSession {
                schema_version: 1,
                account_id: "example".into(),
                identity: ForgeSessionIdentity::Anonymous {},
            },
            ProjectionPin {
                view_version: [1; 32],
                revision: [2; 32],
                policy_epoch: 1,
                root_prefix: String::new(),
                projected_commit: [3; 32],
                projected_tree: [4; 32],
            },
        )
    }
    #[test]
    fn descriptor_replay_preserves_destination_and_refuses_foreign_saved_binding() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let d = descriptor(temp.path(), &temp.path().join("workspace"))?;
        let path = d.save()?;
        ensure!(ClientDescriptor::load(&path)?.digest()? == d.digest()?);
        ensure!(d.save()? == path);
        let other = descriptor(temp.path(), &temp.path().join("other"))?;
        ensure!(other.state_dir() != d.state_dir());
        let directory = PrivateDir::open_existing(d.state_dir())?;
        directory.replace("descriptor.json", b"{}")?;
        ensure!(d.save().is_err());
        Ok(())
    }
    #[test]
    fn derived_client_has_no_upstream_and_rejects_reserved_credential_references() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let d = descriptor(temp.path(), &temp.path().join("workspace"))?;
        d.config().validate_forge_client()?;
        ensure!(d.config().cache.upstream.is_empty());
        for name in [
            "HOME",
            "PATH",
            "LD_PRELOAD",
            "TINYTREE_REGISTRY",
            "XDG_RUNTIME_DIR",
        ] {
            ensure!(validate_token_reference(name).is_err());
        }
        validate_token_reference("FORGE_TOKEN")?;
        Ok(())
    }
}
