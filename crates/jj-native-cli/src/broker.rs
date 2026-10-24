//! Durable broker selection; jj operation and workspace stores remain local.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::Instant,
};

use jj_lib::{
    backend::{Backend, BackendInitError, BackendLoadError},
    repo::StoreFactories,
};
use jj_native_backend::{Config, NativeBackend};
use serde::{Deserialize, Serialize};

const LOCATOR: &str = "native-broker.toml";
const SPOOL: &str = "native-broker-spool";
type Failure = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Locator {
    version: u32,
    pub(crate) socket: PathBuf,
    pub(crate) store: String,
}
impl Locator {
    pub fn new(socket: PathBuf, store: String) -> Result<Self, io::Error> {
        let locator = Self {
            version: 1,
            socket,
            store,
        };
        locator.validate()?;
        Ok(locator)
    }
    fn validate(&self) -> Result<(), io::Error> {
        if self.version != 1
            || !self.socket.is_absolute()
            || self.socket.as_os_str().len() > 4096
            || self
                .socket
                .components()
                .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
            || self.store.is_empty()
            || self.store.len() > 128
            || !self
                .store
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(io::Error::other("invalid native broker locator"));
        }
        Ok(())
    }
}

fn connect(locator: Locator, spool: PathBuf) -> Result<NativeBackend, Failure> {
    let repo = spool
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| io::Error::other("native spool has no repository"))?;
    let token = crate::commands::token(repo)?;
    // StoreFactories has a synchronous loader, sometimes called from jj's
    // futures executor. A separate owned thread avoids nesting that executor.
    std::thread::spawn(move || {
        futures::executor::block_on(async move {
            let provider = native_broker::BrokerObjectIo::connect(
                locator.socket,
                locator.store,
                spool,
                native_broker::Config::default(),
            )
            .await?;
            let provider = if let Some(token) = token {
                provider.with_command_token(token)
            } else {
                provider
            };
            NativeBackend::from_io(Arc::new(provider), Config::default()).await
        })
    })
    .join()
    .map_err(|_| io::Error::other("native broker loader failed"))?
    .map_err(|error| Box::new(error) as Failure)
}

pub(crate) fn init(path: &Path, locator: Locator) -> Result<Box<dyn Backend>, BackendInitError> {
    let initialize = || -> Result<Box<dyn Backend>, Failure> {
        locator.validate()?;
        crate::commands::token(
            path.parent()
                .ok_or_else(|| io::Error::other("native store has no repository"))?,
        )?;
        let spool = path.join(SPOOL);
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&spool)?;
        let backend = connect(locator.clone(), spool)?;
        let bytes = toml::to_string(&locator)
            .map_err(|_| io::Error::other("cannot encode native broker locator"))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path.join(LOCATOR))?;
        file.write_all(bytes.as_bytes())?;
        file.sync_all()?;
        File::open(path)?.sync_all()?;
        Ok(Box::new(backend))
    };
    initialize().map_err(BackendInitError)
}

pub(crate) fn locator(path: &Path) -> Result<Locator, Failure> {
    let locator = path.join(LOCATOR);
    let metadata = fs::symlink_metadata(&locator)?;
    if !metadata.is_file() || metadata.len() > 16 * 1024 {
        return Err(io::Error::other("invalid native broker locator file").into());
    }
    let mut bytes = Vec::new();
    File::open(locator)?
        .take(16 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 16 * 1024 {
        return Err(io::Error::other("native broker locator exceeds byte limit").into());
    }
    let locator: Locator = toml::from_str(
        std::str::from_utf8(&bytes)
            .map_err(|_| io::Error::other("invalid native broker locator encoding"))?,
    )
    .map_err(|_| io::Error::other("invalid native broker locator configuration"))?;
    locator.validate()?;
    Ok(locator)
}

fn load(path: &Path) -> Result<Box<dyn Backend>, BackendLoadError> {
    let locator = path.join(LOCATOR);
    let metadata = match fs::symlink_metadata(&locator) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return NativeBackend::load(path, Config::default())
                .map(|backend| Box::new(backend) as Box<dyn Backend>);
        }
        Err(error) => return Err(BackendLoadError(Box::new(error))),
    };
    let restore = || -> Result<Box<dyn Backend>, Failure> {
        if !metadata.is_file() || metadata.len() > 16 * 1024 {
            return Err(io::Error::other("invalid native broker locator file").into());
        }
        let mut bytes = Vec::new();
        File::open(locator)?
            .take(16 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 16 * 1024 {
            return Err(io::Error::other("native broker locator exceeds byte limit").into());
        }
        let locator: Locator = toml::from_str(
            std::str::from_utf8(&bytes)
                .map_err(|_| io::Error::other("invalid native broker locator encoding"))?,
        )
        .map_err(|_| io::Error::other("invalid native broker locator configuration"))?;
        locator.validate()?;
        Ok(Box::new(connect(locator, path.join(SPOOL))?))
    };
    restore().map_err(BackendLoadError)
}

// Handles do not retain repository/index cores. A bounded process-exit drain
// completes accepted cleanup after all command-owned repositories are dropped.
static CLEANUP: OnceLock<Mutex<Vec<jj_native_index::CleanupHandle>>> = OnceLock::new();
fn cleanup_capacity() -> Result<(), Failure> {
    if CLEANUP
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| io::Error::other("native cleanup registry unavailable"))?
        .len()
        >= 64
    {
        return Err(io::Error::other("native index loader capacity exceeded").into());
    }
    Ok(())
}
fn retain_cleanup(index: &jj_native_index::NativeIndexStore) -> Result<(), Failure> {
    let mut handles = CLEANUP
        .get_or_init(Default::default)
        .lock()
        .map_err(|_| io::Error::other("native cleanup registry unavailable"))?;
    if handles.len() >= 64 {
        return Err(io::Error::other("native index loader capacity exceeded").into());
    }
    handles.push(index.cleanup_handle());
    Ok(())
}
pub(crate) fn drain_cleanup(deadline: Instant) {
    let handles = CLEANUP
        .get_or_init(Default::default)
        .lock()
        .map(|mut h| std::mem::take(&mut *h));
    let failure = match handles {
        Ok(handles) => {
            let mut first = None;
            for handle in handles {
                let left = deadline.saturating_duration_since(Instant::now());
                let result = if left.is_zero() {
                    Err(jj_native_index::Error::Query(tonic::Code::DeadlineExceeded))
                } else {
                    handle.wait(left)
                };
                if let Err(error) = result {
                    let code = match error {
                        jj_native_index::Error::Query(code) => code,
                        _ => tonic::Code::Internal,
                    };
                    first.get_or_insert(handle.first_failure().unwrap_or(("driver", "wait", code)));
                }
            }
            first
        }
        Err(_) => Some(("driver", "registry", tonic::Code::Internal)),
    };
    if let Some((operation, origin, code)) = failure {
        // Codes are a fixed public enum. Never print transport messages, opaque
        // query handles, provider identities or local connection paths here.
        eprintln!(
            "Warning: native query cleanup remains pending ({operation}/{origin}: {code:?}); bounded server expiry will reclaim it."
        );
    }
}

fn query_client(locator: &Locator) -> Result<native_broker::BrokerQueryClient, Failure> {
    let locator = locator.clone();
    std::thread::spawn(move || {
        futures::executor::block_on(native_broker::BrokerQueryClient::connect(
            locator.socket,
            locator.store,
            Default::default(),
        ))
    })
    .join()
    .map_err(|_| io::Error::other("native query loader failed"))?
    .map_err(|e| Box::new(e) as Failure)
}
fn description(
    query: &native_broker::BrokerQueryClient,
) -> Result<native_broker::ProviderDescription, Failure> {
    let query = query.clone();
    std::thread::spawn(move || futures::executor::block_on(query.describe_provider()))
        .join()
        .map_err(|_| io::Error::other("native provider handshake failed"))?
        .map_err(|e| Box::new(e) as Failure)
}
/// Creation policy is live, not persisted trust. Existing repositories instead
/// select the factory named by their recorded index/type.
pub(crate) fn creation_policy(
    locator: &Locator,
) -> Result<native_broker::ProviderIndexMode, Failure> {
    let description = description(&query_client(locator)?)?;
    if description.index_mode == native_broker::ProviderIndexMode::Federated {
        #[cfg(windows)]
        return Err(
            io::Error::other("federated native index writers are unsupported on Windows").into(),
        );
        if !description.query_capable {
            return Err(io::Error::other(
                "federated native index requires a query-capable provider",
            )
            .into());
        }
    }
    Ok(description.index_mode)
}
fn open_index(
    path: &Path,
    initial: Option<(&jj_lib::backend::CommitId, &jj_lib::op_store::OperationId)>,
) -> Result<Box<dyn jj_lib::index::IndexStore>, Failure> {
    use jj_lib::object_id::ObjectId as _;
    cleanup_capacity()?;
    #[cfg(windows)]
    return Err(
        io::Error::other("federated native index writers are unsupported on Windows").into(),
    );
    #[cfg(not(windows))]
    {
        let store_path = path
            .parent()
            .ok_or_else(|| io::Error::other("native index has no repository"))?
            .join("store");
        let locator = locator(&store_path)?;
        let token = crate::commands::token(
            path.parent()
                .ok_or_else(|| io::Error::other("native index has no repository"))?,
        )?;
        let query = query_client(&locator)?;
        let query = if let Some(token) = token {
            query.with_command_token(token)
        } else {
            query
        };
        let live = description(&query)?;
        if !live.query_capable {
            return Err(io::Error::other("native index provider has no query capability").into());
        }
        let config = jj_native_index::Config {
            endpoint: jj_native_index::Endpoint::UnixSocket(locator.socket.clone()),
            store_alias: locator.store.clone(),
            provider_fingerprint: live.identity,
        };
        let objects = std::thread::spawn(move || {
            futures::executor::block_on(native_broker::BrokerObjectIo::connect(
                locator.socket,
                locator.store,
                store_path.join(SPOOL),
                Default::default(),
            ))
        })
        .join()
        .map_err(|_| io::Error::other("native index object loader failed"))??;
        let objects = if let Some(token) = token {
            objects.with_command_token(token)
        } else {
            objects
        };
        let objects: Arc<dyn jj_native_backend::NativeObjectIo> = Arc::new(objects);
        let index = match initial {
            Some((root, operation)) => {
                let root = tinytree_model::CommitId::from_slice(root.as_bytes())?;
                let operation: &[u8; 64] = operation
                    .as_bytes()
                    .try_into()
                    .map_err(|_| io::Error::other("invalid root operation width"))?;
                jj_native_index::NativeIndexStore::init_fresh_for_root_operation(
                    path,
                    config,
                    Default::default(),
                    Arc::new(query),
                    objects,
                    vec![root],
                    operation,
                )?
            }
            None => jj_native_index::NativeIndexStore::open(
                path,
                &config,
                Default::default(),
                Arc::new(query),
                objects,
            )?,
        };
        retain_cleanup(&index)?;
        Ok(Box::new(index))
    }
}
pub(crate) fn init_index(
    path: &Path,
    mode: native_broker::ProviderIndexMode,
    settings: &jj_lib::settings::UserSettings,
    root: &jj_lib::backend::CommitId,
    operation: &jj_lib::op_store::OperationId,
) -> Result<Box<dyn jj_lib::index::IndexStore>, BackendInitError> {
    crate::commands::token(path.parent().ok_or_else(|| {
        BackendInitError(Box::new(io::Error::other("native index has no repository")))
    })?)
    .map_err(|error| BackendInitError(error.into()))?;
    match mode {
        native_broker::ProviderIndexMode::Legacy => {
            jj_lib::repo::ReadonlyRepo::default_index_store_initializer()(settings, path)
        }
        native_broker::ProviderIndexMode::Federated => {
            open_index(path, Some((root, operation))).map_err(BackendInitError)
        }
    }
}

pub(crate) fn register(factories: &mut StoreFactories) {
    factories.add_backend(NativeBackend::NAME, Box::new(|_, path| load(path)));
    factories.add_index_store(
        jj_native_index::NAME,
        Box::new(|_, path| open_index(path, None).map_err(BackendLoadError)),
    );
}
