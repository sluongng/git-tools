//! Durable broker selection; jj operation and workspace stores remain local.
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
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
    socket: PathBuf,
    store: String,
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

pub(crate) fn register(factories: &mut StoreFactories) {
    factories.add_backend(NativeBackend::NAME, Box::new(|_, path| load(path)));
}
