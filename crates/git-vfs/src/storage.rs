//! Durable Fjall storage shared by the blob-size cache and writable overlay.
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

static FJALL_DATABASES: LazyLock<Mutex<HashMap<PathBuf, Weak<fjall::SingleWriterTxDatabase>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Fjall permits one owning process per database. In-process callers share an
/// owner; a separate CLI cannot open a mounted instance's size cache directly.
pub fn open_fjall(path: &Path) -> Result<Arc<fjall::SingleWriterTxDatabase>> {
    std::fs::create_dir_all(path).context("create Fjall database directory")?;
    let path = path
        .canonicalize()
        .context("resolve Fjall database directory")?;
    let mut databases = FJALL_DATABASES
        .lock()
        .map_err(|_| anyhow::anyhow!("Fjall database registry lock poisoned"))?;
    if let Some(database) = databases.get(&path).and_then(Weak::upgrade) {
        return Ok(database);
    }
    // A Weak can expire before the old owner's destructor releases its native
    // file lock. Retry only this known close/reopen race, never retain an owner
    // in the registry merely to keep a database open.
    let closing = databases.contains_key(&path);
    let mut attempts = 0;
    let database = loop {
        match fjall::SingleWriterTxDatabase::builder(&path).open() {
            Ok(database) => break database,
            Err(fjall::Error::Locked) if closing && attempts < 100 => {
                attempts += 1;
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => return Err(error).context(
                "open Fjall database: Fjall requires exclusive process ownership; another process may own this database (direct size-cache CLI access while mounted is unsupported); close the owner and retry"),
        }
    };
    databases.retain(|_, database| database.strong_count() != 0);
    let database = Arc::new(database);
    databases.insert(path, Arc::downgrade(&database));
    Ok(database)
}
