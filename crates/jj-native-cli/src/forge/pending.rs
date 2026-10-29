//! Exact, owner-private outbound intent. A timeout never generates a new key.
use super::context::Context;
use fs2::FileExt as _;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{fs::File, io};

const MAX_RECORD_BYTES: usize = 2 * 1024 * 1024;
const MAX_RECORDS: usize = 1024;
const MAX_TOTAL_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Source {
    pub workspace: String,
    pub operation_id: String,
    pub base: String,
    pub head: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    schema_version: u32,
    pub request_id: String,
    pub context: Context,
    pub operation: i32,
    pub payload: serde_json::Value,
    pub source: Option<Source>,
}

pub(super) struct Pending {
    _directory: PrivateDir,
    _lock: File,
    pub request_id: String,
}

fn invalid() -> io::Error {
    io::Error::other("Tinytree request identity is invalid or bound to different inputs")
}

pub(super) fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
}

impl Pending {
    pub fn load(context: &Context, request_id: &str) -> io::Result<Intent> {
        if !valid_id(request_id) {
            return Err(invalid());
        }
        let root = super::context::open()?;
        let directory = PrivateDir::open_existing(root.path().join("requests"))?;
        let bytes = directory.read(format!("request-{request_id}.json"), MAX_RECORD_BYTES)?;
        let value: Intent = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if value.schema_version != 1 || value.request_id != request_id || &value.context != context
        {
            return Err(invalid());
        }
        Ok(value)
    }

    pub fn prepare(
        context: &Context,
        operation: i32,
        payload: &serde_json::Value,
        request_id: Option<String>,
        source: Option<Source>,
    ) -> io::Result<Self> {
        let request_id = request_id.unwrap_or_else(|| uuid::Uuid::new_v4().simple().to_string());
        if !valid_id(&request_id) {
            return Err(invalid());
        }
        context.validate()?;
        let root = super::context::create()?;
        let directory = PrivateDir::create(root.path().join("requests"))?;
        let lock = directory.open_lock("owner.lock")?;
        lock.try_lock_exclusive().map_err(|_| {
            io::Error::other(
                "another Tinytree request owns this context; retry the original intent",
            )
        })?;
        let intended = Intent {
            schema_version: 1,
            request_id: request_id.clone(),
            context: context.clone(),
            operation,
            payload: payload.clone(),
            source,
        };
        let name = format!("request-{request_id}.json");
        match directory.read(&name, MAX_RECORD_BYTES) {
            Ok(bytes) => {
                let saved: Intent = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                if saved != intended {
                    return Err(invalid());
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let bytes = serde_json::to_vec(&intended).map_err(|_| invalid())?;
                if bytes.len() > MAX_RECORD_BYTES {
                    return Err(io::Error::other(
                        "Tinytree request exceeds saved-intent limit",
                    ));
                }
                let mut count = 0;
                let mut total = bytes.len() as u64;
                for entry in std::fs::read_dir(directory.path())? {
                    let entry = entry?;
                    let metadata = entry.metadata()?;
                    if !metadata.is_file() || entry.file_type()?.is_symlink() {
                        return Err(invalid());
                    }
                    if entry.file_name() == "owner.lock" {
                        continue;
                    }
                    count += 1;
                    total = total.checked_add(metadata.len()).ok_or_else(invalid)?;
                }
                if count >= MAX_RECORDS || total > MAX_TOTAL_BYTES {
                    return Err(io::Error::other(
                        "Tinytree saved-request capacity is exhausted; retained intents were not deleted",
                    ));
                }
                directory.replace(&name, &bytes)?;
            }
            Err(error) => return Err(error),
        }
        Ok(Self {
            _directory: directory,
            _lock: lock,
            request_id,
        })
    }
}
