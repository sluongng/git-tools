use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    time::Duration,
};
/// Exact retained source DATA; never an endpoint, credential or authority token.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeRestoreSelection {
    pub workspace_id: [u8; 32],
    pub generation: u64,
    pub manifest_id: [u8; 32],
    pub manifest_blake3: [u8; 32],
    pub manifest_size_bytes: u64,
}
/// Discovery rows are immutable selectors, not retention leases or restore proof.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCloudWorkspace {
    pub workspace_id: String,
    pub generation: u64,
    pub manifest_id: Option<String>,
    pub local_destination: Option<PathBuf>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCloudWorkspaces {
    pub workspaces: Vec<NativeCloudWorkspace>,
    pub continuation: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCloudVersion {
    pub generation: u64,
    pub manifest_id: String,
    pub previous_manifest_id: Option<String>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCloudVersions {
    pub workspace: NativeCloudWorkspace,
    pub versions: Vec<NativeCloudVersion>,
    pub through_generation: u64,
    pub retired_through: u64,
    pub retention_revision: u64,
    pub continuation: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCloudRestored {
    pub mounted: crate::native_workspaces::Mounted,
    pub source: NativeRestoreSelection,
    pub reused_destination: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCapsuleRestored {
    pub mounted: crate::native_workspaces::Mounted,
    pub receipt: native_vfs_rpc::PrivateBackupReceipt,
    pub base: String,
    pub tree: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeCapsulesPruned {
    pub current_capsule: String,
    pub retired: u32,
    pub local_files: u32,
    pub pending: u32,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
    /// Exact owner-private derived Forge client only; unsupported in full mode.
    ForgeClientReady {
        descriptor_digest: [u8; 32],
    },
    ForgeClientStop {
        descriptor_digest: [u8; 32],
    },
    IdentityChallenge,
    Scoped {
        version: u32,
        digest: String,
        request: Box<Request>,
    },
    RetryBes,
    Endpoints,
    Mount {
        request: agent_session::MountRequest,
    },
    Unmount {
        session: PathBuf,
    },
    /// Same-user native creation selects only configured repository aliases.
    NativeMount {
        root: PathBuf,
        store: String,
        commit: String,
        workspace_id: String,
    },
    /// Checked creation selection; never an upstream authorization credential.
    NativeMountPlanned {
        root: PathBuf,
        store: String,
        commit: String,
        workspace_id: String,
        creation_plan: Vec<u8>,
    },
    /// Exact unpublished commit for an immutable trusted-check source.
    NativeCandidateMount {
        root: PathBuf,
        store: String,
        commit: String,
        workspace_id: String,
    },
    /// Retire a stopped immutable check source, never a user working copy.
    NativeCandidateInspect {
        workspace_id: String,
        commit: String,
    },
    NativeCandidateRetire {
        workspace_id: String,
        expected_commit: String,
    },
    /// Owner-private WIP restore in a descriptor-bound native-only client.
    /// Explicit cleanup in the exact derived Forge client, without a writer lease.
    NativePrunePrivateCapsules {
        root: PathBuf,
    },
    NativeRestoreCapsule {
        root: PathBuf,
        capsule_id: [u8; 32],
    },
    NativeRestore {
        root: PathBuf,
        store: String,
        workspace_id: String,
        source: NativeRestoreSelection,
    },
    NativeCloudWorkspaces {
        store: String,
        page_size: u16,
        continuation: String,
    },
    NativeCloudVersions {
        store: String,
        workspace_id: String,
        page_size: u16,
        continuation: String,
    },
    /// Resolve a version once, then retain its exact pin in the restore journal.
    NativeCloudRestore {
        root: PathBuf,
        store: String,
        source_workspace: String,
        generation: Option<u64>,
    },
    NativeResume {
        root: PathBuf,
    },
    NativeUnmount {
        workspace_id: String,
    },
    NativeMounted {
        workspace_id: String,
    },
    /// Preview by default; apply only unreachable objects in a managed cohort.
    NativeGc {
        store: String,
        apply: bool,
    },
    /// Detach only a privately recorded, proven disconnected native mount.
    NativeRecover {
        workspace_id: String,
    },

    Mounted {
        session: PathBuf,
    },
    Recover {
        session: Option<PathBuf>,
    },
    Shutdown,
}
#[derive(Serialize, Deserialize)]
pub struct Response {
    pub ok: bool,
    pub result: Value,
    pub error: Option<String>,
}
pub struct Client {
    pub socket: PathBuf,
}
impl Client {
    pub fn call(&self, request: Request) -> Result<Value> {
        let mut stream = UnixStream::connect(&self.socket)
            .context("workspace daemon unavailable; run workspace serve")?;
        let seconds = if matches!(
            request,
            Request::NativeRestoreCapsule { .. } | Request::NativePrunePrivateCapsules { .. }
        ) {
            310
        } else {
            180
        };
        stream.set_read_timeout(Some(Duration::from_secs(seconds)))?;
        stream.set_write_timeout(Some(Duration::from_secs(10)))?;
        let mut bytes = serde_json::to_vec(&request)?;
        bytes.push(b'\n');
        ensure!(bytes.len() <= 65536, "control request too large");
        stream.write_all(&bytes)?;
        let mut reader = BufReader::new(stream);
        let mut bytes = Vec::new();
        use std::io::Read;
        reader
            .by_ref()
            .take(1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes)?;
        ensure!(bytes.len() <= 1024 * 1024, "control response too large");
        let response: Response = serde_json::from_slice(&bytes)?;
        ensure!(
            response.ok,
            "{}",
            response
                .error
                .unwrap_or_else(|| "daemon operation failed".into())
        );
        Ok(response.result)
    }
}
/// A configuration-bound controller. Each operation is verified by the daemon
/// in the same dispatch as its side effects, including after daemon replacement.
pub struct ScopedClient {
    client: Client,
    config: crate::config::Config,
}
impl ScopedClient {
    pub fn new(config: &crate::config::Config) -> Self {
        Self {
            client: Client {
                socket: config.socket(),
            },
            config: config.clone(),
        }
    }
    pub fn available(&self) -> bool {
        self.client.call(Request::Status).is_ok()
    }
    pub fn validate_identity(&self) -> Result<()> {
        self.call(Request::Status)?;
        Ok(())
    }
    pub fn call(&self, request: Request) -> Result<Value> {
        let challenge: crate::identity::Challenge = serde_json::from_value(
            self.client.call(Request::IdentityChallenge)
                .context("daemon cannot verify configuration identity; restart it using its matching configuration")?,
        )?;
        let credentials = crate::identity::Credentials::load(&self.config)?;
        let digest = challenge.digest(&self.config, &credentials)?;
        self.client.call(Request::Scoped {
            version: challenge.version,
            digest,
            request: Box::new(request),
        })
    }
}

macro_rules! impl_controller {
    ($client:ty) => {
        impl agent_session::MountController for $client {
            fn mount(&self, request: agent_session::MountRequest) -> Result<()> {
                self.call(Request::Mount { request })?;
                Ok(())
            }
            fn unmount(&self, session: &Path) -> Result<()> {
                self.call(Request::Unmount {
                    session: session.into(),
                })?;
                Ok(())
            }
            fn is_mounted(&self, session: &Path) -> Result<bool> {
                Ok(self
                    .call(Request::Mounted {
                        session: session.into(),
                    })?
                    .as_bool()
                    .unwrap_or(false))
            }
            fn bazel_args(&self) -> Result<Vec<String>> {
                // Credentials never travel through status or general-purpose responses.
                let status = self.call(Request::Status)?;
                let endpoint = status["cache_endpoint"]
                    .as_str()
                    .context("missing cache endpoint")?;
                let mut args = vec![
                    format!("--remote_cache={endpoint}"),
                    format!("--remote_executor={endpoint}"),
                    format!("--experimental_remote_downloader={endpoint}"),
                    format!(
                        "--remote_bytestream_uri_prefix={}",
                        status["bytestream_uri_prefix"]
                            .as_str()
                            .context("missing canonical bytestream prefix")?
                    ),
                    format!(
                        "--remote_instance_name={}",
                        status["instance_name"]
                            .as_str()
                            .context("missing cache instance")?
                    ),
                ];
                if let Some(endpoint) = status["bes_endpoint"].as_str() {
                    args.push(format!("--bes_backend={endpoint}"));
                }
                if let Some(endpoint) = status["bos_endpoint"].as_str() {
                    args.push(format!("--experimental_remote_output_service={endpoint}"));
                    let prefix = status["output_path_prefix"]
                        .as_str()
                        .context("missing output prefix")?;
                    args.push(format!(
                        "--experimental_remote_output_service_output_path_prefix={prefix}"
                    ));
                }
                Ok(args)
            }
        }
    };
}
impl_controller!(Client);
impl_controller!(ScopedClient);

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retained_restore_requests_are_exact_pins_not_connection_or_credential_configuration() {
        let request = Request::NativeRestore {
            root: PathBuf::from("/workspace"),
            store: "example".into(),
            workspace_id: "01".repeat(32),
            source: NativeRestoreSelection {
                workspace_id: [2; 32],
                generation: 3,
                manifest_id: [4; 32],
                manifest_blake3: [5; 32],
                manifest_size_bytes: 1024,
            },
        };
        let value = serde_json::to_value(&request).unwrap();
        let decoded: Request = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), value);
        let mut endpoint = value.clone();
        endpoint["source"]["endpoint"] = "http://example.invalid".into();
        assert!(serde_json::from_value::<Request>(endpoint).is_err());
        let mut credential = value;
        credential["source"]["token"] = "not-a-supported-field".into();
        assert!(serde_json::from_value::<Request>(credential).is_err());
        let resume = Request::NativeResume {
            root: PathBuf::from("/workspace"),
        };
        assert_eq!(
            serde_json::to_value(
                serde_json::from_value::<Request>(serde_json::to_value(&resume).unwrap()).unwrap()
            )
            .unwrap(),
            serde_json::to_value(resume).unwrap()
        );
        for request in [
            Request::NativeCloudWorkspaces {
                store: "example".into(),
                page_size: 2,
                continuation: String::new(),
            },
            Request::NativeCloudVersions {
                store: "example".into(),
                workspace_id: "02".repeat(32),
                page_size: 2,
                continuation: String::new(),
            },
            Request::NativeCloudRestore {
                root: PathBuf::from("/workspace"),
                store: "example".into(),
                source_workspace: "02".repeat(32),
                generation: None,
            },
        ] {
            let value = serde_json::to_value(request).unwrap();
            let decoded: Request = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), value);
            for key in ["endpoint", "token"] {
                let mut altered = value.clone();
                altered[key] = "not-a-supported-connection-override".into();
                assert!(serde_json::from_value::<Request>(altered).is_err());
            }
        }
    }
}
