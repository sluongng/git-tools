use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    time::Duration,
};
#[derive(Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Status,
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
    NativeUnmount {
        workspace_id: String,
    },
    NativeMounted {
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
        stream.set_read_timeout(Some(Duration::from_secs(180)))?;
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
