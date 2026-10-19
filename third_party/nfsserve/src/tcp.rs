use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use anyhow;
use async_trait::async_trait;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::context::RPCContext;
use crate::rpcwire::*;
use crate::transaction_tracker::TransactionTracker;
use crate::vfs::NFSFileSystem;

/// A NFS Tcp Connection Handler
pub struct NFSTcpListener<T: NFSFileSystem + Send + Sync + 'static> {
    listener: TcpListener,
    port: u16,
    arcfs: Arc<T>,
    mount_signal: Option<mpsc::Sender<bool>>,
    export_name: Arc<String>,
    transaction_tracker: Arc<TransactionTracker>,
}

pub fn generate_host_ip(hostnum: u16) -> String {
    format!("127.88.{}.{}", ((hostnum >> 8) & 0xFF) as u8, (hostnum & 0xFF) as u8)
}

/// processes an established socket
async fn process_socket(socket: tokio::net::TcpStream, context: RPCContext) -> Result<(), anyhow::Error> {
    let (mut message_handler, mut socksend, mut replies) = SocketMessageHandler::new(&context);
    let _ = socket.set_nodelay(true);
    let (mut reader,mut writer) = socket.into_split();
    // Independent forwarding prevents request backpressure from starving replies.
    let reader_task = tokio::spawn(async move { let _ = tokio::io::copy(&mut reader,&mut socksend).await; });
    let parser_task = tokio::spawn(async move {
        while message_handler.read().await.is_ok() {}
    });
    let result = async {
        while let Some(reply) = replies.recv().await {
            write_fragment(&mut writer,&reply?).await?;
        }
        Ok(())
    }.await;
    reader_task.abort();
    parser_task.abort();
    result
}

#[async_trait]
pub trait NFSTcp: Send + Sync {
    /// Gets the true listening port. Useful if the bound port number is 0
    fn get_listen_port(&self) -> u16;

    /// Gets the true listening IP. Useful on windows when the IP may be random
    fn get_listen_ip(&self) -> IpAddr;

    /// Sets a mount listener. A "true" signal will be sent on a mount
    /// and a "false" will be sent on an unmount
    fn set_mount_listener(&mut self, signal: mpsc::Sender<bool>);

    /// Loops forever and never returns handling all incoming connections.
    async fn handle_forever(&self) -> io::Result<()>;
}

impl<T: NFSFileSystem + Send + Sync + 'static> NFSTcpListener<T> {
    /// Binds to a ipstr of the form [ip address]:port. For instance
    /// "127.0.0.1:12000". fs is an instance of an implementation
    /// of NFSFileSystem.
    pub async fn bind(ipstr: &str, fs: T) -> io::Result<NFSTcpListener<T>> {
        let (ip, port) = ipstr
            .split_once(':')
            .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "IP Address must be of form ip:port"))?;
        let port = port
            .parse::<u16>()
            .map_err(|_| io::Error::new(io::ErrorKind::AddrNotAvailable, "Port not in range 0..=65535"))?;

        let arcfs: Arc<T> = Arc::new(fs);

        if ip == "auto" {
            let mut num_tries_left = 32;

            for try_ip in 1u16.. {
                let ip = generate_host_ip(try_ip);

                let result = NFSTcpListener::bind_internal(&ip, port, arcfs.clone()).await;

                match &result {
                    Err(_) => {
                        if num_tries_left == 0 {
                            return result;
                        } else {
                            num_tries_left -= 1;
                            continue;
                        }
                    },
                    Ok(_) => {
                        return result;
                    },
                }
            }
            unreachable!(); // Does not detect automatically that loop above never terminates.
        } else {
            // Otherwise, try this.
            NFSTcpListener::bind_internal(ip, port, arcfs).await
        }
    }

    async fn bind_internal(ip: &str, port: u16, arcfs: Arc<T>) -> io::Result<NFSTcpListener<T>> {
        let ipstr = format!("{ip}:{port}");
        let listener = TcpListener::bind(&ipstr).await?;
        info!("Listening on {:?}", &ipstr);

        let port = match listener.local_addr().unwrap() {
            SocketAddr::V4(s) => s.port(),
            SocketAddr::V6(s) => s.port(),
        };
        Ok(NFSTcpListener {
            listener,
            port,
            arcfs,
            mount_signal: None,
            export_name: Arc::from("/".to_string()),
            transaction_tracker: Arc::new(TransactionTracker::new(Duration::from_secs(60))),
        })
    }

    /// Sets an optional NFS export name.
    ///
    /// - `export_name`: The desired export name without slashes.
    ///
    /// Example: Name `foo` results in the export path `/foo`.
    /// Default path is `/` if not set.
    pub fn with_export_name<S: AsRef<str>>(&mut self, export_name: S) {
        self.export_name = Arc::new(format!("/{}", export_name.as_ref().trim_end_matches('/').trim_start_matches('/')))
    }
}

#[async_trait]
impl<T: NFSFileSystem + Send + Sync + 'static> NFSTcp for NFSTcpListener<T> {
    /// Gets the true listening port. Useful if the bound port number is 0
    fn get_listen_port(&self) -> u16 {
        let addr = self.listener.local_addr().unwrap();
        addr.port()
    }

    fn get_listen_ip(&self) -> IpAddr {
        let addr = self.listener.local_addr().unwrap();
        addr.ip()
    }

    /// Sets a mount listener. A "true" signal will be sent on a mount
    /// and a "false" will be sent on an unmount
    fn set_mount_listener(&mut self, signal: mpsc::Sender<bool>) {
        self.mount_signal = Some(signal);
    }

    /// Loops forever and never returns handling all incoming connections.
    async fn handle_forever(&self) -> io::Result<()> {
        let connections = Arc::new(tokio::sync::Semaphore::new(16));
        let requests = Arc::new(tokio::sync::Semaphore::new(self.arcfs.max_inflight_requests().clamp(1, 1024)));
        loop {
            let connection = connections.clone().acquire_owned().await.map_err(io::Error::other)?;
            let (socket, _) = self.listener.accept().await?;
            let context = RPCContext {
                local_port: self.port,
                requests: requests.clone(),
                client_addr: socket.peer_addr().unwrap().to_string(),
                auth: crate::rpc::auth_unix::default(),
                vfs: self.arcfs.clone(),
                mount_signal: self.mount_signal.clone(),
                export_name: self.export_name.clone(),
                transaction_tracker: self.transaction_tracker.clone(),
            };
            info!("Accepting connection from {}", context.client_addr);
            debug!("Accepting socket {:?} {:?}", socket, context);
            tokio::spawn(async move {
                let _connection = connection;
                let _ = process_socket(socket, context).await;
            });
        }
    }
}
