use bazel_output_service::{
    Config, OutputService, proto::bazel_output_service_server::BazelOutputServiceServer,
};
use clap::Parser;
use std::{net::SocketAddr, path::PathBuf, time::Duration};

/// Local Bazel Remote Output Service; files are eagerly verified and materialized.
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8377")]
    listen: SocketAddr,
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    cas_endpoint: String,
    #[arg(long, default_value = "")]
    instance_name: String,
    /// Name of the environment variable holding a BuildBuddy API key (not its value).
    #[arg(long, default_value = "BUILDBUDDY_API_KEY")]
    api_key_env: String,
    #[arg(long, default_value_t = 17_179_869_184)]
    max_blob_bytes: u64,
    #[arg(long, default_value_t = 120)]
    rpc_timeout_secs: u64,
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    anyhow::ensure!(
        args.listen.ip().is_loopback(),
        "unauthenticated service must bind loopback"
    );
    let api_key = match std::env::var(&args.api_key_env) {
        Ok(value) => Some(value),
        Err(std::env::VarError::NotPresent) => None,
        Err(_) => anyhow::bail!("credential environment variable is not valid Unicode"),
    };
    let service = OutputService::new(Config {
        root: args.root,
        cas_endpoint: args.cas_endpoint,
        instance_name: args.instance_name,
        api_key,
        max_blob_bytes: args.max_blob_bytes,
        rpc_timeout: Duration::from_secs(args.rpc_timeout_secs),
    })
    .await?;
    eprintln!("Bazel Remote Output Service listening on {}", args.listen);
    tonic::transport::Server::builder()
        .add_service(BazelOutputServiceServer::new(service.clone()))
        .serve_with_shutdown(args.listen, async {
            #[cfg(windows)]
            {
                // Python and other supervising terminals send CTRL_BREAK to an
                // owned process group, without interrupting their own console.
                let mut breaks =
                    tokio::signal::windows::ctrl_break().expect("install CTRL_BREAK handler");
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {},
                    _ = breaks.recv() => {},
                }
            }
            #[cfg(not(windows))]
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    eprintln!("output_service_stats {:?}", service.metrics());
    Ok(())
}
