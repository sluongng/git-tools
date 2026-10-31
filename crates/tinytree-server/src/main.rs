use anyhow::{Context, Result, bail};
use std::path::PathBuf;
use tinytree_server::Config;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let mut config = None;
    let mut check_config = false;
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--config") if config.is_none() => {
                config = Some(PathBuf::from(
                    args.next().context("--config requires a path")?,
                ))
            }
            Some("--check-config") if !check_config => check_config = true,
            Some("--help") => {
                println!("tinytree --config FILE [--check-config]");
                return Ok(());
            }
            _ => bail!("unknown or repeated server option"),
        }
    }
    let path = config.context("--config is required")?;
    let config = Config::load(&path)?;
    if check_config {
        println!("configuration valid");
        return Ok(());
    }
    tinytree_server::serve(config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await
}
