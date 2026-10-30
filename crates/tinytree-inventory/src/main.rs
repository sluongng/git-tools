use std::{path::PathBuf, time::Duration};
use tinytree_client::{Config, NativeClient};
use tinytree_inventory::{Budgets, Report, inventory, temporary_directory};

fn arguments() -> Result<(PathBuf, Vec<String>, Budgets), String> {
    let mut args = std::env::args().skip(1);
    let mut config = None;
    let mut refs = Vec::new();
    let mut b = Budgets::default();
    while let Some(arg) = args.next() {
        if arg == "--help" {
            return Err("usage: inventory --config PATH --ref NAME [--ref NAME ...] [--max-objects N] [--max-bytes N] [--max-queue N] [--max-edges N] [--timeout-seconds N]".into());
        }
        let value = args.next().ok_or("missing_argument_value")?;
        match arg.as_str() {
            "--config" => {
                if config.replace(PathBuf::from(value)).is_some() {
                    return Err("duplicate_config".into());
                }
            }
            "--ref" => refs.push(value),
            "--max-objects" => b.max_objects = value.parse().map_err(|_| "invalid_number")?,
            "--max-bytes" => b.max_bytes = value.parse().map_err(|_| "invalid_number")?,
            "--max-queue" => b.max_queue = value.parse().map_err(|_| "invalid_number")?,
            "--max-edges" => b.max_edges = value.parse().map_err(|_| "invalid_number")?,
            "--timeout-seconds" => {
                b.timeout_seconds = value.parse().map_err(|_| "invalid_number")?
            }
            _ => return Err("unknown_argument".into()),
        }
    }
    if refs.is_empty()
        || refs.len() > 256
        || refs.iter().any(|name| name.is_empty() || name.len() > 4096)
        || refs.iter().collect::<std::collections::HashSet<_>>().len() != refs.len()
    {
        return Err("invalid_reference_selection".into());
    }
    if !b.valid() {
        return Err("invalid_budgets".into());
    }
    Ok((config.ok_or("explicit_config_required")?, refs, b))
}
#[tokio::main]
async fn main() {
    let report = match arguments() {
        Err(error) => Report::failed(Budgets::default(), &error),
        Ok((path, names, budgets)) => {
            let run = async {
                let config =
                    Config::load(&path).map_err(|_| "configuration_load_failed".to_string())?;
                let client = NativeClient::connect(config)
                    .await
                    .map_err(|e| format!("connect_{:?}", e.code()))?;
                let temporary =
                    temporary_directory().map_err(|_| "temporary_directory_failed".to_string())?;
                Ok::<_, String>(inventory(&client, names, temporary.path(), budgets.clone()).await)
            };
            match tokio::time::timeout(Duration::from_secs(budgets.timeout_seconds), run).await {
                Ok(Ok(report)) => report,
                Ok(Err(error)) => Report::failed(budgets, &error),
                Err(_) => Report::failed(budgets, "deadline_exceeded"),
            }
        }
    };
    println!(
        "{}",
        serde_json::to_string(&report).expect("serializable report")
    );
    if !report.complete {
        std::process::exit(1);
    }
}
