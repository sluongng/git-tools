//! Pinned internal snapshot supervisor. Only the trusted runner supplies argv.
use clap::Parser;
use std::path::PathBuf;
use tinytree_check_runner::snapshot_linux::{HelperPlan, diagnostic_stage, run_helper};
#[derive(Parser)]
struct Args {
    #[arg(long)]
    manifest: PathBuf,
    #[arg(long)]
    staging: PathBuf,
    #[arg(long)]
    source: PathBuf,
    #[arg(long)]
    proof: PathBuf,
    #[arg(long)]
    invocation: String,
    #[arg(long)]
    bazel: PathBuf,
    #[arg(last = true, required = true)]
    args: Vec<String>,
}
fn main() {
    let a = Args::parse();
    match run_helper(HelperPlan {
        manifest: a.manifest,
        staging: a.staging,
        source: a.source,
        proof: a.proof,
        invocation: a.invocation,
        bazel: a.bazel,
        args: a.args,
    }) {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!(
                "readonly_snapshot_unavailable: {}",
                diagnostic_stage(&error)
            );
            std::process::exit(125)
        }
    }
}
