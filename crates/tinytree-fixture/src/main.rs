use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, bail};
use tinytree_fixture::{FixtureConfig, build_fixture};

fn main() -> Result<()> {
    let mut out = None;
    let mut config = FixtureConfig::default();
    let mut files_seen = false;
    let mut bytes_seen = false;
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--out") if out.is_none() => {
                out = Some(PathBuf::from(
                    args.next().context("--out requires a directory")?,
                ))
            }
            Some("--files") if !files_seen => {
                files_seen = true;
                config.unused_files = args
                    .next()
                    .context("--files requires a count")?
                    .to_str()
                    .context("invalid file count")?
                    .parse()
                    .context("invalid file count")?
            }
            Some("--bytes-per-file") if !bytes_seen => {
                bytes_seen = true;
                config.bytes_per_file = args
                    .next()
                    .context("--bytes-per-file requires a size")?
                    .to_str()
                    .context("invalid byte size")?
                    .parse()
                    .context("invalid byte size")?
            }
            Some("--help") => {
                println!("tinytree-fixture --out DIR [--files N] [--bytes-per-file N]");
                return Ok(());
            }
            _ => bail!("unknown or repeated argument: {}", arg.to_string_lossy()),
        }
    }
    let out = out.context("--out is required")?;
    let started = Instant::now();
    let fixture = build_fixture(config)?;
    let mut report = fixture.write_bundle(&out)?;
    // Include generation as well as writing; the stable manifest has no timing.
    report.elapsed_seconds = started.elapsed().as_secs_f64();
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
