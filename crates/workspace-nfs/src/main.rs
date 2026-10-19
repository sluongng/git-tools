use anyhow::{Result, ensure};
use clap::Parser;
use nfsserve::tcp::{NFSTcp, NFSTcpListener};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::PathBuf;
use workspace_core::{
    backend::{GitRepo, SizeFallback, SizeWarmOptions},
    filesystem::Snapshot,
};
use workspace_nfs::GitNfs;
use workspace_overlay::{overlay::Overlay, worktree::WorkingTree};

#[derive(Parser)]
#[command(about = "Serve a writable lazy Git workspace over loopback NFSv3")]
struct Args {
    #[arg(long)]
    repo: PathBuf,
    #[arg(long)]
    overlay: PathBuf,
    /// Local directory where this export will be mounted (for native Git admin).
    #[arg(long)]
    mountpoint: PathBuf,
    #[arg(long, default_value = "git")]
    git: PathBuf,
    /// Permit downloading content to discover missing sizes (may hydrate a directory listing).
    #[arg(long)]
    fetch_missing_sizes: bool,
    /// Skip GitHub tree size-metadata warming.
    #[arg(long)]
    skip_size_warm: bool,
    #[arg(long, default_value = "HEAD")]
    revision: String,
    #[arg(long, default_value = "origin")]
    remote: String,
    #[arg(long, default_value = "127.0.0.1:11111")]
    listen: SocketAddrV4,
}
#[derive(Parser)]
struct FsmonitorArgs {
    #[arg(long)]
    overlay: PathBuf,
    version: u8,
    token: String,
}
fn fsmonitor() -> Result<()> {
    use std::io::Write;
    let args = FsmonitorArgs::parse_from(std::env::args_os().skip(1));
    ensure!(
        args.version == 2,
        "git-nfs fsmonitor requires hook protocol version 2"
    );
    let (token, paths) = workspace_overlay::overlay::changes_since(&args.overlay, &args.token)?;
    let mut output = std::io::stdout().lock();
    output.write_all(token.as_bytes())?;
    output.write_all(&[0])?;
    for path in paths {
        output.write_all(&path)?;
        output.write_all(&[0])?;
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("fsmonitor")) {
        return fsmonitor();
    }
    let args = Args::parse();
    ensure!(
        *args.listen.ip() == Ipv4Addr::LOCALHOST,
        "only 127.0.0.1 loopback exports are supported"
    );
    std::fs::create_dir_all(&args.mountpoint)?;
    let mountpoint = args.mountpoint.canonicalize()?;
    let working = WorkingTree::lock(&args.overlay)?;
    ensure!(
        !working.directory.starts_with(&mountpoint) && !mountpoint.starts_with(&working.directory),
        "mountpoint and overlay must not overlap"
    );
    let mut repo = GitRepo::open(
        &args.repo,
        &args.git,
        &args.remote,
        if args.fetch_missing_sizes {
            SizeFallback::Fetch
        } else {
            SizeFallback::Error
        },
    )?;
    ensure!(
        !repo.repository_path().starts_with(&mountpoint)
            && !mountpoint.starts_with(repo.repository_path()),
        "mountpoint and repository must not overlap"
    );
    let snapshot = Snapshot::load(&repo, &args.revision)?;
    if !args.skip_size_warm && repo.is_github_remote()? {
        let sizes = repo.warm_snapshot_sizes(&snapshot, &SizeWarmOptions::default())?;
        eprintln!(
            "Warmed {} GitHub object sizes without fetching content",
            sizes.learned_blobs
        );
    }
    let overlay = Overlay::open(&working.directory, &snapshot, &working.gitdir)?;
    working.prepare(&repo, &snapshot, &mountpoint, &std::env::current_exe()?)?;
    let fs = GitNfs::new(repo, overlay, working);
    let server = NFSTcpListener::bind(&args.listen.to_string(), fs.clone()).await?;
    let port = server.get_listen_port();
    eprintln!(
        "NFS export ready on 127.0.0.1:{port}; macOS mount: sudo mount_nfs -o vers=3,tcp,nolocks,resvport,port={port},mountport={port},nobrowse 127.0.0.1:/ {}",
        mountpoint.display()
    );
    tokio::select! {
        result = server.handle_forever() => result?,
        result = tokio::signal::ctrl_c() => { result?; fs.sync().await.map_err(|e| anyhow::anyhow!("flush NFS overlay: {e:?}"))?; }
    }
    Ok(())
}
