//! Test-only Linux kernel acceptance. Its child is deliberately NOT Bazel;
//! this proves read-only mount enforcement, not remote build/check evidence.
use anyhow::{Context, Result, ensure};
use std::{fs, io::Write, path::Path, process::Command};
use tinytree_check_runner::snapshot_linux::{HelperPlan, read_completed_proof};
use tinytree_forge_core::{check::Request, checker::*};
use tinytree_model as model;
#[cfg(target_os = "linux")]
fn fixture_child(args: &[String]) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let stage = Path::new(args.last().context("fixture staging missing")?);
    let before = fs::read("tracked")?;
    ensure!(before == b"immutable source\n", "fixture source differs");
    for result in [
        fs::OpenOptions::new()
            .write(true)
            .open("tracked")
            .map(|_| ()),
        fs::write("new-file", b"no"),
        fs::remove_file("tracked"),
        fs::rename("tracked", "moved"),
        fs::set_permissions("tracked", fs::Permissions::from_mode(0o600)),
    ] {
        ensure!(
            result.is_err_and(|error| error.raw_os_error() == Some(libc::EROFS)),
            "source write was not rejected by read-only filesystem"
        );
    }
    ensure!(fs::read("link")? == before, "fixture link differs");
    // Mutating the original writable backing path must not mutate the source
    // inode the checker sees. This is an intentional fixture-only write.
    fs::write(stage.join("tracked"), b"changed staging\n")?;
    ensure!(
        fs::read("tracked")? == before,
        "writable staging alias changed checked source"
    );
    let status = fs::read_to_string("/proc/self/status")?;
    for field in ["CapInh:", "CapPrm:", "CapEff:", "CapBnd:", "CapAmb:"] {
        let value = status
            .lines()
            .find_map(|line| line.strip_prefix(field))
            .context("missing capability status")?
            .trim();
        ensure!(
            u64::from_str_radix(value, 16)? == 0,
            "checker capability remains"
        );
    }
    ensure!(
        status.lines().any(|line| line
            .strip_prefix("NoNewPrivs:")
            .is_some_and(|v| v.trim() == "1")),
        "no_new_privs absent"
    );
    // No writable descriptor into source is inherited; regular opens fail too.
    ensure!(
        fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open("tracked")
            .is_err(),
        "writable source descriptor available"
    );
    ensure!(
        unsafe { libc::getpid() } == 1,
        "checker is not PID namespace init"
    );
    // Deliberately escape the outer process group. Namespace-init exit must
    // still destroy this descendant; a PG-only teardown would miss it.
    use std::os::unix::process::CommandExt;
    let mut detached = Command::new(std::env::current_exe()?);
    detached.arg("--fixture-detached").arg(stage);
    unsafe {
        detached.pre_exec(|| {
            if libc::setsid() < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let _owned_descendant = detached.spawn()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !stage.join("detached.identity").try_exists()?
        || fs::metadata(stage.join("detached.heartbeat")).map_or(true, |m| m.len() < 5)
    {
        ensure!(
            std::time::Instant::now() < deadline,
            "detached fixture failed to start"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    println!("test-only readonly snapshot child verified; not Bazel evidence");
    Ok(())
}
#[cfg(target_os = "linux")]
fn process_identity(text: &str) -> Result<(u32, u64)> {
    let pid = text
        .split_whitespace()
        .next()
        .context("missing process ID")?
        .parse()?;
    let split = text.rfind(')').context("missing process name boundary")?;
    let start = text[split + 1..]
        .split_whitespace()
        .nth(19)
        .context("missing process start")?
        .parse()?;
    Ok((pid, start))
}
#[cfg(target_os = "linux")]
fn detached_fixture(args: &[String]) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let stage = Path::new(args.last().context("detached staging missing")?);
    ensure!(
        unsafe { libc::getsid(0) } == unsafe { libc::getpid() },
        "fixture did not detach session"
    );
    let identity = process_identity(&fs::read_to_string("/proc/self/stat")?)?;
    fs::write(
        stage.join("detached.identity"),
        serde_json::to_vec(&identity)?,
    )?;
    let mut heartbeat = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(stage.join("detached.heartbeat"))?;
    loop {
        heartbeat.write_all(b"tick\n")?;
        heartbeat.flush()?;
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}
#[cfg(target_os = "linux")]
fn acceptance() -> Result<()> {
    use std::os::unix::{
        fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    };
    let dir = tempfile::tempdir()?;
    fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o700))?;
    let stage = dir.path().join("staging");
    fs::DirBuilder::new().mode(0o700).create(&stage)?;
    fs::write(stage.join("tracked"), b"immutable source\n")?;
    fs::set_permissions(stage.join("tracked"), fs::Permissions::from_mode(0o644))?;
    std::os::unix::fs::symlink("tracked", stage.join("link"))?;
    let file = model::describe_file(b"immutable source\n");
    let link = model::encode(
        &model::Object::Symlink("tracked".into()),
        model::Limits::default(),
    )?;
    let tree = model::encode(
        &model::Object::Tree(model::Tree {
            entries: vec![
                model::TreeEntry {
                    name: "link".into(),
                    value: model::TreeValue::Symlink {
                        id: model::SymlinkId(link.descriptor.key.id.0),
                        target_bytes: 7,
                    },
                },
                model::TreeEntry {
                    name: "tracked".into(),
                    value: model::TreeValue::File {
                        id: model::FileId(file.key.id.0),
                        size_bytes: file.size_bytes,
                        executable: false,
                        copy_id: None,
                    },
                },
            ],
        }),
        model::Limits::default(),
    )?;
    let root = Descriptor::from_native(&tree.descriptor);
    let entries = vec![
        ManifestEntry {
            path: String::new(),
            descriptor: root.clone(),
            value: EntryValue::Directory {},
        },
        ManifestEntry {
            path: "link".into(),
            descriptor: Descriptor::from_native(&link.descriptor),
            value: EntryValue::Symlink { target_bytes: 7 },
        },
        ManifestEntry {
            path: "tracked".into(),
            descriptor: Descriptor::from_native(&file),
            value: EntryValue::File {
                executable: false,
                copy_id: None,
            },
        },
    ];
    let request = Request {
        schema: 1,
        job_id: "1".repeat(64),
        attempt_id: "2".repeat(64),
        repository_id: "forge-fixture".into(),
        candidate_commit: "3".repeat(64),
        root_tree: root.id.clone(),
        profile_name: "fixture".into(),
        profile_version: "4".repeat(64),
    };
    let limits = SnapshotLimits::default();
    let mut hasher = ManifestHasher::new(&request.candidate_commit, &request.root_tree)?;
    for entry in &entries {
        hasher.push(entry)?;
    }
    let totals = hasher.finish(&limits)?;
    let manifest = SnapshotManifest {
        schema: 1,
        lease: CheckerLease {
            schema: 1,
            lease_id: "5".repeat(64),
            generation: 1,
            scope: CandidateScope {
                account_id: "fixture".into(),
                view_version: "6".repeat(64),
                policy_epoch: 1,
                change_id: "7".repeat(32),
                patchset_id: "8".repeat(64),
                candidate_id: request.job_id.clone(),
                target_head: "9".repeat(64),
                target_generation: 1,
                publication_generation: 1,
            },
            manifest: ManifestBinding {
                id: "a".repeat(64),
                digest: totals.digest,
                entries: totals.entries,
                file_bytes: totals.file_bytes,
                path_bytes: totals.path_bytes,
                metadata_bytes: totals.metadata_bytes,
            },
            request,
            expires_at: 4102444800,
            limits,
        },
        entries,
    };
    manifest.validate()?;
    let path = dir.path().join("snapshot.json");
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    output.write_all(&serde_json::to_vec(&manifest)?)?;
    output.sync_all()?;
    drop(output);
    let plan = HelperPlan {
        manifest: path,
        staging: stage.clone(),
        source: dir.path().join("source"),
        proof: dir.path().join("proof.json"),
        invocation: manifest.lease.request.workspace_id(),
        bazel: std::env::current_exe()?,
        args: vec![
            "--batch".into(),
            "--fixture-child".into(),
            stage.to_str().context("fixture path encoding")?.into(),
        ],
    };
    let helper = fs::canonicalize(
        std::env::var_os("SNAPSHOT_HELPER")
            .or_else(|| option_env!("SNAPSHOT_HELPER").map(std::ffi::OsString::from))
            .context("declared SNAPSHOT_HELPER missing")?,
    )?;
    let mut command = Command::new(helper);
    command
        .env_clear()
        .current_dir(dir.path())
        .process_group(0)
        .args(["--manifest"])
        .arg(&plan.manifest)
        .arg("--staging")
        .arg(&plan.staging)
        .arg("--source")
        .arg(&plan.source)
        .arg("--proof")
        .arg(&plan.proof)
        .arg("--invocation")
        .arg(&plan.invocation)
        .arg("--bazel")
        .arg(&plan.bazel)
        .arg("--")
        .args(&plan.args);
    let mut child = command.spawn()?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if std::time::Instant::now() >= deadline {
            unsafe {
                libc::kill(-(child.id() as i32), libc::SIGKILL);
            }
            child.wait()?;
            anyhow::bail!("snapshot acceptance timeout");
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    ensure!(
        status.success(),
        "readonly snapshot helper unavailable or failed: {status}"
    );
    let proof = read_completed_proof(&plan, &manifest, 0)?;
    ensure!(
        proof.pid_namespace_contained && proof.pid_namespace_inode > 0,
        "PID namespace proof absent"
    );
    let identity: (u32, u64) = serde_json::from_slice(&fs::read(stage.join("detached.identity"))?)?;
    match fs::read_to_string(format!("/proc/{}/stat", identity.0)) {
        Ok(text) => ensure!(
            process_identity(&text)? != identity,
            "detached descendant survived namespace init exit"
        ),
        Err(error) => ensure!(
            error.kind() == std::io::ErrorKind::NotFound,
            "cannot inspect detached descendant identity"
        ),
    }
    let heartbeat = fs::read(stage.join("detached.heartbeat"))?;
    std::thread::sleep(std::time::Duration::from_millis(150));
    ensure!(
        fs::read(stage.join("detached.heartbeat"))? == heartbeat,
        "detached descendant heartbeat survived namespace init exit"
    );
    ensure!(
        fs::read(stage.join("tracked"))? == b"changed staging\n",
        "fixture did not exercise writable backing alias"
    );
    ensure!(
        fs::read_dir(&plan.source)?.next().is_none(),
        "snapshot source not torn down"
    );
    let mut rebound = plan.clone();
    rebound.args.push("different-intent".into());
    ensure!(
        read_completed_proof(&rebound, &manifest, 0).is_err(),
        "proof accepted rebound command"
    );
    println!(
        "test-only Linux namespace/tmpfs/readonly/capability/alias/teardown acceptance passed; no Bazel/RBE verdict claimed"
    );
    Ok(())
}
fn main() {
    #[cfg(target_os = "linux")]
    let result = {
        let args: Vec<String> = std::env::args().collect();
        if args.iter().any(|arg| arg == "--fixture-detached") {
            detached_fixture(&args)
        } else if args.iter().any(|arg| arg == "--fixture-child") {
            fixture_child(&args)
        } else {
            acceptance()
        }
    };
    #[cfg(not(target_os = "linux"))]
    let result: Result<()> = Err(anyhow::anyhow!("Linux acceptance requires Linux"));
    if let Err(error) = result {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
