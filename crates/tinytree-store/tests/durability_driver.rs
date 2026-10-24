//! Test-only process: never a product executable or a substitute for power loss.
mod common;

use std::io::{self, Read, Write};
use std::path::PathBuf;
use tinytree_store::{Config, Error, Store};

fn checkpoint(value: &str) -> io::Result<()> {
    println!("{value}");
    io::stdout().flush()
}
fn wait_for_kill() -> ! {
    loop {
        std::thread::park();
    }
}

struct InterruptedUpload {
    first: bool,
}
impl Read for InterruptedUpload {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.first {
            self.first = false;
            let prefix = &common::UNACKNOWLEDGED_BYTES[..3];
            output[..prefix.len()].copy_from_slice(prefix);
            return Ok(prefix.len());
        }
        // put_stream has already written the prefix to its private temporary
        // file, but cannot validate/install/index or acknowledge the object.
        checkpoint("MID_UPLOAD")?;
        wait_for_kill()
    }
}

#[cfg(unix)]
fn fail_regular_file_writes() -> io::Result<()> {
    // This process is disposable. Its own regular-file writes fail with EFBIG;
    // the parent, host filesystem capacity and pipe checkpoints are unaffected.
    unsafe {
        if libc::signal(libc::SIGXFSZ, libc::SIG_IGN) == libc::SIG_ERR {
            return Err(io::Error::last_os_error());
        }
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn fail_publication(store: &Store, retention: bool) -> Result<(), Box<dyn std::error::Error>> {
    let replacement = common::replacement();
    common::upload(store, &replacement)?;
    checkpoint("REPLACEMENT_PREPARED")?;
    let owner = tinytree_store::RetentionOwner {
        principal: [1; 32],
        client: [2; 16],
    };
    let hold = common::retention_publication("retention-attempt", 0, common::initial().head, true);
    fail_regular_file_writes()?;
    let result = if retention {
        store.publish_retention(
            common::REPOSITORY,
            owner,
            &hold,
            &tinytree_store::RetentionLimits::default(),
            100,
        )
    } else {
        store.publish(
            common::REPOSITORY,
            &common::publication("replacement", 1, replacement.head),
            100,
        )
    };
    let error = result.expect_err("RLIMIT_FSIZE must reject the actual journal write");
    if !matches!(error, Error::Database(fjall::Error::Io(ref io)) if io.raw_os_error() == Some(libc::EFBIG))
    {
        return Err(format!("expected an actual journal EFBIG failure, got {error}").into());
    }
    if !matches!(
        store.references(common::REPOSITORY, &[common::REFERENCE.into()]),
        Err(Error::Poisoned)
    ) || !matches!(
        store.publication(common::REPOSITORY, "replacement"),
        Err(Error::Poisoned)
    ) {
        return Err("an ambiguous journal failure must poison subsequent operations".into());
    }
    checkpoint("WRITE_FAILED_POISONED")?;
    wait_for_kill()
}

fn landing(store: &Store, mode: &str) -> Result<(), Box<dyn std::error::Error>> {
    common::upload(store, &common::replacement())?;
    store.begin_landing(
        common::REPOSITORY,
        common::LANDING_OWNER,
        &common::landing_request(),
        100,
    )?;
    let prepared =
        store.prepare_landing(common::REPOSITORY, common::LANDING_OWNER, "landing", 100)?;
    if mode == "landing-prepared" {
        checkpoint("LANDING_PREPARED_ACK")?;
        wait_for_kill()
    }
    // This trusted test fixture records a deterministic verdict; it does not
    // claim that a Bazel runner or external execution actually took place.
    store.record_landing_check(
        common::REPOSITORY,
        common::LANDING_OWNER,
        "landing",
        &common::landing_identity(&prepared),
        tinytree_store::LandingVerdict {
            passed: true,
            evidence: [8; 32],
        },
        100,
    )?;
    if mode == "landing-checked" {
        checkpoint("LANDING_CHECKED_ACK")?;
        wait_for_kill()
    }
    #[cfg(unix)]
    if mode == "landing-write-failure" {
        checkpoint("LANDING_CHECKED_ACK")?;
        fail_regular_file_writes()?;
        let error = store
            .finalize_landing(common::REPOSITORY, common::LANDING_OWNER, "landing", 100)
            .expect_err("journal write must fail");
        if !matches!(error,Error::Database(fjall::Error::Io(ref io)) if io.raw_os_error()==Some(libc::EFBIG))
        {
            return Err(format!("expected journal EFBIG, got {error}").into());
        }
        if !matches!(
            store.landing_job(common::REPOSITORY, common::LANDING_OWNER, "landing"),
            Err(Error::Poisoned)
        ) || !matches!(
            store.begin_landing(
                common::REPOSITORY,
                common::LANDING_OWNER,
                &common::landing_request(),
                100
            ),
            Err(Error::Poisoned)
        ) || !matches!(
            store.references(common::REPOSITORY, &[common::REFERENCE.into()]),
            Err(Error::Poisoned)
        ) {
            return Err("uncertain landing write did not poison subsequent ACKs".into());
        }
        checkpoint("WRITE_FAILED_POISONED")?;
        wait_for_kill()
    }
    store.finalize_landing(common::REPOSITORY, common::LANDING_OWNER, "landing", 100)?;
    checkpoint("LANDING_COMMITTED_ACK")?;
    wait_for_kill()
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let mode = args.next().ok_or("missing fixture mode")?;
    let root = PathBuf::from(args.next().ok_or("missing fixture root")?);
    if args.next().is_some() {
        return Err("unexpected fixture argument".into());
    }
    let store = Store::open(root, Config::default())?;
    store.create_repository(common::REPOSITORY)?;
    let initial = common::initial();
    common::upload(&store, &initial)?;
    store.publish(
        common::REPOSITORY,
        &common::publication("initial", 0, initial.head),
        100,
    )?;
    checkpoint("BASELINE_DURABLE")?;
    match mode.to_str().ok_or("non-UTF8 fixture mode")? {
        mode @ ("landing-prepared" | "landing-checked" | "landing-committed") => {
            landing(&store, mode)
        }
        #[cfg(unix)]
        "landing-write-failure" => landing(&store, "landing-write-failure"),
        "after-ack" => {
            checkpoint("DURABLE_ACK")?;
            wait_for_kill()
        }
        "after-pack" => {
            let report = store.pack(common::REPOSITORY, tinytree_store::PackOptions::default())?;
            let expected_members = initial.metadata.len() as u64 + 1;
            if report.pack_id.is_none()
                || report.packed_objects != expected_members
                || report.removed_loose_files != expected_members
            {
                return Err("acknowledged pack must replace every fixture payload".into());
            }
            // The pack and catalog publication have returned successfully, but
            // this owning process stays alive until the parent terminates it.
            checkpoint("PACK_DURABLE_ACK")?;
            wait_for_kill()
        }
        "mid-upload" => {
            let descriptor = tinytree_model::describe_file(common::UNACKNOWLEDGED_BYTES);
            store.put_stream(
                common::REPOSITORY,
                descriptor,
                &mut InterruptedUpload { first: true },
                100,
            )?;
            checkpoint("UNEXPECTED_UPLOAD_ACK")?;
            Err("interrupted upload must not finish".into())
        }
        #[cfg(unix)]
        "write-failure" => fail_publication(&store, false),
        #[cfg(unix)]
        "retention-write-failure" => fail_publication(&store, true),
        _ => Err("unsupported fixture mode".into()),
    }
}
fn main() {
    if let Err(error) = run() {
        // stdout is a pipe even in write-failure mode, so diagnosis is not lost
        // if RLIMIT_FSIZE also prevents writing a file-backed stderr handle.
        let _ = checkpoint(&format!("DRIVER_ERROR {error}"));
        std::process::exit(1);
    }
}
