//! Local bookkeeping for verified private bundles, not remote source authority.
//! The caller owns current context/coverage and durable remote-Deleted outbox.
use anyhow::{Context, Result, ensure};
use native_retention::{
    BundleDescriptor, CaptureCleanupObligation, CaptureId, CaptureUploadCleanup,
    OwnedCaptureCoverage, PendingCapture, Registration, Registry, RegistryId, RootCheckpoint,
};
use native_workspace_admin::private_bundle::VerifiedPrivateBundle;
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, fs::File, io::Read, path::PathBuf};

const MAX_PENDING: usize = 1024;
const MAX_BUNDLE: u64 = 512 * 1024 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalCaptureHandle {
    pub registry: RegistryId,
    pub registration: Registration,
    pub capture_id: CaptureId,
    pub ticket_binding: [u8; 32],
    pub bundle: BundleDescriptor,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExactFile {
    pub name: String,
    pub descriptor: BundleDescriptor,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CleanupFiles {
    pub bundle: ExactFile,
    pub intent: ExactFile,
    pub seal: ExactFile,
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn operation(text: &str) -> Result<Vec<u8>> {
    ensure!(
        text.len() == 128
            && text
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "private capture operation identity"
    );
    (0..text.len())
        .step_by(2)
        .map(|i| Ok(u8::from_str_radix(&text[i..i + 2], 16)?))
        .collect()
}
pub(crate) fn checkpoint(bundle: &VerifiedPrivateBundle) -> Result<RootCheckpoint> {
    let data = bundle.capture_data();
    let mut roots = data
        .roots
        .iter()
        .map(|r| {
            Ok(tinytree_model::ObjectKey {
                kind: tinytree_model::Kind::try_from(r.kind)?,
                id: tinytree_model::ObjectId(r.id),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    roots.sort_by_key(|root| (root.kind as u8, root.id.0));
    ensure!(
        roots.windows(2).all(|p| p[0] != p[1]),
        "private capture duplicate root"
    );
    let mut op_heads = data
        .operation_heads
        .iter()
        .map(|head| operation(head))
        .collect::<Result<Vec<_>>>()?;
    op_heads.sort();
    ensure!(
        op_heads.windows(2).all(|p| p[0] != p[1]),
        "private capture duplicate operation head"
    );
    Ok(RootCheckpoint {
        roots,
        op_heads,
        working_copy_operation: Some(operation(&data.working_copy.wc_operation)?),
    })
}
pub(crate) fn record(
    pending: &PendingCapture,
    bundle: &VerifiedPrivateBundle,
) -> Result<LocalCaptureHandle> {
    ensure!(
        pending.bundle
            == BundleDescriptor {
                blake3: bundle.blake3(),
                size_bytes: bundle.size_bytes()
            }
            && pending.checkpoint == checkpoint(bundle)?,
        "private bundle differs from native pending capture"
    );
    Ok(LocalCaptureHandle {
        registry: pending.registry,
        registration: pending.registration,
        capture_id: pending.id,
        ticket_binding: pending.binding(),
        bundle: pending.bundle,
    })
}
/// One bounded inventory, never a journal-sized checkpoint or digest-only match.
/// The caller keeps its actual capture/command admission across reconciliation.
pub(crate) async fn reconcile(
    registry: &Registry,
    registration: Registration,
    bundle: &VerifiedPrivateBundle,
) -> Result<LocalCaptureHandle> {
    let expected = checkpoint(bundle)?;
    let descriptor = BundleDescriptor {
        blake3: bundle.blake3(),
        size_bytes: bundle.size_bytes(),
    };
    let tickets = registry
        .pending_captures_for_registration(registration, MAX_PENDING, 32 * 1024 * 1024)
        .await?;
    let mut matched = None;
    for ticket in tickets {
        if ticket.registry == registry.id()
            && ticket.registration == registration
            && ticket.bundle == descriptor
            && ticket.checkpoint == expected
        {
            ensure!(
                matched.is_none(),
                "ambiguous private native capture inventory"
            );
            matched = Some(record(&ticket, bundle)?);
        }
    }
    matched.context("exact private native capture unavailable")
}
/// Return only a cloned exact trusted row; release the read fence before any
/// maintenance request. Bundle semantic proof is not authentication authority.
pub(crate) async fn resolve(
    registry: &Registry,
    handle: &LocalCaptureHandle,
    bundle: &VerifiedPrivateBundle,
) -> Result<PendingCapture> {
    ensure!(
        handle.registry == registry.id(),
        "private capture belongs to another registry"
    );
    let guard = registry.read_capture(handle.capture_id).await?;
    ensure!(
        record(guard.pending(), bundle)? == *handle,
        "private capture full binding changed"
    );
    let pending = guard.pending().clone();
    drop(guard);
    Ok(pending)
}
pub(crate) async fn retire(
    registry: &Registry,
    old: PendingCapture,
    successor: PendingCapture,
    intent: BundleDescriptor,
    seal: BundleDescriptor,
    coverage: OwnedCaptureCoverage,
) -> Result<CaptureCleanupObligation> {
    ensure!(
        old.registry == registry.id()
            && successor.registry == registry.id()
            && old.registration == successor.registration,
        "private retirement registration mismatch"
    );
    registry
        .retire_redundant_capture_with_coverage(
            old,
            successor,
            CaptureUploadCleanup {
                job: Some(intent),
                published: Some(seal),
            },
            coverage,
        )
        .await
        .map_err(Into::into)
}
fn file_limit(file: &ExactFile) -> Result<u64> {
    let (stem, suffix) = file
        .name
        .rsplit_once('.')
        .context("private artifact basename")?;
    ensure!(
        stem.len() == 64
            && stem
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "private artifact basename"
    );
    let limit = match suffix {
        "intent" => 32 * 1024 * 1024,
        "seal" => 4096,
        "private" => {
            ensure!(
                stem == hex(&file.descriptor.blake3),
                "private bundle basename differs"
            );
            MAX_BUNDLE
        }
        _ => anyhow::bail!("unsupported private artifact"),
    };
    ensure!(
        file.descriptor.size_bytes > 0 && file.descriptor.size_bytes <= limit,
        "private artifact size"
    );
    Ok(limit)
}
struct CheckedFile {
    path: PathBuf,
    file: File,
}
fn same_file(checked: &CheckedFile) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let path = std::fs::symlink_metadata(&checked.path)?;
        let file = checked.file.metadata()?;
        ensure!(
            path.is_file()
                && !path.file_type().is_symlink()
                && path.dev() == file.dev()
                && path.ino() == file.ino()
                && path.len() == file.len()
                && path.nlink() == 1
                && file.nlink() == 1
                && path.uid() == unsafe { libc::geteuid() }
                && path.mode() & 0o077 == 0,
            "private cleanup file identity changed"
        );
    }
    #[cfg(not(unix))]
    {
        anyhow::bail!("private cleanup requires Unix file identity");
    }
    Ok(())
}
fn verify(directory: &PrivateDir, exact: &ExactFile, missing: bool) -> Result<Option<CheckedFile>> {
    file_limit(exact)?;
    let path = directory.path().join(&exact.name);
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let mut file = match options.open(&path) {
        Ok(file) => file,
        Err(error) if missing && error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).context("private artifact unavailable"),
    };
    ensure!(
        file.metadata()?.len() == exact.descriptor.size_bytes,
        "private artifact size changed"
    );
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0u8; 65536];
    let mut read = 0u64;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        read = read
            .checked_add(count as u64)
            .context("private artifact size overflow")?;
        ensure!(read <= exact.descriptor.size_bytes, "private artifact grew");
        hash.update(&buffer[..count]);
    }
    ensure!(
        read == exact.descriptor.size_bytes
            && hash.finalize().as_bytes() == &exact.descriptor.blake3,
        "private artifact bytes changed"
    );
    let checked = CheckedFile { path, file };
    same_file(&checked)?;
    Ok(Some(checked))
}
/// Exact immutable format-specific bytes, not a fabricated native publication.
pub(crate) fn write_artifact(
    directory: &PrivateDir,
    name: &str,
    bytes: &[u8],
) -> Result<ExactFile> {
    let exact = ExactFile {
        name: name.into(),
        descriptor: BundleDescriptor {
            blake3: *blake3::hash(bytes).as_bytes(),
            size_bytes: bytes.len() as u64,
        },
    };
    file_limit(&exact)?;
    ensure!(
        name.ends_with(".intent") || name.ends_with(".seal"),
        "private journal artifact suffix"
    );
    match directory.create_descendant_file(name, bytes) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    let checked =
        verify(directory, &exact, false)?.context("private immutable artifact missing")?;
    // An earlier create may have reached visibility before a failed fsync ACK.
    // Exact replay repeats durability, never merely adopts visible bytes.
    checked.file.sync_all()?;
    File::open(directory.path())?.sync_all()?;
    Ok(exact)
}
fn validate_files(
    expected: &CaptureCleanupObligation,
    old: &CleanupFiles,
    new: &CleanupFiles,
) -> Result<()> {
    ensure!(
        expected.basis.is_none()
            && old.bundle.descriptor == expected.retired.bundle
            && new.bundle.descriptor == expected.successor.bundle
            && Some(old.intent.descriptor) == expected.upload.job
            && Some(old.seal.descriptor) == expected.upload.published,
        "private cleanup obligation/files differ"
    );
    for files in [old, new] {
        ensure!(
            files.bundle.name.ends_with(".private")
                && files.intent.name.ends_with(".intent")
                && files.seal.name.ends_with(".seal")
                && files.intent.name.trim_end_matches(".intent")
                    == files.seal.name.trim_end_matches(".seal"),
            "private cleanup artifact names differ"
        );
        for file in [&files.bundle, &files.intent, &files.seal] {
            file_limit(file)?;
        }
    }
    let protected = BTreeSet::from([&new.bundle.name, &new.intent.name, &new.seal.name]);
    ensure!(
        !protected.contains(&old.intent.name)
            && !protected.contains(&old.seal.name)
            && (!expected.remove_bundle || !protected.contains(&old.bundle.name)),
        "private cleanup would unlink successor"
    );
    Ok(())
}
fn check_protected(directory: &PrivateDir, files: &CleanupFiles) -> Result<()> {
    for file in [&files.bundle, &files.intent, &files.seal] {
        verify(directory, file, false)?.context("protected private successor missing")?;
    }
    Ok(())
}
/// Only an exact saved native obligation permits missing-file crash prefixes.
/// This function never retires a remote capsule or chooses a successor itself.
pub(crate) async fn cleanup(
    registry: &Registry,
    expected: &CaptureCleanupObligation,
    directory: &PrivateDir,
    files: CleanupFiles,
    successor_files: CleanupFiles,
) -> Result<u32> {
    ensure!(
        expected.registry == registry.id(),
        "private cleanup registry mismatch"
    );
    validate_files(expected, &files, &successor_files)?;
    let directory = directory.path().to_owned();
    let expected = expected.clone();
    let Some(guard) = registry.try_capture_cleanup().await? else {
        // ACK may have committed before the host outbox checkpoint. No unlink
        // occurs here: prove completed exact facts, never mint cleanup authority.
        match registry.read_capture(expected.retired.id).await {
            Err(native_retention::Error::UnknownCapture) => {}
            Err(error) => return Err(error.into()),
            Ok(_) => anyhow::bail!("private retired ticket remains without cleanup obligation"),
        }
        let successor = registry.read_capture(expected.successor.id).await?;
        let row = successor.pending();
        ensure!(
            row.registry == expected.registry
                && row.registration == expected.registration
                && row.id == expected.successor.id
                && row.bundle == expected.successor.bundle
                && row.binding() == expected.successor_binding,
            "private cleanup successor changed"
        );
        ensure!(
            registry.capture_cleanup().await?.is_none(),
            "another native cleanup is pending"
        );
        return tokio::task::spawn_blocking(move || {
            let _successor = successor;
            let directory = PrivateDir::open_existing(directory)?;
            check_protected(&directory, &successor_files)?;
            for file in [&files.intent, &files.seal] {
                ensure!(
                    verify(&directory, file, true)?.is_none(),
                    "private cleanup ACK lacks completed unlink"
                );
            }
            if expected.remove_bundle {
                ensure!(
                    verify(&directory, &files.bundle, true)?.is_none(),
                    "private retired bundle remains after cleanup ACK"
                );
            } else {
                verify(&directory, &files.bundle, false)?
                    .context("shared private bundle missing")?;
            }
            Ok(0)
        })
        .await
        .context("private cleanup completion worker failed")?;
    };
    ensure!(
        guard.obligation() == &expected,
        "private cleanup obligation changed"
    );
    let (guard, count) = tokio::task::spawn_blocking(move || -> Result<_> {
        let directory = PrivateDir::open_existing(directory)?;
        check_protected(&directory, &successor_files)?;
        let mut selected = Vec::new();
        if expected.remove_bundle {
            selected.push(verify(&directory, &files.bundle, true)?);
        } else {
            verify(&directory, &files.bundle, false)?.context("shared private bundle missing")?;
        }
        selected.push(verify(&directory, &files.intent, true)?);
        selected.push(verify(&directory, &files.seal, true)?);
        let mut count = 0;
        for file in selected.into_iter().flatten() {
            same_file(&file)?;
            std::fs::remove_file(&file.path)?;
            count += 1;
        }
        File::open(directory.path())?.sync_all()?;
        Ok((guard, count))
    })
    .await
    .context("private cleanup worker failed")??;
    guard.acknowledge_capture_cleanup().await?;
    Ok(count)
}
