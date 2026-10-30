//! Kernel read-only snapshots for operator-trusted checks. This is not a
//! hostile-code sandbox. The supervisor owns namespace custody until teardown;
//! a missing terminal proof is never evidence of successful cleanup.
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tinytree_forge_core::checker::{EntryValue, ManifestEntry, SnapshotManifest};

/// Only these internal static labels are suitable for subprocess diagnostics.
/// Underlying errors may contain private local paths and are never printed by
/// the helper entry point.
#[derive(Debug)]
struct SnapshotStage(&'static str);
impl std::fmt::Display for SnapshotStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for SnapshotStage {}
#[derive(Debug)]
struct SnapshotContext {
    stage: &'static str,
    source: anyhow::Error,
}
impl std::fmt::Display for SnapshotContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.stage)
    }
}
impl std::error::Error for SnapshotContext {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
fn phase_error(stage: &'static str, source: impl Into<anyhow::Error>) -> anyhow::Error {
    anyhow::Error::new(SnapshotContext {
        stage,
        source: source.into(),
    })
}
pub fn diagnostic_stage(error: &anyhow::Error) -> &'static str {
    error
        .chain()
        .filter_map(|cause| {
            cause
                .downcast_ref::<SnapshotStage>()
                .map(|stage| stage.0)
                .or_else(|| {
                    cause
                        .downcast_ref::<SnapshotContext>()
                        .map(|context| context.stage)
                })
        })
        .last()
        .unwrap_or("snapshot_unclassified")
}

const MANIFEST_BYTES: u64 = 32 * 1024 * 1024;
const PROOF_BYTES: u64 = 16 * 1024;
#[derive(Clone, Debug)]
pub struct HelperPlan {
    pub manifest: PathBuf,
    pub staging: PathBuf,
    pub source: PathBuf,
    pub proof: PathBuf,
    pub invocation: String,
    pub bazel: PathBuf,
    pub args: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotProof {
    pub schema: u32,
    pub workspace_id: String,
    pub manifest_binding: String,
    pub command_digest: String,
    pub source: PathBuf,
    pub namespace_inode: u64,
    pub pid_namespace_inode: u64,
    pub pid_namespace_contained: bool,
    pub mount_id: u64,
    pub device: u64,
    pub inode: u64,
    pub before_verified: bool,
    pub after_verified: bool,
    pub readonly_superblock: bool,
    pub capabilities_dropped: bool,
    pub no_new_privs: bool,
    pub teardown_confirmed: bool,
    pub exit_code: i32,
}
fn validate_plan(plan: &HelperPlan) -> Result<()> {
    use std::path::Component;
    for p in [
        &plan.manifest,
        &plan.staging,
        &plan.source,
        &plan.proof,
        &plan.bazel,
    ] {
        ensure!(
            p.is_absolute()
                && p.as_os_str().len() <= 4096
                && p.components()
                    .all(|c| matches!(c, Component::RootDir | Component::Normal(_))),
            "invalid helper path"
        );
    }
    ensure!(
        plan.invocation.len() == 64
            && plan
                .invocation
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid helper invocation"
    );
    ensure!(
        plan.args.len() <= 256
            && plan.args.iter().map(|a| a.len()).sum::<usize>() <= 64 * 1024
            && plan.args.iter().all(|a| !a.contains('\0')),
        "helper command limit"
    );
    ensure!(
        plan.args.iter().filter(|a| a.as_str() == "--batch").count() == 1,
        "snapshot checks require batch mode"
    );
    ensure!(
        !plan
            .args
            .iter()
            .any(|a| a == "--nobatch" || a.starts_with("--batch=")),
        "conflicting batch mode"
    );
    ensure!(
        plan.source != plan.staging
            && !plan.source.starts_with(&plan.staging)
            && !plan.staging.starts_with(&plan.source),
        "overlapping snapshot paths"
    );
    ensure!(
        !plan.proof.starts_with(&plan.source)
            && !plan.manifest.starts_with(&plan.source)
            && !plan.bazel.starts_with(&plan.source),
        "helper control path in source"
    );
    Ok(())
}
fn command_digest(plan: &HelperPlan) -> Result<String> {
    let bytes = serde_json::to_vec(&(&plan.bazel, &plan.args, &plan.invocation, &plan.source))?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"tinytree-readonly-check-command-v1\0");
    hash.update(&bytes);
    Ok(hash.finalize().to_hex().to_string())
}
fn bounded_private_read(path: &Path, max: u64) -> Result<Vec<u8>> {
    private_fs::PrivateDir::open_existing(path.parent().context("missing private parent")?)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    }
    let file = options.open(path)?;
    let meta = file.metadata()?;
    ensure!(
        meta.is_file() && meta.len() <= max,
        "helper input size/type differs"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            meta.uid() == unsafe { libc::geteuid() }
                && meta.mode() & 0o077 == 0
                && meta.nlink() == 1,
            "helper input is not private"
        );
    }
    let mut bytes = Vec::new();
    file.take(max + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64 <= max, "helper input grew");
    Ok(bytes)
}
pub fn read_manifest(path: &Path) -> Result<SnapshotManifest> {
    let value: SnapshotManifest =
        serde_json::from_slice(&bounded_private_read(path, MANIFEST_BYTES)?)?;
    value.validate()?;
    Ok(value)
}
pub fn read_completed_proof(
    plan: &HelperPlan,
    manifest: &SnapshotManifest,
    expected_exit: i32,
) -> Result<SnapshotProof> {
    validate_plan(plan)?;
    manifest.validate()?;
    ensure!(
        matches!(expected_exit, 0 | 1 | 3),
        "helper did not complete a supported check"
    );
    let proof: SnapshotProof =
        serde_json::from_slice(&bounded_private_read(&plan.proof, PROOF_BYTES)?)?;
    ensure!(
        proof.schema == 1
            && proof.workspace_id == manifest.lease.request.workspace_id()
            && proof.workspace_id == plan.invocation
            && proof.manifest_binding == manifest.immutable_binding()?
            && proof.command_digest == command_digest(plan)?
            && proof.source == plan.source
            && proof.namespace_inode > 0
            && proof.pid_namespace_inode > 0
            && proof.pid_namespace_contained
            && proof.mount_id > 0
            && proof.inode > 0
            && proof.before_verified
            && proof.after_verified
            && proof.readonly_superblock
            && proof.capabilities_dropped
            && proof.no_new_privs
            && proof.teardown_confirmed
            && proof.exit_code == expected_exit,
        "snapshot terminal proof differs"
    );
    Ok(proof)
}
#[cfg(not(target_os = "linux"))]
pub fn run_helper(_: HelperPlan) -> Result<i32> {
    anyhow::bail!("read-only snapshot helper requires Linux")
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::{
        collections::{BTreeMap, BTreeSet, VecDeque},
        ffi::{CString, OsStr},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::{
                ffi::OsStrExt,
                fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
                process::CommandExt,
            },
        },
        process::Command,
    };
    fn c(value: &OsStr) -> Result<CString> {
        Ok(CString::new(value.as_bytes())?)
    }
    fn checked(result: libc::c_int) -> std::io::Result<()> {
        if result < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
    fn open_root(path: &Path) -> Result<File> {
        Ok(OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?)
    }
    fn open_relative(root: &File, path: &str, flags: i32, mode: u32) -> Result<File> {
        let parts = path.split('/').collect::<Vec<_>>();
        ensure!(
            !parts.is_empty()
                && parts
                    .iter()
                    .all(|p| !p.is_empty() && *p != "." && *p != ".." && !p.contains('\0')),
            "invalid snapshot path"
        );
        let mut parent = root.try_clone()?;
        for part in &parts[..parts.len() - 1] {
            let name = CString::new(*part)?;
            let fd = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            ensure!(fd >= 0, "snapshot directory unavailable");
            parent = unsafe { File::from_raw_fd(fd) };
        }
        let name = CString::new(*parts.last().unwrap())?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode,
            )
        };
        ensure!(fd >= 0, "snapshot entry unavailable");
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    fn parent_and_name(root: &File, path: &str) -> Result<(File, CString)> {
        let (parent, name) = path.rsplit_once('/').unwrap_or(("", path));
        let fd = if parent.is_empty() {
            root.try_clone()?
        } else {
            open_relative(root, parent, libc::O_RDONLY | libc::O_DIRECTORY, 0)?
        };
        Ok((fd, CString::new(name)?))
    }
    fn symlink_bytes(root: &File, path: &str) -> Result<Vec<u8>> {
        let (parent, name) = parent_and_name(root, path)?;
        let mut bytes = vec![0u8; 4097];
        let n = unsafe {
            libc::readlinkat(
                parent.as_raw_fd(),
                name.as_ptr(),
                bytes.as_mut_ptr().cast(),
                bytes.len(),
            )
        };
        ensure!((0..4097).contains(&n), "snapshot link limit");
        bytes.truncate(n as usize);
        Ok(bytes)
    }
    fn check_link(path: &str, bytes: &[u8]) -> Result<()> {
        let target = std::str::from_utf8(bytes)?;
        ensure!(
            !target.is_empty() && !target.starts_with('/') && !target.contains('\0'),
            "unsupported snapshot link"
        );
        let mut depth = path.split('/').count() - 1;
        for component in target.split('/') {
            match component {
                "" | "." => {}
                ".." => {
                    ensure!(depth > 0, "escaping snapshot link");
                    depth -= 1;
                }
                _ => depth += 1,
            }
        }
        Ok(())
    }
    fn entry_bytes(root: &File, entry: &ManifestEntry) -> Result<Option<Vec<u8>>> {
        let limits = tinytree_model::Limits {
            max_object_bytes: 1024 * 1024,
            max_items: 32768,
            max_field_bytes: 1024 * 1024,
            max_path_bytes: 4096,
        };
        match &entry.value {
            EntryValue::Directory {} => {
                let file = if entry.path.is_empty() {
                    root.try_clone()?
                } else {
                    open_relative(root, &entry.path, libc::O_RDONLY | libc::O_DIRECTORY, 0)?
                };
                let m = file.metadata()?;
                ensure!(
                    m.is_dir() && m.mode() & 0o7777 == 0o700,
                    "snapshot directory mode differs"
                );
                Ok(None)
            }
            EntryValue::File { executable, .. } => {
                let file = open_relative(root, &entry.path, libc::O_RDONLY, 0)?;
                let m = file.metadata()?;
                ensure!(
                    m.is_file()
                        && m.nlink() == 1
                        && m.len() == entry.descriptor.size_bytes
                        && m.mode() & 0o7777 == if *executable { 0o755 } else { 0o644 },
                    "snapshot file attributes differ"
                );
                let mut bytes = Vec::new();
                file.take(entry.descriptor.size_bytes + 1)
                    .read_to_end(&mut bytes)?;
                tinytree_model::verify(&entry.descriptor.native()?, &bytes, limits)?;
                Ok(Some(bytes))
            }
            EntryValue::Symlink { target_bytes } => {
                let bytes = symlink_bytes(root, &entry.path)?;
                ensure!(
                    bytes.len() as u64 == *target_bytes,
                    "snapshot link length differs"
                );
                check_link(&entry.path, &bytes)?;
                let encoded = tinytree_model::encode(
                    &tinytree_model::Object::Symlink(std::str::from_utf8(&bytes)?.to_owned()),
                    limits,
                )?;
                ensure!(
                    encoded.descriptor == entry.descriptor.native()?,
                    "snapshot link identity differs"
                );
                Ok(Some(bytes))
            }
        }
    }
    fn coverage(root: &File, manifest: &SnapshotManifest) -> Result<()> {
        let expected: BTreeSet<&str> = manifest.entries.iter().map(|e| e.path.as_str()).collect();
        let mut count = 1u64;
        let mut stack = vec![(root.try_clone()?, String::new(), 0u32)];
        while let Some((directory, prefix, depth)) = stack.pop() {
            for item in fs::read_dir(format!("/proc/self/fd/{}", directory.as_raw_fd()))? {
                let item = item?;
                let name = item.file_name();
                let name = name.to_str().context("snapshot entry encoding")?;
                let path = if prefix.is_empty() {
                    name.to_owned()
                } else {
                    format!("{prefix}/{name}")
                };
                count += 1;
                ensure!(
                    count <= manifest.lease.limits.max_entries && expected.contains(path.as_str()),
                    "snapshot coverage differs"
                );
                let ty = item.file_type()?;
                if ty.is_dir() {
                    ensure!(
                        depth < manifest.lease.limits.max_depth,
                        "snapshot tree depth"
                    );
                    stack.push((
                        open_relative(root, &path, libc::O_RDONLY | libc::O_DIRECTORY, 0)?,
                        path,
                        depth + 1,
                    ));
                } else {
                    ensure!(ty.is_file() || ty.is_symlink(), "snapshot special entry");
                }
            }
        }
        ensure!(
            count == manifest.entries.len() as u64,
            "snapshot coverage incomplete"
        );
        Ok(())
    }
    fn validate_links(root: &File, manifest: &SnapshotManifest) -> Result<()> {
        let paths: BTreeMap<&str, &ManifestEntry> = manifest
            .entries
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect();
        for entry in &manifest.entries {
            if matches!(entry.value, EntryValue::Symlink { .. }) {
                let target = symlink_bytes(root, &entry.path)?;
                let mut parts: Vec<String> = entry.path.split('/').map(str::to_owned).collect();
                parts.pop();
                let mut pending: VecDeque<String> = std::str::from_utf8(&target)?
                    .split('/')
                    .map(str::to_owned)
                    .collect();
                let mut links = 0;
                let mut steps = 0;
                while let Some(part) = pending.pop_front() {
                    steps += 1;
                    ensure!(steps <= 4096, "snapshot link expansion limit");
                    match part.as_str() {
                        "" | "." => {}
                        ".." => {
                            ensure!(parts.pop().is_some(), "escaping snapshot link");
                        }
                        _ => {
                            parts.push(part);
                            ensure!(
                                parts.iter().map(|p| p.len() + 1).sum::<usize>() <= 4096,
                                "snapshot link path limit"
                            );
                            if let Some(next) = paths.get(parts.join("/").as_str()) {
                                if matches!(next.value, EntryValue::Symlink { .. }) {
                                    links += 1;
                                    ensure!(links <= 40, "snapshot link expansion limit");
                                    parts.pop();
                                    let bytes = symlink_bytes(root, &next.path)?;
                                    check_link(&next.path, &bytes)?;
                                    for component in std::str::from_utf8(&bytes)?.split('/').rev() {
                                        pending.push_front(component.to_owned());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
    fn validate_trees(manifest: &SnapshotManifest) -> Result<()> {
        use tinytree_forge_core::checker::parse_id;
        use tinytree_model::{
            CopyId, FileId, Object, SymlinkId, Tree, TreeEntry, TreeId, TreeValue,
        };
        let mut trees: BTreeMap<&str, Vec<TreeEntry>> = manifest
            .entries
            .iter()
            .filter_map(|entry| {
                matches!(entry.value, EntryValue::Directory {})
                    .then_some((entry.path.as_str(), Vec::new()))
            })
            .collect();
        for entry in &manifest.entries {
            if entry.path.is_empty() {
                continue;
            }
            let (parent, name) = entry
                .path
                .rsplit_once('/')
                .unwrap_or(("", entry.path.as_str()));
            let value = match &entry.value {
                EntryValue::Directory {} => {
                    TreeValue::Tree(TreeId(parse_id(&entry.descriptor.id)?))
                }
                EntryValue::File {
                    executable,
                    copy_id,
                } => TreeValue::File {
                    id: FileId(parse_id(&entry.descriptor.id)?),
                    executable: *executable,
                    size_bytes: entry.descriptor.size_bytes,
                    copy_id: copy_id
                        .as_ref()
                        .map(|id| parse_id(id).map(CopyId))
                        .transpose()?,
                },
                EntryValue::Symlink { target_bytes } => TreeValue::Symlink {
                    id: SymlinkId(parse_id(&entry.descriptor.id)?),
                    target_bytes: *target_bytes,
                },
            };
            trees
                .get_mut(parent)
                .context("snapshot parent directory missing")?
                .push(TreeEntry {
                    name: name.to_owned(),
                    value,
                });
        }
        let limits = tinytree_model::Limits {
            max_object_bytes: 256 * 1024,
            max_items: 32768,
            max_field_bytes: 1024 * 1024,
            max_path_bytes: 4096,
        };
        for entry in &manifest.entries {
            if matches!(entry.value, EntryValue::Directory {}) {
                let entries = trees
                    .remove(entry.path.as_str())
                    .context("snapshot directory missing")?;
                let object = tinytree_model::encode(&Object::Tree(Tree { entries }), limits)?;
                ensure!(
                    object.descriptor == entry.descriptor.native()?,
                    "snapshot tree identity differs"
                );
            }
        }
        Ok(())
    }
    fn verify(root: &File, manifest: &SnapshotManifest) -> Result<()> {
        validate_trees(manifest)?;
        coverage(root, manifest)?;
        for entry in &manifest.entries {
            entry_bytes(root, entry)?;
        }
        validate_links(root, manifest)?;
        Ok(())
    }
    fn copy(root: &File, target: &File, manifest: &SnapshotManifest) -> Result<()> {
        coverage(root, manifest)?;
        for entry in &manifest.entries {
            let bytes = entry_bytes(root, entry)?;
            match &entry.value {
                EntryValue::Directory {} => {
                    if !entry.path.is_empty() {
                        let (parent, name) = parent_and_name(target, &entry.path)?;
                        checked(unsafe {
                            libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700)
                        })?;
                    }
                }
                EntryValue::File { executable, .. } => {
                    let mut file = open_relative(
                        target,
                        &entry.path,
                        libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                        0o600,
                    )?;
                    file.write_all(&bytes.context("missing file bytes")?)?;
                    checked(unsafe {
                        libc::fchmod(file.as_raw_fd(), if *executable { 0o755 } else { 0o644 })
                    })?;
                }
                EntryValue::Symlink { .. } => {} // Links are created only after all regular writes.
            }
        }
        for entry in &manifest.entries {
            if matches!(entry.value, EntryValue::Symlink { .. }) {
                let bytes = entry_bytes(root, entry)?.context("missing link bytes")?;
                let value = CString::new(bytes)?;
                let (parent, name) = parent_and_name(target, &entry.path)?;
                checked(unsafe {
                    libc::symlinkat(value.as_ptr(), parent.as_raw_fd(), name.as_ptr())
                })?;
            }
        }
        Ok(())
    }
    fn namespace() -> Result<()> {
        let uid = unsafe { libc::geteuid() };
        let gid = unsafe { libc::getegid() };
        checked(unsafe { libc::unshare(libc::CLONE_NEWUSER | libc::CLONE_NEWNS) })
            .map_err(|error| phase_error("snapshot_user_mount_namespace", error))?;
        fs::write("/proc/self/setgroups", "deny")
            .map_err(|error| phase_error("snapshot_setgroups_map", error))?;
        fs::write("/proc/self/uid_map", format!("0 {uid} 1\n"))
            .map_err(|error| phase_error("snapshot_uid_map", error))?;
        fs::write("/proc/self/gid_map", format!("0 {gid} 1\n"))
            .map_err(|error| phase_error("snapshot_gid_map", error))?;
        checked(unsafe {
            libc::mount(
                std::ptr::null(),
                c"/".as_ptr(),
                std::ptr::null(),
                libc::MS_REC | libc::MS_PRIVATE,
                std::ptr::null(),
            )
        })
        .map_err(|error| phase_error("snapshot_private_propagation", error))?;
        checked(unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) })?;
        Ok(())
    }
    #[derive(PartialEq, Eq)]
    struct MountIdentity {
        namespace: u64,
        mount: u64,
        device: u64,
        inode: u64,
    }
    fn mount_identity(root: &File) -> Result<MountIdentity> {
        let mut stat: libc::statx = unsafe { std::mem::zeroed() };
        checked(unsafe {
            libc::statx(
                root.as_raw_fd(),
                c"".as_ptr(),
                libc::AT_EMPTY_PATH,
                libc::STATX_MNT_ID,
                &mut stat,
            )
        })
        .map_err(|error| phase_error("snapshot_mount_statx", error))?;
        ensure!(
            stat.stx_mask & libc::STATX_MNT_ID != 0,
            SnapshotStage("snapshot_mount_id_unsupported")
        );
        let mut vfs: libc::statvfs = unsafe { std::mem::zeroed() };
        checked(unsafe { libc::fstatvfs(root.as_raw_fd(), &mut vfs) })
            .map_err(|error| phase_error("snapshot_mount_statvfs", error))?;
        let mut fsinfo: libc::statfs = unsafe { std::mem::zeroed() };
        checked(unsafe { libc::fstatfs(root.as_raw_fd(), &mut fsinfo) })
            .map_err(|error| phase_error("snapshot_mount_statfs", error))?;
        ensure!(
            fsinfo.f_type == libc::TMPFS_MAGIC,
            SnapshotStage("snapshot_mount_not_tmpfs")
        );
        ensure!(
            vfs.f_flag & libc::ST_RDONLY != 0,
            SnapshotStage("snapshot_mount_not_readonly")
        );
        // statvfs reports the mount view. Also require the superblock's own ro
        // option: a per-mount read-only bind is deliberately insufficient.
        let mut bytes = Vec::new();
        File::open("/proc/self/mountinfo")
            .map_err(|error| phase_error("snapshot_mountinfo_open", error))?
            .take(8 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| phase_error("snapshot_mountinfo_read", error))?;
        ensure!(
            bytes.len() <= 8 * 1024 * 1024,
            SnapshotStage("snapshot_mountinfo_limit")
        );
        let text = std::str::from_utf8(&bytes)?;
        let mount_id = stat.stx_mnt_id.to_string();
        let line = text
            .lines()
            .find(|line| line.split_whitespace().next() == Some(mount_id.as_str()))
            .context("source mount absent")
            .map_err(|error| phase_error("snapshot_mountinfo_id_absent", error))?;
        let (_, tail) = line.split_once(" - ").context("invalid mount inventory")?;
        ensure!(
            tail.split_whitespace()
                .nth(2)
                .is_some_and(|options| options.split(',').any(|o| o == "ro")),
            SnapshotStage("snapshot_superblock_not_readonly")
        );
        let m = root.metadata()?;
        Ok(MountIdentity {
            namespace: fs::metadata("/proc/self/ns/mnt")
                .map_err(|error| phase_error("snapshot_mount_namespace_stat", error))?
                .ino(),
            mount: stat.stx_mnt_id,
            device: m.dev(),
            inode: m.ino(),
        })
    }
    #[repr(C)]
    struct CapHeader {
        version: u32,
        pid: i32,
    }
    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct CapData {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    fn drop_child_privileges() -> std::io::Result<()> {
        // Fixed-stack/syscall-only: invoked in Command::pre_exec after fork.
        // The sole spawned child must be init of our newly created PIDNS.
        if unsafe { libc::getpid() } != 1 {
            return Err(std::io::Error::from_raw_os_error(libc::EPERM));
        }
        checked(unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) })?;
        checked(unsafe { libc::prctl(libc::PR_SET_SECUREBITS, 1 | 2 | 4 | 8 | 32, 0, 0, 0) })?;
        checked(unsafe {
            libc::prctl(
                libc::PR_CAP_AMBIENT,
                libc::PR_CAP_AMBIENT_CLEAR_ALL,
                0,
                0,
                0,
            )
        })?;
        for cap in 0..64 {
            let result = unsafe { libc::prctl(libc::PR_CAPBSET_DROP, cap, 0, 0, 0) };
            if result < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EINVAL) {
                return Err(std::io::Error::last_os_error());
            }
            let check = unsafe { libc::prctl(libc::PR_CAPBSET_READ, cap, 0, 0, 0) };
            if check > 0 {
                return Err(std::io::Error::from_raw_os_error(libc::EPERM));
            }
            if check < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EINVAL) {
                return Err(std::io::Error::last_os_error());
            }
        }
        let header = CapHeader {
            version: 0x20080522,
            pid: 0,
        };
        let mut data = [CapData::default(); 2];
        checked(unsafe { libc::syscall(libc::SYS_capset, &header, data.as_ptr()) as i32 })?;
        checked(unsafe { libc::syscall(libc::SYS_capget, &header, data.as_mut_ptr()) as i32 })?;
        if data
            .iter()
            .any(|d| d.effective != 0 || d.permitted != 0 || d.inheritable != 0)
            || unsafe { libc::prctl(libc::PR_GET_NO_NEW_PRIVS, 0, 0, 0, 0) } != 1
        {
            return Err(std::io::Error::from_raw_os_error(libc::EPERM));
        }
        // Do not close Command's internal exec-error pipe before exec. CLOEXEC
        // on every fd >=3 closes management/namespace descriptors atomically.
        checked(unsafe { libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 4u32) as i32 })?;
        Ok(())
    }
    pub(super) fn run(plan: HelperPlan) -> Result<i32> {
        validate_plan(&plan).map_err(|error| phase_error("snapshot_plan", error))?;
        let manifest = read_manifest(&plan.manifest)
            .map_err(|error| phase_error("snapshot_manifest", error))?;
        ensure!(
            plan.invocation == manifest.lease.request.workspace_id(),
            SnapshotStage("snapshot_request_binding")
        );
        ensure!(
            !plan.proof.try_exists()? && !plan.source.try_exists()?,
            SnapshotStage("snapshot_attempt_exists")
        );
        private_fs::PrivateDir::open_existing(
            plan.source.parent().context("source parent missing")?,
        )
        .map_err(|error| phase_error("snapshot_source_parent", error))?;
        private_fs::PrivateDir::open_existing(plan.proof.parent().context("proof parent missing")?)
            .map_err(|error| phase_error("snapshot_proof_parent", error))?;
        private_fs::PrivateDir::open_existing(&plan.staging)
            .map_err(|error| phase_error("snapshot_staging_private", error))?;
        let staging = open_root(&plan.staging)
            .map_err(|error| phase_error("snapshot_staging_open", error))?;
        verify(&staging, &manifest)
            .map_err(|error| phase_error("snapshot_staging_verify", error))?;
        // Keep already-validated parent descriptors across userns entry: host
        // root-owned ancestors are intentionally unmapped inside that userns.
        let source_parent = open_root(plan.source.parent().context("source parent missing")?)?;
        let proof_parent = open_root(plan.proof.parent().context("proof parent missing")?)?;
        let source_name = plan
            .source
            .file_name()
            .context("source name missing")?
            .to_str()
            .context("source name encoding")?
            .to_owned();
        let proof_name = plan
            .proof
            .file_name()
            .context("proof name missing")?
            .to_str()
            .context("proof name encoding")?
            .to_owned();
        fs::DirBuilder::new()
            .mode(0o700)
            .create(&plan.source)
            .map_err(|error| phase_error("snapshot_source_create", error))?;
        namespace()?;
        let target = c(plan.source.as_os_str())?;
        // A separate tmpfs superblock eliminates writable staging inode aliases.
        // Explicit capacity bounds data plus directory/inode overhead; ENOSPC is
        // Unavailable, never a reason to weaken source limits or use chmod.
        let limit = manifest
            .lease
            .limits
            .max_total_bytes
            .checked_add(64 * 1024 * 1024)
            .context("snapshot capacity overflow")?;
        let options = CString::new(format!(
            "size={limit},nr_inodes={},mode=0700",
            manifest.lease.limits.max_entries + 1
        ))?;
        checked(unsafe {
            libc::mount(
                c"tinytree-check-snapshot".as_ptr(),
                target.as_ptr(),
                c"tmpfs".as_ptr(),
                libc::MS_NOSUID | libc::MS_NODEV,
                options.as_ptr().cast(),
            )
        })
        .map_err(|error| phase_error("snapshot_tmpfs_mount", error))?;
        let mounted_work = (|| -> Result<(i32, MountIdentity, u64)> {
            // A retained directory fd still names its pre-unshare mount tree.
            // Reopen this validated parent in the CURRENT mount namespace so
            // following its source child reaches the new tmpfs, not backing
            // disk inodes. Compare the parent identity before using it.
            let namespace_parent =
                open_root(plan.source.parent().context("source parent missing")?)
                    .map_err(|error| phase_error("snapshot_namespace_parent_open", error))?;
            let old_parent = source_parent
                .metadata()
                .map_err(|error| phase_error("snapshot_parent_identity", error))?;
            let new_parent = namespace_parent
                .metadata()
                .map_err(|error| phase_error("snapshot_parent_identity", error))?;
            ensure!(
                (old_parent.dev(), old_parent.ino()) == (new_parent.dev(), new_parent.ino()),
                SnapshotStage("snapshot_parent_rebound")
            );
            let root = open_relative(
                &namespace_parent,
                &source_name,
                libc::O_RDONLY | libc::O_DIRECTORY,
                0,
            )
            .map_err(|error| phase_error("snapshot_namespace_source_open", error))?;
            copy(&staging, &root, &manifest)
                .map_err(|error| phase_error("snapshot_copy", error))?;
            drop(staging);
            // No writable file descriptors survive copy(). This is a filesystem
            // remount, without MS_BIND, so all source aliases inherit SB_RDONLY.
            checked(unsafe {
                libc::mount(
                    std::ptr::null(),
                    target.as_ptr(),
                    std::ptr::null(),
                    libc::MS_REMOUNT | libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV,
                    std::ptr::null(),
                )
            })
            .map_err(|error| phase_error("snapshot_readonly_remount", error))?;
            verify(&root, &manifest)
                .map_err(|error| phase_error("snapshot_before_verify", error))?;
            let before = mount_identity(&root)
                .map_err(|error| phase_error("snapshot_before_mount", error))?;
            // The supervisor remains outside the child PID namespace and is
            // still process::execute's direct group leader. Only the next
            // child enters this PIDNS. Its init exit causes the kernel to kill
            // and reap even setsid/nested-namespace descendants before wait
            // completes, unlike a process-group-only descendant census.
            let supervisor_pid_namespace = fs::metadata("/proc/self/ns/pid")?.ino();
            checked(unsafe { libc::unshare(libc::CLONE_NEWPID) })
                .map_err(|error| phase_error("snapshot_pid_namespace", error))?;
            let child_pid_namespace = fs::metadata("/proc/self/ns/pid_for_children")?.ino();
            ensure!(
                child_pid_namespace > 0 && child_pid_namespace != supervisor_pid_namespace,
                "child PID namespace absent"
            );
            let mut command = Command::new(&plan.bazel);
            command.args(&plan.args).current_dir(&plan.source);
            // Environment is the runner's already cleared build environment;
            // this helper never resolves credentials or management variables.
            unsafe {
                command.pre_exec(drop_child_privileges);
            }
            let status = command
                .status()
                .map_err(|error| phase_error("snapshot_child_exec", error))?;
            let exit = status.code().context("checker process was signalled")?;
            verify(&root, &manifest)
                .map_err(|error| phase_error("snapshot_after_verify", error))?;
            ensure!(
                mount_identity(&root)? == before,
                "source mount identity changed"
            );
            Ok((exit, before, child_pid_namespace))
        })();
        // A normal unmount (not lazy detach) is required. On failure there is
        // no terminal proof. Namespace/process custody remains an outer-runner
        // cleanup obligation; never acknowledge cleanup merely on process exit.
        let unmounted = checked(unsafe { libc::umount2(target.as_ptr(), 0) });
        // Attempt cleanup even on an earlier failure, but preserve that first
        // static phase for diagnostics. Neither error can produce a proof.
        let (exit, identity, pid_namespace_inode) = mounted_work?;
        unmounted.map_err(|error| phase_error("snapshot_unmount", error))?;
        let underlying = open_relative(
            &source_parent,
            &source_name,
            libc::O_RDONLY | libc::O_DIRECTORY,
            0,
        )?;
        ensure!(
            underlying.metadata()?.dev() != identity.device
                || underlying.metadata()?.ino() != identity.inode,
            "source mount remains attached"
        );
        ensure!(
            fs::read_dir(&plan.source)?.next().is_none(),
            "source mountpoint not empty after teardown"
        );
        let proof = SnapshotProof {
            schema: 1,
            workspace_id: plan.invocation.clone(),
            manifest_binding: manifest.immutable_binding()?,
            command_digest: command_digest(&plan)?,
            source: plan.source.clone(),
            namespace_inode: identity.namespace,
            pid_namespace_inode,
            pid_namespace_contained: true,
            mount_id: identity.mount,
            device: identity.device,
            inode: identity.inode,
            before_verified: true,
            after_verified: true,
            readonly_superblock: true,
            capabilities_dropped: true,
            no_new_privs: true,
            teardown_confirmed: true,
            exit_code: exit,
        };
        let bytes = serde_json::to_vec(&proof)?;
        ensure!(bytes.len() as u64 <= PROOF_BYTES, "snapshot proof limit");
        let mut file = open_relative(
            &proof_parent,
            &proof_name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        file.write_all(&bytes)
            .map_err(|error| phase_error("snapshot_proof_write", error))?;
        file.sync_all()
            .map_err(|error| phase_error("snapshot_proof_sync", error))?;
        proof_parent
            .sync_all()
            .map_err(|error| phase_error("snapshot_proof_parent_sync", error))?;
        Ok(exit)
    }
}
#[cfg(target_os = "linux")]
pub fn run_helper(plan: HelperPlan) -> Result<i32> {
    linux::run(plan).map_err(|error| phase_error("snapshot_setup_or_proof", error))
}
