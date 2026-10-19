//! A fixed Git snapshot projected through Windows ProjFS.
//!
//! ProjFS, not an in-memory overlay, owns modified files and deletion tombstones.
//! Restarting the provider never replaces local data or updates the base commit.
use anyhow::{Context, Result, bail, ensure};
use sha1::{Digest, Sha1};
use std::{
    collections::HashMap,
    io::Read,
    path::{Component, Path},
};
use workspace_core::{
    backend::GitRepo,
    filesystem::{Kind, Snapshot},
};

pub mod worktree;

#[cfg(all(windows, feature = "native"))]
pub mod windows;

#[derive(Clone, Debug)]
pub struct Entry {
    pub path: String,
    pub name: String,
    pub parent: String,
    pub inode: u64,
    pub parent_inode: u64,
    pub kind: Kind,
    pub oid: Option<String>,
}

/// Validates Win32 names before creating any placeholders. Ambiguous names fail
/// closed rather than silently hiding or aliasing Git entries.
#[derive(Clone)]
pub struct Namespace {
    pub commit: String,
    pub entries: Vec<Entry>,
    paths: HashMap<String, usize>,
}
impl Namespace {
    pub fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        let mut entries = Vec::new();
        let mut paths = HashMap::new();
        for node in snapshot.nodes().iter().skip(1) {
            let mut components = Vec::new();
            for component in node.path.components() {
                let Component::Normal(name) = component else {
                    bail!("non-relative Git path")
                };
                let name = name
                    .to_str()
                    .context("Windows projection requires UTF-8 Git names")?;
                validate_component(name)?;
                components.push(name.to_owned());
            }
            let name = components.last().context("empty Git path")?.clone();
            let path = components.join("\\");
            let key = path.to_lowercase();
            ensure!(
                !paths.contains_key(&key),
                "case-insensitive Git path collision: {path}"
            );
            paths.insert(key, entries.len());
            entries.push(Entry {
                parent: components[..components.len() - 1].join("\\"),
                path,
                name,
                inode: node.inode,
                parent_inode: node.parent,
                kind: node.kind,
                oid: node.oid.clone(),
            });
        }
        Ok(Self {
            commit: snapshot.commit.clone(),
            entries,
            paths,
        })
    }
    pub fn lookup(&self, path: &str) -> Option<&Entry> {
        self.paths
            .get(&path.to_lowercase())
            .map(|&index| &self.entries[index])
    }
    pub fn children(&self, path: &str) -> Vec<&Entry> {
        self.entries
            .iter()
            .filter(|entry| entry.parent.eq_ignore_ascii_case(path))
            .collect()
    }
    pub fn size(&self, entry: &Entry, repo: &mut GitRepo) -> Result<u64> {
        if entry.kind.is_dir() {
            return Ok(0);
        }
        let oid = entry.oid.as_ref().context("blob without object ID")?;
        repo.blob_sizes(std::slice::from_ref(oid))?
            .get(oid)
            .copied()
            .context("missing blob size")
    }
}

pub fn validate_component(name: &str) -> Result<()> {
    ensure!(
        !name.is_empty() && name != "." && name != "..",
        "invalid Windows name"
    );
    ensure!(
        !name.ends_with(['.', ' ']),
        "Windows name ends in dot or space: {name}"
    );
    ensure!(
        !name
            .chars()
            .any(|ch| ch < ' ' || "<>:\"/\\|?*".contains(ch)),
        "invalid Windows name: {name}"
    );
    ensure!(
        name.encode_utf16().count() <= 255,
        "Windows filename exceeds 255 UTF-16 units"
    );
    let stem = name
        .split('.')
        .next()
        .unwrap()
        .trim_end_matches(' ')
        .to_uppercase();
    ensure!(
        !matches!(
            stem.as_str(),
            "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
        ) && !(stem.len() == 4
            && (stem.starts_with("COM") || stem.starts_with("LPT"))
            && matches!(stem.as_bytes()[3], b'1'..=b'9'))
            && !["COM¹", "COM²", "COM³", "LPT¹", "LPT²", "LPT³"].contains(&stem.as_str()),
        "reserved Windows device name: {name}"
    );
    ensure!(
        !name.eq_ignore_ascii_case(".git")
            && !name
                .to_ascii_lowercase()
                .strip_prefix("git~")
                .is_some_and(
                    |suffix| !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
                ),
        "Git control paths may not be projected"
    );
    Ok(())
}

/// Hashes the mounted file, including native modifications. Reading an untouched
/// placeholder intentionally hydrates it; this never returns a stale base OID.
pub fn gitsha1(path: &Path) -> Result<String> {
    if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
        let target = std::fs::read_link(path)?;
        #[cfg(unix)]
        let bytes = {
            use std::os::unix::ffi::OsStrExt;
            target.as_os_str().as_bytes().to_vec()
        };
        #[cfg(windows)]
        let bytes = target
            .to_str()
            .context("symlink target must be UTF-8")?
            .replace('\\', "/")
            .into_bytes();
        let mut hash = Sha1::new();
        hash.update(format!("blob {}\0", bytes.len()));
        hash.update(&bytes);
        return Ok(hex::encode(hash.finalize()));
    }
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let mut hash = Sha1::new();
    hash.update(format!("blob {size}\0"));
    let mut buffer = [0u8; 64 * 1024];
    let mut read = 0;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        read += count as u64;
        hash.update(&buffer[..count]);
    }
    ensure!(read == size, "file changed size during digest computation");
    Ok(hex::encode(hash.finalize()))
}

#[cfg(any(all(windows, feature = "native"), test))]
pub(crate) fn hydration_range(
    offset: u64,
    length: u32,
    size: u64,
    alignment: u32,
) -> Result<(u64, u64)> {
    ensure!(alignment > 0, "zero ProjFS write alignment");
    let end = offset
        .checked_add(length as u64)
        .context("hydration range overflow")?;
    ensure!(end <= size, "hydration range exceeds Git blob");
    let alignment = alignment as u64;
    let start = offset / alignment * alignment;
    let remainder = end % alignment;
    let end = if remainder == 0 {
        end
    } else {
        end.checked_add(alignment - remainder)
            .unwrap_or(size)
            .min(size)
    };
    Ok((start, end))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_ambiguous_or_unsafe_win32_names() {
        for name in [
            "",
            ".",
            "..",
            "nul.txt",
            "CON .txt",
            "COM¹",
            "LPT².txt",
            "CoM1",
            "LPT9.log",
            "foo:",
            "a\\b",
            "a/b",
            "a.",
            "a ",
            ".git",
            "GiT~1",
            "git~2",
            "\0",
        ] {
            assert!(validate_component(name).is_err(), "{name:?}");
        }
        for name in ["BUILD.bazel", "normal", "COM10", "résumé.rs", ".github"] {
            validate_component(name).unwrap();
        }
    }
    #[test]
    fn hydration_covers_unaligned_requests_and_stops_at_eof() {
        assert_eq!(hydration_range(123, 512, 4096, 512).unwrap(), (0, 1024));
        assert_eq!(hydration_range(512, 700, 1212, 512).unwrap(), (512, 1212));
        assert_eq!(hydration_range(512, 512, 4096, 512).unwrap(), (512, 1024));
        assert!(hydration_range(4000, 1000, 4096, 512).is_err());
        assert!(hydration_range(u64::MAX, 1, u64::MAX, 512).is_err());
        assert!(hydration_range(0, 1, 1, 0).is_err());
    }
    #[test]
    fn digest_includes_native_edits_not_base_oid() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("file");
        std::fs::write(&path, b"hello\n").unwrap();
        assert_eq!(
            gitsha1(&path).unwrap(),
            "ce013625030ba8dba906f756967f9e9ca394464a"
        );
        std::fs::write(&path, b"changed\n").unwrap();
        assert_ne!(
            gitsha1(&path).unwrap(),
            "ce013625030ba8dba906f756967f9e9ca394464a"
        );
    }
}
