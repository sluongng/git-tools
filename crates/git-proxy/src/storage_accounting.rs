//! Bounded, read-only accounting of proxy-owned storage, not an eviction policy.
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File, OpenOptions},
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt},
        io::AsRawFd,
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Semaphore};

const MAX_VISITED: u64 = 100_000;
const MAX_ELAPSED: Duration = Duration::from_secs(2);
const MAX_DEPTH: usize = 64;
const MAX_ERRORS: usize = 16;
const CACHE_TTL: Duration = Duration::from_secs(5);

#[derive(Clone, Default, Serialize)]
struct Usage {
    files: u64,
    logical_bytes: u64,
    allocated_bytes_estimate: u64,
}
#[derive(Clone, Serialize)]
struct ScanError {
    category: &'static str,
    operation: &'static str,
    kind: String,
}
#[derive(Clone, Serialize)]
pub(crate) struct Snapshot {
    schema: u32,
    captured_unix_ms: u64,
    elapsed_ms: u64,
    complete: bool,
    visited: u64,
    entry_limit: u64,
    time_limit_ms: u64,
    depth_limit: usize,
    symlinks_skipped: u64,
    duplicate_files_skipped: u64,
    errors_dropped: u64,
    errors: Vec<ScanError>,
    categories: BTreeMap<&'static str, Usage>,
    // Directory metadata, filesystem sharing/compression, and unrecognized files
    // are intentionally not represented as exclusive physical disk usage.
    scope: &'static str,
}

pub(crate) struct StorageAccounting {
    roots: Vec<(PathBuf, &'static str)>,
    cached: Mutex<Option<(Instant, Arc<Snapshot>)>>,
    permit: Arc<Semaphore>,
}
impl StorageAccounting {
    pub(crate) fn new(cache: &Path, mirrors: impl Iterator<Item = PathBuf>) -> Self {
        let mut roots: Vec<_> = mirrors.map(|path| (path, "mirror_auxiliary")).collect();
        roots.extend([
            (cache.join("generated-packs"), "generated_responses"),
            (cache.join("digests.fjall"), "digest_database"),
            (cache.join("cas-spool"), "auxiliary"),
            (cache.join("cas-content-v1"), "cas_content"),
            (cache.join("owner.lock"), "auxiliary"),
        ]);
        Self {
            roots,
            cached: Mutex::new(None),
            permit: Arc::new(Semaphore::new(1)),
        }
    }

    pub(crate) async fn snapshot(&self) -> Result<Arc<Snapshot>, &'static str> {
        if let Some((at, value)) = &*self.cached.lock().await
            && at.elapsed() < CACHE_TTL
        {
            return Ok(value.clone());
        }
        // The permit belongs to the blocking task, so request cancellation cannot
        // permit a second concurrent scan while the first still runs.
        let permit = self
            .permit
            .clone()
            .try_acquire_owned()
            .map_err(|_| "storage scan in progress\n")?;
        let roots = self.roots.clone();
        let value = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            Arc::new(scan(roots, MAX_VISITED, MAX_ELAPSED))
        })
        .await
        .map_err(|_| "storage scan failed\n")?;
        *self.cached.lock().await = Some((Instant::now(), value.clone()));
        Ok(value)
    }
}

struct Scanner {
    snapshot: Snapshot,
    start: Instant,
    timeout: Duration,
    files: HashSet<(u64, u64)>,
    directories: HashSet<(u64, u64)>,
}
impl Scanner {
    fn error(&mut self, category: &'static str, operation: &'static str, error: std::io::Error) {
        self.snapshot.complete = false;
        if self.snapshot.errors.len() < MAX_ERRORS {
            self.snapshot.errors.push(ScanError {
                category,
                operation,
                kind: format!("{:?}", error.kind()),
            });
        } else {
            self.snapshot.errors_dropped += 1;
        }
    }
    fn budget(&mut self) -> bool {
        if self.snapshot.visited >= self.snapshot.entry_limit
            || self.start.elapsed() >= self.timeout
        {
            self.snapshot.complete = false;
            return false;
        }
        self.snapshot.visited += 1;
        true
    }
    fn visit(&mut self, path: &Path, relative: &Path, root_category: &'static str, depth: usize) {
        if !self.budget() {
            return;
        }
        let category = category(root_category, relative);
        let metadata = match fs::symlink_metadata(path) {
            Ok(value) => value,
            Err(error) if depth == 0 && error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) => {
                self.error(category, "metadata", error);
                return;
            }
        };
        if metadata.file_type().is_symlink() {
            self.snapshot.symlinks_skipped += 1;
            return;
        }
        if metadata.is_file() {
            if !self.files.insert((metadata.dev(), metadata.ino())) {
                self.snapshot.duplicate_files_skipped += 1;
                return;
            }
            let usage = self.snapshot.categories.entry(category).or_default();
            usage.files += 1;
            usage.logical_bytes = usage.logical_bytes.saturating_add(metadata.len());
            usage.allocated_bytes_estimate = usage
                .allocated_bytes_estimate
                .saturating_add(metadata.blocks().saturating_mul(512));
            return;
        }
        if !metadata.is_dir() {
            return;
        }
        if depth >= MAX_DEPTH {
            self.snapshot.complete = false;
            return;
        }
        // Opening each directory with O_NOFOLLOW and iterating the held fd avoids
        // following a directory swapped for a symlink during the walk. Linux's
        // proc fd path refers to that open directory, not its mutable pathname.
        let directory: File = match OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
        {
            Ok(value) => value,
            Err(error) => {
                self.error(category, "open_directory", error);
                return;
            }
        };
        let identity = match directory.metadata() {
            Ok(value) => (value.dev(), value.ino()),
            Err(error) => {
                self.error(category, "directory_metadata", error);
                return;
            }
        };
        if !self.directories.insert(identity) {
            return;
        }
        let anchored = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd()));
        let entries = match fs::read_dir(&anchored) {
            Ok(value) => value,
            Err(error) => {
                self.error(category, "read_directory", error);
                return;
            }
        };
        for entry in entries {
            if self.snapshot.visited >= self.snapshot.entry_limit
                || self.start.elapsed() >= self.timeout
            {
                self.snapshot.complete = false;
                break;
            }
            match entry {
                Ok(entry) => self.visit(
                    &anchored.join(entry.file_name()),
                    &relative.join(entry.file_name()),
                    root_category,
                    depth + 1,
                ),
                Err(error) => {
                    self.snapshot.visited += 1;
                    self.error(category, "directory_entry", error);
                }
            }
        }
    }
}
fn category(root: &'static str, relative: &Path) -> &'static str {
    if root != "mirror_auxiliary" {
        return root;
    }
    let mut parts = relative.components();
    if parts.next().is_some_and(|p| p.as_os_str() == "objects")
        && let Some(second) = parts.next()
    {
        if second.as_os_str() == "pack" {
            return match relative.extension().and_then(|s| s.to_str()) {
                Some("pack") => "mirror_packs",
                Some("idx" | "rev" | "bitmap") => "mirror_indexes",
                _ => "mirror_auxiliary",
            };
        }
        let name = second.as_os_str().to_string_lossy();
        if name.len() == 2
            && name.bytes().all(|b| b.is_ascii_hexdigit())
            && let Some(object) = parts.next()
        {
            let object = object.as_os_str().to_string_lossy();
            if object.len() == 38
                && object.bytes().all(|b| b.is_ascii_hexdigit())
                && parts.next().is_none()
            {
                return "mirror_loose_objects";
            }
        }
    }
    "mirror_auxiliary"
}
fn scan(roots: Vec<(PathBuf, &'static str)>, entry_limit: u64, timeout: Duration) -> Snapshot {
    let mut scanner = Scanner {
        snapshot: Snapshot {
            schema: 1,
            captured_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64,
            elapsed_ms: 0,
            complete: true,
            visited: 0,
            entry_limit,
            time_limit_ms: timeout.as_millis().min(u64::MAX as u128) as u64,
            depth_limit: MAX_DEPTH,
            symlinks_skipped: 0,
            duplicate_files_skipped: 0,
            errors_dropped: 0,
            errors: Vec::new(),
            categories: [
                "mirror_packs",
                "mirror_indexes",
                "mirror_loose_objects",
                "mirror_auxiliary",
                "generated_responses",
                "digest_database",
                "cas_content",
                "auxiliary",
            ]
            .into_iter()
            .map(|name| (name, Usage::default()))
            .collect(),
            scope: "recognized managed roots; regular files only; allocated bytes are st_blocks estimates, not exclusive physical usage; no eviction; live non-atomic snapshot",
        },
        start: Instant::now(),
        timeout,
        files: HashSet::new(),
        directories: HashSet::new(),
    };
    for (path, category) in roots {
        scanner.visit(&path, Path::new(""), category, 0);
    }
    scanner.snapshot.elapsed_ms = scanner.start.elapsed().as_millis().min(u64::MAX as u128) as u64;
    scanner.snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    #[test]
    fn accounts_managed_categories_without_following_symlinks_or_double_counting_hardlinks() {
        let temp = tempfile::tempdir().unwrap();
        let cache = temp.path().join("cache");
        let repo = cache.join("repo.git");
        for directory in [
            repo.join("objects/pack"),
            repo.join("objects/ab"),
            cache.join("generated-packs/schema-v2"),
            cache.join("digests.fjall"),
            cache.join("cas-content-v1"),
        ] {
            fs::create_dir_all(directory).unwrap();
        }
        fs::write(repo.join("objects/pack/a.pack"), [0; 20]).unwrap();
        fs::write(repo.join("objects/pack/a.idx"), [0; 7]).unwrap();
        fs::write(repo.join("objects/ab").join("c".repeat(38)), [0; 3]).unwrap();
        fs::write(repo.join("objects/ab/not-an-object"), [0; 5]).unwrap();
        let nested = repo.join("objects/ab").join("d".repeat(38));
        fs::create_dir(&nested).unwrap();
        fs::write(nested.join("e".repeat(38)), [0; 9]).unwrap();
        fs::write(cache.join("generated-packs/schema-v2/a.response"), [0; 11]).unwrap();
        fs::write(cache.join("digests.fjall/data"), [0; 13]).unwrap();
        fs::write(cache.join("cas-content-v1/blob"), [0; 17]).unwrap();
        fs::write(temp.path().join("private"), [0; 100]).unwrap();
        symlink(temp.path().join("private"), repo.join("secret-link")).unwrap();
        symlink(temp.path(), repo.join("directory-link")).unwrap();
        fs::hard_link(
            repo.join("objects/pack/a.pack"),
            repo.join("objects/pack/b.pack"),
        )
        .unwrap();
        let accounting = StorageAccounting::new(&cache, std::iter::once(repo));
        let result = scan(accounting.roots, MAX_VISITED, MAX_ELAPSED);
        assert!(result.complete);
        assert_eq!(result.categories["mirror_packs"].logical_bytes, 20);
        assert_eq!(result.categories["mirror_indexes"].logical_bytes, 7);
        assert_eq!(result.categories["mirror_loose_objects"].logical_bytes, 3);
        assert_eq!(result.categories["mirror_loose_objects"].files, 1);
        assert_eq!(result.categories["mirror_auxiliary"].logical_bytes, 14);
        assert_eq!(result.categories["generated_responses"].logical_bytes, 11);
        assert_eq!(result.categories["digest_database"].logical_bytes, 13);
        assert_eq!(result.categories["cas_content"].logical_bytes, 17);
        assert_eq!(result.symlinks_skipped, 2);
        assert_eq!(result.duplicate_files_skipped, 1);
        let json = serde_json::to_string(&result).unwrap();
        assert!(!json.contains(temp.path().to_str().unwrap()));
        assert!(!json.contains("secret-link"));
    }

    #[test]
    fn bounded_partial_scan_and_root_symlinks_are_explicit() {
        let temp = tempfile::tempdir().unwrap();
        for i in 0..20 {
            fs::write(temp.path().join(i.to_string()), [0; 1]).unwrap();
        }
        let result = scan(vec![(temp.path().to_owned(), "auxiliary")], 5, MAX_ELAPSED);
        assert!(!result.complete);
        assert_eq!(result.visited, 5);
        let link = temp.path().join("link");
        symlink(temp.path(), &link).unwrap();
        let result = scan(vec![(link, "auxiliary")], MAX_VISITED, MAX_ELAPSED);
        assert!(result.complete);
        assert_eq!(result.symlinks_skipped, 1);
        assert_eq!(result.categories["auxiliary"].files, 0);
    }

    #[test]
    fn scan_errors_are_bounded_and_do_not_disclose_paths() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("private-token-path");
        fs::write(&file, "not a directory").unwrap();
        let roots = (0..30).map(|_| (file.join("child"), "auxiliary")).collect();
        let result = scan(roots, MAX_VISITED, MAX_ELAPSED);
        assert!(!result.complete);
        assert_eq!(result.errors.len(), MAX_ERRORS);
        assert_eq!(result.errors_dropped, 30 - MAX_ERRORS as u64);
        let json = serde_json::to_string(&result).unwrap();
        assert!(!json.contains("private-token-path"));
        assert!(!json.contains(temp.path().to_str().unwrap()));
    }

    #[tokio::test]
    async fn cached_snapshot_and_busy_gate_bound_repeated_requests() {
        let temp = tempfile::tempdir().unwrap();
        let accounting = StorageAccounting::new(temp.path(), std::iter::empty());
        let permit = accounting.permit.clone().acquire_owned().await.unwrap();
        assert!(accounting.snapshot().await.is_err());
        drop(permit);
        let first = accounting.snapshot().await.unwrap();
        fs::write(temp.path().join("owner.lock"), "later").unwrap();
        let second = accounting.snapshot().await.unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(second.categories["auxiliary"].files, 0);
    }
}
