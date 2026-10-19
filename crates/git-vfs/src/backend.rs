use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;

// Git cat-file accepts at most 10,000 remote-object-info arguments. Each SHA1
// is a separate protocol pkt-line; 4,096 IDs use ~197 KiB on the wire, safely
// below our proxy's 2 MiB body limit and Git's ~658 KiB command-line limit.
const REMOTE_SIZE_BATCH: usize = 4096;

#[path = "size_cache.rs"]
mod size_cache;
use size_cache::SizeCache;

#[path = "blob_reader.rs"]
mod blob_reader;
use blob_reader::BlobReader;

#[path = "background_process.rs"]
mod background_process;
pub use background_process::BackgroundCancellation;
use background_process::NativeChild;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeFallback {
    Error,
    Fetch,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SizeSource {
    #[default]
    Auto,
    Git,
    GitHub,
}

#[derive(Clone, Debug, Default)]
pub struct SizeWarmOptions {
    pub source: SizeSource,
    pub github_token_env: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SizeWarmReport {
    pub pinned_commit: String,
    pub pinned_tree: String,
    pub source: String,
    pub cached_blobs: usize,
    pub learned_blobs: usize,
    pub total_unique_blobs: usize,
    pub http_requests: usize,
    pub http_bytes: u64,
    pub elapsed_seconds: f64,
}

#[derive(Debug, Serialize)]
pub struct CacheStats {
    pub entries: u64,
    pub total_blob_bytes: u64,
    pub database_path: PathBuf,
}

#[derive(Debug, Serialize)]
pub struct BlobPrefetchReport {
    pub requested_blobs: usize,
    pub already_local_blobs: usize,
    pub fetched_blobs: usize,
    pub fetch_batches: usize,
}

pub struct GitRepo {
    repository: PathBuf,
    git_binary: PathBuf,
    remote: String,
    fallback: SizeFallback,
    cache: Arc<Mutex<SizeCache>>,
    hydration: Arc<Mutex<()>>,
    cancellation: Option<BackgroundCancellation>,
    blob_reader: Option<BlobReader>,
}

fn canonical_oid(oid: &str) -> Result<String> {
    ensure!(
        oid.len() == 40 && oid.bytes().all(|c| c.is_ascii_hexdigit()),
        "expected a full 40-character SHA1 object ID"
    );
    Ok(oid.to_ascii_lowercase())
}

impl GitRepo {
    pub fn open(
        repo: impl AsRef<Path>,
        git_binary: impl AsRef<Path>,
        remote: &str,
        fallback: SizeFallback,
    ) -> Result<Self> {
        // This name is passed through cat-file's command-line parser, not a shell.
        ensure!(
            !remote.is_empty()
                && !remote.starts_with('-')
                && remote
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"._/-".contains(&c)),
            "remote must be a configured remote name using letters, digits, '.', '_', '/', or '-'"
        );
        let repository = repo
            .as_ref()
            .canonicalize()
            .context("resolve bare repository path")?;
        let git_binary = if git_binary.as_ref().components().count() > 1 {
            git_binary
                .as_ref()
                .canonicalize()
                .context("resolve Git executable")?
        } else {
            git_binary.as_ref().to_owned()
        };
        // Validate before creating any cache files in the selected directory.
        let output = run_git(
            &repository,
            &git_binary,
            &["rev-parse", "--is-bare-repository", "--show-object-format"],
            false,
            None,
            None,
        )?;
        ensure!(
            output == b"true\nsha1\n",
            "git-vfs requires a bare SHA1 repository (clone with --bare --filter=blob:none)"
        );
        let cache = SizeCache::open(&repository)?;
        Ok(Self {
            repository,
            git_binary,
            remote: remote.to_owned(),
            fallback,
            cache: Arc::new(Mutex::new(cache)),
            hydration: Arc::new(Mutex::new(())),
            cancellation: None,
            blob_reader: None,
        })
    }

    /// Independent readers sharing the cache, content gate, and shutdown scope.
    /// Cancellation stops both mounted demand reads and background prefetch, so
    /// a demand read waiting behind a cancelled batch cannot start a new fetch.
    pub fn background_handle(&mut self) -> Result<(Self, BackgroundCancellation)> {
        let cancellation = if let Some(cancellation) = &self.cancellation {
            cancellation.check()?;
            cancellation.clone()
        } else {
            // A reader started before opting into background prefetch is not in
            // the shutdown registry. Reap it before installing the shared scope.
            self.blob_reader = None;
            let cancellation = BackgroundCancellation::default();
            self.cancellation = Some(cancellation.clone());
            cancellation
        };
        Ok((
            Self {
                repository: self.repository.clone(),
                git_binary: self.git_binary.clone(),
                remote: self.remote.clone(),
                fallback: self.fallback,
                cache: Arc::clone(&self.cache),
                hydration: Arc::clone(&self.hydration),
                cancellation: Some(cancellation.clone()),
                blob_reader: None,
            },
            cancellation,
        ))
    }

    fn cache_lock(&self) -> Result<MutexGuard<'_, SizeCache>> {
        self.cache
            .lock()
            .map_err(|_| anyhow::anyhow!("blob-size cache lock poisoned"))
    }

    fn hydration_lock<'a>(&self, gate: &'a Mutex<()>) -> Result<MutexGuard<'a, ()>> {
        if let Some(cancellation) = &self.cancellation {
            loop {
                cancellation.check()?;
                match gate.try_lock() {
                    Ok(guard) => return Ok(guard),
                    Err(TryLockError::WouldBlock) => {
                        std::thread::sleep(std::time::Duration::from_millis(10))
                    }
                    Err(TryLockError::Poisoned(_)) => bail!("blob hydration lock poisoned"),
                }
            }
        }
        gate.lock()
            .map_err(|_| anyhow::anyhow!("blob hydration lock poisoned"))
    }

    pub fn repository_path(&self) -> &Path {
        &self.repository
    }

    pub fn git_binary(&self) -> &Path {
        &self.git_binary
    }

    pub fn git_output(&self, args: &[&str], allow_fetch: bool) -> Result<Vec<u8>> {
        run_git(
            &self.repository,
            &self.git_binary,
            args,
            allow_fetch,
            None,
            self.cancellation.as_ref(),
        )
    }

    fn git_input(&self, args: &[&str], input: &[u8]) -> Result<Vec<u8>> {
        run_git(
            &self.repository,
            &self.git_binary,
            args,
            false,
            Some(input),
            self.cancellation.as_ref(),
        )
    }

    pub fn cache_stats(&self) -> Result<CacheStats> {
        self.cache_lock()?.stats(&self.repository)
    }

    fn persist_sizes(&mut self, sizes: &HashMap<String, u64>) -> Result<()> {
        self.cache_lock()?.persist(sizes)
    }

    pub fn is_github_remote(&self) -> Result<bool> {
        let remotes = self.git_output(&["remote"], false)?;
        if !remotes
            .split(|b| *b == b'\n')
            .any(|name| name == self.remote.as_bytes())
        {
            return Ok(false);
        }
        let remote = self
            .git_output(&["remote", "get-url", &self.remote], false)
            .context("inspect configured size-warming remote")?;
        Ok(crate::github_sizes::github_repository(&remote).is_some())
    }

    /// Warm snapshot metadata without hydrating content on the GitHub path.
    pub fn warm_snapshot_sizes(
        &mut self,
        snapshot: &crate::filesystem::Snapshot,
        options: &SizeWarmOptions,
    ) -> Result<SizeWarmReport> {
        use crate::filesystem::Kind;
        let started = std::time::Instant::now();
        let tree_spec = format!("{}^{{tree}}", snapshot.commit);
        let tree = canonical_oid(
            std::str::from_utf8(&self.git_output(
                &["rev-parse", "--verify", "--end-of-options", &tree_spec],
                false,
            )?)?
            .trim(),
        )?;
        let oids: Vec<_> = snapshot
            .nodes
            .iter()
            .filter(|n| matches!(n.kind, Kind::File | Kind::Symlink))
            .filter_map(|n| n.oid.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let cached = self.cache_lock()?.get_many(&oids)?;
        let mut report = SizeWarmReport {
            pinned_commit: snapshot.commit.clone(),
            pinned_tree: tree.clone(),
            source: "cache".into(),
            cached_blobs: cached.len(),
            learned_blobs: 0,
            total_unique_blobs: oids.len(),
            http_requests: 0,
            http_bytes: 0,
            elapsed_seconds: 0.0,
        };
        if cached.len() != oids.len() {
            let uncached: Vec<_> = oids
                .iter()
                .filter(|oid| !cached.contains_key(*oid))
                .cloned()
                .collect();
            // One process for the whole snapshot. GIT_NO_LAZY_FETCH forbids hydration.
            let (mut learned, missing) = self.local_blob_sizes(&uncached)?;
            report.source = "local".into();
            if !missing.is_empty() {
                let github = if options.source == SizeSource::Git {
                    None
                } else if self.is_github_remote()? {
                    let remote = self.git_output(&["remote", "get-url", &self.remote], false)?;
                    crate::github_sizes::github_repository(&remote)
                } else {
                    None
                };
                if let Some(repository) = github {
                    let metadata = crate::github_sizes::fetch_sizes(
                        snapshot,
                        &tree,
                        &repository,
                        options.github_token_env.as_deref(),
                    )
                    .context(
                        "GitHub snapshot size warming failed; no content fallback was attempted",
                    )?;
                    for (oid, size) in &learned {
                        ensure!(
                            metadata.sizes.get(oid) == Some(size),
                            "GitHub blob size conflicts with locally available content"
                        );
                    }
                    learned = metadata.sizes;
                    report.source = "github".into();
                    report.http_requests = metadata.requests;
                    report.http_bytes = metadata.bytes;
                } else {
                    ensure!(
                        options.source != SizeSource::GitHub,
                        "GitHub size source requires an exact github.com HTTPS or git@github.com SSH remote"
                    );
                    learned.extend(self.remote_blob_sizes(&missing)?);
                    report.source = "git".into();
                }
            }
            // Local discoveries remain staged until the entire provider succeeds.
            self.persist_sizes(&learned)?;
            report.learned_blobs = oids.len() - cached.len();
        }
        report.elapsed_seconds = started.elapsed().as_secs_f64();
        Ok(report)
    }

    /// Fetch only explicitly requested blobs, using Git's native promisor fetch recipe.
    pub fn prefetch_blobs(&mut self, oids: &[String]) -> Result<BlobPrefetchReport> {
        // Only content operations wait for a running batch. Metadata operations
        // use independent no-fetch processes and never acquire this gate.
        let gate = Arc::clone(&self.hydration);
        let _hydration = self.hydration_lock(&gate)?;
        let mut requested = Vec::new();
        let mut seen = HashSet::new();
        for oid in oids {
            let oid = canonical_oid(oid)?;
            if seen.insert(oid.clone()) {
                requested.push(oid);
            }
        }
        let mut sizes = HashMap::new();
        let mut missing = Vec::new();
        for chunk in requested.chunks(128) {
            let (local, absent) = self.local_blob_sizes(chunk)?;
            sizes.extend(local);
            missing.extend(absent);
        }
        let mut report = BlobPrefetchReport {
            requested_blobs: requested.len(),
            already_local_blobs: sizes.len(),
            fetched_blobs: 0,
            fetch_batches: 0,
        };
        if !missing.is_empty() {
            // Require a configured remote, not an arbitrary URL or filesystem path.
            let configured = self
                .git_output(&["remote", "get-url", &self.remote], false)
                .context("prefetch requires the configured Git remote")?;
            ensure!(
                !configured.is_empty(),
                "prefetch remote has no configured URL"
            );
        }
        if !missing.is_empty() {
            let input = format!("{}\n", missing.join("\n"));
            // blob:none excludes unrelated source blobs, while explicit blob wants
            // remain included. No branch refspec, tags, or submodules are fetched.
            self.git_input(
                &[
                    "-c",
                    "fetch.negotiationAlgorithm=noop",
                    "-c",
                    "fetch.writeCommitGraph=false",
                    "fetch",
                    "--no-tags",
                    "--refmap=",
                    "--no-prune",
                    "--no-prune-tags",
                    "--no-write-fetch-head",
                    "--recurse-submodules=no",
                    "--filter=blob:none",
                    "--stdin",
                    &self.remote,
                ],
                input.as_bytes(),
            )
            .context("batch blob prefetch failed")?;
            for chunk in missing.chunks(128) {
                let (fetched, absent) = self.local_blob_sizes(chunk)?;
                ensure!(
                    absent.is_empty(),
                    "prefetch completed without all requested blobs"
                );
                report.fetched_blobs += fetched.len();
                sizes.extend(fetched);
            }
            report.fetch_batches = 1;
        }
        self.persist_sizes(&sizes)?;
        Ok(report)
    }

    fn local_blob_sizes(&self, oids: &[String]) -> Result<(HashMap<String, u64>, Vec<String>)> {
        let input = format!("{}\n", oids.join("\n"));
        let output = self.git_input(
            &[
                "cat-file",
                "--batch-check=%(objectname) %(objecttype) %(objectsize)",
            ],
            input.as_bytes(),
        )?;
        parse_sizes(&output, oids, true)
    }

    pub fn blob_sizes(&mut self, oids: &[String]) -> Result<HashMap<String, u64>> {
        let mut requested = Vec::new();
        let mut seen = HashSet::new();
        for oid in oids {
            let oid = canonical_oid(oid)?;
            if seen.insert(oid.clone()) {
                requested.push(oid);
            }
        }
        let mut result = self.cache_lock()?.get_many(&requested)?;
        let uncached: Vec<_> = requested
            .into_iter()
            .filter(|oid| !result.contains_key(oid))
            .collect();
        let mut learned = HashMap::new();
        let mut missing = Vec::new();
        for chunk in uncached.chunks(128) {
            let (local, absent) = self.local_blob_sizes(chunk)?;
            learned.extend(local);
            missing.extend(absent);
        }
        learned.extend(self.remote_blob_sizes(&missing)?);
        self.persist_sizes(&learned)?;
        result.extend(learned);
        Ok(result)
    }

    fn remote_blob_sizes(&mut self, missing: &[String]) -> Result<HashMap<String, u64>> {
        let mut learned = HashMap::new();
        for chunk in missing.chunks(REMOTE_SIZE_BATCH) {
            let input = format!("remote-object-info {} {}\n", self.remote, chunk.join(" "));
            let remote_sizes = self
                .git_input(
                    &[
                        "-c",
                        "protocol.version=2",
                        "cat-file",
                        "--batch-command=%(objectname) %(objectsize)",
                    ],
                    input.as_bytes(),
                )
                .and_then(|output| {
                    let (sizes, absent) = parse_sizes(&output, chunk, false)?;
                    ensure!(
                        absent.is_empty(),
                        "remote did not return all requested blob sizes"
                    );
                    Ok(sizes)
                });
            match remote_sizes {
                Ok(sizes) => learned.extend(sizes),
                Err(error) if self.fallback == SizeFallback::Fetch => {
                    // Explicit fallback downloads only objects whose local sizes were unavailable.
                    let _ = error;
                    for oid in chunk {
                        let data = self.raw_blob(oid)
                            .context("explicit size fallback could not fetch blob")?;
                        learned.insert(oid.clone(), data.len() as u64);
                    }
                }
                Err(error) => return Err(error).context(
                    "blob-size metadata unavailable: use a Git client and remote supporting protocol-v2 object-info, prefetch the blobs, or explicitly select the fetch size fallback (which downloads content)"),
            }
        }
        Ok(learned)
    }

    pub fn read_blob(&mut self, oid: &str) -> Result<Vec<u8>> {
        let oid = canonical_oid(oid)?;
        // Git's promisor machinery fetches missing objects; cat-file emits raw blob bytes.
        let data = self.raw_blob(&oid)?;
        self.persist_sizes(&HashMap::from([(oid, data.len() as u64)]))?;
        Ok(data)
    }

    fn raw_blob(&mut self, oid: &str) -> Result<Vec<u8>> {
        let gate = Arc::clone(&self.hydration);
        let _hydration = self.hydration_lock(&gate)?;
        if self.blob_reader.is_none() {
            self.blob_reader = Some(BlobReader::start(
                &self.repository,
                &self.git_binary,
                self.cancellation.as_ref(),
            )?);
        }
        let result = self
            .blob_reader
            .as_mut()
            .expect("initialized blob reader")
            .read(oid);
        if result.is_err() {
            // Never continue from an uncertain protocol position or retry a fetch
            // implicitly. The next caller may start a new native Git process.
            self.blob_reader = None;
        }
        result
    }
}

// Shared repository/environment isolation for one-shot commands and the owned
// content reader. Authentication and ordinary Git configuration remain native.
fn git_command(repository: &Path, binary: &Path, args: &[&str], allow_fetch: bool) -> Command {
    let mut command = Command::new(binary);
    command
        .arg("--no-replace-objects")
        .arg("--git-dir")
        .arg(repository)
        .args(args)
        .current_dir(repository)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Keep authentication/configuration intact, but pin all repository state to
    // the selected bare clone rather than an enclosing Git command's environment.
    for variable in [
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_COMMON_DIR",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_INDEX_FILE",
        "GIT_PREFIX",
        "GIT_NAMESPACE",
        "GIT_SHALLOW_FILE",
    ] {
        command.env_remove(variable);
    }
    if allow_fetch {
        command.env_remove("GIT_NO_LAZY_FETCH");
    } else {
        command.env("GIT_NO_LAZY_FETCH", "1");
    }
    command
}

fn run_git(
    repository: &Path,
    binary: &Path,
    args: &[&str],
    allow_fetch: bool,
    input: Option<&[u8]>,
    cancellation: Option<&BackgroundCancellation>,
) -> Result<Vec<u8>> {
    let mut command = git_command(repository, binary, args, allow_fetch);
    command.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    if let Some(cancellation) = cancellation {
        return run_background_git(&mut command, input, cancellation);
    }
    let mut child = command.spawn().context("start Git subprocess")?;
    // Drain stdout/stderr while supplying stdin: a verbose wrapper can fill its
    // stderr pipe before Git starts consuming a large explicit-object request.
    let writer = input.map(|input| {
        let data = input.to_owned();
        let mut pipe = child.stdin.take().expect("piped Git stdin");
        std::thread::spawn(move || pipe.write_all(&data))
    });
    let output = child.wait_with_output();
    if let Some(writer) = writer {
        writer
            .join()
            .map_err(|_| anyhow::anyhow!("Git input writer failed"))?
            .context("write Git subprocess input")?;
    }
    let output = output.context("wait for Git subprocess")?;
    // Git's stderr can contain credential-bearing URLs. Do not include it in errors.
    ensure!(
        output.status.success(),
        "Git subprocess failed ({})",
        output.status
    );
    Ok(output.stdout)
}

fn run_background_git(
    command: &mut Command,
    input: Option<&[u8]>,
    cancellation: &BackgroundCancellation,
) -> Result<Vec<u8>> {
    let mut child = NativeChild::spawn(command, Some(cancellation))?;
    let mut stdout = child.child.stdout.take().expect("piped Git stdout");
    let mut stderr = child.child.stderr.take().expect("piped Git stderr");
    let output = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).map(|_| bytes)
    });
    let diagnostics = std::thread::spawn(move || std::io::copy(&mut stderr, &mut std::io::sink()));
    let writer = input.map(|input| {
        let data = input.to_owned();
        let mut pipe = child.child.stdin.take().expect("piped Git stdin");
        std::thread::spawn(move || pipe.write_all(&data))
    });
    // Drain all pipes before reaping: the registry's child PID stays reserved
    // while cancellation can kill its whole group, including pipe-owning children.
    let bytes = output.join();
    let errors = diagnostics.join();
    let written = writer.map(|writer| writer.join());
    let status = child.finish()?;
    cancellation.check()?;
    ensure!(status.success(), "Git subprocess failed ({status})");
    errors.map_err(|_| anyhow::anyhow!("Git diagnostics reader failed"))??;
    if let Some(written) = written {
        written
            .map_err(|_| anyhow::anyhow!("Git input writer failed"))?
            .context("write Git subprocess input")?;
    }
    bytes
        .map_err(|_| anyhow::anyhow!("Git output reader failed"))?
        .context("read Git subprocess output")
}

fn parse_sizes(
    output: &[u8],
    requested: &[String],
    with_type: bool,
) -> Result<(HashMap<String, u64>, Vec<String>)> {
    let text = std::str::from_utf8(output).context("Git returned non-UTF8 size metadata")?;
    let expected: HashSet<&str> = requested.iter().map(String::as_str).collect();
    let mut seen = HashSet::new();
    let mut sizes = HashMap::new();
    let mut missing = Vec::new();
    for line in text.lines() {
        let fields: Vec<_> = line.split_whitespace().collect();
        ensure!(fields.len() >= 2, "malformed Git size metadata");
        let oid = canonical_oid(fields[0])?;
        ensure!(
            expected.contains(oid.as_str()) && seen.insert(oid.clone()),
            "unexpected or duplicate Git size metadata"
        );
        if fields.len() == 2 && fields[1] == "missing" {
            missing.push(oid);
        } else {
            let size_field = if with_type {
                ensure!(
                    fields.len() == 3 && fields[1] == "blob",
                    "requested object is not a Git blob"
                );
                fields[2]
            } else {
                ensure!(fields.len() == 2, "malformed remote Git size metadata");
                fields[1]
            };
            ensure!(
                !size_field.is_empty() && size_field.bytes().all(|c| c.is_ascii_digit()),
                "invalid Git blob size"
            );
            sizes.insert(
                oid,
                size_field.parse().context("Git blob size exceeds u64")?,
            );
        }
    }
    if seen.len() != expected.len() {
        bail!("Git returned incomplete size metadata");
    }
    Ok((sizes, missing))
}
