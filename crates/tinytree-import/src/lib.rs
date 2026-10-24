//! One-way immutable Git snapshot seeding. Git storage/history and native
//! repository serving remain separate; no working-tree files are imported.
use anyhow::{Context, Result, bail, ensure};
use serde::Serialize;
use sha2::Digest as _;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};
use tinytree_client::{NativeClient, protocol as p};
use tinytree_model as m;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    time::Instant,
};

#[derive(Clone, Debug)]
pub struct Limits {
    pub max_entries: usize,
    pub max_file_bytes: usize,
    pub max_total_bytes: u64,
    pub max_manifest_bytes: usize,
    pub max_metadata_bytes: usize,
    pub timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_file_bytes: 64 * 1024 * 1024,
            max_total_bytes: 2 * 1024 * 1024 * 1024,
            max_manifest_bytes: 64 * 1024 * 1024,
            max_metadata_bytes: 16 * 1024 * 1024,
            timeout: Duration::from_secs(600),
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=1_000_000).contains(&self.max_entries)
                && (1..=256 * 1024 * 1024).contains(&self.max_file_bytes)
                && (1..=16 * 1024 * 1024 * 1024).contains(&self.max_total_bytes)
                && (1024..=256 * 1024 * 1024).contains(&self.max_manifest_bytes)
                && (1024..=64 * 1024 * 1024).contains(&self.max_metadata_bytes)
                && !self.timeout.is_zero()
                && self.timeout <= Duration::from_secs(1800),
            "invalid snapshot import limits"
        );
        Ok(())
    }
    fn model_remaining(&self, remaining: usize) -> m::Limits {
        let mut limits = self.model();
        limits.max_object_bytes = limits.max_object_bytes.min(remaining);
        limits.max_field_bytes = limits.max_field_bytes.min(remaining);
        limits
    }
    fn model(&self) -> m::Limits {
        m::Limits {
            max_object_bytes: self.max_metadata_bytes,
            max_items: self.max_entries,
            max_field_bytes: self.max_metadata_bytes,
            max_path_bytes: 4096,
        }
    }
}
#[derive(Clone, Debug)]
pub struct Options {
    pub source_git: PathBuf,
    pub git_program: PathBuf,
    pub revision: String,
    pub repository_id: String,
    pub reference: String,
    pub expected_generation: u64,
    pub limits: Limits,
}
#[derive(Clone, Debug, Serialize)]
pub struct Report {
    pub git_revision: String,
    pub native_commit: String,
    pub native_tree: String,
    pub reference: String,
    pub generation: u64,
    pub files: usize,
    pub symlinks: usize,
    pub source_bytes: u64,
    pub uploaded_objects: usize,
    pub reused_current_reference: bool,
}
#[derive(Clone)]
struct Entry {
    path: String,
    oid: String,
    mode: u32,
    size: usize,
}
struct Git {
    source: PathBuf,
    program: PathBuf,
}
impl Git {
    fn command(&self) -> Command {
        let mut cmd = Command::new(&self.program);
        cmd.args([
            "--no-pager",
            "--no-replace-objects",
            "--no-optional-locks",
            "-C",
        ])
        .arg(&self.source)
        .args(["-c", "core.fsmonitor=false"])
        .kill_on_drop(true)
        .stdin(Stdio::null())
        .stderr(Stdio::null());
        for key in [
            "GIT_DIR",
            "GIT_WORK_TREE",
            "GIT_INDEX_FILE",
            "GIT_OBJECT_DIRECTORY",
            "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_NAMESPACE",
            "GIT_CONFIG_COUNT",
        ] {
            cmd.env_remove(key);
        }
        cmd.env("GIT_CONFIG_NOSYSTEM", "1").env(
            "GIT_CONFIG_GLOBAL",
            if cfg!(windows) { "NUL" } else { "/dev/null" },
        );
        cmd
    }
    async fn output(&self, args: &[&str], maximum: usize) -> Result<Vec<u8>> {
        let mut child = self
            .command()
            .args(args)
            .stdout(Stdio::piped())
            .spawn()
            .context("cannot start Git object reader")?;
        let mut bytes = Vec::new();
        child
            .stdout
            .take()
            .context("missing Git output")?
            .take(maximum as u64 + 1)
            .read_to_end(&mut bytes)
            .await
            .context("cannot read Git object metadata")?;
        ensure!(
            bytes.len() <= maximum,
            "Git snapshot metadata exceeds limit"
        );
        ensure!(
            child
                .wait()
                .await
                .context("cannot wait for Git object reader")?
                .success(),
            "Git object reader failed"
        );
        Ok(bytes)
    }
    async fn verify_revision(&self, revision: &str) -> Result<()> {
        ensure!(
            matches!(revision.len(), 40 | 64)
                && revision
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
            "revision must be a full lowercase Git commit ID"
        );
        let peeled = self
            .output(
                &["rev-parse", "--verify", &format!("{revision}^{{commit}}")],
                128,
            )
            .await?;
        ensure!(
            peeled.strip_suffix(b"\n") == Some(revision.as_bytes()),
            "revision must identify the exact commit, not a tag"
        );
        Ok(())
    }
    fn batch(&self) -> Result<Batch> {
        let mut child = self
            .command()
            .args(["cat-file", "--batch"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .context("cannot start Git blob reader")?;
        let input = child.stdin.take().context("missing Git blob input")?;
        let output = BufReader::new(child.stdout.take().context("missing Git blob output")?);
        Ok(Batch {
            child,
            input: Some(input),
            output,
        })
    }
}
struct Batch {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}
impl Batch {
    async fn object(
        &mut self,
        oid: &str,
        kind: &str,
        expected_size: Option<usize>,
        maximum: usize,
    ) -> Result<Vec<u8>> {
        let input = self.input.as_mut().context("closed Git object reader")?;
        input.write_all(oid.as_bytes()).await?;
        input.write_all(b"\n").await?;
        input.flush().await?;
        let mut header = Vec::new();
        loop {
            ensure!(header.len() < 128, "Git object header exceeds limit");
            let byte = self
                .output
                .read_u8()
                .await
                .context("truncated Git object header")?;
            if byte == b'\n' {
                break;
            }
            header.push(byte);
        }
        let header = std::str::from_utf8(&header).context("invalid Git object header")?;
        let fields: Vec<_> = header.split_ascii_whitespace().collect();
        ensure!(
            fields.len() == 3 && fields[0] == oid && fields[1] == kind,
            "Git object identity/type mismatch"
        );
        let size: usize = fields[2].parse().context("invalid Git object size")?;
        ensure!(
            size <= maximum && expected_size.is_none_or(|value| value == size),
            "Git object size exceeds bounds or tree declaration"
        );
        let mut bytes = vec![0; size];
        self.output
            .read_exact(&mut bytes)
            .await
            .context("truncated Git object")?;
        ensure!(
            self.output.read_u8().await? == b'\n',
            "invalid Git object delimiter"
        );
        let prefix = format!("{kind} {}\0", bytes.len());
        let digest = if oid.len() == 40 {
            let mut hash = sha1::Sha1::new();
            hash.update(prefix.as_bytes());
            hash.update(&bytes);
            format!("{:x}", hash.finalize())
        } else {
            let mut hash = sha2::Sha256::new();
            hash.update(prefix.as_bytes());
            hash.update(&bytes);
            format!("{:x}", hash.finalize())
        };
        ensure!(digest == oid, "Git {kind} checksum mismatch");
        Ok(bytes)
    }
    async fn blob(&mut self, entry: &Entry) -> Result<Vec<u8>> {
        self.object(&entry.oid, "blob", Some(entry.size), entry.size)
            .await
    }
    async fn finish(mut self) -> Result<()> {
        self.input.take();
        ensure!(
            self.child
                .wait()
                .await
                .context("cannot wait for Git blob reader")?
                .success(),
            "Git blob reader failed"
        );
        Ok(())
    }
}
#[derive(Default)]
struct TreeNode(BTreeMap<String, Node>);
enum Node {
    Tree(TreeNode),
    Leaf(m::TreeValue),
}
impl TreeNode {
    fn directory(&mut self, parts: &[&str]) -> Result<&mut TreeNode> {
        if parts.is_empty() {
            return Ok(self);
        }
        let value = self
            .0
            .entry(parts[0].to_owned())
            .or_insert_with(|| Node::Tree(TreeNode::default()));
        match value {
            Node::Tree(tree) => tree.directory(&parts[1..]),
            Node::Leaf(_) => bail!("Git tree path conflicts with a file"),
        }
    }
    fn insert(&mut self, path: &str, value: Option<m::TreeValue>) -> Result<()> {
        let parts: Vec<_> = path.split('/').collect();
        if let Some(value) = value {
            let tree = self.directory(&parts[..parts.len() - 1])?;
            ensure!(
                tree.0
                    .insert(parts[parts.len() - 1].to_owned(), Node::Leaf(value))
                    .is_none(),
                "duplicate Git leaf path"
            );
        } else {
            self.directory(&parts)?;
        }
        Ok(())
    }
    fn encode(
        self,
        objects: &mut Vec<m::EncodedObject>,
        limits: &Limits,
        total: &mut usize,
    ) -> Result<m::TreeId> {
        let mut entries = Vec::with_capacity(self.0.len());
        for (name, value) in self.0 {
            let value = match value {
                Node::Tree(tree) => m::TreeValue::Tree(tree.encode(objects, limits, total)?),
                Node::Leaf(value) => value,
            };
            entries.push(m::TreeEntry { name, value });
        }
        let object = m::encode(
            &m::Object::Tree(m::Tree { entries }),
            limits.model_remaining(limits.max_manifest_bytes.saturating_sub(*total)),
        )?;
        *total = total
            .checked_add(object.bytes.len())
            .context("metadata size overflow")?;
        ensure!(
            *total <= limits.max_manifest_bytes,
            "native metadata total limit exceeded"
        );
        let id = m::TreeId(object.descriptor.key.id.0);
        objects.push(object);
        Ok(id)
    }
}
struct Snapshot {
    entries: Vec<Entry>,
    files: BTreeMap<String, m::Descriptor>,
    metadata: Vec<m::EncodedObject>,
    commit: m::EncodedObject,
    tree: m::TreeId,
    source_bytes: u64,
    symlinks: usize,
}
async fn prepare(git: &Git, options: &Options) -> Result<Snapshot> {
    git.verify_revision(&options.revision).await?;
    let mut batch = git.batch()?;
    // Git's read commands parse object contents, but do not promise to rehash
    // every metadata object. Verify the exact commit and all trees as well as
    // blobs before trusting a pinned snapshot from a potentially damaged ODB.
    let commit = batch
        .object(
            &options.revision,
            "commit",
            None,
            options
                .limits
                .max_metadata_bytes
                .min(options.limits.max_manifest_bytes),
        )
        .await?;
    let first = commit
        .split(|byte| *byte == b'\n')
        .next()
        .context("missing Git commit tree")?;
    let root_oid = std::str::from_utf8(first)
        .context("invalid Git commit tree header")?
        .strip_prefix("tree ")
        .context("missing Git commit tree header")?
        .to_owned();
    ensure!(
        root_oid.len() == options.revision.len()
            && root_oid
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "invalid Git root TREE ID"
    );
    let mut git_metadata_bytes = commit.len();
    drop(commit);
    // Traverse only authenticated raw TREE bytes. A separate Git listing is
    // not an authority: it could describe different bytes during an ODB race.
    let oid_bytes = options.revision.len() / 2;
    let mut pending = vec![(String::new(), root_oid)];
    let mut entries = Vec::new();
    let mut paths = BTreeSet::new();
    let mut manifest_bytes = 0usize;
    while let Some((parent, oid)) = pending.pop() {
        let maximum = options.limits.max_metadata_bytes.min(
            options
                .limits
                .max_manifest_bytes
                .saturating_sub(git_metadata_bytes),
        );
        let bytes = batch.object(&oid, "tree", None, maximum).await?;
        git_metadata_bytes = git_metadata_bytes
            .checked_add(bytes.len())
            .context("Git metadata size overflow")?;
        let mut remaining = bytes.as_slice();
        while !remaining.is_empty() {
            ensure!(
                entries.len() < options.limits.max_entries,
                "snapshot entry limit exceeded"
            );
            let space = remaining
                .iter()
                .position(|b| *b == b' ')
                .context("invalid Git TREE mode framing")?;
            ensure!(space <= 6, "invalid Git TREE mode");
            let mode_text =
                std::str::from_utf8(&remaining[..space]).context("invalid Git TREE mode")?;
            ensure!(
                matches!(mode_text, "40000" | "100644" | "100755" | "120000"),
                "unsupported Git entry (including submodules)"
            );
            let mode = u32::from_str_radix(mode_text, 8)?;
            remaining = &remaining[space + 1..];
            let nul = remaining
                .iter()
                .position(|b| *b == 0)
                .context("invalid Git TREE name framing")?;
            ensure!(nul <= 4096, "unsupported Git snapshot path");
            let name = std::str::from_utf8(&remaining[..nul])
                .context("non-UTF8 Git paths are unsupported")?;
            ensure!(
                !name.is_empty()
                    && !matches!(name, "." | "..")
                    && !name.eq_ignore_ascii_case(".git")
                    && !name.eq_ignore_ascii_case(".jj")
                    && !name.contains(['/', '\\']),
                "unsupported Git snapshot path"
            );
            let path = if parent.is_empty() {
                name.to_owned()
            } else {
                format!("{parent}/{name}")
            };
            ensure!(
                path.len() <= 4096 && path.split('/').count() <= 128,
                "unsupported Git snapshot path"
            );
            manifest_bytes = manifest_bytes
                .checked_add(path.len() + options.revision.len() + 64)
                .context("snapshot manifest size overflow")?;
            ensure!(
                manifest_bytes <= options.limits.max_manifest_bytes,
                "Git snapshot metadata exceeds limit"
            );
            ensure!(paths.insert(path.clone()), "duplicate Git snapshot path");
            remaining = &remaining[nul + 1..];
            ensure!(remaining.len() >= oid_bytes, "truncated Git TREE object ID");
            let oid: String = remaining[..oid_bytes]
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            remaining = &remaining[oid_bytes..];
            if mode == 0o040000 {
                pending.push((path.clone(), oid.clone()));
            }
            entries.push(Entry {
                path,
                oid,
                mode,
                size: 0,
            });
        }
    }
    let mut tree = TreeNode::default();
    let mut files = BTreeMap::new();
    let mut metadata = Vec::new();
    let mut metadata_bytes = 0usize;
    let mut source_bytes = 0u64;
    let mut symlinks = 0;
    for entry in &mut entries {
        if entry.mode == 0o040000 {
            tree.insert(&entry.path, None)?;
            continue;
        }
        let maximum = options.limits.max_file_bytes.min(
            usize::try_from(options.limits.max_total_bytes.saturating_sub(source_bytes))
                .unwrap_or(usize::MAX),
        );
        let maximum = if entry.mode == 0o120000 {
            maximum.min(4096)
        } else {
            maximum
        };
        let bytes = batch.object(&entry.oid, "blob", None, maximum).await?;
        entry.size = bytes.len();
        source_bytes = source_bytes
            .checked_add(entry.size as u64)
            .context("snapshot size overflow")?;
        let value = if entry.mode == 0o120000 {
            let target =
                String::from_utf8(bytes).context("non-UTF8 symlink targets are unsupported")?;
            let object = m::encode(
                &m::Object::Symlink(target),
                options.limits.model_remaining(
                    options
                        .limits
                        .max_manifest_bytes
                        .saturating_sub(metadata_bytes),
                ),
            )?;
            let value = m::TreeValue::Symlink {
                id: m::SymlinkId(object.descriptor.key.id.0),
                target_bytes: entry.size as u64,
            };
            metadata_bytes = metadata_bytes
                .checked_add(object.bytes.len())
                .context("metadata size overflow")?;
            ensure!(
                metadata_bytes <= options.limits.max_manifest_bytes,
                "native metadata total limit exceeded"
            );
            metadata.push(object);
            symlinks += 1;
            value
        } else {
            let descriptor = m::describe_file(&bytes);
            if let Some(previous) = files.insert(entry.oid.clone(), descriptor) {
                ensure!(
                    previous == descriptor,
                    "Git duplicate blob identity mismatch"
                );
            }
            m::TreeValue::File {
                id: m::FileId(descriptor.key.id.0),
                executable: entry.mode == 0o100755,
                size_bytes: entry.size as u64,
                copy_id: None,
            }
        };
        tree.insert(&entry.path, Some(value))?;
    }
    batch.finish().await?;
    let mut total = metadata.iter().map(|o| o.bytes.len()).sum();
    let root = tree.encode(&mut metadata, &options.limits, &mut total)?;
    let signature = m::Signature {
        name: "Snapshot importer".into(),
        email: "snapshot@example.invalid".into(),
        timestamp: m::Timestamp {
            millis_since_epoch: 0,
            tz_offset_minutes: 0,
        },
    };
    let mut change = blake3::Hasher::new();
    change.update(b"tinytree-git-snapshot-change-v1\0");
    change.update(options.revision.as_bytes());
    change.update(&root.0);
    let commit = m::encode(
        &m::Object::Commit(m::Commit {
            parents: vec![m::CommitId::ROOT],
            predecessors: vec![],
            root_tree: m::TreeMerge {
                terms: vec![root],
                labels: vec![String::new()],
            },
            change_id: m::ChangeId(change.finalize().as_bytes()[..16].try_into().unwrap()),
            description: format!("Immutable Git snapshot {}", options.revision),
            author: signature.clone(),
            committer: signature,
            secure_signature: None,
        }),
        options.limits.model(),
    )?;
    Ok(Snapshot {
        entries,
        files,
        metadata,
        commit,
        tree: root,
        source_bytes,
        symlinks,
    })
}
fn wire(key: m::ObjectKey) -> p::ObjectKey {
    p::ObjectKey {
        kind: key.kind as i32,
        native_id: key.id.0.to_vec(),
    }
}
fn current(reference: &p::Reference, key: m::ObjectKey) -> bool {
    reference
        .commit
        .as_ref()
        .and_then(|descriptor| descriptor.key.as_ref())
        .is_some_and(|value| value.kind == key.kind as i32 && value.native_id == key.id.0)
}

/// Prepare from immutable Git objects, upload the verified canonical closure and
/// generation-CAS publish one native reference. Source files and refs are never changed.
pub async fn import_snapshot(client: NativeClient, options: Options) -> Result<Report> {
    options.limits.validate()?;
    let deadline = Instant::now() + options.limits.timeout;
    import_snapshot_at(client, options, deadline).await
}

/// Use a caller's original absolute deadline, including connection/setup time.
/// The configured timeout can shorten this allowance, never extend it.
pub async fn import_snapshot_at(
    client: NativeClient,
    options: Options,
    deadline: Instant,
) -> Result<Report> {
    options.limits.validate()?;
    ensure!(
        !options.reference.is_empty()
            && options.reference.len() <= 4096
            && !options.reference.starts_with("_tinytree"),
        "invalid snapshot reference"
    );
    let deadline = deadline.min(Instant::now() + options.limits.timeout);
    tokio::time::timeout_at(
        deadline,
        import_inner(client.with_operation_deadline(deadline), options),
    )
    .await
    .context("snapshot import deadline exceeded")?
}

async fn import_inner(client: NativeClient, options: Options) -> Result<Report> {
    let git = Git {
        source: options.source_git.clone(),
        program: options.git_program.clone(),
    };
    let snapshot = prepare(&git, &options).await?;
    let view = client.open(vec![options.reference.clone()]).await?;
    let observed = view
        .references
        .first()
        .context("missing native reference observation")?;
    ensure!(
        view.repository_id == options.repository_id,
        "native repository mismatch"
    );
    let mut report = Report {
        git_revision: options.revision.clone(),
        native_commit: blake3::Hash::from(snapshot.commit.descriptor.key.id.0)
            .to_hex()
            .to_string(),
        native_tree: blake3::Hash::from(snapshot.tree.0).to_hex().to_string(),
        reference: options.reference.clone(),
        generation: observed.generation,
        files: snapshot
            .entries
            .iter()
            .filter(|entry| matches!(entry.mode, 0o100644 | 0o100755))
            .count(),
        symlinks: snapshot.symlinks,
        source_bytes: snapshot.source_bytes,
        uploaded_objects: 0,
        reused_current_reference: false,
    };
    if current(observed, snapshot.commit.descriptor.key) {
        report.reused_current_reference = true;
        return Ok(report);
    }
    ensure!(
        observed.generation == options.expected_generation,
        "native reference generation differs; explicit expected generation required"
    );
    let mut builder = tempfile::Builder::new();
    builder.prefix("tinytree-import-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        builder.permissions(std::fs::Permissions::from_mode(0o700));
    }
    let stage = builder
        .tempdir()
        .context("cannot create private import staging")?;
    let mut batch = git.batch()?;
    let mut uploaded = BTreeSet::new();
    for entry in &snapshot.entries {
        if !matches!(entry.mode, 0o100644 | 0o100755) || !uploaded.insert(entry.oid.clone()) {
            continue;
        }
        let bytes = batch.blob(entry).await?;
        let descriptor = snapshot.files[&entry.oid];
        ensure!(
            m::describe_file(&bytes) == descriptor,
            "Git source content changed between passes"
        );
        let mut source = tempfile::NamedTempFile::new_in(stage.path())?;
        std::io::Write::write_all(&mut source, &bytes)?;
        drop(bytes);
        let prepared = client
            .prepare_upload_file(source.path(), descriptor, stage.path())
            .await?;
        drop(source);
        client.upload_file(prepared).await?;
        report.uploaded_objects += 1;
    }
    batch.finish().await?;
    for object in snapshot.metadata {
        let object = client
            .verify_metadata(object.descriptor, object.bytes)
            .await?;
        client.upload_metadata(object).await?;
        report.uploaded_objects += 1;
    }
    let commit_key = snapshot.commit.descriptor.key;
    let metadata = client
        .verify_metadata(snapshot.commit.descriptor, snapshot.commit.bytes)
        .await?;
    let receipt = client.upload_metadata(metadata).await?;
    report.uploaded_objects += 1;
    // Publication validates the complete closure. The final COMMIT staging root
    // protects that closure; thousands of child tokens need not fit one request.
    let mut replay = blake3::Hasher::new();
    replay.update(b"tinytree-git-snapshot-publication-v1\0");
    for value in [
        &options.repository_id,
        &options.reference,
        &options.revision,
    ] {
        replay.update(&(value.len() as u64).to_be_bytes());
        replay.update(value.as_bytes());
    }
    replay.update(&options.expected_generation.to_be_bytes());
    replay.update(&commit_key.id.0);
    let publication = client
        .publish(p::PublishReferenceRequest {
            repository_id: options.repository_id,
            idempotency_key: format!("snapshot-{}", replay.finalize().to_hex()),
            reference_name: options.reference.clone(),
            expected_generation: options.expected_generation,
            new_commit: Some(wire(commit_key)),
            staging_leases: vec![receipt.staging_lease],
        })
        .await?;
    let published = publication
        .reference
        .context("missing snapshot publication reference")?;
    let after = client.open(vec![options.reference]).await?;
    let current = after
        .references
        .first()
        .context("missing current snapshot reference")?;
    ensure!(
        self::current(current, commit_key) && current.generation >= published.generation,
        "snapshot publication is no longer current"
    );
    report.generation = current.generation;
    Ok(report)
}
