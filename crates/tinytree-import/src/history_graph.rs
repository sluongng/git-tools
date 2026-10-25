use super::{
    HistoryLimits, POLICY,
    io::Git,
    state::{hex, id, valid_hex},
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Arc,
};
use tinytree_model as m;
use tokio::time::Instant;
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    pub git: String,
    pub native: String,
    pub raw: Vec<u8>,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Blob {
    pub git: String,
    pub kind: u8,
    pub native: String,
    pub size: u64,
    pub digest: [u8; 32],
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Manifest {
    pub version: u32,
    pub policy: String,
    pub object_format: String,
    pub repository: String,
    pub reference: String,
    pub expected_generation: u64,
    pub scope: [u8; 32],
    pub job: String,
    pub client: [u8; 16],
    pub git_head: String,
    pub native_head: String,
    pub commits: Vec<Record>,
    pub trees: Vec<Record>,
    pub blobs: Vec<Blob>,
    pub source_bytes: u64,
}
#[derive(Clone)]
pub(super) struct Raw {
    pub kind: String,
    pub bytes: Arc<Vec<u8>>,
}
pub(super) enum Source {
    Git(Git),
    Memory(BTreeMap<String, Raw>),
}
#[derive(Clone)]
pub(super) struct Stored {
    pub descriptor: m::Descriptor,
    pub bytes: Arc<Vec<u8>>,
}
pub(super) struct Graph {
    source: Source,
    pub raw: BTreeMap<String, Raw>,
    pub native: BTreeMap<(u8, [u8; 32]), Stored>,
    pub commits: BTreeMap<String, Record>,
    pub trees: BTreeMap<String, Record>,
    pub blobs: BTreeMap<(u8, String), Blob>,
    tree_heights: BTreeMap<String, usize>,
    tree_paths: BTreeMap<String, usize>,
    pub head: String,
    pub source_bytes: u64,
    charged: u64,
    entries: usize,
    limits: HistoryLimits,
    reserve: u64,
    deadline: Instant,
}
impl Graph {
    pub fn new(
        source: Source,
        limits: HistoryLimits,
        reserve: u64,
        deadline: Instant,
    ) -> Result<Self> {
        ensure!(
            reserve <= limits.max_memory_bytes,
            "SDK/history memory reservation exceeds limit"
        );
        Ok(Self {
            source,
            raw: BTreeMap::new(),
            native: BTreeMap::new(),
            commits: BTreeMap::new(),
            trees: BTreeMap::new(),
            blobs: BTreeMap::new(),
            tree_heights: BTreeMap::new(),
            tree_paths: BTreeMap::new(),
            head: String::new(),
            source_bytes: 0,
            charged: 0,
            entries: 0,
            limits,
            reserve,
            deadline,
        })
    }
    fn check(&self) -> Result<()> {
        ensure!(
            Instant::now() < self.deadline,
            "history graph deadline exceeded"
        );
        Ok(())
    }
    async fn read(&mut self, oid: &str, kind: &str) -> Result<Arc<Vec<u8>>> {
        self.check()?;
        ensure!(valid_hex(oid, 40), "invalid Git object identity");
        if let Some(raw) = self.raw.get(oid) {
            ensure!(raw.kind == kind, "Git object type alias differs");
            return Ok(raw.bytes.clone());
        }
        ensure!(
            self.raw.len() < self.limits.max_objects,
            "history object limit exceeded"
        );
        let cap = if kind == "blob" {
            self.limits.max_file_bytes
        } else {
            self.limits.max_metadata_bytes
        };
        let raw = match &self.source {
            Source::Git(git) => Raw {
                kind: kind.into(),
                bytes: Arc::new(git.read(oid, kind, cap).await?),
            },
            Source::Memory(map) => map
                .get(oid)
                .context("provenance Git object absent")?
                .clone(),
        };
        ensure!(
            raw.kind == kind && raw.bytes.len() <= cap,
            "provenance object exceeds type/byte bound"
        );
        super::io::verify(oid, kind, &raw.bytes)?;
        let bytes = raw.bytes.len() as u64;
        self.source_bytes = self
            .source_bytes
            .checked_add(bytes)
            .context("history byte overflow")?;
        ensure!(
            self.source_bytes <= self.limits.max_total_bytes,
            "history aggregate byte limit exceeded"
        );
        self.charged = self
            .charged
            .checked_add(
                bytes
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(4096))
                    .context("history memory overflow")?,
            )
            .context("history memory overflow")?;
        ensure!(
            self.reserve.saturating_add(self.charged) <= self.limits.max_memory_bytes,
            "history aggregate memory limit exceeded"
        );
        self.raw.insert(oid.into(), raw.clone());
        Ok(raw.bytes)
    }
    fn store(&mut self, object: m::Object) -> Result<String> {
        let encoded = m::encode(
            &object,
            m::Limits {
                max_object_bytes: self.limits.max_metadata_bytes,
                ..Default::default()
            },
        )?;
        let name = hex(&encoded.descriptor.key.id.0);
        self.native.insert(
            (
                encoded.descriptor.key.kind as u8,
                encoded.descriptor.key.id.0,
            ),
            Stored {
                descriptor: encoded.descriptor,
                bytes: Arc::new(encoded.bytes),
            },
        );
        Ok(name)
    }
    pub async fn build(&mut self, head: &str) -> Result<()> {
        let mut stack = vec![(head.to_string(), false)];
        let mut pending = BTreeMap::<String, ParsedCommit>::new();
        let mut visiting = BTreeSet::new();
        while let Some((oid, exit)) = stack.pop() {
            self.check()?;
            if self.commits.contains_key(&oid) {
                continue;
            }
            if exit {
                let parsed = pending
                    .remove(&oid)
                    .context("history traversal state absent")?;
                let tree = self.tree(&parsed.tree, 0).await?;
                let parents = if parsed.parents.is_empty() {
                    vec![m::CommitId::ROOT]
                } else {
                    parsed
                        .parents
                        .iter()
                        .map(|p| {
                            self.commits
                                .get(p)
                                .context("public Git parent missing")
                                .and_then(|r| Ok(m::CommitId(id(&r.native)?)))
                        })
                        .collect::<Result<Vec<_>>>()?
                };
                let mut change = blake3::Hasher::new();
                change.update(b"tinytree-git-history-change-v1\0");
                change.update(oid.as_bytes());
                let native = self.store(m::Object::Commit(m::Commit {
                    parents,
                    predecessors: vec![],
                    root_tree: m::TreeMerge {
                        terms: vec![m::TreeId(id(&tree)?)],
                        labels: vec![String::new()],
                    },
                    change_id: m::ChangeId(change.finalize().as_bytes()[..16].try_into()?),
                    description: parsed.message,
                    author: parsed.author,
                    committer: parsed.committer,
                    secure_signature: None,
                }))?;
                self.commits.insert(
                    oid.clone(),
                    Record {
                        git: oid.clone(),
                        native,
                        raw: self.raw[&oid].bytes.as_ref().clone(),
                    },
                );
                visiting.remove(&oid);
            } else {
                ensure!(visiting.insert(oid.clone()), "Git parent graph cycle");
                ensure!(
                    self.commits.len() + pending.len() < self.limits.max_commits,
                    "history commit limit exceeded"
                );
                let raw = self.read(&oid, "commit").await?;
                let parsed = parse_commit(&raw)?;
                stack.push((oid.clone(), true));
                for parent in parsed.parents.iter().rev() {
                    stack.push((parent.clone(), false));
                }
                pending.insert(oid, parsed);
            }
        }
        self.head = self
            .commits
            .get(head)
            .context("imported head absent")?
            .native
            .clone();
        Ok(())
    }
    fn tree<'a>(
        &'a mut self,
        oid: &'a str,
        depth: usize,
    ) -> Pin<Box<dyn Future<Output = Result<String>> + Send + 'a>> {
        Box::pin(async move {
            self.check()?;
            ensure!(
                depth < self.limits.max_depth,
                "Git tree depth limit exceeded"
            );
            if let Some(r) = self.trees.get(oid) {
                ensure!(
                    depth.saturating_add(self.tree_heights[oid]) < self.limits.max_depth,
                    "cached Git subtree exceeds path depth limit"
                );
                return Ok(r.native.clone());
            }
            let raw = self.read(oid, "tree").await?;
            let mut rest = raw.as_slice();
            let mut entries = vec![];
            let mut previous = None;
            let mut height = 0usize;
            let mut path_bytes = 0usize;
            while !rest.is_empty() {
                self.entries = self.entries.checked_add(1).context("entry overflow")?;
                ensure!(
                    self.entries <= self.limits.max_entries,
                    "history entry limit exceeded"
                );
                let nul = rest
                    .iter()
                    .position(|b| *b == 0)
                    .context("Git tree framing")?;
                let header = &rest[..nul];
                let space = header
                    .iter()
                    .position(|b| *b == b' ')
                    .context("Git tree mode framing")?;
                let mode = std::str::from_utf8(&header[..space])?;
                ensure!(
                    matches!(mode, "40000" | "100644" | "100755" | "120000"),
                    "unsupported Git mode/submodule"
                );
                let name = std::str::from_utf8(&header[space + 1..])?.to_string();
                ensure!(
                    !name.is_empty()
                        && name.len() <= 4096
                        && !matches!(name.as_str(), "." | "..")
                        && !name.eq_ignore_ascii_case(".git")
                        && !name.eq_ignore_ascii_case(".jj")
                        && !name.contains(['/', '\\']),
                    "unsupported Git path"
                );
                ensure!(rest.len() >= nul + 1 + 20, "truncated Git tree identity");
                let child = hex(&rest[nul + 1..nul + 21]);
                rest = &rest[nul + 21..];
                let mut order = name.as_bytes().to_vec();
                order.push(if mode == "40000" { b'/' } else { 0 });
                ensure!(
                    previous.as_ref().is_none_or(|p: &Vec<u8>| p < &order),
                    "noncanonical Git tree order"
                );
                previous = Some(order);
                let value = if mode == "40000" {
                    m::TreeValue::Tree(m::TreeId(id(&self.tree(&child, depth + 1).await?)?))
                } else {
                    let bytes = self.read(&child, "blob").await?;
                    if mode == "120000" {
                        let target = std::str::from_utf8(&bytes)?.to_string();
                        ensure!(
                            !target.contains('\0') && target.len() <= 4096,
                            "unsupported symlink target"
                        );
                        let native = self.store(m::Object::Symlink(target))?;
                        self.blobs.insert(
                            (m::Kind::Symlink as u8, child.clone()),
                            Blob {
                                git: child.clone(),
                                kind: m::Kind::Symlink as u8,
                                native: native.clone(),
                                size: bytes.len() as u64,
                                digest: *blake3::hash(&bytes).as_bytes(),
                            },
                        );
                        m::TreeValue::Symlink {
                            id: m::SymlinkId(id(&native)?),
                            target_bytes: bytes.len() as u64,
                        }
                    } else {
                        let descriptor = m::describe_file(&bytes);
                        let native = hex(&descriptor.key.id.0);
                        self.native.insert(
                            (m::Kind::File as u8, descriptor.key.id.0),
                            Stored {
                                descriptor,
                                bytes: bytes.clone(),
                            },
                        );
                        self.blobs.insert(
                            (m::Kind::File as u8, child.clone()),
                            Blob {
                                git: child.clone(),
                                kind: m::Kind::File as u8,
                                native: native.clone(),
                                size: bytes.len() as u64,
                                digest: *blake3::hash(&bytes).as_bytes(),
                            },
                        );
                        m::TreeValue::File {
                            id: m::FileId(descriptor.key.id.0),
                            executable: mode == "100755",
                            size_bytes: bytes.len() as u64,
                            copy_id: None,
                        }
                    }
                };
                match &value {
                    m::TreeValue::Tree(_) => {
                        height = height.max(self.tree_heights[&child] + 1);
                        path_bytes = path_bytes.max(name.len() + 1 + self.tree_paths[&child]);
                    }
                    _ => path_bytes = path_bytes.max(name.len()),
                }
                ensure!(
                    depth.saturating_add(height) < self.limits.max_depth && path_bytes <= 4096,
                    "Git history path exceeds depth/byte limit"
                );
                entries.push(m::TreeEntry { name, value });
            }
            entries.sort_by(|a, b| a.name.as_bytes().cmp(b.name.as_bytes()));
            let native = self.store(m::Object::Tree(m::Tree { entries }))?;
            self.tree_heights.insert(oid.into(), height);
            self.tree_paths.insert(oid.into(), path_bytes);
            self.trees.insert(
                oid.into(),
                Record {
                    git: oid.into(),
                    native: native.clone(),
                    raw: raw.as_ref().clone(),
                },
            );
            Ok(native)
        })
    }
    pub fn manifest(
        &self,
        repository: String,
        reference: String,
        expected_generation: u64,
        scope: [u8; 32],
        job: String,
        client: [u8; 16],
        git_head: String,
    ) -> Manifest {
        Manifest {
            version: 1,
            policy: POLICY.into(),
            object_format: "sha1".into(),
            repository,
            reference,
            expected_generation,
            scope,
            job,
            client,
            git_head,
            native_head: self.head.clone(),
            commits: self.commits.values().cloned().collect(),
            trees: self.trees.values().cloned().collect(),
            blobs: self.blobs.values().cloned().collect(),
            source_bytes: self.source_bytes,
        }
    }
    pub fn validate(&self, manifest: &Manifest) -> Result<()> {
        ensure!(
            manifest.version == 1
                && manifest.policy == POLICY
                && manifest.object_format == "sha1"
                && self.head == manifest.native_head
                && self.commits.values().eq(manifest.commits.iter())
                && self.trees.values().eq(manifest.trees.iter())
                && self.blobs.values().eq(manifest.blobs.iter())
                && self.source_bytes == manifest.source_bytes,
            "forged provenance/native-tree-parent association"
        );
        Ok(())
    }
}
struct ParsedCommit {
    tree: String,
    parents: Vec<String>,
    author: m::Signature,
    committer: m::Signature,
    message: String,
}
fn parse_commit(raw: &[u8]) -> Result<ParsedCommit> {
    let split = raw
        .windows(2)
        .position(|b| b == b"\n\n")
        .context("Git commit framing")?;
    let header = std::str::from_utf8(&raw[..split])?;
    let mut lines = header.lines();
    let tree = lines
        .next()
        .and_then(|l| l.strip_prefix("tree "))
        .context("Git commit tree absent")?
        .to_string();
    ensure!(valid_hex(&tree, 40), "invalid Git tree");
    let mut parents = vec![];
    let mut next = lines.next().context("Git author absent")?;
    while let Some(p) = next.strip_prefix("parent ") {
        ensure!(
            valid_hex(p, 40) && parents.len() < 4096,
            "invalid/excessive Git parent"
        );
        parents.push(p.to_string());
        next = lines.next().context("Git author absent")?;
    }
    ensure!(
        parents.iter().collect::<BTreeSet<_>>().len() == parents.len(),
        "duplicate Git parents cannot be exported by policy v1"
    );
    let author = parse_person(
        next.strip_prefix("author ")
            .context("unsupported Git headers/order/signature")?,
    )?;
    let committer = parse_person(
        lines
            .next()
            .and_then(|l| l.strip_prefix("committer "))
            .context("Git committer absent")?,
    )?;
    ensure!(
        lines.next().is_none(),
        "unsupported Git signatures/extra headers"
    );
    let message = std::str::from_utf8(&raw[split + 2..])?.to_string();
    ensure!(!message.contains('\0'), "Git message contains NUL");
    // Reject noncanonical serialization (including CRLF/negative-zero offsets).
    let mut canonical = format!("tree {tree}\n");
    for p in &parents {
        canonical.push_str(&format!("parent {p}\n"));
    }
    canonical.push_str(&format!(
        "author {}\ncommitter {}\n\n{message}",
        person(&author),
        person(&committer)
    ));
    ensure!(
        canonical.as_bytes() == raw,
        "Git metadata is not losslessly representable by policy v1"
    );
    Ok(ParsedCommit {
        tree,
        parents,
        author,
        committer,
        message,
    })
}
fn parse_person(raw: &str) -> Result<m::Signature> {
    let (identity, time) = raw.rsplit_once("> ").context("Git identity framing")?;
    let (name, email) = identity.rsplit_once(" <").context("Git identity framing")?;
    ensure!(
        !name.is_empty()
            && name.trim() == name
            && !name.contains(['\0', '\r', '\n', '<', '>'])
            && !email.is_empty()
            && !email
                .chars()
                .any(|c| c.is_whitespace() || matches!(c, '\0' | '<' | '>')),
        "unsupported Git identity"
    );
    let (sec, tz) = time.split_once(' ').context("Git timestamp framing")?;
    let seconds: i64 = sec.parse()?;
    ensure!(
        seconds >= 0
            && tz.len() == 5
            && matches!(tz.as_bytes()[0], b'+' | b'-')
            && tz[1..].bytes().all(|b| b.is_ascii_digit()),
        "unsupported Git timestamp"
    );
    let hour: i32 = tz[1..3].parse()?;
    let minute: i32 = tz[3..5].parse()?;
    ensure!(hour <= 23 && minute <= 59, "unsupported Git timezone");
    let offset = (hour * 60 + minute) * if tz.starts_with('-') { -1 } else { 1 };
    Ok(m::Signature {
        name: name.into(),
        email: email.into(),
        timestamp: m::Timestamp {
            millis_since_epoch: seconds
                .checked_mul(1000)
                .context("Git timestamp overflow")?,
            tz_offset_minutes: offset,
        },
    })
}
fn person(p: &m::Signature) -> String {
    let off = p.timestamp.tz_offset_minutes;
    format!(
        "{} <{}> {} {}{:02}{:02}",
        p.name,
        p.email,
        p.timestamp.millis_since_epoch / 1000,
        if off < 0 { '-' } else { '+' },
        off.abs() / 60,
        off.abs() % 60
    )
}
