//! Portable capture DATA codec. Structural validity and a declared policy are
//! not live frontend proof, authenticated cloud attachment, or restore authority.
use crate as c;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use tinytree_model as m;
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapturePolicy {
    TreeFreezeCandidateV1,
    HostQuiescentV1,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureTree {
    pub terms: Vec<[u8; 32]>,
    pub labels: Vec<String>,
}
impl CaptureTree {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.terms.is_empty()
                && self.terms.len() <= 4095
                && self.terms.len() % 2 == 1
                && (self.labels.is_empty() || self.labels.len() == self.terms.len())
                && self
                    .labels
                    .iter()
                    .try_fold(0usize, |sum, label| sum.checked_add(label.len()))
                    .is_some_and(|n| n <= 1024 * 1024),
            "invalid capture tree"
        );
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RemoteObservation {
    pub reference: String,
    pub bookmark: String,
    pub generation: u64,
    pub head: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArchivedIntent {
    pub reference: String,
    pub bookmark: String,
    pub generation: u64,
    pub head: Option<String>,
    pub request_id: String,
    pub base_operation: String,
    pub confirmed_generation: Option<u64>,
}
#[derive(Clone, Copy, Debug)]
pub struct CaptureDataLimits {
    pub max_bytes: usize,
    pub max_decode_bytes: usize,
    pub max_operations: usize,
    pub max_parent_edges: usize,
    pub max_roots: usize,
    pub max_blobs: usize,
}
impl Default for CaptureDataLimits {
    fn default() -> Self {
        Self {
            max_bytes: 8 * 1024 * 1024,
            max_decode_bytes: 256 * 1024 * 1024,
            max_operations: 1024,
            max_parent_edges: 100_000,
            max_roots: 100_000,
            max_blobs: 100_000,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PortableWorkingCopy {
    pub workspace: String,
    pub wc_operation: String,
    pub snapshot_operation: String,
    pub snapshot_commit: [u8; 32],
    pub tree: CaptureTree,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct OperationRecord {
    pub operation: String,
    pub view: String,
    pub parents: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BlobRecord {
    pub kind: u8,
    pub id: String,
    pub blake3: [u8; 32],
    pub size: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IndexRecord {
    pub operation: String,
    pub root: [u8; 32],
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PortableRemoteState {
    pub observations: Vec<RemoteObservation>,
    pub archived_pushes: Vec<ArchivedIntent>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureData {
    pub version: u32,
    pub policy: CapturePolicy,
    pub codec: String,
    pub operation_heads: Vec<String>,
    pub working_copy: PortableWorkingCopy,
    pub operations: Vec<OperationRecord>,
    pub indexes: Vec<IndexRecord>,
    pub blobs: Vec<BlobRecord>,
    pub roots: Vec<CapturedRoot>,
    pub remote: Option<PortableRemoteState>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct CapturedRoot {
    pub kind: u8,
    pub id: [u8; 32],
}
pub fn data_digest(bytes: &[u8]) -> [u8; 32] {
    m::cloud_capture_data_id(bytes)
}
/// Digest/canonical-schema check only, NOT source quiescence or capture authority.
/// Only read_candidate can create LocalCaptureCandidate with physical ownership.
pub fn decode_data(
    bytes: &[u8],
    expected: [u8; 32],
    limits: &CaptureDataLimits,
) -> Result<CaptureData> {
    ensure!(
        limits.max_bytes > 0
            && limits.max_bytes <= 64 * 1024 * 1024
            && limits.max_decode_bytes <= 1024 * 1024 * 1024
            && limits.max_operations > 0
            && limits.max_parent_edges > 0
            && limits.max_roots > 0
            && limits.max_blobs > 0,
        "invalid capture data bounds"
    );
    ensure!(
        bytes.len() <= limits.max_bytes && data_digest(bytes) == expected,
        "capture data identity/size mismatch"
    );
    ensure!(
        bytes
            .len()
            .checked_mul(32)
            .and_then(|n| n.checked_add(4096))
            .is_some_and(|n| n <= limits.max_decode_bytes),
        "capture data decode budget"
    );
    let data: CaptureData = serde_json::from_slice(bytes).context("invalid capture data schema")?;
    validate_capture_data(&data, limits)?;
    ensure!(
        encode_capture_data(&data, *limits)? == bytes,
        "noncanonical capture data"
    );
    Ok(data)
}
pub fn validate_capture_data(data: &CaptureData, limits: &CaptureDataLimits) -> Result<()> {
    ensure!(
        data.version == 1 && data.codec == c::CODEC,
        "unsupported capture data format"
    );
    ensure!(
        !data.operation_heads.is_empty()
            && data.operations.len() <= limits.max_operations
            && data.indexes.len() == data.operations.len(),
        "capture operation inventory mismatch"
    );
    ensure!(
        data.operations
            .first()
            .is_some_and(|entry| entry.operation == "00".repeat(64))
            && data
                .operations
                .windows(2)
                .all(|p| p[0].operation < p[1].operation),
        "capture operations must include explicit op0 in order"
    );
    let mut edges = 0usize;
    for operation in &data.operations {
        ensure!(valid_hex(&operation.operation, 128), "invalid operation64");
        ensure!(valid_hex(&operation.view, 128), "invalid view64");
        edges = edges
            .checked_add(operation.parents.len())
            .context("capture edge overflow")?;
        ensure!(edges <= limits.max_parent_edges, "capture edge limit");
        for parent in &operation.parents {
            ensure!(valid_hex(parent, 128), "invalid parent operation64");
            ensure!(
                data.operations
                    .binary_search_by(|entry| entry.operation.cmp(parent))
                    .is_ok(),
                "capture missing operation ancestry"
            );
        }
    }
    ensure!(
        data.operations[0].parents.is_empty() && data.operations[0].view == "00".repeat(64),
        "intrinsic capture operation shape"
    );
    let mut state = vec![0u8; data.operations.len()];
    for start in 0..data.operations.len() {
        let mut stack = vec![(start, false)];
        while let Some((at, exit)) = stack.pop() {
            if exit {
                state[at] = 2;
                continue;
            }
            if state[at] == 2 {
                continue;
            }
            ensure!(state[at] != 1, "capture operation cycle");
            state[at] = 1;
            ensure!(
                stack
                    .len()
                    .checked_add(data.operations[at].parents.len())
                    .is_some_and(|n| n
                        <= limits
                            .max_parent_edges
                            .saturating_add(limits.max_operations)),
                "capture operation frontier"
            );
            stack.push((at, true));
            for parent in data.operations[at].parents.iter().rev() {
                let index = data
                    .operations
                    .binary_search_by(|entry| entry.operation.cmp(parent))
                    .unwrap();
                stack.push((index, false));
            }
        }
    }
    for (operation, index) in data.operations.iter().zip(&data.indexes) {
        ensure!(
            operation.operation == index.operation,
            "capture missing exact index association"
        );
    }
    for head in data.operation_heads.iter().chain([
        &data.working_copy.wc_operation,
        &data.working_copy.snapshot_operation,
    ]) {
        ensure!(valid_hex(head, 128), "invalid selected operation64");
        ensure!(
            data.operations
                .binary_search_by(|entry| entry.operation.cmp(head))
                .is_ok(),
            "capture missing selected operation"
        );
    }
    ensure!(
        data.operation_heads.windows(2).all(|p| p[0] < p[1])
            && !data.working_copy.workspace.is_empty()
            && data.working_copy.workspace.len() <= 1024,
        "invalid portable working copy"
    );
    data.working_copy.tree.validate()?;
    ensure!(
        data.roots.len() <= limits.max_roots && data.roots.windows(2).all(|p| p[0] < p[1]),
        "invalid capture roots"
    );
    for root in &data.roots {
        m::Kind::try_from(root.kind)?;
        ensure!(
            !(root.kind == m::Kind::Commit as u8 && root.id == [0; 32]),
            "intrinsic commit cannot be stored root"
        );
    }
    ensure!(
        data.blobs
            .windows(2)
            .all(|p| (p[0].kind, &p[0].id) < (p[1].kind, &p[1].id)),
        "capture blob order"
    );
    ensure!(
        data.blobs.len() <= limits.max_blobs,
        "capture blob count limit"
    );
    for blob in &data.blobs {
        ensure!(
            (1..=4).contains(&blob.kind)
                && valid_hex(&blob.id, if blob.kind <= 2 { 128 } else { 64 })
                && blob.size > 0,
            "invalid capture blob"
        );
    }
    for operation in &data.operations {
        for (kind, id) in [(1, &operation.operation), (2, &operation.view)] {
            if id != "00".repeat(64).as_str() {
                ensure!(
                    data.blobs
                        .binary_search_by(
                            |blob| (blob.kind, blob.id.as_str()).cmp(&(kind, id.as_str()))
                        )
                        .is_ok(),
                    "capture selected OP/VIEW payload missing"
                );
            }
        }
    }
    for index in &data.indexes {
        let id = index
            .root
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        ensure!(
            data.blobs
                .binary_search_by(|blob| (blob.kind, blob.id.as_str()).cmp(&(3, id.as_str())))
                .is_ok(),
            "capture selected index root missing"
        );
    }
    if let Some(remote) = &data.remote {
        ensure!(
            remote.observations.len() <= 128 && remote.archived_pushes.len() <= 128,
            "capture remote record limit"
        );
        let names_valid = |reference: &str, bookmark: &str| {
            !reference.is_empty()
                && reference.len() <= 512
                && !reference.contains('\0')
                && reference != "_tinytree"
                && !reference.starts_with("_tinytree/")
                && !bookmark.is_empty()
                && bookmark.len() <= 512
                && !bookmark.contains('\0')
        };
        let head_valid = |head: &Option<String>| {
            head.as_ref()
                .is_none_or(|id| valid_hex(id, 64) && id != &"00".repeat(32))
        };
        let mut references = std::collections::BTreeSet::new();
        let mut bookmarks = std::collections::BTreeSet::new();
        for observation in &remote.observations {
            ensure!(
                names_valid(&observation.reference, &observation.bookmark)
                    && head_valid(&observation.head)
                    && (observation.head.is_none() || observation.generation > 0)
                    && references.insert(&observation.reference)
                    && bookmarks.insert(&observation.bookmark),
                "invalid capture remote observation"
            );
        }
        let mut requests = std::collections::BTreeSet::new();
        for intent in &remote.archived_pushes {
            ensure!(
                names_valid(&intent.reference, &intent.bookmark)
                    && intent.head.is_some()
                    && head_valid(&intent.head)
                    && valid_hex(&intent.request_id, 32)
                    && requests.insert(&intent.request_id)
                    && intent
                        .confirmed_generation
                        .is_none_or(|n| intent.generation.checked_add(1) == Some(n))
                    && valid_hex(&intent.base_operation, 128)
                    && data
                        .operations
                        .binary_search_by(|op| op.operation.cmp(&intent.base_operation))
                        .is_ok(),
                "capture archived operation missing"
            );
        }
    }
    Ok(())
}

pub fn encode_capture_data(data: &CaptureData, limits: CaptureDataLimits) -> Result<Vec<u8>> {
    validate_capture_data(data, &limits)?;
    struct Bounded {
        bytes: Vec<u8>,
        cap: usize,
    }
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if self
                .bytes
                .len()
                .checked_add(bytes.len())
                .is_none_or(|n| n > self.cap)
            {
                return Err(std::io::Error::other("capture data byte cap"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded {
        bytes: vec![],
        cap: limits.max_bytes,
    };
    serde_json::to_writer(&mut output, data)?;
    Ok(output.bytes)
}
fn valid_hex(text: &str, length: usize) -> bool {
    text.len() == length
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
