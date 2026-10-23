use crate::{Limits, proto::Envelope};
use jj_vfs::native::{NativeTree, ProjectionBinding, ProjectionReceipt, ProjectionState};
use native_vfs_controller::{PolicyCandidate, PolicyDecision, PolicyIgnore};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::path::PathBuf;
use tonic::Status;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Binding {
    pub root: PathBuf,
    pub state_path: PathBuf,
    pub repository: String,
}
impl From<&ProjectionBinding> for Binding {
    fn from(v: &ProjectionBinding) -> Self {
        Self {
            root: v.root.clone(),
            state_path: v.state_path.clone(),
            repository: v.repository.clone(),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Registration {
    pub binding: Binding,
    pub incarnation: [u8; 16],
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Unary {
    Inspect {
        mount: String,
        expected: Option<Registration>,
    },
    Receipt {
        mount: String,
        registration: Registration,
        request_id: String,
    },
    Acknowledge {
        mount: String,
        registration: Registration,
        state: ProjectionState,
        operation: Vec<u8>,
        receipt: Option<ProjectionReceipt>,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Reply {
    Inspected {
        binding: Binding,
        incarnation: [u8; 16],
        state: ProjectionState,
    },
    Applied {
        receipt: ProjectionReceipt,
    },
    NotApplied,
    Unknown,
    Acknowledged,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Input {
    Begin {
        mount: String,
        registration: Registration,
        expected: ProjectionState,
    },
    Snapshot {
        sequence: u64,
        request_id: String,
        fingerprint: [u8; 32],
    },
    Checkout {
        sequence: u64,
        request_id: String,
        tree: NativeTree,
    },
    Reset {
        sequence: u64,
        request_id: String,
        tree: NativeTree,
        recover: bool,
    },
    Finish {
        sequence: u64,
        state: ProjectionState,
        operation: Vec<u8>,
        receipt: Option<ProjectionReceipt>,
    },
    Decision {
        index: u64,
        decision: PolicyDecision,
    },
    PolicyAbort,
    DecisionsComplete {
        count: u64,
        digest: [u8; 32],
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Output {
    Began {
        state: ProjectionState,
    },
    Candidate {
        entry: PolicyCandidate,
    },
    Ignore {
        entry: PolicyIgnore,
    },
    InventoryComplete {
        candidates: u64,
        ignores: u64,
        digest: [u8; 32],
    },
    Untracked {
        path: String,
        max_size: Option<u64>,
        size: u64,
    },
    Snapshot {
        sequence: u64,
        receipt: ProjectionReceipt,
    },
    Checkout {
        sequence: u64,
        receipt: ProjectionReceipt,
        updated: u32,
        added: u32,
        removed: u32,
        skipped: u32,
    },
    Reset {
        sequence: u64,
        receipt: ProjectionReceipt,
    },
    Finished {
        sequence: u64,
    },
    Failure {
        sequence: u64,
        rejected: bool,
        message: String,
    },
}
pub(crate) fn encode(value: &impl Serialize, limits: &Limits) -> Result<Envelope, Status> {
    let payload = serde_json::to_vec(value)
        .map_err(|_| Status::internal("cannot encode projection frame"))?;
    if payload.len() > limits.max_frame_bytes {
        return Err(Status::resource_exhausted("projection frame exceeds limit"));
    }
    Ok(Envelope { payload })
}
pub(crate) fn decode<T: DeserializeOwned>(frame: Envelope, limits: &Limits) -> Result<T, Status> {
    if frame.payload.len() > limits.max_frame_bytes {
        return Err(Status::resource_exhausted("projection frame exceeds limit"));
    }
    crate::bounded_json::check(&frame.payload, limits)?;
    serde_json::from_slice(&frame.payload)
        .map_err(|_| Status::invalid_argument("invalid projection domain frame"))
}
pub(crate) fn mount(id: &str) -> Result<(), Status> {
    if id.is_empty()
        || id.len() > 128
        || !id
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || b"-_.".contains(&v))
    {
        return Err(Status::invalid_argument(
            "invalid configured projection identity",
        ));
    }
    Ok(())
}
pub(crate) fn state(v: &ProjectionState, repository: &str) -> Result<(), Status> {
    v.validate(repository)
        .map_err(|_| Status::invalid_argument("invalid projection state"))
}
pub(crate) fn receipt(v: &ProjectionReceipt, repository: &str) -> Result<(), Status> {
    request_id(&v.request_id)?;
    state(&v.before, repository)?;
    state(&v.after, repository)?;
    if v.after.generation <= v.before.generation {
        return Err(Status::invalid_argument(
            "invalid projection receipt generation",
        ));
    }
    Ok(())
}
pub(crate) fn request_id(id: &str) -> Result<(), Status> {
    if id.len() != 36
        || !id.bytes().enumerate().all(|(index, v)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                v == b'-'
            } else {
                v.is_ascii_hexdigit()
            }
        })
    {
        return Err(Status::invalid_argument(
            "invalid projection intent identity",
        ));
    }
    Ok(())
}
pub(crate) fn operation(bytes: Vec<u8>) -> Result<jj_lib::op_store::OperationId, Status> {
    if bytes.is_empty() || bytes.len() > 128 {
        return Err(Status::invalid_argument("invalid jj operation identity"));
    }
    Ok(jj_lib::op_store::OperationId::new(bytes))
}
pub(crate) fn hash_frame(hash: &mut blake3::Hasher, frame: &Envelope) {
    hash.update(&(frame.payload.len() as u64).to_be_bytes());
    hash.update(&frame.payload);
}
