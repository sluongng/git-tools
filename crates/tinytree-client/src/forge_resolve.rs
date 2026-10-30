//! Bounded projection preparation. Pending control assertions are never object
//! proofs; callers still verify native objects before importing the snapshot.
use super::*;
use serde::Deserialize;
use tinytree_forge_core::{
    account_commands::{ResolveViewPending, ResolveViewRequest},
    accounts::{validate_name, validate_path},
};
use tinytree_proto::forge_v1 as f;

// Preparation includes bounded traversal, output batches and closure pages.
// Keep a finite round-trip envelope with headroom for the measured repository;
// the existing absolute operation deadline still bounds the whole workflow.
const MAX_STEPS: usize = 512;
const MAX_SAFE_INTEGER: u64 = (1u64 << 53) - 1;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    account_id: String,
    view: String,
    view_version: [u8; 32],
    revision: [u8; 32],
    policy_epoch: u64,
    root_prefix: String,
    projected_commit: [u8; 32],
    projected_tree: [u8; 32],
    paths: Vec<String>,
    readme: Option<serde_json::Value>,
    readme_error: Option<serde_json::Value>,
    complete: bool,
}
fn invalid() -> Status {
    Status::data_loss("invalid forge projection assertion")
}
fn uuid(value: &str) -> bool {
    value.len() == 36
        && value != "00000000-0000-0000-0000-000000000000"
        && value.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}
fn identity(
    input: &f::ForgeCallRequest,
    account: &str,
    view: &str,
    version: [u8; 32],
    revision: [u8; 32],
    epoch: u64,
    prefix: &str,
    pinned: Option<[u8; 32]>,
) -> Result<(), Status> {
    if account != input.account_id
        || (!input.view.is_empty() && view != input.view)
        || validate_name(view).is_err()
        || version == [0; 32]
        || revision == [0; 32]
        || epoch == 0
        || epoch > MAX_SAFE_INTEGER
        || validate_path(prefix, true).is_err()
        || pinned.is_some_and(|v| v != version)
    {
        return Err(invalid());
    }
    Ok(())
}
impl NativeClient {
    /// Resolve one immutable view, preparing its projection when necessary.
    /// The first selection is pinned for every continuation; transport retries
    /// replay that exact continuation, never a new unnamed/latest selection.
    /// All calls and pacing share one operation deadline and a finite step cap.
    pub async fn forge_resolve_view(
        &self,
        mut input: f::ForgeCallRequest,
    ) -> Result<f::ForgeCallResponse, Status> {
        if input.operation != f::ForgeOperation::ResolveView as i32 {
            return Err(Status::invalid_argument("expected ResolveView operation"));
        }
        let mut request: ResolveViewRequest = serde_json::from_slice(&input.payload_json)
            .map_err(|_| Status::invalid_argument("invalid ResolveView request"))?;
        if request.version == Some([0; 32])
            || request.progress.as_ref().is_some_and(|p| {
                request.version.is_none()
                    || !uuid(&p.job_id)
                    || p.expected_generation > MAX_SAFE_INTEGER
            })
        {
            return Err(Status::invalid_argument("invalid projection continuation"));
        }
        let deadline = self.deadline();
        let client = self.clone().with_operation_deadline(deadline);
        let mut selected: Option<ResolveViewPending> = None;
        for step in 0..MAX_STEPS {
            if step != 0 {
                tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(10)))
                    .await
                    .map_err(|_| {
                        Status::deadline_exceeded("projection resolution deadline exceeded")
                    })?;
            }
            let reply = client.forge_call(input.clone()).await?;
            match reply.state.as_str() {
                "pending" => {
                    let pending: ResolveViewPending =
                        serde_json::from_slice(&reply.result_json).map_err(|_| invalid())?;
                    identity(
                        &input,
                        &pending.account_id,
                        &pending.view,
                        pending.view_version,
                        pending.revision,
                        pending.policy_epoch,
                        &pending.root_prefix,
                        request.version,
                    )?;
                    if !uuid(&pending.progress.job_id)
                        || pending.progress.expected_generation > MAX_SAFE_INTEGER
                    {
                        return Err(invalid());
                    }
                    if request.progress.as_ref().is_some_and(|p| {
                        p.job_id != pending.progress.job_id
                            || p.expected_generation > pending.progress.expected_generation
                    }) {
                        return Err(invalid());
                    }
                    if let Some(first) = &selected {
                        if first.view != pending.view
                            || first.view_version != pending.view_version
                            || first.revision != pending.revision
                            || first.policy_epoch != pending.policy_epoch
                            || first.root_prefix != pending.root_prefix
                            || first.progress.job_id != pending.progress.job_id
                        {
                            return Err(Status::aborted(
                                "projection selection changed during preparation",
                            ));
                        }
                    }
                    request.version = Some(pending.view_version);
                    request.progress = Some(pending.progress.clone());
                    input.payload_json = serde_json::to_vec(&request)
                        .map_err(|_| Status::internal("projection request encoding failed"))?;
                    selected = Some(pending);
                }
                "complete" => {
                    let snapshot: Snapshot =
                        serde_json::from_slice(&reply.result_json).map_err(|_| invalid())?;
                    identity(
                        &input,
                        &snapshot.account_id,
                        &snapshot.view,
                        snapshot.view_version,
                        snapshot.revision,
                        snapshot.policy_epoch,
                        &snapshot.root_prefix,
                        request.version,
                    )?;
                    if !snapshot.complete
                        || snapshot.projected_commit == [0; 32]
                        || snapshot.projected_tree == [0; 32]
                        || snapshot
                            .paths
                            .iter()
                            .any(|path| validate_path(path, false).is_err())
                    {
                        return Err(invalid());
                    }
                    if let Some(first) = &selected {
                        if first.view != snapshot.view
                            || first.revision != snapshot.revision
                            || first.policy_epoch != snapshot.policy_epoch
                            || first.root_prefix != snapshot.root_prefix
                        {
                            return Err(Status::aborted(
                                "projection selection changed during preparation",
                            ));
                        }
                    }
                    // README remains a control-plane DTO, not a native FILE proof.
                    let _ = (&snapshot.readme, &snapshot.readme_error);
                    return Ok(reply);
                }
                _ => return Err(invalid()),
            }
        }
        Err(Status::resource_exhausted(
            "projection preparation step limit exceeded",
        ))
    }
}
