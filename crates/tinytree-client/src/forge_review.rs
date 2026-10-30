//! Finite same-intent source admission, not automatic check completion/landing.
use super::*;
use tinytree_forge_core::review_commands::{
    self as review, ADMISSION_PENDING_STATE, AdmissionPending, ReviewCommand,
};
use tinytree_proto::forge_v1 as f;

impl NativeClient {
    /// Advance one exact original Push intent until admission or a finite bound.
    /// Pre-admission replies do not reserve/acknowledge a patchset. Accepted
    /// rebase_pending remains a real review and is returned without auto-drain.
    pub async fn forge_push_review(
        &self,
        input: f::ForgeCallRequest,
    ) -> Result<f::ForgeCallResponse, Status> {
        if input.operation != f::ForgeOperation::PushReview as i32 || input.request_id.is_empty() {
            return Err(Status::invalid_argument("expected saved PushReview intent"));
        }
        let Some(ReviewCommand::Push(intent)) = review::parse(20, &input.payload_json)
            .map_err(|_| Status::invalid_argument("invalid PushReview intent"))?
        else {
            return Err(Status::invalid_argument("expected PushReview intent"));
        };
        let deadline = self
            .deadline()
            .min(Instant::now() + Duration::from_secs(60));
        let client = self.clone().with_operation_deadline(deadline);
        for round in 0..512 {
            if round != 0 {
                tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(10)))
                    .await
                    .map_err(|_| {
                        Status::deadline_exceeded(
                            "review admission deadline; resume the original request",
                        )
                    })?;
            }
            let reply = client.forge_call(input.clone()).await?;
            if reply.state == ADMISSION_PENDING_STATE {
                let pending: AdmissionPending = serde_json::from_slice(&reply.result_json)
                    .map_err(|_| Status::data_loss("invalid review admission reply"))?;
                pending
                    .validate_against(&input.request_id, &intent)
                    .map_err(|_| Status::data_loss("review admission intent binding changed"))?;
            } else {
                if !matches!(
                    reply.state.as_str(),
                    "rebase_pending"
                        | "prepared"
                        | "conflict"
                        | "stale"
                        | "checks_pending"
                        | "ready_to_land"
                        | "landed"
                        | "unsupported"
                        | "check_failed"
                        | "expired"
                ) {
                    return Err(Status::data_loss("unexpected review admission state"));
                }
                return Ok(reply);
            }
        }
        Err(Status::resource_exhausted(
            "review admission round limit; resume the original request",
        ))
    }
}
