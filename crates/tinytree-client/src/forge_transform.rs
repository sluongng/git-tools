//! Finite exact-intent transform preparation; no automatic checking or landing.
use super::*;
use tinytree_forge_core::transform_commands::{
    self as transform, Command, TRANSFORM_PENDING_STATE, TransformPending,
};
use tinytree_proto::forge_v1 as f;

impl NativeClient {
    pub async fn forge_transform(
        &self,
        input: f::ForgeCallRequest,
    ) -> Result<f::ForgeCallResponse, Status> {
        if input.request_id.is_empty()
            || ![
                f::ForgeOperation::StartTransform as i32,
                f::ForgeOperation::SetPin as i32,
            ]
            .contains(&input.operation)
        {
            return Err(Status::invalid_argument("expected saved transform intent"));
        }
        let intent = match transform::parse(input.operation, &input.payload_json)
            .map_err(|_| Status::invalid_argument("invalid transform intent"))?
        {
            Some(Command::Start(intent) | Command::SetPin(intent)) => intent,
            _ => return Err(Status::invalid_argument("expected transform mutation")),
        };
        let deadline = self
            .deadline()
            .min(Instant::now() + Duration::from_secs(60));
        let client = self.clone().with_operation_deadline(deadline);
        let mut selected = None;
        let mut generation = None;
        for round in 0..512 {
            if round != 0 {
                tokio::time::timeout_at(deadline, tokio::time::sleep(Duration::from_millis(10)))
                    .await
                    .map_err(|_| {
                        Status::deadline_exceeded(
                            "transform preparation deadline; resume the original request",
                        )
                    })?;
            }
            let reply = client.forge_call(input.clone()).await?;
            if reply.state == TRANSFORM_PENDING_STATE {
                let pending: TransformPending = serde_json::from_slice(&reply.result_json)
                    .map_err(|_| Status::data_loss("invalid transform pending assertion"))?;
                let binding = pending
                    .validate_against(
                        &input.request_id,
                        input.operation,
                        &input.account_id,
                        &input.view,
                        &intent,
                        selected.as_ref(),
                    )
                    .map_err(|_| Status::data_loss("transform pending binding changed"))?;
                if generation.is_some_and(|old| pending.generation < old) {
                    return Err(Status::data_loss("transform pending generation regressed"));
                }
                generation = Some(pending.generation);
                selected = Some(binding);
            } else {
                transform::validate_ack(
                    &reply.result_json,
                    &reply.state,
                    &input.account_id,
                    &intent,
                    selected.as_ref(),
                )
                .map_err(|_| Status::data_loss("invalid transform acknowledgement"))?;
                return Ok(reply);
            }
        }
        Err(Status::resource_exhausted(
            "transform preparation round limit; resume the original request",
        ))
    }
}
