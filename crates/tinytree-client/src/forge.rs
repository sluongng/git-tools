//! Authenticated product-operation assertions, not canonical object proofs.
//! Callers must decode the operation's concrete result schema and verify native
//! objects before importing them into a workspace. A pending reply is not land.
use super::*;
use tinytree_proto::{FORGE_MAX_REQUEST_BYTES, FORGE_MAX_RESPONSE_BYTES, forge_v1 as f};

impl NativeClient {
    /// Execute an account-scoped operation using its already-saved intent ID.
    /// Transport retry preserves the exact account/view/payload and request ID.
    pub async fn forge_call(
        &self,
        input: f::ForgeCallRequest,
    ) -> Result<f::ForgeCallResponse, Status> {
        let operation = f::ForgeOperation::try_from(input.operation)
            .map_err(|_| Status::invalid_argument("unknown forge operation"))?;
        if operation == f::ForgeOperation::Unspecified
            || input.account_id.is_empty()
            || input.account_id.len() > 64
            || input.view.len() > 128
            || input.request_id.len() > 128
            || input.account_id.chars().any(char::is_control)
            || input.view.chars().any(char::is_control)
            || input.request_id.chars().any(char::is_control)
            || input.encoded_len() > FORGE_MAX_REQUEST_BYTES
            || std::str::from_utf8(&input.payload_json).is_err()
        {
            return Err(Status::invalid_argument("invalid bounded forge request"));
        }
        let deadline = self.deadline();
        // Retained request plus retry/encoding, bounded response and decoding.
        // This uses the same physical-owner budget as other native RPCs.
        let memory = input
            .encoded_len()
            .saturating_mul(3)
            .saturating_add(FORGE_MAX_RESPONSE_BYTES * 3);
        let budget = self.admit(deadline, memory).await?;
        let response = self
            .retry(deadline, false, |allowance| {
                let request = self.request(input.clone(), allowance);
                let mut grpc = self.rpc_client(budget.clone()).uncompressed();
                async move { grpc.forge_call(request).await.map(|r| r.into_inner()) }
            })
            .await?;
        if response.operation != input.operation
            || response.account_id != input.account_id
            || response.view != input.view
            || response.request_id != input.request_id
            || response.state.is_empty()
            || response.state.len() > 64
            || !response
                .state
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || response.result_json.is_empty()
            || std::str::from_utf8(&response.result_json).is_err()
            || response.encoded_len() > FORGE_MAX_RESPONSE_BYTES
        {
            return Err(Status::data_loss("forge response binding is invalid"));
        }
        Ok(response)
    }
}
