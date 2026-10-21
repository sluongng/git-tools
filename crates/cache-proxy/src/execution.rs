//! Transparent scheduler forwarding. Bazel owns Execute/WaitExecution recovery;
//! replaying Execute here could submit the same action twice.
use super::*;
use reapi::proto::google::longrunning::{self as lro, operations_server::Operations};

#[derive(Clone, Copy)]
pub(super) struct Budget(Option<tokio::time::Instant>);
impl Budget {
    pub(super) fn capture<T>(
        request: &Request<T>,
        limit: Option<Duration>,
    ) -> Result<Self, Status> {
        let now = tokio::time::Instant::now();
        let configured = limit.and_then(|d| now.checked_add(d));
        let caller = request
            .metadata()
            .get("grpc-timeout")
            .map(|value| {
                let value = value
                    .to_str()
                    .map_err(|_| Status::invalid_argument("invalid RPC timeout"))?;
                if !(2..=9).contains(&value.len()) {
                    return Err(Status::invalid_argument("invalid RPC timeout"));
                }
                let (number, unit) = value.split_at(value.len() - 1);
                if !number.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(Status::invalid_argument("invalid RPC timeout"));
                }
                let number: u64 = number
                    .parse()
                    .map_err(|_| Status::invalid_argument("invalid RPC timeout"))?;
                let duration = match unit {
                    "H" => Duration::from_secs(number * 3600),
                    "M" => Duration::from_secs(number * 60),
                    "S" => Duration::from_secs(number),
                    "m" => Duration::from_millis(number),
                    "u" => Duration::from_micros(number),
                    "n" => Duration::from_nanos(number),
                    _ => return Err(Status::invalid_argument("invalid RPC timeout")),
                };
                now.checked_add(duration)
                    .ok_or_else(|| Status::invalid_argument("RPC timeout exceeds clock range"))
            })
            .transpose()?;
        Ok(Self(match (configured, caller) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }))
    }
    pub(super) async fn run<T>(
        &self,
        operation: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        match self.0 {
            Some(deadline) => tokio::time::timeout_at(deadline, operation)
                .await
                .map_err(|_| Status::deadline_exceeded("scheduler RPC deadline exceeded"))?,
            None => operation.await,
        }
    }
    pub(super) fn request<T>(
        &self,
        upstream: &Upstream,
        value: T,
        metadata: &CallerMetadata,
    ) -> Result<Request<T>, Status> {
        let mut request = upstream.request_with_metadata(value, metadata);
        // Cache RPC timeout is not an execution lifetime.
        request.metadata_mut().remove("grpc-timeout");
        if let Some(deadline) = self.0 {
            let remaining = deadline
                .checked_duration_since(tokio::time::Instant::now())
                .ok_or_else(|| Status::deadline_exceeded("scheduler RPC deadline exceeded"))?;
            request.set_timeout(remaining);
        }
        Ok(request)
    }
}
impl CacheProxy {
    async fn scheduler_call<T>(
        &self,
        budget: Budget,
        operation: impl Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        let mut stop = self.stop.subscribe();
        if *stop.borrow() {
            return Err(Status::unavailable("RBE proxy shutting down"));
        }
        tokio::select! {
            result=budget.run(operation)=>result,
            _=stop.wait_for(|value|*value)=>Err(Status::unavailable("RBE proxy shutting down")),
        }
    }
    fn execution_stream(
        &self,
        mut stream: tonic::Streaming<lro::Operation>,
        budget: Budget,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> RpcStream<lro::Operation> {
        let mut stop = self.stop.subscribe();
        Box::pin(async_stream::try_stream! {
            let _permit=permit;
            loop {
                let operation=tokio::select! {
                    result=budget.run(stream.message())=>result,
                    _=stop.wait_for(|value|*value)=>Err(Status::unavailable("RBE proxy shutting down")),
                }?;
                match operation {Some(operation)=>yield operation,None=>break}
            }
        })
    }
    fn execution_client(&self, method: RpcMethod) -> execution_client::ExecutionClient<Channel> {
        let client = execution_client::ExecutionClient::new(self.upstream.stream_channel.clone())
            .accept_compressed(CompressionEncoding::Gzip)
            .max_encoding_message_size(self.upstream.config.max_batch_bytes + 1024 * 1024)
            .max_decoding_message_size(self.upstream.config.max_batch_bytes + 1024 * 1024);
        if self.upstream.send_gzip(method) {
            client.send_compressed(CompressionEncoding::Gzip)
        } else {
            client
        }
    }
    fn operations_client(
        &self,
        method: RpcMethod,
    ) -> lro::operations_client::OperationsClient<Channel> {
        let channel = if matches!(method, RpcMethod::WaitOperation) {
            self.upstream.stream_channel.clone()
        } else {
            self.upstream.operations_channel.clone()
        };
        let client = lro::operations_client::OperationsClient::new(channel)
            .accept_compressed(CompressionEncoding::Gzip)
            .max_encoding_message_size(self.upstream.config.max_batch_bytes + 1024 * 1024)
            .max_decoding_message_size(self.upstream.config.max_batch_bytes + 1024 * 1024);
        if self.upstream.send_gzip(method) {
            client.send_compressed(CompressionEncoding::Gzip)
        } else {
            client
        }
    }
}
#[tonic::async_trait]
impl execution_server::Execution for CacheProxy {
    type ExecuteStream = RpcStream<lro::Operation>;
    type WaitExecutionStream = RpcStream<lro::Operation>;
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStream>, Status> {
        self.upstream.scope(&request.get_ref().instance_name)?;
        let metadata = CallerMetadata::execution(&request)?;
        let budget = Budget::capture(&request, self.upstream.config.execution_timeout)?;
        let permit = self
            .scheduler_call(budget, async {
                self.executions
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("scheduler admission closed"))
            })
            .await?;
        self.metrics
            .execution_requests
            .fetch_add(1, Ordering::Relaxed);
        let response = self
            .scheduler_call(
                budget,
                self.execution_client(RpcMethod::Execute)
                    .execute(budget.request(&self.upstream, request.into_inner(), &metadata)?),
            )
            .await?;
        self.upstream.observe_gzip(RpcMethod::Execute, &response);
        Ok(Response::new(self.execution_stream(
            response.into_inner(),
            budget,
            permit,
        )))
    }
    async fn wait_execution(
        &self,
        request: Request<WaitExecutionRequest>,
    ) -> Result<Response<Self::WaitExecutionStream>, Status> {
        let metadata = CallerMetadata::execution(&request)?;
        let budget = Budget::capture(&request, self.upstream.config.execution_timeout)?;
        let permit = self
            .scheduler_call(budget, async {
                self.executions
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("scheduler admission closed"))
            })
            .await?;
        // Operation names are opaque and survive daemon restarts. The fixed
        // upstream authenticates/authorizes them; never infer instance by path.
        self.metrics.execution_waits.fetch_add(1, Ordering::Relaxed);
        let response = self
            .scheduler_call(
                budget,
                self.execution_client(RpcMethod::WaitExecution)
                    .wait_execution(budget.request(
                        &self.upstream,
                        request.into_inner(),
                        &metadata,
                    )?),
            )
            .await?;
        self.upstream
            .observe_gzip(RpcMethod::WaitExecution, &response);
        Ok(Response::new(self.execution_stream(
            response.into_inner(),
            budget,
            permit,
        )))
    }
}

#[tonic::async_trait]
impl Operations for CacheProxy {
    async fn list_operations(
        &self,
        request: Request<lro::ListOperationsRequest>,
    ) -> Result<Response<lro::ListOperationsResponse>, Status> {
        let metadata = CallerMetadata::execution(&request)?;
        let budget = Budget::capture(&request, Some(self.upstream.config.policy.rpc_timeout()))?;
        let response = self
            .scheduler_call(
                budget,
                self.operations_client(RpcMethod::ListOperations)
                    .list_operations(budget.request(
                        &self.upstream,
                        request.into_inner(),
                        &metadata,
                    )?),
            )
            .await?;
        self.upstream
            .observe_gzip(RpcMethod::ListOperations, &response);
        Ok(clean_response(response))
    }
    async fn get_operation(
        &self,
        request: Request<lro::GetOperationRequest>,
    ) -> Result<Response<lro::Operation>, Status> {
        let metadata = CallerMetadata::execution(&request)?;
        let budget = Budget::capture(&request, Some(self.upstream.config.policy.rpc_timeout()))?;
        let response = self
            .scheduler_call(
                budget,
                self.operations_client(RpcMethod::GetOperation)
                    .get_operation(budget.request(
                        &self.upstream,
                        request.into_inner(),
                        &metadata,
                    )?),
            )
            .await?;
        self.upstream
            .observe_gzip(RpcMethod::GetOperation, &response);
        Ok(clean_response(response))
    }
    async fn delete_operation(
        &self,
        request: Request<lro::DeleteOperationRequest>,
    ) -> Result<Response<()>, Status> {
        let metadata = CallerMetadata::execution(&request)?;
        let budget = Budget::capture(&request, Some(self.upstream.config.policy.rpc_timeout()))?;
        let response = self
            .scheduler_call(
                budget,
                self.operations_client(RpcMethod::DeleteOperation)
                    .delete_operation(budget.request(
                        &self.upstream,
                        request.into_inner(),
                        &metadata,
                    )?),
            )
            .await?;
        self.upstream
            .observe_gzip(RpcMethod::DeleteOperation, &response);
        Ok(clean_response(response))
    }
    async fn cancel_operation(
        &self,
        request: Request<lro::CancelOperationRequest>,
    ) -> Result<Response<()>, Status> {
        let metadata = CallerMetadata::execution(&request)?;
        let budget = Budget::capture(&request, Some(self.upstream.config.policy.rpc_timeout()))?;
        let response = self
            .scheduler_call(
                budget,
                self.operations_client(RpcMethod::CancelOperation)
                    .cancel_operation(budget.request(
                        &self.upstream,
                        request.into_inner(),
                        &metadata,
                    )?),
            )
            .await?;
        self.upstream
            .observe_gzip(RpcMethod::CancelOperation, &response);
        Ok(clean_response(response))
    }
    async fn wait_operation(
        &self,
        request: Request<lro::WaitOperationRequest>,
    ) -> Result<Response<lro::Operation>, Status> {
        let metadata = CallerMetadata::execution(&request)?;
        let budget = Budget::capture(&request, self.upstream.config.execution_timeout)?;
        let _permit = self
            .scheduler_call(budget, async {
                self.executions
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Status::unavailable("scheduler admission closed"))
            })
            .await?;
        let response = self
            .scheduler_call(
                budget,
                self.operations_client(RpcMethod::WaitOperation)
                    .wait_operation(budget.request(
                        &self.upstream,
                        request.into_inner(),
                        &metadata,
                    )?),
            )
            .await?;
        self.upstream
            .observe_gzip(RpcMethod::WaitOperation, &response);
        Ok(clean_response(response))
    }
}
