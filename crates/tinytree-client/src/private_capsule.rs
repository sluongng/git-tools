//! Private opaque capture bytes. Integrity receipts are not verified native
//! capture manifests, provenance, or authority to import an operation graph.
use crate::{Config, Transport, forge_http::configured_client};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{io::Read as _, sync::Arc};
use tokio::{sync::Semaphore, time::Instant};
use tonic::{Code, Status};
pub const CHUNK_BYTES: usize = 65536;
pub const MAX_WIRE_BYTES: usize = 160 * 1024;
pub const MAX_CAPSULE_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_CHUNKS: u64 = 8192;
pub const ZERO_CHAIN: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const SAFE: u64 = (1u64 << 53) - 1;
fn invalid() -> Status {
    Status::data_loss("invalid bounded private capsule response")
}
fn id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn name(value: &str, empty: bool) -> bool {
    (empty || !value.is_empty())
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn decode_hex(value: &str) -> Result<Vec<u8>, Status> {
    if value.is_empty()
        || value.len() > CHUNK_BYTES * 2
        || value.len() % 2 != 0
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid());
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|p| {
            u8::from_str_radix(std::str::from_utf8(p).map_err(|_| invalid())?, 16)
                .map_err(|_| invalid())
        })
        .collect()
}
pub fn chunk_sha256(bytes: &[u8]) -> Result<String, Status> {
    if bytes.is_empty() || bytes.len() > CHUNK_BYTES {
        return Err(Status::invalid_argument(
            "capsule chunk must contain 1..65536 bytes",
        ));
    }
    Ok(hex(&Sha256::digest(bytes)))
}
/// Advance exactly one raw chunk using the frozen domain and decimal framing.
/// This does not verify that opaque bytes form a valid native capture.
pub fn chain_chunk(previous: &str, sequence: u64, bytes: &[u8]) -> Result<String, Status> {
    if !id(previous) || sequence >= MAX_CHUNKS {
        return Err(Status::invalid_argument("invalid capsule chain input"));
    }
    let digest = chunk_sha256(bytes)?;
    let mut hash = Sha256::new();
    hash.update(b"tinytree-private-capsule-chunk-v1\0");
    hash.update(previous.as_bytes());
    hash.update(b"\0");
    hash.update(sequence.to_string().as_bytes());
    hash.update(b"\0");
    hash.update(bytes.len().to_string().as_bytes());
    hash.update(b"\0");
    hash.update(digest.as_bytes());
    Ok(hex(&hash.finalize()))
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CapsuleSelection {
    pub view: String,
    pub view_version: String,
    pub policy_epoch: u64,
    pub projected_commit: String,
    pub projected_tree: String,
}
impl CapsuleSelection {
    pub fn validate(&self) -> Result<(), Status> {
        if (!self.view.is_empty()
            && tinytree_forge_core::accounts::validate_name(&self.view).is_err())
            || !id(&self.view_version)
            || !id(&self.projected_commit)
            || !id(&self.projected_tree)
            || self.view_version == ZERO_CHAIN
            || self.projected_commit == ZERO_CHAIN
            || self.projected_tree == ZERO_CHAIN
            || self.policy_epoch == 0
            || self.policy_epoch > SAFE
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CapsuleDescriptor {
    pub format: String,
    pub total_bytes: u64,
    pub chunk_count: u64,
    pub chain_root: String,
}
impl CapsuleDescriptor {
    pub fn validate(&self) -> Result<(), Status> {
        if self.format != "tinytree-private-capture-v1"
            || self.total_bytes == 0
            || self.total_bytes > MAX_CAPSULE_BYTES
            || self.chunk_count == 0
            || self.chunk_count > MAX_CHUNKS
            || self.chunk_count != self.total_bytes.div_ceil(CHUNK_BYTES as u64)
            || !id(&self.chain_root)
        {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn chunk_size(&self, sequence: u64) -> Result<usize, Status> {
        self.validate()?;
        if sequence >= self.chunk_count {
            return Err(Status::out_of_range(
                "capsule chunk sequence outside descriptor",
            ));
        }
        Ok((self.total_bytes - sequence * CHUNK_BYTES as u64).min(CHUNK_BYTES as u64) as usize)
    }
    fn offset(&self, next: u64) -> Result<u64, Status> {
        if next > self.chunk_count {
            return Err(invalid());
        }
        Ok((next * CHUNK_BYTES as u64).min(self.total_bytes))
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapsuleAction {
    Begin,
    Status,
    Append,
    Finish,
    Read,
    Retire,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleCall {
    pub schema: u32,
    pub account_id: String,
    pub action: CapsuleAction,
    pub capsule_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<CapsuleSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor: Option<CapsuleDescriptor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_hex: Option<String>,
}
impl CapsuleCall {
    pub fn new(account_id: &str, capsule_id: &str, action: CapsuleAction) -> Self {
        Self {
            schema: 1,
            account_id: account_id.into(),
            action,
            capsule_id: capsule_id.into(),
            workspace_id: None,
            expected_generation: None,
            selection: None,
            descriptor: None,
            sequence: None,
            bytes_hex: None,
        }
    }
    pub fn begin(binding: &CapsuleBinding) -> Result<Self, Status> {
        binding.validate()?;
        Ok(Self {
            workspace_id: Some(binding.workspace_id.clone()),
            expected_generation: Some(binding.generation - 1),
            selection: Some(binding.selection.clone()),
            descriptor: Some(binding.descriptor.clone()),
            ..Self::new(
                &binding.account_id,
                &binding.capsule_id,
                CapsuleAction::Begin,
            )
        })
    }
    pub fn append(binding: &CapsuleBinding, sequence: u64, bytes: &[u8]) -> Result<Self, Status> {
        binding.validate()?;
        if bytes.len() != binding.descriptor.chunk_size(sequence)? {
            return Err(Status::invalid_argument(
                "capsule chunk length differs from descriptor",
            ));
        }
        Ok(Self {
            sequence: Some(sequence),
            bytes_hex: Some(hex(bytes)),
            ..Self::new(
                &binding.account_id,
                &binding.capsule_id,
                CapsuleAction::Append,
            )
        })
    }
    fn validate(
        &self,
        expected: Option<&CapsuleBinding>,
    ) -> Result<Option<CapsuleBinding>, Status> {
        if self.schema != 1 || !name(&self.account_id, false) || !id(&self.capsule_id) {
            return Err(Status::invalid_argument("invalid private capsule request"));
        }
        let has_begin = self.workspace_id.is_some()
            || self.expected_generation.is_some()
            || self.selection.is_some()
            || self.descriptor.is_some();
        let derived = if self.action == CapsuleAction::Begin {
            let binding = CapsuleBinding {
                account_id: self.account_id.clone(),
                capsule_id: self.capsule_id.clone(),
                workspace_id: self.workspace_id.clone().ok_or_else(invalid)?,
                generation: self
                    .expected_generation
                    .filter(|g| *g < SAFE)
                    .ok_or_else(invalid)?
                    + 1,
                selection: self.selection.clone().ok_or_else(invalid)?,
                descriptor: self.descriptor.clone().ok_or_else(invalid)?,
            };
            binding.validate()?;
            Some(binding)
        } else {
            if has_begin {
                return Err(Status::invalid_argument("unexpected capsule begin fields"));
            }
            None
        };
        if let Some(expected) = expected {
            expected.validate()?;
            if expected.account_id != self.account_id
                || expected.capsule_id != self.capsule_id
                || derived.as_ref().is_some_and(|b| b != expected)
            {
                return Err(Status::invalid_argument(
                    "capsule request differs from saved binding",
                ));
            }
        }
        if !matches!(self.action, CapsuleAction::Begin | CapsuleAction::Status)
            && expected.is_none()
        {
            return Err(Status::invalid_argument(
                "capsule operation requires frozen first receipt binding",
            ));
        }
        match self.action {
            CapsuleAction::Append => {
                let seq = self.sequence.ok_or_else(invalid)?;
                let bytes = decode_hex(self.bytes_hex.as_deref().ok_or_else(invalid)?)?;
                if bytes.len() != expected.ok_or_else(invalid)?.descriptor.chunk_size(seq)? {
                    return Err(Status::invalid_argument(
                        "capsule chunk differs from descriptor",
                    ));
                }
            }
            CapsuleAction::Read => {
                if self.bytes_hex.is_some() {
                    return Err(Status::invalid_argument("read cannot supply bytes"));
                }
                expected
                    .ok_or_else(invalid)?
                    .descriptor
                    .chunk_size(self.sequence.ok_or_else(invalid)?)?;
            }
            _ => {
                if self.sequence.is_some() || self.bytes_hex.is_some() {
                    return Err(Status::invalid_argument("unexpected capsule chunk fields"));
                }
            }
        }
        Ok(derived.or_else(|| expected.cloned()))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CapsuleBinding {
    pub account_id: String,
    pub capsule_id: String,
    pub workspace_id: String,
    pub generation: u64,
    pub selection: CapsuleSelection,
    pub descriptor: CapsuleDescriptor,
}
impl CapsuleBinding {
    pub fn validate(&self) -> Result<(), Status> {
        if !name(&self.account_id, false)
            || !id(&self.capsule_id)
            || !id(&self.workspace_id)
            || self.generation == 0
            || self.generation > SAFE
        {
            return Err(invalid());
        }
        self.selection.validate()?;
        self.descriptor.validate()
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CapsuleState {
    Uploading,
    Sealed,
    Retiring,
    Deleted,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapsuleReply {
    pub schema: u32,
    pub account_id: String,
    pub capsule_id: String,
    pub state: CapsuleState,
    pub workspace_id: String,
    pub generation: u64,
    pub selection: CapsuleSelection,
    pub descriptor: CapsuleDescriptor,
    pub next_sequence: u64,
    pub next_offset: u64,
    pub chain_root: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes_hex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}
impl CapsuleReply {
    /// Frozen server assertion only, not native producer/restore verification.
    pub fn binding(&self) -> CapsuleBinding {
        CapsuleBinding {
            account_id: self.account_id.clone(),
            capsule_id: self.capsule_id.clone(),
            workspace_id: self.workspace_id.clone(),
            generation: self.generation,
            selection: self.selection.clone(),
            descriptor: self.descriptor.clone(),
        }
    }
    fn validate_for(
        &self,
        call: &CapsuleCall,
        expected: Option<&CapsuleBinding>,
    ) -> Result<(), Status> {
        let binding = self.binding();
        binding.validate()?;
        if self.schema != 1
            || self.account_id != call.account_id
            || self.capsule_id != call.capsule_id
            || expected.is_some_and(|e| e != &binding)
            || self.next_offset != self.descriptor.offset(self.next_sequence)?
            || !id(&self.chain_root)
        {
            return Err(invalid());
        }
        if self.next_sequence == 0 && self.chain_root != ZERO_CHAIN {
            return Err(invalid());
        }
        if self.next_sequence == self.descriptor.chunk_count
            && matches!(self.state, CapsuleState::Uploading | CapsuleState::Sealed)
            && self.chain_root != self.descriptor.chain_root
        {
            return Err(invalid());
        }
        if self.state == CapsuleState::Sealed
            && (self.next_sequence != self.descriptor.chunk_count
                || self.next_offset != self.descriptor.total_bytes)
        {
            return Err(invalid());
        }
        match call.action {
            CapsuleAction::Append => {
                if !matches!(self.state, CapsuleState::Uploading | CapsuleState::Sealed)
                    || self.next_sequence < call.sequence.ok_or_else(invalid)? + 1
                {
                    return Err(invalid());
                }
                if call.sequence == Some(0) && self.next_sequence == 1 {
                    let bytes = decode_hex(call.bytes_hex.as_deref().ok_or_else(invalid)?)?;
                    if self.chain_root != chain_chunk(ZERO_CHAIN, 0, &bytes)? {
                        return Err(invalid());
                    }
                }
            }
            CapsuleAction::Finish => {
                if self.state != CapsuleState::Sealed {
                    return Err(invalid());
                }
            }
            CapsuleAction::Retire => {
                if !matches!(self.state, CapsuleState::Retiring | CapsuleState::Deleted) {
                    return Err(invalid());
                }
            }
            CapsuleAction::Read => {
                if self.state != CapsuleState::Sealed || self.sequence != call.sequence {
                    return Err(invalid());
                }
                let sequence = self.sequence.ok_or_else(invalid)?;
                let bytes = decode_hex(self.bytes_hex.as_deref().ok_or_else(invalid)?)?;
                if bytes.len() != self.descriptor.chunk_size(sequence)?
                    || self.sha256.as_deref() != Some(chunk_sha256(&bytes)?.as_str())
                {
                    return Err(invalid());
                }
                return Ok(());
            }
            _ => {}
        }
        if self.sequence.is_some() || self.bytes_hex.is_some() || self.sha256.is_some() {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn read_bytes(&self) -> Result<Vec<u8>, Status> {
        self.binding().validate()?;
        if self.schema != 1
            || self.state != CapsuleState::Sealed
            || self.next_sequence != self.descriptor.chunk_count
            || self.next_offset != self.descriptor.total_bytes
            || self.chain_root != self.descriptor.chain_root
        {
            return Err(invalid());
        }
        let bytes = decode_hex(self.bytes_hex.as_deref().ok_or_else(invalid)?)?;
        let seq = self.sequence.ok_or_else(invalid)?;
        if bytes.len() != self.descriptor.chunk_size(seq)?
            || self.sha256.as_deref() != Some(chunk_sha256(&bytes)?.as_str())
        {
            return Err(invalid());
        }
        Ok(bytes)
    }
}
struct Wire(Vec<u8>);
impl std::io::Write for Wire {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > MAX_WIRE_BYTES.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("capsule wire limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Failure {
    schema: u32,
    error: FailureCode,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FailureCode {
    code: String,
}
#[derive(Clone)]
pub struct PrivateCapsuleClient {
    http: reqwest::Client,
    endpoint: String,
    authorization: reqwest::header::HeaderValue,
    timeout: std::time::Duration,
    admission: Arc<Semaphore>,
}
impl PrivateCapsuleClient {
    pub async fn connect(
        config: Config,
        expected_ca_digest: Option<[u8; 32]>,
    ) -> Result<Self, Status> {
        if config.transport != Transport::ForgeHttpsV1 {
            return Err(Status::failed_precondition(
                "private capsules require explicit forge-https-v1 transport",
            ));
        }
        config
            .validate()
            .map_err(|_| Status::invalid_argument("invalid capsule transport configuration"))?;
        let reference = config.token_env.as_deref().ok_or_else(|| {
            Status::unauthenticated("private capsules require a current Forge credential")
        })?;
        let token = std::env::var(reference)
            .map_err(|_| Status::failed_precondition("cannot resolve current Forge credential"))?;
        if !(16..=4096).contains(&token.len()) || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Status::failed_precondition(
                "invalid current Forge credential",
            ));
        }
        let mut authorization = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| Status::failed_precondition("invalid current Forge credential"))?;
        authorization.set_sensitive(true);
        let ca = if let Some(tls) = &config.tls {
            let mut bytes = Vec::new();
            std::fs::File::open(&tls.ca_certificate)
                .and_then(|f| f.take(1048577).read_to_end(&mut bytes))
                .map_err(|_| Status::failed_precondition("cannot read capsule trust roots"))?;
            if bytes.is_empty() || bytes.len() > 1048576 {
                return Err(Status::failed_precondition("invalid capsule trust roots"));
            }
            Some(Arc::from(bytes))
        } else {
            None
        };
        if ca
            .as_ref()
            .map(|bytes: &Arc<[u8]>| *blake3::hash(bytes).as_bytes())
            != expected_ca_digest
        {
            return Err(Status::failed_precondition(
                "capsule trust roots changed before dispatch",
            ));
        }
        Ok(Self {
            http: configured_client(&config, ca)?,
            endpoint: format!(
                "{}/api/v1/private-capsule/call",
                config.endpoint.trim_end_matches('/')
            ),
            authorization,
            timeout: config.rpc.operation_timeout(),
            admission: Arc::new(Semaphore::new(1)),
        })
    }
    /// Status without a binding discovers an authenticated server assertion.
    /// Every byte/finish/retire operation requires the frozen immutable binding.
    pub async fn call(
        &self,
        input: CapsuleCall,
        expected: Option<&CapsuleBinding>,
    ) -> Result<CapsuleReply, Status> {
        let client = self.clone();
        let expected = expected.cloned();
        let deadline = Instant::now() + self.timeout;
        let job = tokio::spawn(async move {
            tokio::time::timeout_at(deadline, async {
                let _permit = client
                    .admission
                    .acquire()
                    .await
                    .map_err(|_| Status::unavailable("capsule transport closed"))?;
                let binding = input.validate(expected.as_ref()).map_err(|_| {
                    Status::invalid_argument("invalid private capsule request or frozen binding")
                })?;
                let mut wire = Wire(Vec::new());
                serde_json::to_writer(&mut wire, &input)
                    .map_err(|_| Status::invalid_argument("private capsule wire limit"))?;
                let body = wire.0;
                for attempt in 0..3 {
                    let result = client.once(&input, binding.as_ref(), body.clone()).await;
                    if attempt == 2
                        || !matches!(
                            result.as_ref().err().map(Status::code),
                            Some(Code::Unavailable)
                        )
                    {
                        return result;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
                unreachable!()
            })
            .await
            .map_err(|_| Status::deadline_exceeded("private capsule deadline exceeded"))?
        });
        job.await
            .map_err(|_| Status::internal("private capsule worker failed"))?
    }
    async fn once(
        &self,
        input: &CapsuleCall,
        expected: Option<&CapsuleBinding>,
        body: Vec<u8>,
    ) -> Result<CapsuleReply, Status> {
        let mut response = self
            .http
            .post(&self.endpoint)
            .header("content-type", "application/json")
            .header("accept", "application/json")
            .header("accept-encoding", "identity")
            .header(reqwest::header::AUTHORIZATION, self.authorization.clone())
            .body(body)
            .send()
            .await
            .map_err(|_| {
                Status::unavailable("private capsule request uncertain; retry original intent")
            })?;
        let status = response.status().as_u16();
        let cap = if status == 200 { MAX_WIRE_BYTES } else { 8192 };
        if response
            .headers()
            .get("content-encoding")
            .is_some_and(|v| v != "identity")
            || response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_none_or(|v| v.split(';').next().unwrap_or("").trim() != "application/json")
            || response.content_length().is_some_and(|n| n > cap as u64)
        {
            return Err(invalid());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            Status::unavailable("private capsule response uncertain; retry original intent")
        })? {
            if chunk.len() > cap.saturating_sub(bytes.len()) {
                return Err(invalid());
            }
            bytes.extend_from_slice(&chunk);
        }
        if status != 200 {
            let failure: Failure = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if failure.schema != 1
                || failure.error.code.is_empty()
                || failure.error.code.len() > 128
                || !failure
                    .error
                    .code
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            {
                return Err(invalid());
            }
            let code = match status {
                400 => Code::InvalidArgument,
                401 => Code::Unauthenticated,
                403 => Code::PermissionDenied,
                404 => Code::NotFound,
                409 => Code::Aborted,
                429 => Code::ResourceExhausted,
                503 => Code::Unavailable,
                _ => return Err(invalid()),
            };
            return Err(Status::new(code, failure.error.code));
        }
        let reply: CapsuleReply = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        reply.validate_for(input, expected)?;
        Ok(reply)
    }
}
