//! Fixed-route, independently authorized account archives. No raw SQL/URL API.
use crate::{Config, Transport};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::{io::Read as _, sync::Arc};
use tokio::{sync::Semaphore, time::Instant};
use tonic::{Code, Status};
pub const MAX_PAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_PAGES: u64 = 1_000_000;
pub const MAX_ROWS: u64 = 8_000_000;
pub const MAX_BYTES: u64 = 64 * 1024 * 1024 * 1024;
pub const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
// Complete archives enumerate both native attempts and all private issued tuples.
pub const MAX_ARCHIVE_PAYLOADS: u64 = 131_072 + 131_072;
const SAFE: u64 = (1u64 << 53) - 1;
fn invalid() -> Status {
    Status::data_loss("invalid bounded account archive")
}
pub fn valid_hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn account(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
}
fn unhex(s: &str) -> Result<Vec<u8>, Status> {
    if s.len() % 2 != 0 {
        return Err(invalid());
    }
    s.as_bytes()
        .chunks_exact(2)
        .map(|p| {
            std::str::from_utf8(p)
                .ok()
                .and_then(|p| u8::from_str_radix(p, 16).ok())
                .ok_or_else(invalid)
        })
        .collect()
}
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    ExportBegin,
    ExportNext,
    ExportFinish,
    ExportAbort,
    Status,
    RestoreBegin,
    RestorePage,
    RestoreFinish,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveRequest {
    pub schema: u32,
    pub account_id: String,
    pub archive_id: String,
    pub action: Action,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ArchiveManifest>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub page: Option<ArchivePage>,
}
impl ArchiveRequest {
    pub fn new(account: &str, archive: &str, action: Action) -> Self {
        Self {
            schema: 1,
            account_id: account.into(),
            archive_id: archive.into(),
            action,
            build_id: None,
            sequence: None,
            manifest: None,
            page: None,
        }
    }
    fn validate(&self) -> Result<(), Status> {
        if self.schema != 1 || !account(&self.account_id) || !valid_hash(&self.archive_id) {
            return Err(Status::invalid_argument("invalid archive request binding"));
        }
        let shape = match self.action {
            Action::ExportBegin => {
                self.sequence.is_none()
                    && self.manifest.is_none()
                    && self.page.is_none()
                    && self.build_id.as_ref().is_none_or(|b| valid_hash(b))
            }
            Action::ExportNext => {
                self.sequence.is_some_and(|n| n <= MAX_PAGES)
                    && self.manifest.is_none()
                    && self.page.is_none()
                    && self.build_id.is_none()
            }
            Action::ExportFinish | Action::RestoreBegin => {
                self.manifest.is_some()
                    && self.sequence.is_none()
                    && self.page.is_none()
                    && self.build_id.is_none()
            }
            Action::RestorePage => {
                self.page.is_some()
                    && self.sequence.is_none()
                    && self.manifest.is_none()
                    && self.build_id.is_none()
            }
            _ => {
                self.sequence.is_none()
                    && self.manifest.is_none()
                    && self.page.is_none()
                    && self.build_id.is_none()
            }
        };
        if !shape {
            return Err(Status::invalid_argument("invalid archive action fields"));
        }
        if let Some(m) = &self.manifest {
            m.validate()?;
            if m.account_id != self.account_id || m.archive_id != self.archive_id {
                return Err(invalid());
            }
        }
        if let Some(p) = &self.page {
            p.validate()?;
            if p.account_id != self.account_id || p.archive_id != self.archive_id {
                return Err(invalid());
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Exporting,
    Restoring,
    Complete,
    Aborted,
    Page,
    Exported,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveReply {
    pub schema: u32,
    pub account_id: String,
    pub archive_id: String,
    pub build_id: String,
    pub state: State,
    pub next_sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<ArchivePage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_page: Option<ArchivePage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<ArchiveManifest>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Cell {
    Null {},
    Text { value: String },
    Integer { value: i64 },
    Real { value: String },
    Blob { hex: String },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Row {
    pub rowid: i64,
    pub cells: Vec<Cell>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Content {
    Rows {
        table: String,
        columns: Vec<String>,
        rows: Vec<Row>,
    },
    Payload {
        attempt_id: String,
        object_kind: u8,
        object_id: String,
        size: u64,
        locator: String,
        state: String,
        source_etag: Option<String>,
        source_version: Option<String>,
        presence: String,
        bytes_hex: Option<String>,
    },
    PrivatePayload {
        capsule_id: String,
        sequence: u64,
        attempt_id: String,
        size: u64,
        sha256: String,
        locator: String,
        state: String,
        source_etag: Option<String>,
        source_version: Option<String>,
        presence: String,
        bytes_hex: Option<String>,
    },
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivePage {
    pub schema: u32,
    pub account_id: String,
    pub archive_id: String,
    pub build_id: String,
    pub sequence: u64,
    pub previous: String,
    pub hash: String,
    pub content: Content,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    pub name: String,
    pub columns: Vec<String>,
    pub rows: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveManifest {
    pub schema: u32,
    pub account_id: String,
    pub archive_id: String,
    pub build_id: String,
    pub tables: Vec<Table>,
    pub page_count: u64,
    pub chain_root: String,
    pub rows: u64,
    pub bytes: u64,
    pub payloads: u64,
}
struct Bounded(Vec<u8>, usize);
impl std::io::Write for Bounded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.1.saturating_sub(self.0.len()) {
            return Err(std::io::Error::other("archive encoded limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Status> {
    let mut b = Bounded(Vec::new(), MAX_PAGE_BYTES);
    serde_json::to_writer(&mut b, value).map_err(|_| invalid())?;
    Ok(b.0)
}
fn name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control)
}
fn columns_valid(c: &[String]) -> bool {
    !c.is_empty()
        && c.len() <= 256
        && c.iter().all(|s| name(s))
        && c.iter().collect::<std::collections::BTreeSet<_>>().len() == c.len()
}
impl ArchiveManifest {
    pub fn validate(&self) -> Result<(), Status> {
        if self.schema != 1
            || !account(&self.account_id)
            || !valid_hash(&self.archive_id)
            || !valid_hash(&self.build_id)
            || !valid_hash(&self.chain_root)
            || self.page_count > MAX_PAGES
            || self.rows > MAX_ROWS
            || self.bytes > MAX_BYTES
            || self.payloads > MAX_ARCHIVE_PAYLOADS
            || self.tables.len() > 512
        {
            return Err(invalid());
        }
        let mut names = std::collections::BTreeSet::new();
        let mut rows = 0u64;
        for t in &self.tables {
            if !name(&t.name) || !columns_valid(&t.columns) || !names.insert(&t.name) {
                return Err(invalid());
            }
            rows = rows.checked_add(t.rows).ok_or_else(invalid)?;
        }
        if rows != self.rows {
            return Err(invalid());
        }
        Ok(())
    }
}
impl ArchivePage {
    pub fn validate(&self) -> Result<(), Status> {
        if self.schema != 1
            || !account(&self.account_id)
            || !valid_hash(&self.archive_id)
            || !valid_hash(&self.build_id)
            || !valid_hash(&self.previous)
            || !valid_hash(&self.hash)
            || self.sequence >= MAX_PAGES
        {
            return Err(invalid());
        }
        // Bound the complete encoded page before cloning it for canonical hash.
        let _ = encode(self)?;
        match &self.content {
            Content::Rows {
                table,
                columns,
                rows,
            } => {
                if !name(table) || !columns_valid(columns) || rows.is_empty() || rows.len() > 128 {
                    return Err(invalid());
                }
                let mut last = None;
                for row in rows {
                    if row.rowid.unsigned_abs() > SAFE
                        || last.is_some_and(|n| n >= row.rowid)
                        || row.cells.len() != columns.len()
                    {
                        return Err(invalid());
                    }
                    last = Some(row.rowid);
                    for cell in &row.cells {
                        match cell {
                            Cell::Integer { value } if value.unsigned_abs() > SAFE => {
                                return Err(invalid());
                            }
                            Cell::Real { value }
                                if value.len() > 128
                                    || value.parse::<f64>().ok().is_none_or(|n| {
                                        !n.is_finite() || n.abs() > SAFE as f64
                                    }) =>
                            {
                                return Err(invalid());
                            }
                            Cell::Blob { hex }
                                if hex.len() > 16 * 1024 * 1024
                                    || hex.len() % 2 != 0
                                    || !hex.bytes().all(|b| {
                                        b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
                                    }) =>
                            {
                                return Err(invalid());
                            }
                            _ => {}
                        }
                    }
                    if encode(row)?.len() > 8 * 1024 * 1024 {
                        return Err(invalid());
                    }
                }
            }
            Content::Payload {
                attempt_id,
                object_kind,
                object_id,
                size,
                locator,
                state,
                source_etag,
                source_version,
                presence,
                bytes_hex,
            } => {
                if !valid_hash(attempt_id)
                    || !valid_hash(object_id)
                    || !(1..=5).contains(object_kind)
                    || *size
                        > if *object_kind == 1 {
                            1024 * 1024
                        } else {
                            256 * 1024
                        }
                    || locator
                        != &format!(
                            "native/v2/{}/{}/{}/{}",
                            self.account_id, object_kind, object_id, attempt_id
                        )
                    || !["pending", "written", "retiring"].contains(&state.as_str())
                    || source_etag.as_ref().is_some_and(|s| s.len() > 1024)
                    || source_version.as_ref().is_some_and(|s| s.len() > 1024)
                {
                    return Err(invalid());
                }
                match (presence.as_str(), bytes_hex) {
                    ("present", Some(bytes))
                        if bytes.len() == *size as usize * 2
                            && bytes
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
                    {
                        let data = unhex(bytes)?;
                        let id: [u8; 32] = unhex(object_id)?.try_into().map_err(|_| invalid())?;
                        let descriptor = tinytree_model::Descriptor {
                            key: tinytree_model::ObjectKey {
                                kind: tinytree_model::Kind::try_from(*object_kind)
                                    .map_err(|_| invalid())?,
                                id: tinytree_model::ObjectId(id),
                            },
                            size_bytes: *size,
                            format_version: tinytree_model::FORMAT_VERSION,
                        };
                        tinytree_model::verify(&descriptor, &data, Default::default())
                            .map_err(|_| invalid())?;
                    }
                    ("absent", None) if state != "written" => {}
                    _ => return Err(invalid()),
                }
            }
            Content::PrivatePayload {
                capsule_id,
                sequence,
                attempt_id,
                size,
                sha256,
                locator,
                state,
                source_etag,
                source_version,
                presence,
                bytes_hex,
            } => {
                if !valid_hash(capsule_id)
                    || !valid_hash(attempt_id)
                    || !valid_hash(sha256)
                    || *sequence >= 8192
                    || *size == 0
                    || *size > 65536
                    || locator
                        != &format!(
                            "private/v1/{}/{}/{}/{}",
                            self.account_id, capsule_id, sequence, attempt_id
                        )
                    || !["pending", "written", "retiring", "tombstoned"].contains(&state.as_str())
                    || source_etag.as_ref().is_some_and(|s| s.len() > 1024)
                    || source_version.as_ref().is_some_and(|s| s.len() > 1024)
                {
                    return Err(invalid());
                }
                match (presence.as_str(), bytes_hex) {
                    ("present", Some(bytes))
                        if matches!(state.as_str(), "retiring" | "tombstoned")
                            && bytes.is_empty() => {}
                    ("present", Some(bytes))
                        if state != "tombstoned"
                            && bytes.len() == *size as usize * 2
                            && bytes
                                .bytes()
                                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
                    {
                        let actual = Sha256::digest(unhex(bytes)?);
                        if hex(&actual) != *sha256 {
                            return Err(invalid());
                        }
                    }
                    ("absent", None) if state != "written" && state != "tombstoned" => {}
                    _ => return Err(invalid()),
                }
            }
        }
        if self.computed_hash()? != self.hash {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn content_bytes(&self) -> Result<usize, Status> {
        Ok(encode(&self.content)?.len())
    }
    pub fn computed_hash(&self) -> Result<String, Status> {
        let mut value = serde_json::to_value(self).map_err(|_| invalid())?;
        value.as_object_mut().ok_or_else(invalid)?.remove("hash");
        let bytes = encode(&value)?;
        let mut hash = Sha256::new();
        hash.update(b"tinytree-account-archive-page-v1\0");
        hash.update(self.previous.as_bytes());
        hash.update(b"\0");
        hash.update(bytes);
        Ok(hex(&hash.finalize()))
    }
}
#[derive(Clone)]
pub struct ArchiveClient {
    http: reqwest::Client,
    endpoint: String,
    authorization: reqwest::header::HeaderValue,
    timeout: std::time::Duration,
    admission: Arc<Semaphore>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Failure {
    schema: u32,
    error: ErrorCode,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorCode {
    code: String,
}
impl ArchiveClient {
    pub async fn connect(
        mut config: Config,
        token_env: &str,
        expected_ca_digest: Option<[u8; 32]>,
    ) -> Result<Self, Status> {
        if config.transport != Transport::ForgeHttpsV1 {
            return Err(Status::failed_precondition(
                "account archives require explicit forge-https-v1 transport",
            ));
        }
        if matches!(
            token_env,
            "HOME"
                | "PATH"
                | "TMPDIR"
                | "XDG_CONFIG_HOME"
                | "XDG_RUNTIME_DIR"
                | "SHELL"
                | "ENV"
                | "BASH_ENV"
        ) || token_env.starts_with("LD_")
            || token_env.starts_with("DYLD_")
        {
            return Err(Status::invalid_argument(
                "reserved archive credential reference",
            ));
        }
        config.token_env = Some(token_env.into());
        config
            .validate()
            .map_err(|_| Status::invalid_argument("invalid archive transport configuration"))?;
        let token = std::env::var(token_env)
            .map_err(|_| Status::failed_precondition("cannot resolve archive credential"))?;
        if !(16..=4096).contains(&token.len()) || !token.bytes().all(|c| c.is_ascii_graphic()) {
            return Err(Status::failed_precondition("invalid archive credential"));
        }
        let mut authorization = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| Status::failed_precondition("invalid archive credential"))?;
        authorization.set_sensitive(true);
        let ca = if let Some(tls) = &config.tls {
            let mut bytes = Vec::new();
            std::fs::File::open(&tls.ca_certificate)
                .and_then(|f| f.take(1048577).read_to_end(&mut bytes))
                .map_err(|_| Status::failed_precondition("cannot read archive trust roots"))?;
            if bytes.is_empty() || bytes.len() > 1048576 {
                return Err(Status::failed_precondition("invalid archive trust roots"));
            }
            Some(Arc::from(bytes))
        } else {
            None
        };
        let captured_digest = ca
            .as_ref()
            .map(|bytes: &Arc<[u8]>| *blake3::hash(bytes).as_bytes());
        if captured_digest != expected_ca_digest {
            return Err(Status::failed_precondition(
                "archive trust roots changed before dispatch",
            ));
        }
        Ok(Self {
            http: crate::forge_http::configured_client(&config, ca)?,
            endpoint: format!(
                "{}/api/v1/archive/call",
                config.endpoint.trim_end_matches('/')
            ),
            authorization,
            timeout: config.rpc.operation_timeout(),
            admission: Arc::new(Semaphore::new(1)),
        })
    }
    pub async fn call(&self, input: ArchiveRequest) -> Result<ArchiveReply, Status> {
        let client = self.clone();
        let deadline = Instant::now() + self.timeout;
        let job = tokio::spawn(async move {
            tokio::time::timeout_at(deadline, async {
                let _permit = client
                    .admission
                    .acquire()
                    .await
                    .map_err(|_| Status::unavailable("archive transport closed"))?;
                // Queued calls own only their caller-supplied DTO. No encoded,
                // canonicalized or decoded buffers exist before admission.
                input.validate()?;
                let mut encoded = Bounded(Vec::new(), MAX_PAGE_BYTES + 65536);
                serde_json::to_writer(&mut encoded, &input).map_err(|_| invalid())?;
                let body = encoded.0;
                for attempt in 0..3 {
                    let result = client.once(&input, body.clone()).await;
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
            .map_err(|_| Status::deadline_exceeded("account archive deadline exceeded"))?
        });
        job.await
            .map_err(|_| Status::internal("account archive worker failed"))?
    }
    async fn once(&self, input: &ArchiveRequest, body: Vec<u8>) -> Result<ArchiveReply, Status> {
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
                Status::unavailable("account archive transport uncertain; retry original archive")
            })?;
        let status = response.status().as_u16();
        let cap = if status == 200 {
            MAX_PAGE_BYTES + 65536
        } else {
            8192
        };
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
            Status::unavailable("account archive response uncertain; retry original archive")
        })? {
            if chunk.len() > cap.saturating_sub(bytes.len()) {
                return Err(invalid());
            }
            bytes.extend_from_slice(&chunk);
        }
        if status != 200 {
            let failure: Failure = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if failure.schema != 1 || failure.error.code.len() > 128 {
                return Err(invalid());
            }
            if failure.error.code.is_empty()
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
                409 => Code::Aborted,
                412 => Code::FailedPrecondition,
                413 | 429 => Code::ResourceExhausted,
                503 => Code::Unavailable,
                _ => return Err(invalid()),
            };
            return Err(Status::new(code, failure.error.code));
        }
        let reply: ArchiveReply = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if reply.schema != 1
            || reply.account_id != input.account_id
            || reply.archive_id != input.archive_id
            || !valid_hash(&reply.build_id)
            || reply.next_sequence > MAX_PAGES
        {
            return Err(invalid());
        }
        if let Some(page) = &reply.page {
            page.validate()?;
            if reply.state != State::Page
                || page.account_id != reply.account_id
                || page.archive_id != reply.archive_id
                || page.build_id != reply.build_id
                || input.sequence != Some(page.sequence)
                || reply.next_sequence != page.sequence + 1
            {
                return Err(invalid());
            }
        } else if reply.state == State::Page {
            return Err(invalid());
        }
        if let Some(page) = &reply.last_page {
            page.validate()?;
            if input.action != Action::Status
                || page.account_id != reply.account_id
                || page.archive_id != reply.archive_id
                || page.build_id != reply.build_id
            {
                return Err(invalid());
            }
        }
        if let Some(manifest) = &reply.manifest {
            manifest.validate()?;
            if manifest.account_id != reply.account_id
                || manifest.archive_id != reply.archive_id
                || manifest.build_id != reply.build_id
            {
                return Err(invalid());
            }
        }
        let expected = input
            .build_id
            .as_ref()
            .or_else(|| input.manifest.as_ref().map(|m| &m.build_id))
            .or_else(|| input.page.as_ref().map(|p| &p.build_id));
        if expected.is_some_and(|b| b != &reply.build_id) {
            return Err(invalid());
        }
        Ok(reply)
    }
}
