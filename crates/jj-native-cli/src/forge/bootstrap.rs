//! Explicit one-time native snapshot import. A source stability check is not a
//! cross-service publication fence or an authenticated provenance attestation.
use super::{context, output};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use fs2::FileExt as _;
use jj_cli::{
    command_error::{CommandError, user_error},
    ui::Ui,
};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    io::{self, Read as _},
    path::PathBuf,
    time::Duration,
};
use tinytree_client::{Config, NativeClient, Transport, forge_protocol as f};
use tinytree_inventory::{Budgets, Checkpoint, InventorySink, VerifiedObject};
use tokio::time::Instant;

const MAX_JOURNAL: usize = 12 * 1024 * 1024;
const MAX_CONTROL: usize = 128 * 1024;
const MAX_SAFE: u64 = (1u64 << 53) - 1;
const JOURNAL: &str = "bootstrap-import.json";
#[derive(clap::Args, Clone, Debug)]
pub(super) struct Args {
    #[arg(long)]
    source_config: PathBuf,
    #[arg(long)]
    source_reference: String,
    /// Assert an externally coordinated source publication pause; not a lock.
    #[arg(long)]
    source_writes_paused: bool,
    /// Resume the existing exact private intent, never a newer source selection.
    #[arg(long)]
    resume: bool,
    #[arg(long, default_value_t=3600, value_parser=clap::value_parser!(u64).range(1..=86400))]
    timeout_seconds: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Key {
    kind: u8,
    id: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Source {
    repository_id: String,
    reference: String,
    generation: u64,
    head: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Cas {
    generation: u64,
    head: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Begin {
    root: Key,
    source: Source,
    destination: Cas,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Counts {
    objects: u64,
    bytes: u64,
    edges: u64,
    contexts: u64,
    context_path_bytes: u64,
    obligations: u64,
    checked_objects: u64,
    checked_contexts: u64,
    checked_obligations: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Status {
    job_id: String,
    root: Key,
    phase: String,
    generation: u64,
    complete: bool,
    counts: Counts,
    error: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Limits {
    object_bytes: u64,
    metadata_bytes: u64,
    account_objects: u64,
    account_bytes: u64,
    attempts: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Begun {
    import_id: String,
    closure_job_id: String,
    root: Key,
    declared_source: Source,
    destination: Cas,
    policy_epoch: u64,
    status: Status,
    limits: Limits,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    import_id: String,
    closure_job_id: String,
    root: Key,
    declared_source: Source,
    policy_epoch: u64,
    main: Cas,
    bootstrap_sealed: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Get {
    import_id: String,
    root: Option<Key>,
    declared_source: Option<Source>,
    destination: Option<Cas>,
    policy_epoch: Option<u64>,
    obsolete: Option<bool>,
    bootstrap_sealed: Option<bool>,
    status: Option<Status>,
    state: Option<String>,
    receipt: Option<Receipt>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Account {
    account_id: String,
    monorepo_id: String,
    policy_epoch: u64,
    public_view: Option<[u8; 32]>,
    main_revision: [u8; 32],
    main_generation: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Journal {
    schema_version: u32,
    context: context::Context,
    source_config: PathBuf,
    source_identity: Value,
    begin_key: String,
    begin: Begin,
    begun: Option<Begun>,
    checkpoint: Option<Checkpoint>,
    transfer_complete: bool,
    finish_key: String,
    finish_generation: Option<u64>,
    receipt: Option<Receipt>,
    source_changed_after_publish: bool,
}
fn error(message: &str) -> String {
    message.into()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}
fn native_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && value != "0".repeat(64)
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|_| error("invalid bootstrap operation result"))
}
fn persist(directory: &PrivateDir, journal: &Journal) -> Result<(), String> {
    let bytes =
        serde_json::to_vec(journal).map_err(|_| error("bootstrap journal encoding failed"))?;
    if bytes.len() > MAX_JOURNAL {
        return Err(error("bootstrap journal limit exceeded"));
    }
    directory
        .replace(JOURNAL, &bytes)
        .map_err(|_| error("bootstrap journal durable write failed"))
}
async fn call_status(
    client: &NativeClient,
    context: &context::Context,
    operation: f::ForgeOperation,
    key: &str,
    payload: Value,
) -> Result<Value, tonic::Status> {
    let payload_json = serde_json::to_vec(&payload)
        .map_err(|_| tonic::Status::internal("bootstrap payload encoding failed"))?;
    let reply = client
        .forge_call(f::ForgeCallRequest {
            operation: operation as i32,
            account_id: context.account_id.clone(),
            view: context.view.clone(),
            request_id: key.into(),
            payload_json,
        })
        .await?;
    if reply.state != "complete" || reply.result_json.len() > MAX_CONTROL {
        return Err(tonic::Status::data_loss(
            "invalid bootstrap operation envelope",
        ));
    }
    serde_json::from_slice(&reply.result_json)
        .map_err(|_| tonic::Status::data_loss("invalid bootstrap operation JSON"))
}
async fn call(
    client: &NativeClient,
    context: &context::Context,
    operation: f::ForgeOperation,
    key: &str,
    payload: Value,
) -> Result<Value, String> {
    call_status(client, context, operation, key, payload)
        .await
        .map_err(|s| {
            format!(
                "bootstrap transport {:?}; resume the exact saved import",
                s.code()
            )
        })
}
async fn source_head(client: &NativeClient, repository: &str) -> Result<Source, String> {
    let view = client
        .open(vec!["main".into()])
        .await
        .map_err(|_| error("source main read failed"))?;
    let reference = view
        .references
        .first()
        .ok_or_else(|| error("source main absent"))?;
    let descriptor = reference
        .commit
        .as_ref()
        .ok_or_else(|| error("source main absent"))?;
    let key = descriptor
        .key
        .as_ref()
        .ok_or_else(|| error("invalid source head"))?;
    let head = hex(&key.native_id);
    if reference.name != "main"
        || reference.generation == 0
        || reference.generation > MAX_SAFE
        || key.kind != 3
        || !native_id(&head)
    {
        return Err(error("invalid source main binding"));
    }
    Ok(Source {
        repository_id: repository.into(),
        reference: "main".into(),
        generation: reference.generation,
        head,
    })
}
async fn stable(client: &NativeClient, expected: &Source) -> Result<(), String> {
    if source_head(client, &expected.repository_id).await? != *expected {
        return Err(error(
            "source main changed; saved import cannot select a newer root",
        ));
    }
    Ok(())
}
fn status(journal: &Journal, value: &Status) -> Result<(), String> {
    let begun = journal
        .begun
        .as_ref()
        .ok_or_else(|| error("bootstrap not begun"))?;
    let c = &value.counts;
    if value.job_id != begun.closure_job_id
        || value.root != journal.begin.root
        || value.generation > MAX_SAFE
        || !matches!(
            value.phase.as_str(),
            "graph" | "checks" | "paths" | "ready" | "published" | "limit"
        )
        || value.complete != matches!(value.phase.as_str(), "ready" | "published")
        || c.objects > 32768
        || c.bytes > 512 * 1024 * 1024
        || c.edges > 131072
        || c.contexts > 32768
        || c.context_path_bytes > 4 * 1024 * 1024
        || c.obligations > 131072
        || c.checked_objects > c.objects
        || c.checked_contexts > c.contexts
        || c.checked_obligations > c.obligations
    {
        return Err(error("bootstrap progress binding invalid"));
    }
    Ok(())
}
fn receipt(journal: &Journal, value: &Receipt) -> Result<(), String> {
    let begun = journal
        .begun
        .as_ref()
        .ok_or_else(|| error("bootstrap not begun"))?;
    if value.import_id != begun.import_id
        || value.closure_job_id != begun.closure_job_id
        || value.root != journal.begin.root
        || value.declared_source != journal.begin.source
        || value.policy_epoch != begun.policy_epoch
        || value.main.generation != 2
        || value.main.head != journal.begin.root.id
        || !value.bootstrap_sealed
    {
        return Err(error("bootstrap completion binding invalid"));
    }
    Ok(())
}
async fn get(client: &NativeClient, journal: &Journal) -> Result<Get, String> {
    let begun = journal
        .begun
        .as_ref()
        .ok_or_else(|| error("bootstrap not begun"))?;
    let value: Get = decode(
        call(
            client,
            &journal.context,
            f::ForgeOperation::GetBootstrapImport,
            "",
            json!({"import_id":begun.import_id}),
        )
        .await?,
    )?;
    if value.import_id != begun.import_id {
        return Err(error("bootstrap import binding invalid"));
    }
    if value.state.as_deref() == Some("published") {
        receipt(
            journal,
            value
                .receipt
                .as_ref()
                .ok_or_else(|| error("missing bootstrap receipt"))?,
        )?;
        return Ok(value);
    }
    if value.state.is_some()
        || value.obsolete != Some(false)
        || value.bootstrap_sealed != Some(false)
        || value.root.as_ref() != Some(&journal.begin.root)
        || value.declared_source.as_ref() != Some(&journal.begin.source)
        || value.destination.as_ref() != Some(&journal.begin.destination)
        || value.policy_epoch != Some(begun.policy_epoch)
    {
        return Err(error(
            "bootstrap attempt closed, obsolete or rebound; original intent preserved",
        ));
    }
    status(
        journal,
        value
            .status
            .as_ref()
            .ok_or_else(|| error("bootstrap status missing"))?,
    )?;
    Ok(value)
}
struct Transfer<'a> {
    client: &'a NativeClient,
    source: &'a NativeClient,
    directory: &'a PrivateDir,
    journal: &'a mut Journal,
    last_check: Instant,
    delivered: usize,
}
impl InventorySink for Transfer<'_> {
    async fn object(&mut self, value: VerifiedObject) -> Result<(), String> {
        if self.delivered % 64 == 0 || self.last_check.elapsed() >= Duration::from_secs(5) {
            stable(self.source, &self.journal.begin.source).await?;
            self.last_check = Instant::now();
        }
        let (descriptor, bytes) = match value {
            VerifiedObject::Metadata(value) => {
                let d = value.descriptor();
                if d.size_bytes > 256 * 1024 {
                    return Err(error("bootstrap metadata exceeds256KiB"));
                }
                (d, value.canonical_bytes().to_vec())
            }
            VerifiedObject::File(value) => {
                let d = value.descriptor();
                if d.size_bytes > 1024 * 1024 {
                    return Err(error("bootstrap FILE exceeds1MiB"));
                }
                let mut reader = value
                    .open()
                    .map_err(|_| error("verified source FILE open failed"))?;
                let mut bytes = Vec::with_capacity(d.size_bytes as usize);
                reader
                    .by_ref()
                    .take(d.size_bytes + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|_| error("verified source FILE read failed"))?;
                (d, bytes)
            }
        };
        if bytes.len() as u64 != descriptor.size_bytes
            || tinytree_model::verify(&descriptor, &bytes, tinytree_model::Limits::default())
                .is_err()
        {
            return Err(error("verified source object changed"));
        }
        let import = &self
            .journal
            .begun
            .as_ref()
            .ok_or_else(|| error("bootstrap not begun"))?
            .import_id;
        let kind = descriptor.key.kind as u8;
        let id = hex(&descriptor.key.id.0);
        let key = format!("io-{import}-{kind}-{id}");
        let reply=call(self.client,&self.journal.context,f::ForgeOperation::PutBootstrapObject,&key,json!({"import_id":import,"descriptor":{"kind":kind,"id":id,"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version},"data_base64":STANDARD.encode(&bytes)})).await?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Put {
            import_id: String,
            kind: u8,
            id: String,
            size_bytes: u64,
            format_version: u16,
            state: String,
        }
        let reply: Put = decode(reply)?;
        if reply.import_id != *import
            || reply.kind != kind
            || reply.id != id
            || reply.size_bytes != descriptor.size_bytes
            || reply.format_version != descriptor.format_version
            || reply.state != "ready"
        {
            return Err(error("bootstrap object receipt binding invalid"));
        }
        self.delivered += 1;
        Ok(())
    }
    async fn checkpoint(&mut self, value: Checkpoint) -> Result<(), String> {
        self.journal.checkpoint = Some(value);
        persist(self.directory, self.journal)
    }
}

pub(super) async fn run(
    ui: &Ui,
    machine: bool,
    args: Args,
    context: context::Context,
) -> Result<(), CommandError> {
    if tokio::runtime::Handle::try_current().is_err() {
        return tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(user_error)?
            .block_on(run_inner(ui, machine, args, context));
    }
    run_inner(ui, machine, args, context).await
}
async fn run_inner(
    ui: &Ui,
    _machine: bool,
    args: Args,
    context: context::Context,
) -> Result<(), CommandError> {
    let value = execute(args, context).await.map_err(user_error)?;
    output::write(ui, "bootstrap_import", "complete", value)
}
async fn execute(args: Args, context: context::Context) -> Result<Value, String> {
    if context.transport != Transport::ForgeHttpsV1
        || args.source_reference != "main"
        || !args.source_writes_paused
    {
        return Err(error(
            "bootstrap requires explicit Forge HTTPS destination, source main and --source-writes-paused external fence assertion",
        ));
    }
    let source_path = std::fs::canonicalize(&args.source_config)
        .map_err(|_| error("source config unavailable"))?;
    let mut source_config =
        Config::load(&source_path).map_err(|_| error("invalid explicit source config"))?;
    if source_config.transport != Transport::Grpc {
        return Err(error(
            "bootstrap source requires supported native gRPC reads",
        ));
    }
    let source_identity = json!({"endpoint":source_config.endpoint,"repository_id":source_config.repository_id,"transport":source_config.transport,"token_env":source_config.token_env,"tls":source_config.tls});
    let directory =
        context::create().map_err(|_| error("private bootstrap directory unavailable"))?;
    let lock = directory
        .open_lock("bootstrap-import.lock")
        .map_err(|_| error("bootstrap lock unavailable"))?;
    lock.try_lock_exclusive()
        .map_err(|_| error("another bootstrap process owns this context"))?;
    let saved = match directory.read(JOURNAL, MAX_JOURNAL) {
        Ok(bytes) => Some(
            serde_json::from_slice::<Journal>(&bytes)
                .map_err(|_| error("invalid bootstrap journal"))?,
        ),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(_) => return Err(error("bootstrap journal unreadable")),
    };
    if saved.is_some() != args.resume {
        return Err(error(
            "existing intent requires --resume; new intent must not use --resume",
        ));
    }
    // Import-only source policy: reject unsupported originals before a large
    // download, without changing ordinary client defaults or truncating bytes.
    source_config.limits.max_file_bytes = 1024 * 1024;
    source_config.limits.max_metadata_bytes = 256 * 1024;
    let deadline = Instant::now() + Duration::from_secs(args.timeout_seconds);
    let source = NativeClient::connect(source_config.clone())
        .await
        .map_err(|_| error("source connection failed"))?
        .with_operation_deadline(deadline);
    let mut destination_config = context
        .client_config()
        .map_err(|_| error("invalid destination context"))?;
    destination_config.limits.max_inflight = 1;
    destination_config.compression = false;
    let destination = NativeClient::connect(destination_config)
        .await
        .map_err(|_| error("destination connection failed"))?
        .with_operation_deadline(deadline);
    let mut journal = if let Some(journal) = saved {
        if journal.schema_version != 1
            || journal.context != context
            || journal.source_config != source_path
            || journal.source_identity != source_identity
            || journal.begin.source.repository_id != source_config.repository_id
            || !native_id(&journal.begin.root.id)
            || journal.begin.root.kind != 3
            || journal.begin.source.head != journal.begin.root.id
        {
            return Err(error("bootstrap journal context/source identity changed"));
        }
        journal
    } else {
        let selected = source_head(&source, &source_config.repository_id).await?;
        let account: Account = decode(
            call(
                &destination,
                &context,
                f::ForgeOperation::GetAccount,
                "",
                json!({}),
            )
            .await?,
        )?;
        if account.account_id != context.account_id
            || account.monorepo_id != format!("forge-{}", context.account_id)
            || account.main_generation != 1
            || account.policy_epoch == 0
            || account.main_revision == [0; 32]
        {
            return Err(error("destination is not an untouched bootstrap account"));
        }
        let _ = &account.public_view;
        let journal = Journal {
            schema_version: 1,
            context,
            source_config: source_path,
            source_identity,
            begin_key: format!("bi-{}", uuid::Uuid::new_v4()),
            begin: Begin {
                root: Key {
                    kind: 3,
                    id: selected.head.clone(),
                },
                source: selected,
                destination: Cas {
                    generation: 1,
                    head: hex(&account.main_revision),
                },
            },
            begun: None,
            checkpoint: None,
            transfer_complete: false,
            finish_key: format!("bf-{}", uuid::Uuid::new_v4()),
            finish_generation: None,
            receipt: None,
            source_changed_after_publish: false,
        };
        persist(&directory, &journal)?;
        journal
    };
    if journal.source_changed_after_publish {
        return Err(error(
            "source_changed_after_publish: terminal partial outcome; completed receipt retained",
        ));
    }
    if journal.begun.is_none() {
        stable(&source, &journal.begin.source).await?;
        let begun: Begun = decode(
            call(
                &destination,
                &journal.context,
                f::ForgeOperation::BeginBootstrapImport,
                &journal.begin_key,
                serde_json::to_value(&journal.begin)
                    .map_err(|_| error("bootstrap encoding failed"))?,
            )
            .await?,
        )?;
        if !uuid(&begun.import_id)
            || !uuid(&begun.closure_job_id)
            || begun.root != journal.begin.root
            || begun.declared_source != journal.begin.source
            || begun.destination != journal.begin.destination
            || begun.policy_epoch == 0
            || begun.policy_epoch > MAX_SAFE
            || begun.limits.object_bytes != 1024 * 1024
            || begun.limits.metadata_bytes != 256 * 1024
            || begun.limits.account_objects != 32768
            || begun.limits.account_bytes != 512 * 1024 * 1024
            || begun.limits.attempts != 4
        {
            return Err(error("bootstrap begin binding invalid"));
        }
        journal.begun = Some(begun);
        status(&journal, &journal.begun.as_ref().unwrap().status)?;
        persist(&directory, &journal)?;
    }
    let current = get(&destination, &journal).await?;
    if let Some(receipt) = current.receipt {
        journal.receipt = Some(receipt);
        persist(&directory, &journal)?;
    }
    if journal.receipt.is_none() {
        stable(&source, &journal.begin.source).await?;
        if !journal.transfer_complete {
            let temporary = tinytree_inventory::temporary_directory()
                .map_err(|_| error("private transfer staging unavailable"))?;
            let checkpoint = journal.checkpoint.clone();
            let mut sink = Transfer {
                client: &destination,
                source: &source,
                directory: &directory,
                journal: &mut journal,
                last_check: Instant::now(),
                delivered: 0,
            };
            let budgets = Budgets {
                max_objects: 32768,
                max_bytes: 512 * 1024 * 1024,
                max_queue: 32768,
                max_edges: 131072,
                timeout_seconds: args.timeout_seconds,
            };
            let report = tinytree_inventory::inventory_with_sink(
                &source,
                vec!["main".into()],
                temporary.path(),
                budgets,
                checkpoint,
                &mut sink,
            )
            .await;
            if !report.complete {
                return Err(format!(
                    "bootstrap transfer incomplete: {}; resume the saved intent",
                    report.error.unwrap_or_else(|| "unknown".into())
                ));
            }
            journal.transfer_complete = true;
            persist(&directory, &journal)?;
        }
        let mut progress = get(&destination, &journal)
            .await?
            .status
            .ok_or_else(|| error("bootstrap validation status absent"))?;
        for _ in 0..4096 {
            if progress.complete {
                break;
            }
            if Instant::now() >= deadline {
                return Err(error("bootstrap validation deadline; resume saved intent"));
            }
            stable(&source, &journal.begin.source).await?;
            let import = &journal.begun.as_ref().unwrap().import_id;
            // An uncertain Advance is resolved through authoritative Get. Never
            // persist6754 generic pending mutation files or guess progress.
            let advanced = call_status(
                &destination,
                &journal.context,
                f::ForgeOperation::AdvanceBootstrapImport,
                &format!("ia-{import}-{}", progress.generation),
                json!({"import_id":import,"expected_generation":progress.generation}),
            )
            .await;
            if let Err(status) = advanced {
                if !matches!(
                    status.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Cancelled
                        | tonic::Code::Aborted
                ) {
                    return Err(format!(
                        "bootstrap advance {:?}; saved intent retained",
                        status.code()
                    ));
                }
            }
            let current = get(&destination, &journal).await?;
            progress = current
                .status
                .ok_or_else(|| error("bootstrap validation closed unexpectedly"))?;
            status(&journal, &progress)?;
            if progress.error.is_some() {
                return Err(error("bootstrap closure incomplete; saved intent retained"));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if progress.phase != "ready" || !progress.complete {
            return Err(error(
                "bootstrap validation step limit; resume saved intent",
            ));
        }
        stable(&source, &journal.begin.source).await?;
        if journal.finish_generation.is_none() {
            journal.finish_generation = Some(progress.generation);
            persist(&directory, &journal)?;
        }
        let import = &journal.begun.as_ref().unwrap().import_id;
        let result = call(
            &destination,
            &journal.context,
            f::ForgeOperation::FinishBootstrapImport,
            &journal.finish_key,
            json!({"import_id":import,"expected_generation":journal.finish_generation}),
        )
        .await?;
        let finished: Receipt = decode(result)?;
        receipt(&journal, &finished)?;
        journal.receipt = Some(finished);
        persist(&directory, &journal)?;
    }
    let current = get(&destination, &journal).await?;
    let finished = current
        .receipt
        .ok_or_else(|| error("bootstrap completion readback absent"))?;
    receipt(&journal, &finished)?;
    let account: Account = decode(
        call(
            &destination,
            &journal.context,
            f::ForgeOperation::GetAccount,
            "",
            json!({}),
        )
        .await?,
    )?;
    if account.account_id != journal.context.account_id
        || account.main_generation != 2
        || hex(&account.main_revision) != journal.begin.root.id
    {
        return Err(error(
            "bootstrap account readback changed; receipt retained",
        ));
    }
    let final_source = source_head(&source, &journal.begin.source.repository_id).await
        .map_err(|_| error("source_readback_unknown_after_publish: receipt retained; resume exact saved intent"))?;
    if final_source != journal.begin.source {
        journal.source_changed_after_publish = true;
        persist(&directory, &journal)?;
        return Err(error(
            "source_changed_after_publish: destination may be sealed; receipt retained, no rollback attempted",
        ));
    }
    Ok(
        json!({"receipt":finished,"source_stable":true,"source_exclusivity":"operator_asserted_not_verified","journal_retained":true}),
    )
}
