//! Concrete account/view command schemas. The service derives all authority.
use super::{
    context::{self, Context},
    rpc,
};
use jj_cli::{
    command_error::{CommandError, user_error},
    ui::Ui,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{io::Read as _, path::PathBuf};
use tinytree_client::forge_protocol::ForgeOperation as Op;

#[derive(clap::Subcommand, Clone, Debug)]
pub(super) enum RequestCommand {
    Show {
        request_id: String,
    },
    Resume {
        request_id: String,
    },
    /// Apply an exact landed projected rewrite while preserving local iteration.
    Reconcile {
        request_id: String,
    },
}

pub(super) async fn request(
    ui: &Ui,
    command: &jj_cli::cli_util::CommandHelper,
    machine: bool,
    args: RequestCommand,
) -> Result<(), CommandError> {
    let context = context()?;
    let request_id = match &args {
        RequestCommand::Show { request_id }
        | RequestCommand::Resume { request_id }
        | RequestCommand::Reconcile { request_id } => request_id,
    };
    let saved = super::pending::Pending::load(&context, request_id).map_err(user_error)?;
    match args {
        RequestCommand::Show { .. } => super::output::write(
            ui,
            "request",
            "saved",
            json!({"request_id":saved.request_id,"account_id":context.account_id,"view":context.view,"operation":saved.operation,"source":saved.source}),
        ),
        RequestCommand::Reconcile { .. } => {
            if matches!(saved.operation, 30 | 32 | 34) {
                return Err(user_error("transform and pin requests support exact request resume; inspect their returned review identifiers for review reconciliation"));
            }
            let mut reply = rpc::call(
                &context,
                Op::GetReviewOperation,
                json!({}),
                Some(saved.request_id.clone()),
                false,
                None,
            )
            .await?;
            super::review::admission_pending(&reply, Some(&saved.payload))?;
            if reply.state == "landed" {
                let mut value: Value = serde_json::from_slice(&reply.result_json)
                    .map_err(|_| user_error("invalid landed review result"))?;
                let local_state = super::reconcile::apply(ui, command, &saved, &value).await?;
                value
                    .as_object_mut()
                    .ok_or_else(|| user_error("invalid landed review object"))?
                    .insert("local_reconciliation".into(), json!(local_state));
                reply.result_json = serde_json::to_vec(&value).map_err(user_error)?;
            }
            rpc::print(ui, machine, reply)
        }
        RequestCommand::Resume { .. } => {
            if saved.source.is_some() && saved.operation == Op::PushReview as i32 {
                return super::review::resume(ui, command, machine, saved).await;
            }
            let operation = Op::try_from(saved.operation)
                .map_err(|_| user_error("saved Tinytree operation is unsupported"))?;
            rpc::print(
                ui,
                machine,
                rpc::call(
                    &context,
                    operation,
                    saved.payload,
                    Some(saved.request_id),
                    true,
                    saved.source,
                )
                .await?,
            )
        }
    }
}

#[derive(clap::Args, Clone, Debug)]
pub(super) struct LoginArgs {
    /// Explicit wire protocol; an HTTPS URL does not select the protocol.
    #[arg(long, default_value = "grpc", value_parser = context::transport_parser())]
    transport: tinytree_client::Transport,
    #[arg(long)]
    server: String,
    #[arg(long)]
    account: String,
    #[arg(long, default_value = "public")]
    view: String,
    #[arg(long)]
    token_env: Option<String>,
    #[arg(long)]
    ca_certificate: Option<PathBuf>,
    #[arg(long)]
    tls_domain: Option<String>,
}

#[derive(clap::Subcommand, Clone, Debug)]
pub(super) enum AccountCommand {
    /// Import one exact native source snapshot into untouched account genesis.
    BootstrapImport(super::bootstrap::Args),
    Show,
    Provision {
        #[arg(long)]
        request_id: Option<String>,
    },
}

#[derive(clap::Subcommand, Clone, Debug)]
pub(super) enum ViewCommand {
    /// Retire an unselected historical view version without deleting active roots.
    Retire {
        version: String,
        #[arg(long)]
        expected_epoch: u64,
        #[arg(long)]
        request_id: Option<String>,
    },
    List,
    Show {
        #[arg(long)]
        version: Option<String>,
    },
    Read {
        path: String,
        #[arg(long)]
        version: Option<String>,
    },
    Publish {
        /// A bounded JSON ViewSpec, never arbitrary code or a request envelope.
        #[arg(long)]
        spec: PathBuf,
        #[arg(long)]
        expected_epoch: u64,
        #[arg(long)]
        make_default: bool,
        #[arg(long)]
        request_id: Option<String>,
    },
}

#[derive(clap::Args, Clone, Debug)]
pub(super) struct SearchArgs {
    query: String,
    #[arg(short = 'F', long)]
    fixed_strings: bool,
    #[arg(long, default_value_t = 100)]
    limit: usize,
    #[arg(long)]
    version: Option<String>,
    /// Continue the same query/mode/limit and explicit version; never latest main.
    #[arg(long, requires = "version")]
    cursor: Option<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ViewSpec {
    name: String,
    root_prefix: String,
    include_prefixes: Vec<String>,
    exclude_prefixes: Vec<String>,
    visibility: Visibility,
    revision: Option<[u8; 32]>,
    export_allowed: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Visibility {
    Public,
    Members,
    Roles { roles: Vec<String> },
}

pub(super) fn context() -> Result<Context, CommandError> {
    Context::load(&context::open().map_err(user_error)?).map_err(user_error)
}

pub(super) fn id<const N: usize>(value: &str) -> Result<[u8; N], CommandError> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(user_error(
            "expected an exact lowercase hexadecimal identity",
        ));
    }
    let mut bytes = [0; N];
    for (out, pair) in bytes.iter_mut().zip(value.as_bytes().chunks_exact(2)) {
        *out = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).map_err(user_error)?;
    }
    Ok(bytes)
}

// A bound transport envelope is not proof of a valid account snapshot.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AccountSnapshot {
    account_id: String,
    monorepo_id: String,
    policy_epoch: u64,
    public_view: Option<[u8; 32]>,
    main_revision: [u8; 32],
    main_generation: u64,
}

pub(super) async fn login(ui: &Ui, machine: bool, args: LoginArgs) -> Result<(), CommandError> {
    let value = Context {
        schema_version: 1,
        transport: args.transport,
        server: args.server,
        account_id: args.account,
        view: args.view,
        token_env: args.token_env,
        ca_certificate: args.ca_certificate,
        tls_domain: args.tls_domain,
    };
    let reply = rpc::call(&value, Op::GetAccount, json!({}), None, false, None).await?;
    let snapshot: AccountSnapshot = serde_json::from_slice(&reply.result_json)
        .map_err(|_| user_error("invalid Tinytree account snapshot"))?;
    if reply.state != "complete"
        || snapshot.account_id != value.account_id
        || snapshot.monorepo_id != format!("forge-{}", value.account_id)
        || snapshot.policy_epoch == 0
        || snapshot.main_generation == 0
        || snapshot.main_revision == [0; 32]
        || snapshot.public_view == Some([0; 32])
    {
        return Err(user_error("invalid Tinytree account snapshot"));
    }
    // Decode and bind the expected account before writing the context. A failed
    // login, including malformed semantic data in a valid transport envelope,
    // never replaces a previously working account context.
    value
        .save(&context::create().map_err(user_error)?)
        .map_err(user_error)?;
    rpc::print(ui, machine, reply)
}

pub(super) async fn account(
    ui: &Ui,
    machine: bool,
    args: AccountCommand,
) -> Result<(), CommandError> {
    let context = context()?;
    let (op, request, mutation) = match args {
        AccountCommand::BootstrapImport(args) => return super::bootstrap::run(ui, machine, args, context).await,
        AccountCommand::Show => (Op::GetAccount, None, false),
        AccountCommand::Provision { request_id } => (Op::ProvisionAccount, request_id, true),
    };
    rpc::print(
        ui,
        machine,
        rpc::call(&context, op, json!({}), request, mutation, None).await?,
    )
}

pub(super) async fn view(ui: &Ui, machine: bool, args: ViewCommand) -> Result<(), CommandError> {
    let mut context = context()?;
    let (op, body, request, mutation) = match args {
        ViewCommand::Retire {
            version,
            expected_epoch,
            request_id,
        } => (
            Op::RetireView,
            json!({"version":id::<32>(&version)?,"expected_epoch":expected_epoch}),
            request_id,
            true,
        ),
        ViewCommand::List => (Op::ListViews, json!({}), None, false),
        ViewCommand::Show { version } => (
            Op::ResolveView,
            json!({"version":version.as_deref().map(id::<32>).transpose()?}),
            None,
            false,
        ),
        ViewCommand::Read { path, version } => (
            Op::ReadFile,
            json!({"path":path,"version":version.as_deref().map(id::<32>).transpose()?}),
            None,
            false,
        ),
        ViewCommand::Publish {
            spec,
            expected_epoch,
            make_default,
            request_id,
        } => {
            let mut data = Vec::new();
            std::fs::File::open(spec)
                .map_err(user_error)?
                .take(65537)
                .read_to_end(&mut data)
                .map_err(user_error)?;
            if data.len() > 65536 {
                return Err(user_error("view specification exceeds 64 KiB"));
            }
            let mut spec: ViewSpec = serde_json::from_slice(&data)
                .map_err(|_| user_error("invalid typed view specification"))?;
            if spec.revision.is_none() {
                let account =
                    rpc::call(&context, Op::GetAccount, json!({}), None, false, None).await?;
                let value: Value = serde_json::from_slice(&account.result_json)
                    .map_err(|_| user_error("invalid account snapshot"))?;
                let revision = value
                    .get("main_revision")
                    .ok_or_else(|| user_error("account snapshot has no canonical main revision"))?;
                spec.revision = Some(
                    serde_json::from_value(revision.clone())
                        .map_err(|_| user_error("invalid account main revision"))?,
                );
            }
            context.view = spec.name.clone();
            (
                Op::PublishView,
                json!({"spec":spec,"expected_epoch":expected_epoch,"make_default":make_default}),
                request_id,
                true,
            )
        }
    };
    rpc::print(
        ui,
        machine,
        rpc::call(&context, op, body, request, mutation, None).await?,
    )
}

pub(super) async fn search(ui: &Ui, machine: bool, args: SearchArgs) -> Result<(), CommandError> {
    use tinytree_forge_core::search::{ForgeSearchRequest, ForgeSearchResponse};
    if args.query.is_empty() {
        return Err(user_error("search requires a nonempty query"));
    }
    let context = context()?;
    let mut request = ForgeSearchRequest {
        version: args.version.as_deref().map(id::<32>).transpose()?,
        query: args.query,
        regex: !args.fixed_strings,
        limit: args.limit,
        cursor: args.cursor,
    };
    request.validate().map_err(|_| {
        user_error("search requires query <=1024 bytes, limit 1..100 and an exact cursor/version")
    })?;
    let selection = if request.cursor.is_none() {
        // ResolveView's SDK workflow drives bounded preparation and pins the
        // first selection. A search continuation never repeats this selection.
        let reply = rpc::call(
            &context,
            Op::ResolveView,
            json!({"version":request.version}),
            None,
            false,
            None,
        )
        .await?;
        let snapshot: Value = serde_json::from_slice(&reply.result_json)
            .map_err(|_| user_error("invalid resolved search view"))?;
        request.version = Some(
            serde_json::from_value(snapshot["view_version"].clone())
                .map_err(|_| user_error("invalid resolved search version"))?,
        );
        Some(snapshot)
    } else {
        None
    };
    let body = serde_json::to_value(&request).map_err(user_error)?;
    let mut reply = rpc::call(&context, Op::Search, body, None, false, None).await?;
    let value: ForgeSearchResponse = serde_json::from_slice(&reply.result_json)
        .map_err(|_| user_error("invalid search response"))?;
    let valid_cursor = |cursor: &str| {
        !cursor.is_empty()
            && cursor.len() <= 128
            && cursor
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
    };
    if value.account_id != context.account_id
        || (!context.view.is_empty() && value.view != context.view)
        || Some(value.view_version) != request.version
        || value.revision == [0; 32]
        || value.policy_epoch == 0
        || value.matches.len() > request.limit
        || value
            .cursor
            .as_deref()
            .is_some_and(|cursor| !valid_cursor(cursor))
        || (value.complete && value.cursor.is_some())
        || value
            .matches
            .iter()
            .any(|hit| hit.path.is_empty() || hit.line == 0 || hit.column == 0)
    {
        return Err(user_error("search response binding is invalid"));
    }
    if let Some(selection) = selection {
        let response = serde_json::to_value(&value).map_err(user_error)?;
        if [
            "account_id",
            "view",
            "view_version",
            "revision",
            "policy_epoch",
            "root_prefix",
        ]
        .iter()
        .any(|field| selection[*field] != response[*field])
        {
            return Err(user_error(
                "search view changed after preparation; exact request was not restarted",
            ));
        }
    }
    if !value.complete {
        reply.state = "incomplete".into();
    }
    rpc::print(ui, machine, reply)
}
