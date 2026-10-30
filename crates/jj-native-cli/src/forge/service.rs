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
            let mut reply = rpc::call(
                &context,
                Op::GetReviewOperation,
                json!({}),
                Some(saved.request_id.clone()),
                false,
                None,
            )
            .await?;
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
    if args.query.is_empty() || args.query.len() > 1024 || !(1..=1000).contains(&args.limit) {
        return Err(user_error(
            "search requires a nonempty query <=1024 bytes and limit 1..1000",
        ));
    }
    let body = json!({"version":args.version.as_deref().map(id::<32>).transpose()?,"query":args.query,"regex":!args.fixed_strings,"limit":args.limit});
    let mut reply = rpc::call(&context()?, Op::Search, body, None, false, None).await?;
    let value: Value = serde_json::from_slice(&reply.result_json)
        .map_err(|_| user_error("invalid search response"))?;
    if value.get("complete") == Some(&Value::Bool(false)) {
        reply.state = "incomplete".into();
    }
    rpc::print(ui, machine, reply)
}
