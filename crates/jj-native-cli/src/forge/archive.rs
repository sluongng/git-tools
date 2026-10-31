//! Private page checkpoints, never a credential store or an implicit abort.
use super::{context::Context, output};
use fs2::FileExt as _;
use jj_cli::{
    command_error::{CommandError, user_error},
    ui::Ui,
};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Read as _,
    path::{Component, Path, PathBuf},
};
use tinytree_client::{
    ArchiveClient, Transport,
    archive::{self as a, Action, ArchiveManifest, ArchivePage, ArchiveRequest, Content, State},
};
#[derive(clap::Args, Clone, Debug)]
pub(super) struct BackupArgs {
    pub directory: PathBuf,
    #[arg(long)]
    pub archive_token_env: String,
    #[arg(long)]
    pub archive_id: Option<String>,
    /// Explicitly release the original export barrier; keep private local files.
    #[arg(long)]
    pub abort: bool,
}
#[derive(clap::Args, Clone, Debug)]
pub(super) struct RestoreArgs {
    pub directory: PathBuf,
    #[arg(long)]
    pub archive_token_env: String,
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Binding {
    transport: Transport,
    endpoint: String,
    account_id: String,
    archive_token_env: String,
    ca_certificate: Option<PathBuf>,
    tls_domain: Option<String>,
    trust_digest: Option<[u8; 32]>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    schema_version: u32,
    binding: Binding,
    archive_id: String,
    build_id: Option<String>,
    next_sequence: u64,
    chain_root: String,
    finished: bool,
    #[serde(default)]
    aborted: bool,
}
fn error(e: impl std::fmt::Display) -> CommandError {
    user_error(e.to_string())
}
fn invalid() -> CommandError {
    user_error(
        "archive is incomplete, tampered, or bound to different inputs; private state retained",
    )
}
fn binding(context: &Context, token: &str) -> Result<Binding, CommandError> {
    if context.transport != Transport::ForgeHttpsV1 {
        return Err(user_error(
            "account archives require explicit forge-https-v1 context",
        ));
    }
    workspace_daemon::forge_client::validate_token_reference(token).map_err(error)?;
    let trust_digest = if let Some(path) = &context.ca_certificate {
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .and_then(|f| f.take(1048577).read_to_end(&mut bytes))
            .map_err(|_| user_error("cannot read archive trust roots"))?;
        if bytes.is_empty() || bytes.len() > 1048576 {
            return Err(invalid());
        }
        Some(*blake3::hash(&bytes).as_bytes())
    } else {
        None
    };
    Ok(Binding {
        transport: context.transport,
        endpoint: context.server.clone(),
        account_id: context.account_id.clone(),
        archive_token_env: token.into(),
        ca_certificate: context.ca_certificate.clone(),
        tls_domain: context.tls_domain.clone(),
        trust_digest,
    })
}
fn directory(path: &Path, create: bool) -> Result<PrivateDir, CommandError> {
    if !path.is_absolute() || path.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(user_error(
            "archive directory must be a normalized absolute path",
        ));
    }
    let normalized: PathBuf = path.components().collect();
    if create {
        PrivateDir::create_all_durable(normalized).map_err(error)
    } else {
        PrivateDir::open_existing(normalized).map_err(error)
    }
}
fn write<T: Serialize>(dir: &PrivateDir, name: &str, value: &T) -> Result<(), CommandError> {
    dir.replace(name, &a::encode(value).map_err(error)?)
        .map_err(error)
}
fn read<T: serde::de::DeserializeOwned>(dir: &PrivateDir, name: &str) -> Result<T, CommandError> {
    serde_json::from_slice(&dir.read(name, a::MAX_PAGE_BYTES).map_err(error)?)
        .map_err(|_| invalid())
}
fn page_name(sequence: u64) -> String {
    format!("page-{sequence:08}.json")
}
// PrivateFs atomic replace may leave only this reserved temporary namespace
// after process loss. Under owner.lock, discard a bounded, verified orphan;
// never adopt its partial bytes as an acknowledged page/intent.
fn reconcile_temporaries(dir: &PrivateDir) -> Result<(), CommandError> {
    let mut removed = 0usize;
    for entry in std::fs::read_dir(dir.path()).map_err(error)? {
        let entry = entry.map_err(error)?;
        let name = entry.file_name();
        let Some(text) = name.to_str() else {
            continue;
        };
        let Some(parts) = text
            .strip_prefix(".private-")
            .and_then(|s| s.strip_suffix(".tmp"))
        else {
            continue;
        };
        let Some((pid, counter)) = parts.split_once('-') else {
            continue;
        };
        let canonical_pid = pid
            .parse::<u32>()
            .ok()
            .is_some_and(|n| n > 0 && n.to_string() == pid);
        let canonical_counter = counter
            .parse::<u64>()
            .ok()
            .is_some_and(|n| n.to_string() == counter);
        if !canonical_pid || !canonical_counter {
            continue;
        }
        if removed >= 128 {
            return Err(user_error(
                "archive atomic temporary cleanup limit reached; private files retained",
            ));
        }
        let file = dir
            .open_existing_file(&name, a::MAX_PAGE_BYTES as u64)
            .map_err(error)?;
        let opened = file.metadata().map_err(error)?;
        let named = std::fs::symlink_metadata(entry.path()).map_err(error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            if opened.nlink() != 1
                || named.nlink() != 1
                || opened.dev() != named.dev()
                || opened.ino() != named.ino()
            {
                return Err(invalid());
            }
        }
        if !named.is_file() || named.file_type().is_symlink() || named.len() != opened.len() {
            return Err(invalid());
        }
        std::fs::remove_file(entry.path()).map_err(error)?;
        removed += 1;
    }
    if removed != 0 {
        std::fs::File::open(dir.path())
            .and_then(|file| file.sync_all())
            .map_err(error)?;
    }
    Ok(())
}
fn inventory(dir: &PrivateDir, expected_pages: Option<u64>) -> Result<(), CommandError> {
    let mut count = 0u64;
    let mut bytes = 0u64;
    let mut pages = 0u64;
    for entry in std::fs::read_dir(dir.path()).map_err(error)? {
        let entry = entry.map_err(error)?;
        count += 1;
        if count > a::MAX_PAGES + 3 || !entry.file_type().map_err(error)?.is_file() {
            return Err(invalid());
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(invalid)?;
        if !["intent.json", "manifest.json", "owner.lock"].contains(&name) {
            let number = name
                .strip_prefix("page-")
                .and_then(|n| n.strip_suffix(".json"))
                .filter(|n| n.len() == 8 && n.bytes().all(|c| c.is_ascii_digit()))
                .and_then(|n| n.parse::<u64>().ok())
                .ok_or_else(invalid)?;
            if number >= a::MAX_PAGES || expected_pages.is_some_and(|n| number >= n) {
                return Err(invalid());
            }
            pages += 1;
        }
        let size = entry.metadata().map_err(error)?.len();
        if size > a::MAX_PAGE_BYTES as u64 {
            return Err(invalid());
        }
        bytes = bytes.checked_add(size).ok_or_else(invalid)?;
        if bytes > a::MAX_BYTES + a::MAX_PAGES * 4096 + 2 * a::MAX_PAGE_BYTES as u64 {
            return Err(invalid());
        }
    }
    if expected_pages.is_some_and(|n| pages != n) {
        return Err(invalid());
    }
    Ok(())
}
#[derive(Default)]
struct Counts {
    rows: u64,
    bytes: u64,
    payloads: u64,
    tables: BTreeMap<String, (Vec<String>, u64, Option<i64>)>,
    last_table: Option<String>,
    last_attempt: Option<String>,
    last_private_attempt: Option<String>,
}
impl Counts {
    fn accept(&mut self, page: &ArchivePage) -> Result<(), CommandError> {
        self.bytes = self
            .bytes
            .checked_add(page.content_bytes().map_err(error)? as u64)
            .ok_or_else(invalid)?;
        match &page.content {
            Content::Rows {
                table,
                columns,
                rows,
            } => {
                if self.last_attempt.is_some()
                    || self.last_private_attempt.is_some()
                    || self.last_table.as_ref().is_some_and(|last| last > table)
                {
                    return Err(invalid());
                }
                self.last_table = Some(table.clone());
                let entry = self
                    .tables
                    .entry(table.clone())
                    .or_insert_with(|| (columns.clone(), 0, None));
                if &entry.0 != columns {
                    return Err(invalid());
                }
                for row in rows {
                    if entry.2.is_some_and(|n| n >= row.rowid) {
                        return Err(invalid());
                    }
                    entry.2 = Some(row.rowid);
                }
                entry.1 += rows.len() as u64;
                self.rows += rows.len() as u64;
            }
            Content::Payload { attempt_id, .. } => {
                if self.last_private_attempt.is_some()
                    || self
                        .last_attempt
                        .as_ref()
                        .is_some_and(|last| last >= attempt_id)
                {
                    return Err(invalid());
                }
                self.last_attempt = Some(attempt_id.clone());
                self.payloads += 1;
            }
            Content::PrivatePayload { attempt_id, .. } => {
                if self
                    .last_private_attempt
                    .as_ref()
                    .is_some_and(|last| last >= attempt_id)
                {
                    return Err(invalid());
                }
                self.last_private_attempt = Some(attempt_id.clone());
                self.payloads += 1;
            }
        }
        if self.rows > a::MAX_ROWS
            || self.bytes > a::MAX_BYTES
            || self.payloads > 131072
            || self.tables.len() > 512
        {
            return Err(invalid());
        }
        Ok(())
    }
    fn matches(&self, manifest: &ArchiveManifest) -> bool {
        self.rows == manifest.rows
            && self.bytes == manifest.bytes
            && self.payloads == manifest.payloads
            && manifest.tables.iter().all(|t| {
                self.tables
                    .get(&t.name)
                    .map_or(t.rows == 0, |v| v.0 == t.columns && v.1 == t.rows)
            })
            && self
                .tables
                .keys()
                .all(|name| manifest.tables.iter().any(|t| &t.name == name))
    }
}
fn checked_pages(
    dir: &PrivateDir,
    account: &str,
    id: &str,
    build: &str,
    count: u64,
) -> Result<(String, Counts), CommandError> {
    if count > a::MAX_PAGES {
        return Err(invalid());
    }
    let mut previous = a::ZERO_HASH.to_owned();
    let mut counts = Counts::default();
    for sequence in 0..count {
        let page: ArchivePage = read(dir, &page_name(sequence))?;
        page.validate().map_err(error)?;
        if page.account_id != account
            || page.archive_id != id
            || page.build_id != build
            || page.sequence != sequence
            || page.previous != previous
        {
            return Err(invalid());
        }
        counts.accept(&page)?;
        previous = page.hash;
    }
    Ok((previous, counts))
}
fn complete_manifest(dir: &PrivateDir) -> Result<ArchiveManifest, CommandError> {
    let manifest: ArchiveManifest = read(dir, "manifest.json")?;
    manifest.validate().map_err(error)?;
    inventory(dir, Some(manifest.page_count))?;
    let (chain, counts) = checked_pages(
        dir,
        &manifest.account_id,
        &manifest.archive_id,
        &manifest.build_id,
        manifest.page_count,
    )?;
    if chain != manifest.chain_root || !counts.matches(&manifest) {
        return Err(invalid());
    }
    Ok(manifest)
}
fn same_manifest(a: &ArchiveManifest, b: &ArchiveManifest) -> Result<bool, CommandError> {
    Ok(a::encode(a).map_err(error)? == a::encode(b).map_err(error)?)
}
fn load_intent(
    dir: &PrivateDir,
    binding: &Binding,
    id: Option<&str>,
) -> Result<Option<Intent>, CommandError> {
    match dir.read("intent.json", a::MAX_PAGE_BYTES) {
        Ok(bytes) => {
            let i: Intent = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
            if i.schema_version != 1
                || &i.binding != binding
                || !a::valid_hash(&i.archive_id)
                || i.build_id.as_ref().is_some_and(|b| !a::valid_hash(b))
                || i.next_sequence > a::MAX_PAGES
                || !a::valid_hash(&i.chain_root)
                || id.is_some_and(|id| id != i.archive_id)
            {
                return Err(invalid());
            }
            Ok(Some(i))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(error(e)),
    }
}
async fn runtime<T>(
    future: impl std::future::Future<Output = Result<T, CommandError>>,
) -> Result<T, CommandError> {
    if tokio::runtime::Handle::try_current().is_ok() {
        future.await
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(error)?
            .block_on(future)
    }
}
pub(super) async fn backup(
    ui: &Ui,
    machine: bool,
    args: BackupArgs,
    context: Context,
    path: PathBuf,
) -> Result<(), CommandError> {
    runtime(backup_inner(ui, machine, args, context, path)).await
}
async fn backup_inner(
    ui: &Ui,
    machine: bool,
    args: BackupArgs,
    context: Context,
    path: PathBuf,
) -> Result<(), CommandError> {
    let bound = binding(&context, &args.archive_token_env)?;
    if args
        .archive_id
        .as_ref()
        .is_some_and(|id| !a::valid_hash(id))
    {
        return Err(user_error(
            "archive ID must be 64 lowercase hexadecimal characters",
        ));
    }
    let dir = directory(&path, !args.abort)?;
    let lock = dir.open_lock("owner.lock").map_err(error)?;
    lock.try_lock_exclusive()
        .map_err(|_| user_error("another process owns this archive"))?;
    reconcile_temporaries(&dir)?;
    if !args.abort {
        inventory(&dir, None)?;
    }
    let saved = load_intent(&dir, &bound, args.archive_id.as_deref())?;
    if args.abort {
        let mut intent =
            saved.ok_or_else(|| user_error("abort requires the original saved backup intent"))?;
        if intent.finished {
            return Err(user_error("completed backup cannot be aborted"));
        }
        let client = ArchiveClient::connect(
            context.client_config().map_err(error)?,
            &args.archive_token_env,
            bound.trust_digest,
        )
        .await
        .map_err(error)?;
        let reply = client
            .call(ArchiveRequest::new(
                &context.account_id,
                &intent.archive_id,
                Action::ExportAbort,
            ))
            .await
            .map_err(error)?;
        if reply.state != State::Aborted
            || intent
                .build_id
                .as_ref()
                .is_some_and(|build| build != &reply.build_id)
        {
            return Err(invalid());
        }
        intent.build_id = Some(reply.build_id);
        intent.aborted = true;
        write(&dir, "intent.json", &intent)?;
        if machine {
            return output::write(
                ui,
                "account",
                "aborted",
                serde_json::json!({"action":"backup","archive_id":intent.archive_id,"account_id":context.account_id,"directory":path}),
            );
        }
        use std::io::Write as _;
        writeln!(
            ui.stdout(),
            "Account backup explicitly aborted; private files retained"
        )
        .map_err(error)?;
        return Ok(());
    }
    if saved.as_ref().is_some_and(|intent| intent.aborted) {
        return Err(user_error(
            "backup was explicitly aborted; use a new private archive directory",
        ));
    }
    if saved.is_none()
        && std::fs::read_dir(dir.path())
            .map_err(error)?
            .any(|e| e.map_or(true, |e| e.file_name() != "owner.lock"))
    {
        return Err(invalid());
    }
    let mut intent = saved.unwrap_or_else(|| Intent {
        schema_version: 1,
        binding: bound.clone(),
        archive_id: args.archive_id.unwrap_or_else(|| {
            format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            )
        }),
        build_id: None,
        next_sequence: 0,
        chain_root: a::ZERO_HASH.into(),
        finished: false,
        aborted: false,
    });
    write(&dir, "intent.json", &intent)?;
    let client = ArchiveClient::connect(
        context.client_config().map_err(error)?,
        &args.archive_token_env,
        bound.trust_digest,
    )
    .await
    .map_err(error)?;
    if let Some(build) = &intent.build_id {
        let (chain, _) = checked_pages(
            &dir,
            &context.account_id,
            &intent.archive_id,
            build,
            intent.next_sequence,
        )?;
        if chain != intent.chain_root {
            return Err(invalid());
        }
    }
    let manifest = match dir.read("manifest.json", a::MAX_PAGE_BYTES) {
        Ok(_) => complete_manifest(&dir)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let mut request =
                ArchiveRequest::new(&context.account_id, &intent.archive_id, Action::ExportBegin);
            request.build_id = intent.build_id.clone();
            let begin = client.call(request).await.map_err(error)?;
            if !matches!(begin.state, State::Exporting | State::Exported)
                || begin.next_sequence > intent.next_sequence + 1
            {
                return Err(invalid());
            }
            if intent
                .build_id
                .as_ref()
                .is_some_and(|b| b != &begin.build_id)
            {
                return Err(invalid());
            }
            intent.build_id = Some(begin.build_id);
            write(&dir, "intent.json", &intent)?;
            let mut no_page = 0usize;
            loop {
                let mut request = ArchiveRequest::new(
                    &context.account_id,
                    &intent.archive_id,
                    Action::ExportNext,
                );
                request.sequence = Some(intent.next_sequence);
                let reply = client.call(request).await.map_err(error)?;
                if Some(&reply.build_id) != intent.build_id.as_ref() {
                    return Err(invalid());
                }
                match reply.state {
                    State::Page => {
                        let page = reply.page.ok_or_else(invalid)?;
                        if page.sequence != intent.next_sequence
                            || page.previous != intent.chain_root
                        {
                            return Err(invalid());
                        }
                        let name = page_name(page.sequence);
                        match dir.read(&name, a::MAX_PAGE_BYTES) {
                            Ok(bytes) => {
                                if bytes != a::encode(&page).map_err(error)? {
                                    return Err(invalid());
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                write(&dir, &name, &page)?
                            }
                            Err(e) => return Err(error(e)),
                        }
                        intent.next_sequence += 1;
                        intent.chain_root = page.hash;
                        write(&dir, "intent.json", &intent)?;
                        no_page = 0;
                    }
                    State::Exporting => {
                        if reply.next_sequence != intent.next_sequence || reply.page.is_some() {
                            return Err(invalid());
                        }
                        no_page += 1;
                        if no_page > 4096 {
                            return Err(user_error(
                                "archive audit progress limit reached; repeat original backup",
                            ));
                        }
                    }
                    State::Exported => {
                        let manifest = reply.manifest.ok_or_else(invalid)?;
                        let (chain, counts) = checked_pages(
                            &dir,
                            &context.account_id,
                            &intent.archive_id,
                            intent.build_id.as_deref().ok_or_else(invalid)?,
                            intent.next_sequence,
                        )?;
                        if manifest.page_count != intent.next_sequence
                            || manifest.chain_root != chain
                            || !counts.matches(&manifest)
                        {
                            return Err(invalid());
                        }
                        inventory(&dir, Some(intent.next_sequence))?;
                        write(&dir, "manifest.json", &manifest)?;
                        break manifest;
                    }
                    _ => return Err(invalid()),
                }
            }
        }
        Err(e) => return Err(error(e)),
    };
    if manifest.account_id != bound.account_id
        || manifest.archive_id != intent.archive_id
        || Some(&manifest.build_id) != intent.build_id.as_ref()
    {
        return Err(invalid());
    }
    if !intent.finished {
        if binding(&context, &args.archive_token_env)? != bound {
            return Err(invalid());
        }
        let mut request = ArchiveRequest::new(
            &context.account_id,
            &intent.archive_id,
            Action::ExportFinish,
        );
        request.manifest = Some(manifest.clone());
        let reply = client.call(request).await.map_err(error)?;
        if reply.state != State::Complete
            || !same_manifest(reply.manifest.as_ref().ok_or_else(invalid)?, &manifest)?
        {
            return Err(invalid());
        }
        intent.finished = true;
        write(&dir, "intent.json", &intent)?;
    }
    report(ui, machine, "backup", &path, &manifest)
}
pub(super) async fn restore(
    ui: &Ui,
    machine: bool,
    args: RestoreArgs,
    context: Context,
    path: PathBuf,
) -> Result<(), CommandError> {
    runtime(restore_inner(ui, machine, args, context, path)).await
}
async fn restore_inner(
    ui: &Ui,
    machine: bool,
    args: RestoreArgs,
    context: Context,
    path: PathBuf,
) -> Result<(), CommandError> {
    let bound = binding(&context, &args.archive_token_env)?;
    let source = directory(&path, false)?;
    let lock = source.open_lock("owner.lock").map_err(error)?;
    lock.try_lock_exclusive()
        .map_err(|_| user_error("another process owns this archive"))?;
    reconcile_temporaries(&source)?;
    let manifest = complete_manifest(&source)?;
    if manifest.account_id != context.account_id {
        return Err(invalid());
    }
    let config = super::context::create().map_err(error)?;
    let restores = PrivateDir::create(config.path().join("archive-restores")).map_err(error)?;
    if std::fs::read_dir(restores.path())
        .map_err(error)?
        .take(1025)
        .count()
        > 1024
    {
        return Err(user_error("archive restore intent limit reached"));
    }
    let key = *blake3::hash(
        &serde_json::to_vec(&(
            &bound,
            &manifest.archive_id,
            &manifest.chain_root,
            source.path(),
        ))
        .map_err(error)?,
    )
    .as_bytes();
    let dir = PrivateDir::create(restores.path().join(a::hex(&key))).map_err(error)?;
    let restore_lock = dir.open_lock("owner.lock").map_err(error)?;
    restore_lock
        .try_lock_exclusive()
        .map_err(|_| user_error("another process owns this restore"))?;
    reconcile_temporaries(&dir)?;
    let mut intent = load_intent(&dir, &bound, Some(&manifest.archive_id))?.unwrap_or(Intent {
        schema_version: 1,
        binding: bound.clone(),
        archive_id: manifest.archive_id.clone(),
        build_id: Some(manifest.build_id.clone()),
        next_sequence: 0,
        chain_root: a::ZERO_HASH.into(),
        finished: false,
        aborted: false,
    });
    if intent.aborted
        || intent.build_id.as_ref() != Some(&manifest.build_id)
        || intent.next_sequence > manifest.page_count
    {
        return Err(invalid());
    }
    let (chain, _) = checked_pages(
        &source,
        &manifest.account_id,
        &manifest.archive_id,
        &manifest.build_id,
        intent.next_sequence,
    )?;
    if intent.chain_root != chain {
        return Err(invalid());
    }
    write(&dir, "intent.json", &intent)?;
    if !intent.finished {
        let client = ArchiveClient::connect(
            context.client_config().map_err(error)?,
            &args.archive_token_env,
            bound.trust_digest,
        )
        .await
        .map_err(error)?;
        let mut request = ArchiveRequest::new(
            &context.account_id,
            &manifest.archive_id,
            Action::RestoreBegin,
        );
        request.manifest = Some(manifest.clone());
        let begin = client.call(request).await.map_err(error)?;
        if begin.state == State::Complete {
            if !same_manifest(begin.manifest.as_ref().ok_or_else(invalid)?, &manifest)? {
                return Err(invalid());
            }
        } else {
            if begin.state != State::Restoring
                || begin.next_sequence < intent.next_sequence
                || begin.next_sequence > intent.next_sequence + 1
            {
                return Err(invalid());
            }
            while intent.next_sequence < manifest.page_count {
                let page: ArchivePage = read(&source, &page_name(intent.next_sequence))?;
                page.validate().map_err(error)?;
                if page.previous != intent.chain_root {
                    return Err(invalid());
                }
                let mut request = ArchiveRequest::new(
                    &context.account_id,
                    &manifest.archive_id,
                    Action::RestorePage,
                );
                request.page = Some(page.clone());
                let reply = client.call(request).await.map_err(error)?;
                if reply.state != State::Restoring
                    || reply.next_sequence != intent.next_sequence + 1
                {
                    return Err(invalid());
                }
                intent.next_sequence += 1;
                intent.chain_root = page.hash;
                write(&dir, "intent.json", &intent)?;
            }
            if binding(&context, &args.archive_token_env)? != bound {
                return Err(invalid());
            }
            let reply = client
                .call(ArchiveRequest::new(
                    &context.account_id,
                    &manifest.archive_id,
                    Action::RestoreFinish,
                ))
                .await
                .map_err(error)?;
            if reply.state != State::Complete
                || !same_manifest(reply.manifest.as_ref().ok_or_else(invalid)?, &manifest)?
            {
                return Err(invalid());
            }
        }
        intent.finished = true;
        write(&dir, "intent.json", &intent)?;
    } else {
        // A saved completion is a historical receipt, not proof that this
        // endpoint still hosts that completed target or authorizes this actor.
        let client = ArchiveClient::connect(
            context.client_config().map_err(error)?,
            &args.archive_token_env,
            bound.trust_digest,
        )
        .await
        .map_err(error)?;
        let reply = client
            .call(ArchiveRequest::new(
                &context.account_id,
                &manifest.archive_id,
                Action::Status,
            ))
            .await
            .map_err(error)?;
        if reply.state != State::Complete
            || !same_manifest(reply.manifest.as_ref().ok_or_else(invalid)?, &manifest)?
        {
            return Err(invalid());
        }
    }
    report(ui, machine, "restore", &path, &manifest)
}
fn report(
    ui: &Ui,
    machine: bool,
    action: &str,
    path: &Path,
    manifest: &ArchiveManifest,
) -> Result<(), CommandError> {
    if machine {
        output::write(
            ui,
            "account",
            "complete",
            serde_json::json!({"action":action,"archive_id":manifest.archive_id,"account_id":manifest.account_id,"build_id":manifest.build_id,"page_count":manifest.page_count,"chain_root":manifest.chain_root,"directory":path}),
        )
    } else {
        use std::io::Write as _;
        writeln!(
            ui.stdout(),
            "Account {action} complete: {} pages",
            manifest.page_count
        )
        .map_err(error)
    }
}
