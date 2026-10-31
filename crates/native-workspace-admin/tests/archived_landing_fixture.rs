//! Test-only structural historical data; never a native landing observation or authority.
use anyhow::{Context, Result, ensure};
use fs2::FileExt as _;
use jj_native_backend::{DirectObjectIo, NativeObjectIo};
use native_workspace_admin::{
    Registry,
    remote_state::{self, RemoteScope, State},
};
use private_fs::PrivateDir;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, sync::atomic::{AtomicU8, Ordering}};
static STAGE: AtomicU8 = AtomicU8::new(0);
fn stage(n:u8){STAGE.store(n,Ordering::Relaxed);}
fn stage_name()-> &'static str {match STAGE.load(Ordering::Relaxed){1=>"home_guard",2=>"workspace_descriptor",3=>"stopped_client_lock",4=>"registry_binding",5=>"native_locator",6=>"fixture_input",7=>"local_canonical_pool",8=>"source_commit",9=>"candidate_commit",10=>"base_commit",11=>"rewrite_binding",12=>"historical_dto",13=>"remote_state",14=>"seed_archive",15=>"write_archive",16=>"verify_archive",17=>"reader_fence",_=>"arguments"}}
use tinytree_cloud as c;
use tinytree_model as m;
const MARKER: &str = "tinytree-archive-historical-data-fixture-v1\n";
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    source_base: String,
    source_head: String,
    candidate_head: String,
    source_operation: String,
    candidate_operation: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Locator {
    version: u32,
    socket: PathBuf,
    store: String,
}
fn id(s: &str) -> Result<[u8; 32]> {
    ensure!(
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "fixture ID"
    );
    let mut out = [0; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}
async fn commit(io: &DirectObjectIo, s: &str) -> Result<m::Commit> {
    let value = io
        .read_metadata(m::ObjectKey {
            kind: m::Kind::Commit,
            id: m::ObjectId(id(s)?),
        })
        .await
        .map_err(|error| anyhow::anyhow!("fixture canonical commit unavailable: {error}"))?;
    let m::Object::Commit(commit) = value.into_object() else {
        anyhow::bail!("fixture commit kind")
    };
    Ok(commit)
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
async fn execute() -> Result<serde_json::Value> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().context("fixture mode")?;
    ensure!(matches!(mode.as_str(), "seed" | "verify"), "fixture mode");
    let mut fields = BTreeMap::new();
    while let Some(k) = args.next() {
        ensure!(
            matches!(
                k.as_str(),
                "--fixture-home" | "--workspace" | "--descriptor" | "--input" | "--provider-identity"
            ),
            "fixture argument"
        );
        let v = args.next().context("fixture argument")?;
        ensure!(fields.insert(k, v).is_none(), "duplicate fixture argument");
    }
    stage(1);
    let home = PathBuf::from(fields.remove("--fixture-home").context("fixture home")?);
    ensure!(
        home.is_absolute() && home == std::fs::canonicalize(&home)?,
        "fixture home path"
    );
    ensure!(
        std::env::var_os("HOME").map(PathBuf::from) == Some(home.clone()),
        "fixture HOME mismatch"
    );
    let fixture_root = PrivateDir::open_existing(home.parent().context("fixture parent")?)?;
    ensure!(
        fixture_root.read(".historical-fixture-root", 256)? == MARKER.as_bytes(),
        "not disposable archive fixture"
    );
    let private = PrivateDir::open_existing(&home)?;
    ensure!(
        private.read(".historical-data-fixture", 256)? == MARKER.as_bytes(),
        "fixture marker absent"
    );
    stage(2);
    let workspace =
        std::fs::canonicalize(fields.remove("--workspace").context("fixture workspace")?)?;
    ensure!(workspace.starts_with(&home), "foreign workspace");
    let descriptor = PathBuf::from(
        fields
            .remove("--descriptor")
            .context("fixture descriptor")?,
    );
    ensure!(
        descriptor.starts_with(&home)
            && descriptor
                .file_name()
                .is_some_and(|n| n == "descriptor.json"),
        "foreign descriptor"
    );
    let directory = PrivateDir::open_existing(descriptor.parent().context("descriptor parent")?)?;
    let d: serde_json::Value =
        serde_json::from_slice(&directory.read("descriptor.json", 128 * 1024)?)?;
    let state_dir = PathBuf::from(d["state_dir"].as_str().context("fixture state")?);
    ensure!(
        state_dir == directory.path() && d["destination"].as_str() == workspace.to_str(),
        "descriptor binding"
    );
    stage(3);
    let owner = directory.open_lock("daemon.lock")?;
    owner
        .try_lock_exclusive()
        .context("fixture client must be stopped")?;
    ensure!(
        !state_dir.join("control.sock").exists() && !state_dir.join("rbe.sock").exists(),
        "fixture client still running"
    );
    stage(4);
    let registry_path = Registry::default_path()?;
    ensure!(
        registry_path.starts_with(&home) && d["registry_path"].as_str() == registry_path.to_str(),
        "foreign registry"
    );
    let registry = Registry::open_existing(registry_path)?;
    let binding = if mode == "verify" {
        // Supplied by this fixture's actual shipped `tt work --capsule` Ready
        // response. Never derive identity from the archived origin itself.
        let provider = id(&fields.remove("--provider-identity")
            .context("restored fixture requires current provider identity")?)?;
        ensure!(provider != [0; 32], "fixture provider identity");
        registry.restored_resume_info(&workspace, provider)?.binding
    } else {
        ensure!(!fields.contains_key("--provider-identity"), "unexpected seed provider override");
        registry.resume_info(&workspace)?
            .context("fixture stopped workspace missing")?.binding
    };
    ensure!(
        binding.root == workspace
            && [&binding.admin, &binding.repo, &binding.working_copy]
                .iter()
                .all(|p| p.starts_with(&home)),
        "foreign fixture binding"
    );
    stage(5);
    let repository = PrivateDir::open_existing(&binding.repo)?;
    let locator_bytes = repository.read_descendant("store/native-broker.toml", 16384)?;
    let mut locator: Locator = toml::from_str(std::str::from_utf8(&locator_bytes)?)?;
    ensure!(
        matches!(locator.version, 1 | 2 | 3) && locator.socket == state_dir.join("rbe.sock"),
        "fixture native locator (version={}, socket_matches={})", locator.version, locator.socket == state_dir.join("rbe.sock")
    );
    let scope = RemoteScope::new(locator.socket.clone(), locator.store.clone())?;
    stage(6);
    let input_name = fields.remove("--input").context("fixture input")?;
    ensure!(
        !input_name.contains('/') && !input_name.contains('\\'),
        "fixture input name"
    );
    let input: Input = serde_json::from_slice(&private.read(&input_name, 128 * 1024)?)?;
    stage(7);
    let io = DirectObjectIo::load(
        &state_dir.join("native").join(&scope.store),
        Default::default(),
    )
    .map_err(|error| anyhow::anyhow!("fixture local canonical pool unavailable: {error}"))?;
    stage(8);
    let source = commit(&io, &input.source_head).await?;
    stage(9);
    let candidate = commit(&io, &input.candidate_head).await?;
    stage(10);
    let _base = commit(&io, &input.source_base).await?;
    stage(11);
    ensure!(
        source.change_id == candidate.change_id
            && input.source_head != input.candidate_head
            && source.parents == vec![m::CommitId(id(&input.source_base)?)]
            && candidate.parents == source.parents
            && candidate.root_tree.terms.len() == 1,
        "fixture genuine logical rewrite binding"
    );
    stage(12);
    let landing = c::PortableLanding {
        request_id: "17".repeat(16),
        source_base: input.source_base.clone(),
        source_head: input.source_head.clone(),
        reference: "main".into(),
        bookmark: "main".into(),
        generation: 1,
        expected_head: input.source_base.clone(),
        profile_name: "historical-fixture".into(),
        profile_version: "fixture-v1".into(),
        source_operation: input.source_operation,
        source_stack: vec![input.source_head.clone()],
        fork_operation: Some(input.candidate_operation),
        fork_acknowledged: true,
        job_id: Some("18".repeat(32)),
        result_roots: vec![input.candidate_head.clone()],
        candidate_head: Some(input.candidate_head.clone()),
        apply_operation: None,
        operation_writing: None,
        acknowledged: false,
        terminal: Some("publication_abandoned".into()),
        restored_lookup_only: false,
        observed: Some(c::HistoricalLandingJob {
            job_id: [0x18; 32],
            phase: c::HistoricalLandingPhase::PublicationAbandoned,
            preparation: c::HistoricalLandingPreparation::ProfileConfigured,
            roots_retained: true,
            expires_at_seconds: 1,
            profile_name: "historical-fixture".into(),
            profile_version: "fixture-v1".into(),
            candidate: Some(c::HistoricalLandingCandidate {
                head: id(&input.candidate_head)?,
                root_tree: candidate.root_tree.terms[0].0,
                rewrites: vec![c::HistoricalLandingRewrite {
                    old_commit: id(&input.source_head)?,
                    new_commit: id(&input.candidate_head)?,
                    change_id: source.change_id.0,
                }],
            }),
            receipt: None,
            conflict_path: None,
        }),
    };
    landing.validate()?;
    stage(13);
    let working = PrivateDir::open_existing(&binding.working_copy)?;
    let mut state = match working.read("native-remote.toml", 1024 * 1024) {
        Ok(b) => remote_state::decode(&b, &scope)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => State::new(&scope),
        Err(e) => return Err(e.into()),
    };
    if mode == "seed" {
        stage(14);
        ensure!(
            state.pending.is_none()
                && state.landing.is_none()
                && state.archived_landings.is_empty(),
            "fixture would replace existing intents"
        );
        state.begin_landing(landing.clone())?;
        state.archive_abandoned_landing()?;
        state.validate(&scope)?;
        stage(17);
        // The production CLI's require_forge_reader is crate-private. This
        // guarded test writer mirrors its monotonic reader-3 fence before
        // introducing archived roots; no older reader may silently lose them.
        // The complete strict locator has exactly version/socket/store fields.
        ensure!(repository.read_descendant("store/native-broker.toml", 16384)? == locator_bytes,
            "fixture locator changed before reader fence");
        if locator.version < 3 {
            let store = binding.repo.join("store");
            #[cfg(unix)] {
                use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
                let before = std::fs::symlink_metadata(&store)?;
                ensure!(before.is_dir() && before.uid() == std::fs::metadata(repository.path())?.uid(),
                    "fixture backend directory identity mismatch");
                let handle = std::fs::File::open(&store)?;
                let actual = handle.metadata()?;
                ensure!(before.dev() == actual.dev() && before.ino() == actual.ino(),
                    "fixture backend directory changed");
                handle.set_permissions(std::fs::Permissions::from_mode(0o700))?;
                handle.sync_all()?;
            }
            let store = PrivateDir::open_existing(&store)?;
            ensure!(store.read("native-broker.toml", 16384)? == locator_bytes,
                "fixture locator changed before reader fence");
            locator.version = 3;
            store.replace("native-broker.toml", toml::to_string(&locator)?.as_bytes())?;
        }
        stage(15);
        working.replace("native-remote.toml", toml::to_string(&state)?.as_bytes())?;
    }
    stage(16);
    ensure!(
        locator.version == 3 && state.landing.is_none() && state.archived_landings == vec![landing.clone()],
        "historical archive changed"
    );
    Ok(
        serde_json::json!({"schema_version":1,"kind":"historical_data_fixture","no_native_rpc_authority":true,"mode":mode,"reader_version":locator.version,"registry_binding":if mode=="verify"{"restored_current_state"}else{"normal_ready"},"landing":landing,"candidate_tree":hex(&candidate.root_tree.terms[0].0)}),
    )
}
#[tokio::main]
async fn main() {
    match execute().await {
        Ok(v) => println!("{v}"),
        Err(error) => {
            // Test-only bounded stage/Display, never State, raw bytes, config,
            // headers, environment, or a remote authority assertion.
            let message=error.to_string().chars().take(1024).collect::<String>();
            eprintln!("{}",serde_json::json!({"schema_version":1,"kind":"historical_data_fixture","no_native_rpc_authority":true,"error":{"stage":stage_name(),"message":message}}));
            std::process::exit(1);
        }
    }
}
