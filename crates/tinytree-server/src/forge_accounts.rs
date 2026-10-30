//! Bounded account/view presentation over the authoritative Store resolver.
//!
//! These methods consume opaque Store-resolved views. Ingress derives actors
//! from authenticated transport identity, never from a JSON principal field.
use serde::{Deserialize, Serialize};
use std::sync::Arc;
pub use tinytree_forge_core::search::{ForgeSearchRequest, ForgeSearchHit, ForgeSearchResponse};
use tinytree_forge_core::account_commands::{EmptyRequest, SetMemberRequest, PublishViewRequest, RetireViewRequest, ResolveViewRequest, ReadFileRequest, ReadObjectRequest};
use tinytree_proto::forge_v1;
use tinytree_store::{AccountId, ForgeAction, ForgeActor, PolicyEpoch, ViewSpec, ViewVersion};
use tinytree_store::{ResolvedView, Store};
use tonic::Status;

const MAX_FILES: usize = 10_000;
const MAX_FILE: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteRequest {
    #[serde(default)]
    version: Option<[u8; 32]>,
    path: String,
    #[serde(default = "file_kind")]
    kind: u8,
    #[serde(default)]
    id: Option<[u8; 32]>,
    data: Vec<u8>,
}
fn file_kind() -> u8 {
    tinytree_model::Kind::File as u8
}

fn payload<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Status> {
    if bytes.len() > 5 * 1024 * 1024 {
        return Err(Status::resource_exhausted(
            "forge account request too large",
        ));
    }
    serde_json::from_slice(if bytes.is_empty() { b"{}" } else { bytes })
        .map_err(|_| Status::invalid_argument("invalid forge account payload"))
}
fn value<T: Serialize>(value: T) -> Result<serde_json::Value, Status> {
    serde_json::to_value(value).map_err(|_| Status::internal("forge response encoding failed"))
}
fn required_actor(actor: Option<&ForgeActor>) -> Result<&ForgeActor, Status> {
    actor.ok_or_else(|| Status::unauthenticated("forge authentication required"))
}

/// Shared authenticated dispatch. The caller owns request admission and durable
/// replay binding; these payloads intentionally have no actor/principal field.
pub(crate) fn account_call(
    api: &ForgeReadApi,
    actor: Option<&ForgeActor>,
    operation: forge_v1::ForgeOperation,
    account: &AccountId,
    view: &str,
    request_id: &str,
    bytes: &[u8],
    now: i64,
) -> Result<Option<(String, serde_json::Value)>, Status> {
    use forge_v1::ForgeOperation as Op;
    let store = &api.store;
    let result = match operation {
        Op::ProvisionAccount => {
            let _: EmptyRequest = payload(bytes)?;
            store
                .forge_mutate_account(
                    account,
                    required_actor(actor)?,
                    request_id,
                    tinytree_store::ForgeAccountMutation::Provision,
                )
                .map_err(crate::service::store_status)?
        }
        Op::GetAccount => {
            let _: EmptyRequest = payload(bytes)?;
            store
                .forge_get_account_snapshot(account, required_actor(actor)?)
                .map_err(crate::service::store_status)?
        }
        Op::SetMember => {
            let input: SetMemberRequest = payload(bytes)?;
            store
                .forge_mutate_account(
                    account,
                    required_actor(actor)?,
                    request_id,
                    tinytree_store::ForgeAccountMutation::SetMember {
                        member: input.member,
                        expected_epoch: PolicyEpoch(input.expected_epoch),
                    },
                )
                .map_err(crate::service::store_status)?
        }
        Op::PublishView => {
            let input: PublishViewRequest = payload(bytes)?;
            if !view.is_empty() && view != input.spec.name {
                return Err(Status::invalid_argument("forge view binding mismatch"));
            }
            store
                .forge_mutate_account(
                    account,
                    required_actor(actor)?,
                    request_id,
                    tinytree_store::ForgeAccountMutation::PublishView {
                        spec: input.spec,
                        expected_epoch: PolicyEpoch(input.expected_epoch),
                        make_default: input.make_default,
                        follow_main: input.follow_main,
                    },
                )
                .map_err(crate::service::store_status)?
        }
        Op::RetireView => {
            let input: RetireViewRequest = payload(bytes)?;
            store
                .forge_mutate_account(
                    account,
                    required_actor(actor)?,
                    request_id,
                    tinytree_store::ForgeAccountMutation::RetireView {
                        version: ViewVersion(input.version),
                        expected_epoch: PolicyEpoch(input.expected_epoch),
                        view: view.into(),
                    },
                )
                .map_err(crate::service::store_status)?
        }
        Op::ListViews => {
            let _: EmptyRequest = payload(bytes)?;
            let views = store
                .forge_list_views(account, actor)
                .map_err(crate::service::store_status)?;
            value(
                views
                    .iter()
                    .map(ForgeViewDescription::from)
                    .collect::<Vec<_>>(),
            )?
        }
        Op::ResolveView => {
            let input: ResolveViewRequest = payload(bytes)?;
            if input.progress.is_some() {
                return Err(Status::invalid_argument("view preparation progress is not supported by this backend"));
            }
            let resolved = store
                .forge_resolve_view(account, actor, view, input.version.map(ViewVersion))
                .map_err(crate::service::store_status)?;
            value(api.snapshot(&resolved)?)?
        }
        Op::ReadFile => {
            let input: ReadFileRequest = payload(bytes)?;
            let resolved = store
                .forge_resolve_view(account, actor, view, input.version.map(ViewVersion))
                .map_err(crate::service::store_status)?;
            value(api.file(&resolved, &input.path)?)?
        }
        Op::Search => {
            let input: ForgeSearchRequest = payload(bytes)?;
            let resolved = store
                .forge_resolve_view(account, actor, view, input.version.map(ViewVersion))
                .map_err(crate::service::store_status)?;
            value(api.search(&resolved, actor, &input)?)?
        }
        Op::ReadObject => {
            let input: ReadObjectRequest = payload(bytes)?;
            let kind = tinytree_model::Kind::try_from(input.kind)
                .map_err(|_| Status::invalid_argument("invalid forge object kind"))?;
            let resolved = store
                .forge_resolve_view(account, actor, view, input.version.map(ViewVersion))
                .map_err(crate::service::store_status)?;
            let (descriptor, data, eof) = store
                .forge_read_object_chunk(
                    &resolved,
                    actor,
                    tinytree_model::ObjectKey {
                        kind,
                        id: tinytree_model::ObjectId(input.id),
                    },
                    now,
                    input.offset,
                    input.max_bytes,
                )
                .map_err(crate::service::store_status)?;
            serde_json::json!({"kind":input.kind,"id":input.id,"data":data,"revision":resolved.revision().0,"view_version":resolved.version().0,"policy_epoch":resolved.epoch().0,"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,"offset":input.offset,"eof":eof})
        }
        Op::WriteObject => {
            let input: WriteRequest = payload(bytes)?;
            let actor = required_actor(actor)?;
            let resolved = store
                .forge_resolve_view(account, Some(actor), view, input.version.map(ViewVersion))
                .map_err(crate::service::store_status)?;
            store
                .forge_authorize(account, actor, ForgeAction::Write, Some(resolved.epoch()))
                .map_err(crate::service::store_status)?;
            let canonical = input.path.clone();
            if input.data.len() > MAX_FILE {
                return Err(Status::resource_exhausted("forge upload file too large"));
            }
            let kind = tinytree_model::Kind::try_from(input.kind)
                .map_err(|_| Status::invalid_argument("invalid forge upload kind"))?;
            let descriptor = if kind == tinytree_model::Kind::File {
                tinytree_model::describe_file(&input.data)
            } else {
                let object = tinytree_model::decode(&input.data, tinytree_model::Limits::default())
                    .map_err(|_| Status::invalid_argument("invalid native forge metadata"))?;
                if object.kind() != kind {
                    return Err(Status::invalid_argument("forge upload kind mismatch"));
                }
                tinytree_model::encode(&object, tinytree_model::Limits::default())
                    .map_err(|_| Status::invalid_argument("invalid native forge metadata"))?
                    .descriptor
            };
            if input.id.is_some_and(|id| id != descriptor.key.id.0) {
                return Err(Status::invalid_argument("forge upload digest mismatch"));
            }
            let receipt = store
                .forge_upload_object(&resolved, actor, &canonical, descriptor, &input.data, now)
                .map_err(crate::service::store_status)?;
            store
                .forge_check_view(&resolved)
                .map_err(crate::service::store_status)?;
            serde_json::json!({"kind":descriptor.key.kind as u8,"id":descriptor.key.id.0,"size_bytes":descriptor.size_bytes,"format_version":descriptor.format_version,"staging_lease":receipt.staging_lease,"expires_at":receipt.expires_at})
        }
        _ => return Ok(None),
    };
    Ok(Some(("complete".into(), result)))
}

#[derive(Clone)]
pub struct ForgeReadApi {
    store: Store,
    search: Arc<crate::forge_search::SearchCache>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ForgeViewDescription {
    pub account_id: String,
    pub view: String,
    pub view_version: [u8; 32],
    pub revision: [u8; 32],
    pub policy_epoch: u64,
    pub root_prefix: String,
}
impl From<&ResolvedView> for ForgeViewDescription {
    fn from(view: &ResolvedView) -> Self {
        Self {
            account_id: view.account().0.clone(),
            view: view.name().into(),
            view_version: view.version().0,
            revision: view.revision().0,
            policy_epoch: view.epoch().0,
            root_prefix: view.root_prefix().into(),
        }
    }
}
#[derive(Clone, Debug, Serialize)]
pub struct ForgeTextFile {
    pub path: String,
    pub content: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct ForgeSnapshot {
    #[serde(flatten)]
    pub context: ForgeViewDescription,
    pub projected_commit: [u8; 32],
    pub projected_tree: [u8; 32],
    pub paths: Vec<String>,
    pub readme: Option<ForgeTextFile>,
    pub readme_error: Option<String>,
    pub complete: bool,
}
#[derive(Clone, Debug, Serialize)]
pub struct ForgeFileResponse {
    #[serde(flatten)]
    pub context: ForgeViewDescription,
    #[serde(flatten)]
    pub file: ForgeTextFile,
}
impl ForgeReadApi {
    pub fn new(store: Store) -> Self {
        Self { store, search: Arc::new(crate::forge_search::SearchCache::default()) }
    }
    pub fn describe(&self, view: &ResolvedView) -> Result<ForgeViewDescription, Status> {
        self.store
            .forge_check_view(view)
            .map_err(crate::service::store_status)?;
        Ok(view.into())
    }
    pub fn snapshot(&self, view: &ResolvedView) -> Result<ForgeSnapshot, Status> {
        let files = self
            .store
            .forge_view_files(view, MAX_FILES)
            .map_err(crate::service::store_status)?;
        let readme_path = ["readme.md", "readme.markdown", "readme.txt", "readme"]
            .iter()
            .find_map(|wanted| {
                files
                    .iter()
                    .find(|f| {
                        f.path.eq_ignore_ascii_case(wanted)
                            && f.descriptor.key.kind == tinytree_model::Kind::File
                    })
                    .map(|f| f.path.clone())
            });
        let (readme, readme_error) = if let Some(path) = readme_path {
            match self.file(view, &path) {
                Ok(file) => (Some(file.file), None),
                Err(error)
                    if matches!(
                        error.code(),
                        tonic::Code::ResourceExhausted | tonic::Code::InvalidArgument
                    ) =>
                {
                    (None, Some(error.message().into()))
                }
                Err(error) => return Err(error),
            }
        } else {
            (None, None)
        };
        self.store
            .forge_check_view(view)
            .map_err(crate::service::store_status)?;
        let projection = self
            .store
            .forge_project_view(view, 0)
            .map_err(crate::service::store_status)?;
        Ok(ForgeSnapshot {
            context: view.into(),
            projected_commit: projection.projected_commit,
            projected_tree: projection.projected_tree,
            paths: files.into_iter().map(|f| f.path).collect(),
            readme,
            readme_error,
            complete: true,
        })
    }
    pub fn file(&self, view: &ResolvedView, path: &str) -> Result<ForgeFileResponse, Status> {
        let bytes = self
            .store
            .forge_read_view_file(view, path, MAX_FILE)
            .map_err(crate::service::store_status)?;
        let content = String::from_utf8(bytes)
            .map_err(|_| Status::invalid_argument("forge file preview requires UTF-8"))?;
        self.store
            .forge_check_view(view)
            .map_err(crate::service::store_status)?;
        Ok(ForgeFileResponse {
            context: view.into(),
            file: ForgeTextFile {
                path: path.into(),
                content,
            },
        })
    }
    /// Bounded pages over authorized native FILEs; tokens are service-local
    /// disposable read state, never authorization or repository retention.
    pub fn search(&self, view: &ResolvedView, actor: Option<&ForgeActor>, request: &ForgeSearchRequest) -> Result<ForgeSearchResponse, Status> {
        self.search.search(&self.store, view, actor, request)
    }

}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tinytree_model::{
        ChangeId, Commit, CommitId, FileId, Object, Signature, Timestamp, Tree, TreeEntry, TreeId,
        TreeMerge, TreeValue,
    };
    pub(crate) fn fixture() -> (
        tempfile::TempDir,
        Store,
        AccountId,
        ForgeActor,
        ResolvedView,
    ) {
        fixture_with_files(Vec::new())
    }
    fn fixture_with_files(additional: Vec<(String, Vec<u8>)>) -> (
        tempfile::TempDir, Store, AccountId, ForgeActor, ResolvedView,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path(), Default::default()).unwrap();
        let account = AccountId("example".into());
        let actor = ForgeActor {
            principal: "owner".into(),
        };
        store.forge_provision_account(&account, &actor).unwrap();
        let repository = account.repository_id();
        let mut entries = Vec::new();
        let contents = [
            ("README.md", b"# Welcome\nExplore this tree.\n".as_slice()),
            ("private.txt", b"secret needle\n".as_slice()),
            ("source.rs", b"fn public_needle() {}\n".as_slice()),
        ].into_iter().map(|(name, bytes)| (name.to_string(), bytes.to_vec())).chain(additional);
        for (name, bytes) in contents {
            let descriptor = tinytree_model::describe_file(&bytes);
            store.put(&repository, descriptor, &bytes, 0).unwrap();
            entries.push(TreeEntry {
                name: name.into(),
                value: TreeValue::File {
                    id: FileId(descriptor.key.id.0),
                    size_bytes: descriptor.size_bytes,
                    executable: false,
                    copy_id: None,
                },
            });
        }
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let tree =
            tinytree_model::encode(&Object::Tree(Tree { entries }), Default::default()).unwrap();
        store
            .put(&repository, tree.descriptor, &tree.bytes, 0)
            .unwrap();
        let sig = Signature {
            name: "Fixture".into(),
            email: "fixture@users.noreply.github.com".into(),
            timestamp: Timestamp {
                millis_since_epoch: 0,
                tz_offset_minutes: 0,
            },
        };
        let commit = tinytree_model::encode(
            &Object::Commit(Commit {
                parents: vec![CommitId::ROOT],
                predecessors: vec![],
                root_tree: TreeMerge {
                    terms: vec![TreeId(tree.descriptor.key.id.0)],
                    labels: vec![String::new()],
                },
                change_id: ChangeId([5; 16]),
                description: "private canonical description".into(),
                author: sig.clone(),
                committer: sig,
                secure_signature: None,
            }),
            Default::default(),
        )
        .unwrap();
        store
            .put(&repository, commit.descriptor, &commit.bytes, 0)
            .unwrap();
        let view = store
            .forge_publish_view(
                &account,
                &actor,
                ViewSpec {
                    name: "public".into(),
                    root_prefix: String::new(),
                    include_prefixes: vec![String::new()],
                    exclude_prefixes: vec!["private.txt".into()],
                    visibility: tinytree_store::ForgeViewVisibility::Public,
                    revision: commit.descriptor.key.id.0,
                    export_allowed: true,
                },
                PolicyEpoch(1),
                true,
            )
            .unwrap();
        (directory, store, account, actor, view)
    }
    #[test]
    fn forge_adapter_public_readme_regex_search_and_direct_reads_use_the_same_projection() {
        let (_directory, store, account, _actor, view) = fixture();
        let call = |operation, payload: serde_json::Value| {
            account_call(
                &ForgeReadApi::new(store.clone()),
                None,
                operation,
                &account,
                "",
                "read",
                &serde_json::to_vec(&payload).unwrap(),
                0,
            )
            .unwrap()
            .unwrap()
            .1
        };
        let snapshot = call(forge_v1::ForgeOperation::ResolveView, serde_json::json!({}));
        assert_eq!(snapshot["readme"]["path"], "README.md");
        assert_eq!(
            snapshot["paths"],
            serde_json::json!(["README.md", "source.rs"])
        );
        assert_ne!(
            snapshot["projected_commit"],
            serde_json::json!(view.revision().0)
        );
        let search = call(
            forge_v1::ForgeOperation::Search,
            serde_json::json!({"query":"public_.*needle","regex":true,"limit":10}),
        );
        assert_eq!(search["matches"][0]["path"], "source.rs");
        assert_eq!(search["matches"].as_array().unwrap().len(), 1);
        assert_eq!(search["complete"], true);
        assert_eq!(search["cursor"], serde_json::Value::Null);
        assert_eq!(search["skipped_files"], serde_json::json!({"binary":0,"oversized":0}));
        let error = account_call(
            &ForgeReadApi::new(store.clone()),
            None,
            forge_v1::ForgeOperation::ReadFile,
            &account,
            "",
            "read",
            br#"{"path":"private.txt"}"#,
            0,
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        let error = account_call(
            &ForgeReadApi::new(store.clone()),
            None,
            forge_v1::ForgeOperation::ResolveView,
            &account,
            "",
            "read",
            br#"{"principal":"owner"}"#,
            0,
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    #[test]
    fn forge_adapter_mutation_retry_and_descriptor_only_read_are_bound() {
        let (_directory, store, account, actor, view) = fixture();
        let bootstrap = account_call(
            &ForgeReadApi::new(store.clone()),
            Some(&actor),
            forge_v1::ForgeOperation::GetAccount,
            &account,
            "",
            "inspect",
            b"{}",
            0,
        )
        .unwrap()
        .unwrap()
        .1;
        assert_eq!(bootstrap["main_generation"], 1);
        assert_eq!(
            bootstrap["main_revision"],
            serde_json::json!(
                store
                    .references(&account.repository_id(), &["main".into()])
                    .unwrap()[0]
                    .commit
                    .unwrap()
                    .key
                    .id
                    .0
            )
        );
        let provision = account_call(
            &ForgeReadApi::new(store.clone()),
            Some(&actor),
            forge_v1::ForgeOperation::ProvisionAccount,
            &account,
            "",
            "provision-retry",
            b"{}",
            0,
        )
        .unwrap();
        assert_eq!(
            provision,
            account_call(
                &ForgeReadApi::new(store.clone()),
                Some(&actor),
                forge_v1::ForgeOperation::ProvisionAccount,
                &account,
                "",
                "provision-retry",
                b"{}",
                0
            )
            .unwrap()
        );
        assert_eq!(
            provision.unwrap().1["main_revision"],
            bootstrap["main_revision"]
        );
        let request=serde_json::to_vec(&serde_json::json!({"member":{"principal":"writer","can_write":true,"can_review":false,"is_admin":false,"roles":[]},"expected_epoch":view.epoch().0})).unwrap();
        let first = account_call(
            &ForgeReadApi::new(store.clone()),
            Some(&actor),
            forge_v1::ForgeOperation::SetMember,
            &account,
            "",
            "member-request",
            &request,
            0,
        )
        .unwrap();
        assert_eq!(
            first,
            account_call(
                &ForgeReadApi::new(store.clone()),
                Some(&actor),
                forge_v1::ForgeOperation::SetMember,
                &account,
                "",
                "member-request",
                &request,
                0
            )
            .unwrap()
        );
        let view = store.forge_resolve_view(&account, None, "", None).unwrap();
        let projection = store.forge_project_view(&view, 0).unwrap();
        let request=serde_json::to_vec(&serde_json::json!({"kind":tinytree_model::Kind::Commit as u8,"id":projection.projected_commit,"max_bytes":0})).unwrap();
        let result = account_call(
            &ForgeReadApi::new(store.clone()),
            None,
            forge_v1::ForgeOperation::ReadObject,
            &account,
            "",
            "descriptor",
            &request,
            0,
        )
        .unwrap()
        .unwrap()
        .1;
        assert_eq!(result["data"], serde_json::json!([]));
        assert!(result["size_bytes"].as_u64().unwrap() > 0);
    }
    #[test]
    fn forge_retire_adapter_binds_name_and_replays_exact_receipt() {
        let (_directory, store, account, actor, old) = fixture();
        let main = store
            .references(old.repository(), &["main".into()])
            .unwrap()[0]
            .commit
            .unwrap();
        let current = store
            .forge_publish_view(
                &account,
                &actor,
                ViewSpec {
                    name: old.name().into(),
                    root_prefix: String::new(),
                    include_prefixes: vec![String::new()],
                    exclude_prefixes: vec![],
                    visibility: tinytree_store::ForgeViewVisibility::Public,
                    revision: main.key.id.0,
                    export_allowed: true,
                },
                old.epoch(),
                true,
            )
            .unwrap();
        let request = serde_json::to_vec(
            &serde_json::json!({"version":old.version().0,"expected_epoch":current.epoch().0}),
        )
        .unwrap();
        let call = |view: &str, request_id: &str, bytes: &[u8]| {
            account_call(
                &ForgeReadApi::new(store.clone()),
                Some(&actor),
                forge_v1::ForgeOperation::RetireView,
                &account,
                view,
                request_id,
                bytes,
                0,
            )
        };
        assert_eq!(
            call("wrong", "retire", &request).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let receipt = call("public", "retire", &request).unwrap().unwrap();
        assert_eq!(receipt.0, "complete");
        assert_eq!(
            receipt.1["retired_version"],
            serde_json::json!(old.version().0)
        );
        assert_eq!(
            call("public", "retire", &request).unwrap().unwrap(),
            receipt
        );
        assert!(call("other", "retire", &request).is_err());
        let unknown = serde_json::to_vec(&serde_json::json!({"version":old.version().0,"expected_epoch":current.epoch().0,"principal":"owner"})).unwrap();
        assert_eq!(
            call("public", "bad", &unknown).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    #[test]
    fn forge_search_pages_cross_files_with_exact_replay_skips_and_current_policy() {
        let mut files = vec![
            ("a-hits.txt".into(), (0..120).map(|i| format!("needle {i:03}\n")).collect::<String>().into_bytes()),
            ("b-hits.txt".into(), (120..140).map(|i| format!("needle {i:03}\n")).collect::<String>().into_bytes()),
            ("c-binary.bin".into(), vec![0xff, 0xfe]),
            ("zz-oversized.txt".into(), vec![b'x'; 1024 * 1024 + 1]),
        ];
        files.extend((0..10).map(|i| (format!("z-{i:02}.txt"), b"no matches here\n".to_vec())));
        let (_directory, store, account, actor, view) = fixture_with_files(files);
        let api = ForgeReadApi::new(store.clone());
        let request = ForgeSearchRequest { version: Some(view.version().0), query: "needle ".into(), regex: false, limit: 17, cursor: None };
        let first = api.search(&view, None, &request).unwrap();
        assert!(!first.complete);
        assert_eq!(first.matches.len(), 17);
        let token = first.cursor.clone().unwrap();
        let next_request = ForgeSearchRequest { cursor: Some(token.clone()), ..request.clone() };
        let second = api.search(&view, None, &next_request).unwrap();
        assert_eq!(api.clone().search(&view, None, &next_request).unwrap(), second,
            "immutable input token replay must retain the same page and successor");
        let changed = ForgeSearchRequest { query: "other".into(), ..next_request.clone() };
        assert_eq!(api.search(&view, None, &changed).unwrap_err().code(), tonic::Code::Aborted);
        let unpinned = ForgeSearchRequest { version: None, ..next_request.clone() };
        assert_eq!(api.search(&view, None, &unpinned).unwrap_err().code(), tonic::Code::InvalidArgument);
        assert_eq!(api.search(&view, Some(&actor), &next_request).unwrap_err().code(), tonic::Code::Aborted,
            "another authorized principal cannot borrow an anonymous page token");
        let restarted = ForgeReadApi::new(store.clone());
        assert_eq!(restarted.search(&view, None, &next_request).unwrap_err().code(), tonic::Code::Aborted,
            "a missing disposable cache must not silently restart the scan");
        let mut hits = first.matches.clone();
        let mut page = second.clone();
        let mut pages = 1;
        loop {
            hits.extend(page.matches.clone());
            let Some(cursor) = page.cursor.clone() else { break; };
            pages += 1; assert!(pages < 40);
            page = api.search(&view, None, &ForgeSearchRequest { cursor: Some(cursor), ..request.clone() }).unwrap();
        }
        assert_eq!(hits.len(), 140);
        for (i, hit) in hits.iter().enumerate() {
            assert_eq!(hit.path, if i < 120 { "a-hits.txt" } else { "b-hits.txt" });
            assert_eq!(hit.line, (if i < 120 { i + 1 } else { i - 119 }) as u64);
            assert_eq!(hit.column, 1);
            assert_eq!(hit.preview.trim_end(), format!("needle {i:03}"));
        }
        assert!(!page.complete, "excluded FILE formats cannot claim full coverage");
        assert_eq!(page.skipped_files.binary, 1);
        assert_eq!(page.skipped_files.oversized, 1);
        assert!(pages > 8, "cross-file call budget needs another page even without new hits");
        let regex = ForgeSearchRequest { query: "needle [0-9]+".into(), regex: true, ..request.clone() };
        assert_eq!(api.search(&view, None, &regex).unwrap().matches, first.matches);
        store.forge_publish_view(&account, &actor, ViewSpec {
            name: view.name().into(), root_prefix: String::new(), include_prefixes: vec![String::new()],
            exclude_prefixes: vec!["private.txt".into()], visibility: tinytree_store::ForgeViewVisibility::Members,
            revision: view.revision().0, export_allowed: true,
        }, view.epoch(), false).unwrap();
        assert!(api.search(&view, None, &next_request).is_err(), "warmed cached response must reauthorize after visibility revocation");
        let owner_view = store.forge_resolve_view(&account, Some(&actor), "public", None).unwrap();
        let owner_request = ForgeSearchRequest { version: Some(owner_view.version().0), ..request };
        assert_eq!(api.search(&owner_view, Some(&actor), &owner_request).unwrap().matches, first.matches);
    }

}
