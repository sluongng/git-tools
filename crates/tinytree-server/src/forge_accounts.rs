//! Bounded account/view presentation over the authoritative Store resolver.
//!
//! These methods consume opaque Store-resolved views. Ingress derives actors
//! from authenticated transport identity, never from a JSON principal field.
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use tinytree_forge_core::account_commands::{EmptyRequest, SetMemberRequest, PublishViewRequest, RetireViewRequest, ResolveViewRequest, ReadFileRequest, ReadObjectRequest};
use tinytree_proto::forge_v1;
use tinytree_store::{AccountId, ForgeAction, ForgeActor, PolicyEpoch, ViewSpec, ViewVersion};
use tinytree_store::{ResolvedView, Store};
use tonic::Status;

const MAX_FILES: usize = 10_000;
const MAX_FILE: usize = 1024 * 1024;
const MAX_READ: usize = 8 * 1024 * 1024;
const MAX_MATCHES: usize = 100;

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
    store: &Store,
    actor: Option<&ForgeActor>,
    operation: forge_v1::ForgeOperation,
    account: &AccountId,
    view: &str,
    request_id: &str,
    bytes: &[u8],
    now: i64,
) -> Result<Option<(String, serde_json::Value)>, Status> {
    use forge_v1::ForgeOperation as Op;
    let api = ForgeReadApi::new(store.clone());
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
            value(api.search(&resolved, &input)?)?
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
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeSearchRequest {
    #[serde(default)]
    pub version: Option<[u8; 32]>,
    pub query: String,
    #[serde(default)]
    pub regex: bool,
    pub limit: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct ForgeSearchHit {
    pub path: String,
    pub line: u64,
    pub column: u64,
    pub preview: String,
}
#[derive(Clone, Debug, Serialize)]
pub struct ForgeSearchResponse {
    #[serde(flatten)]
    pub context: ForgeViewDescription,
    pub matches: Vec<ForgeSearchHit>,
    pub complete: bool,
}

impl ForgeReadApi {
    pub fn new(store: Store) -> Self {
        Self { store }
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
    /// Real regex/literal content matching in authorized regular UTF-8 files.
    /// This bounded projection scan deliberately does not claim indexed coverage.
    pub fn search(
        &self,
        view: &ResolvedView,
        request: &ForgeSearchRequest,
    ) -> Result<ForgeSearchResponse, Status> {
        if request.limit == 0 || request.limit > MAX_MATCHES {
            return Err(Status::invalid_argument("forge search limit must be 1-100"));
        }
        let mode = if request.regex {
            tinytree_search::SearchMode::Regex
        } else {
            tinytree_search::SearchMode::Literal
        };
        let pattern = tinytree_search::Pattern::new(request.query.as_bytes(), mode)
            .map_err(|_| Status::invalid_argument("invalid bounded forge search pattern"))?;
        let deadline = Instant::now() + Duration::from_secs(5);
        let files = self
            .store
            .forge_view_files(view, MAX_FILES)
            .map_err(crate::service::store_status)?;
        let mut matches = Vec::new();
        let mut read = 0usize;
        let mut complete = true;
        let mut budget = tinytree_search::MatchBudget::new(tinytree_search::MatchLimits {
            max_input_bytes: MAX_READ,
            max_work: 64 * 1024 * 1024,
            max_output_bytes: 256 * 1024,
        });
        for file in files {
            if file.descriptor.key.kind != tinytree_model::Kind::File {
                continue;
            }
            let size = usize::try_from(file.descriptor.size_bytes).unwrap_or(usize::MAX);
            if size > MAX_FILE
                || read.checked_add(size).is_none_or(|n| n > MAX_READ)
                || Instant::now() >= deadline
            {
                complete = false;
                break;
            }
            let bytes = self
                .store
                .forge_read_view_file(view, &file.path, MAX_FILE)
                .map_err(crate::service::store_status)?;
            read += bytes.len();
            if std::str::from_utf8(&bytes).is_err() {
                continue;
            }
            let page = pattern
                .scan(
                    &bytes,
                    0,
                    request.limit - matches.len(),
                    &mut budget,
                    || Instant::now() < deadline,
                )
                .map_err(|error| match error {
                    tinytree_search::Error::Invalid => {
                        Status::invalid_argument("invalid forge search")
                    }
                    _ => Status::resource_exhausted("forge search work budget exhausted"),
                })?;
            for hit in page.hits {
                matches.push(ForgeSearchHit {
                    path: file.path.clone(),
                    line: hit.line,
                    column: hit.column,
                    preview: String::from_utf8_lossy(&hit.snippet).into_owned(),
                });
            }
            if page.next_offset.is_some() || matches.len() == request.limit {
                complete = false;
                break;
            }
        }
        self.store
            .forge_check_view(view)
            .map_err(crate::service::store_status)?;
        Ok(ForgeSearchResponse {
            context: view.into(),
            matches,
            complete,
        })
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
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(directory.path(), Default::default()).unwrap();
        let account = AccountId("example".into());
        let actor = ForgeActor {
            principal: "owner".into(),
        };
        store.forge_provision_account(&account, &actor).unwrap();
        let repository = account.repository_id();
        let mut entries = Vec::new();
        for (name, bytes) in [
            ("README.md", b"# Welcome\nExplore this tree.\n".as_slice()),
            ("private.txt", b"secret needle\n".as_slice()),
            ("source.rs", b"fn public_needle() {}\n".as_slice()),
        ] {
            let descriptor = tinytree_model::describe_file(bytes);
            store.put(&repository, descriptor, bytes, 0).unwrap();
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
                &store,
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
        let error = account_call(
            &store,
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
            &store,
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
            &store,
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
            &store,
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
                &store,
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
            &store,
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
                &store,
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
            &store,
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
                &store,
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
}
