//! Public adapter/ledger tests with an executor-independent, small reference
//! QueryIo fixture. These are not broker/server or native CLI acceptance.
use super::*;
use futures::{StreamExt as _, TryStreamExt as _, executor::block_on};
use native_broker::{
    BeginQuery, ForkQueryScope, OpenQueryScope, QueryHandle, QueryPage, QueryReply, QueryResources,
    QueryScope,
};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
#[derive(Debug, Default)]
struct FixtureIo {
    scopes: Mutex<BTreeMap<[u8; 32], Vec<m::Commit>>>,
    queries: Mutex<BTreeMap<[u8; 32], (Vec<q::GraphRow>, usize, u64)>>,
    opens: AtomicUsize,
    facts_count: AtomicUsize,
    prefix_count: AtomicUsize,
    fail_fact: AtomicBool,
    begins: AtomicUsize,
    next: AtomicUsize,
    closes: AtomicUsize,
    remote: Mutex<Vec<m::Commit>>,
    admitted: Mutex<BTreeSet<m::CommitId>>,
    root_errors: Mutex<BTreeMap<m::CommitId, tonic::Code>>,
    fork_overlays: Mutex<Vec<Vec<m::CommitId>>>,
    pause_pages: AtomicBool,
    page_waiters: Mutex<Vec<futures::channel::oneshot::Sender<()>>>,
    reply_owners: Mutex<Vec<std::sync::Weak<CanonicalReplyOwner>>>,
}
#[derive(Debug)]
struct CanonicalReplyOwner;
fn reply<T>(value: T, owner: QueryOwner) -> QueryReply<T> {
    QueryReply { value, owner }
}
fn id(v: Option<&q::CommitId>) -> m::CommitId {
    m::CommitId(v.unwrap().value.as_slice().try_into().unwrap())
}
fn selected(expr: &q::GraphExpr, nodes: &[m::Commit]) -> BTreeSet<m::CommitId> {
    use q::graph_expr::Expression as X;
    match expr.expression.as_ref().unwrap() {
        X::Commits(v) => v.commits.iter().map(|v| id(Some(v))).collect(),
        X::Ancestors(v) => {
            let mut out = BTreeSet::new();
            let mut pending: Vec<_> = selected(v.heads.as_deref().unwrap(), nodes)
                .into_iter()
                .map(|id| (id, 0u64))
                .collect();
            let mut seen = BTreeSet::new();
            while let Some((id, distance)) = pending.pop() {
                if !seen.insert((id, distance)) {
                    continue;
                }
                let in_range = v.generations.as_ref().is_none_or(|r| {
                    distance >= r.start && r.end_exclusive.is_none_or(|end| distance < end)
                });
                if in_range {
                    out.insert(id);
                }
                if v.generations
                    .as_ref()
                    .and_then(|r| r.end_exclusive)
                    .is_some_and(|end| distance.saturating_add(1) >= end)
                {
                    continue;
                }
                if let Some(node) = nodes.iter().find(|n| model_commit_id(n) == id) {
                    pending.extend(
                        node.parents
                            .iter()
                            .enumerate()
                            .filter(|(i, _)| {
                                v.parents.as_ref().is_none_or(|r| {
                                    *i >= r.start as usize
                                        && r.end_exclusive.is_none_or(|end| *i < end as usize)
                                })
                            })
                            .map(|(_, id)| (*id, distance + 1)),
                    );
                }
            }
            out
        }
        X::Union(v) => v.inputs.iter().flat_map(|v| selected(v, nodes)).collect(),
        X::Difference(v) => {
            let mut left = selected(v.left.as_deref().unwrap(), nodes);
            let right = selected(v.right.as_deref().unwrap(), nodes);
            left.retain(|id| !right.contains(id));
            left
        }
        X::HasConflict(v) => {
            let mut selected = selected(v.input.as_deref().unwrap(), nodes);
            selected.retain(|id| {
                nodes
                    .iter()
                    .any(|n| model_commit_id(n) == *id && n.root_tree.terms.len() > 1)
            });
            selected
        }
        X::Intersection(v) => {
            let mut sets = v.inputs.iter().map(|e| selected(e, nodes));
            let mut out = sets.next().unwrap_or_default();
            for set in sets {
                out.retain(|id| set.contains(id));
            }
            out
        }
        _ => panic!("fixture operation not exercised"),
    }
}
fn model_commit_id(node: &m::Commit) -> m::CommitId {
    m::CommitId(
        m::encode(&m::Object::Commit(node.clone()), Default::default())
            .unwrap()
            .descriptor
            .key
            .id
            .0,
    )
}
fn append_overlay(nodes: &mut Vec<m::Commit>, overlay: Vec<Vec<u8>>) -> Result<(), tonic::Status> {
    let mut known: BTreeSet<_> = nodes
        .iter()
        .map(model_commit_id)
        .chain([m::CommitId::ROOT])
        .collect();
    for bytes in overlay {
        let m::Object::Commit(node) = m::decode(&bytes, Default::default()).unwrap() else {
            panic!()
        };
        if !known.insert(model_commit_id(&node)) {
            return Err(tonic::Status::invalid_argument("duplicate scoped overlay"));
        }
        if node.parents.iter().any(|id| !known.contains(id)) {
            return Err(tonic::Status::failed_precondition(
                "overlay parent unavailable",
            ));
        }
        nodes.push(node);
    }
    Ok(())
}
impl FixtureIo {
    fn install_scope(&self, nodes: Vec<m::Commit>, owner: QueryOwner) -> QueryReply<QueryScope> {
        let mut handle = [0; 32];
        handle[..8]
            .copy_from_slice(&(self.next.fetch_add(1, Ordering::SeqCst) as u64 + 1).to_le_bytes());
        self.scopes.lock().unwrap().insert(handle, nodes);
        self.opens.fetch_add(1, Ordering::SeqCst);
        reply(
            QueryScope {
                cloud_source_digest: None,
                handle,
                expires_at_unix_seconds: now().unwrap() + 300,
                query_identity_version: 2,
            },
            owner,
        )
    }
}
#[async_trait]
impl NativeQueryIo for FixtureIo {
    fn resources(&self) -> QueryResources {
        QueryResources {
            memory_bytes: 1024 * 1024,
            temporary_bytes: 0,
            persistent_memory_bytes: 1024 * 1024,
        }
    }
    async fn open_scope(
        &self,
        i: OpenQueryScope,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, tonic::Status> {
        let remote = self.remote.lock().unwrap().clone();
        let all: BTreeMap<_, _> = remote
            .iter()
            .map(|node| (model_commit_id(node), node))
            .collect();
        let mut wanted = BTreeSet::new();
        let mut pending = i.roots.clone();
        for root in &i.roots {
            if *root == m::CommitId::ROOT {
                continue;
            }
            if let Some(code) = self.root_errors.lock().unwrap().get(root) {
                return Err(tonic::Status::new(*code, "fixture root rejected"));
            }
            if !self.admitted.lock().unwrap().contains(root) {
                return Err(tonic::Status::permission_denied(
                    "fixture root not admitted",
                ));
            }
        }
        while let Some(root) = pending.pop() {
            if !wanted.insert(root) || root == m::CommitId::ROOT {
                continue;
            }
            let node = all
                .get(&root)
                .ok_or_else(|| tonic::Status::not_found("fixture root missing"))?;
            pending.extend(node.parents.iter().copied());
        }
        let mut nodes: Vec<_> = remote
            .into_iter()
            .filter(|node| wanted.contains(&model_commit_id(node)))
            .collect();
        append_overlay(&mut nodes, i.overlay)?;
        Ok(self.install_scope(nodes, o))
    }
    async fn fork_scope(
        &self,
        i: ForkQueryScope,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, tonic::Status> {
        let mut nodes = self
            .scopes
            .lock()
            .unwrap()
            .get(&i.parent)
            .ok_or_else(|| tonic::Status::failed_precondition("fixture parent missing"))?
            .clone();
        let ids = i
            .overlay
            .iter()
            .map(|bytes| {
                let m::Object::Commit(node) = m::decode(bytes, Default::default()).unwrap() else {
                    panic!()
                };
                model_commit_id(&node)
            })
            .collect();
        self.fork_overlays.lock().unwrap().push(ids);
        append_overlay(&mut nodes, i.overlay)?;
        Ok(self.install_scope(nodes, o))
    }
    async fn facts(
        &self,
        s: [u8; 32],
        facts: Vec<q::GraphFact>,
        o: QueryOwner,
    ) -> Result<QueryReply<q::GraphFacts>, tonic::Status> {
        self.facts_count.fetch_add(1, Ordering::SeqCst);
        if self.fail_fact.swap(false, Ordering::SeqCst) {
            return Err(tonic::Status::unavailable("fixture transient fact error"));
        }
        let scopes = self.scopes.lock().unwrap();
        let nodes = scopes
            .get(&s)
            .ok_or_else(|| tonic::Status::failed_precondition("fixture scope expired"))?;
        let ids: BTreeSet<_> = nodes
            .iter()
            .map(|n| {
                m::CommitId(
                    m::encode(&m::Object::Commit(n.clone()), Default::default())
                        .unwrap()
                        .descriptor
                        .key
                        .id
                        .0,
                )
            })
            .chain([m::CommitId::ROOT])
            .collect();
        let mut answers = Vec::new();
        for fact in facts {
            match fact.fact.unwrap() {
                q::graph_fact::Fact::HasId(v) => answers.push(q::GraphFactAnswer {
                    answer: Some(q::graph_fact_answer::Answer::HasId(
                        ids.contains(&id(v.commit.as_ref())),
                    )),
                }),
                q::graph_fact::Fact::Membership(v) => {
                    let candidate = id(v.candidate.as_ref());
                    let expression = v.expression.as_ref().ok_or_else(|| {
                        tonic::Status::invalid_argument("fixture membership expression missing")
                    })?;
                    answers.push(q::GraphFactAnswer {
                        answer: Some(q::graph_fact_answer::Answer::Membership(
                            selected(expression, nodes).contains(&candidate),
                        )),
                    });
                }
                q::graph_fact::Fact::CommonAncestors(v) => {
                    let ancestors = |commits: Vec<q::CommitId>| {
                        selected(
                            &q::GraphExpr {
                                version: 1,
                                expression: Some(q::graph_expr::Expression::Ancestors(Box::new(
                                    q::AncestorsExpr {
                                        heads: Some(Box::new(q::GraphExpr {
                                            version: 1,
                                            expression: Some(q::graph_expr::Expression::Commits(
                                                q::CommitSet { commits },
                                            )),
                                        })),
                                        generations: None,
                                        parents: None,
                                    },
                                ))),
                            },
                            nodes,
                        )
                    };
                    let left = ancestors(v.left);
                    let right = ancestors(v.right);
                    let common: BTreeSet<_> = left.intersection(&right).copied().collect();
                    let heads: Vec<_> = common
                        .iter()
                        .filter(|candidate| {
                            !common.iter().any(|other| {
                                other != *candidate
                                    && ancestors(vec![q::CommitId {
                                        value: other.0.to_vec(),
                                    }])
                                    .contains(candidate)
                            })
                        })
                        .map(|id| q::CommitId {
                            value: id.0.to_vec(),
                        })
                        .collect();
                    answers.push(q::GraphFactAnswer {
                        answer: Some(q::graph_fact_answer::Answer::CommonAncestors(
                            q::CommitSet { commits: heads },
                        )),
                    });
                }
                _ => return Err(tonic::Status::unimplemented("fixture")),
            }
        }
        Ok(reply(
            q::GraphFacts {
                answers,
                scope_digest: vec![1; 32],
                index_epoch: vec![2; 16],
            },
            o,
        ))
    }
    async fn resolve_commit_prefix(
        &self,
        _: [u8; 32],
        _: String,
        _: QueryOwner,
    ) -> Result<QueryReply<q::CommitPrefixResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn resolve_change_prefix(
        &self,
        scope: [u8; 32],
        prefix: String,
        heads: Vec<m::CommitId>,
        owner: QueryOwner,
    ) -> Result<QueryReply<q::ChangePrefixResult>, tonic::Status> {
        self.prefix_count.fetch_add(1, Ordering::SeqCst);
        let nodes = self.scopes.lock().unwrap().get(&scope).unwrap().clone();
        let visible = selected(
            &q::GraphExpr {
                version: 1,
                expression: Some(q::graph_expr::Expression::Ancestors(Box::new(
                    q::AncestorsExpr {
                        heads: Some(Box::new(q::GraphExpr {
                            version: 1,
                            expression: Some(q::graph_expr::Expression::Commits(q::CommitSet {
                                commits: heads
                                    .into_iter()
                                    .map(|id| q::CommitId {
                                        value: id.0.to_vec(),
                                    })
                                    .collect(),
                            })),
                        })),
                        generations: None,
                        parents: None,
                    },
                ))),
            },
            &nodes,
        );
        let changes: BTreeSet<_> = nodes
            .iter()
            .filter(|node| {
                node.change_id
                    .0
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
                    .starts_with(&prefix)
            })
            .map(|node| node.change_id)
            .collect();
        let result = if changes.is_empty() {
            q::change_prefix_result::Result::NoMatch(q::NoMatch {})
        } else if changes.len() > 1 {
            q::change_prefix_result::Result::Ambiguous(q::AmbiguousChanges {})
        } else {
            let change = *changes.first().unwrap();
            q::change_prefix_result::Result::Single(q::ChangeMatch {
                change: Some(q::ChangeId {
                    value: change.0.to_vec(),
                }),
                targets: nodes
                    .iter()
                    .rev()
                    .filter(|node| node.change_id == change)
                    .map(|node| {
                        let id = model_commit_id(node);
                        q::ChangeTarget {
                            commit: Some(q::CommitId {
                                value: id.0.to_vec(),
                            }),
                            visible: Some(visible.contains(&id)),
                        }
                    })
                    .collect(),
            })
        };
        Ok(reply(
            q::ChangePrefixResult {
                selection_digest: None,
                result: Some(result),
            },
            owner,
        ))
    }
    async fn shortest_prefix(
        &self,
        _: [u8; 32],
        _: q::shortest_prefix_request::Target,
        _: Vec<m::CommitId>,
        _: QueryOwner,
    ) -> Result<QueryReply<q::ShortestPrefixResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn begin(
        &self,
        i: BeginQuery,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryHandle>, tonic::Status> {
        self.begins.fetch_add(1, Ordering::SeqCst);
        QueryScope {
            cloud_source_digest: None,
            handle: i.scope,
            expires_at_unix_seconds: now().unwrap() + 300,
            query_identity_version: 2,
        }
        .validate_query_id(&i.replay_key)?;
        let scopes = self.scopes.lock().unwrap();
        let nodes = scopes.get(&i.scope).unwrap();
        // Match the service's strict literal contract: out-of-scope IDs are an
        // error, not an empty literal. Adapter containing_fn must check first.
        let known: BTreeSet<_> = nodes
            .iter()
            .map(|node| {
                m::CommitId(
                    m::encode(&m::Object::Commit(node.clone()), Default::default())
                        .unwrap()
                        .descriptor
                        .key
                        .id
                        .0,
                )
            })
            .chain([m::CommitId::ROOT])
            .collect();
        let mut pending = vec![&i.expression];
        while let Some(expression) = pending.pop() {
            match expression.expression.as_ref().unwrap() {
                q::graph_expr::Expression::Commits(v) => {
                    if v.commits.iter().any(|v| !known.contains(&id(Some(v)))) {
                        return Err(tonic::Status::invalid_argument(
                            "literal outside fixture scope",
                        ));
                    }
                }
                q::graph_expr::Expression::Ancestors(v) => {
                    pending.push(v.heads.as_deref().unwrap())
                }
                q::graph_expr::Expression::Intersection(v)
                | q::graph_expr::Expression::Union(v) => pending.extend(v.inputs.iter()),
                q::graph_expr::Expression::HasConflict(v) => {
                    pending.push(v.input.as_deref().unwrap())
                }
                q::graph_expr::Expression::Difference(v) => {
                    pending.push(v.left.as_deref().unwrap());
                    pending.push(v.right.as_deref().unwrap());
                }
                _ => panic!("fixture operation not exercised"),
            }
        }
        let set = selected(&i.expression, nodes);
        let mut rows = Vec::new();
        for node in nodes.iter().rev() {
            let encoded = m::encode(&m::Object::Commit(node.clone()), Default::default()).unwrap();
            let commit = m::CommitId(encoded.descriptor.key.id.0);
            if !set.contains(&commit) {
                continue;
            }
            let edges = if i.projection != q::Projection::Ids {
                node.parents
                    .iter()
                    .map(|parent| q::SelectedEdge {
                        edge: Some(if set.contains(parent) {
                            q::selected_edge::Edge::Direct(q::DirectEdge {
                                target: Some(q::CommitId {
                                    value: parent.0.to_vec(),
                                }),
                            })
                        } else {
                            q::selected_edge::Edge::Missing(q::MissingEdge {
                                boundary: Some(q::CommitId {
                                    value: parent.0.to_vec(),
                                }),
                            })
                        }),
                    })
                    .collect()
            } else {
                Vec::new()
            };
            rows.push(q::GraphRow {
                commit: Some(q::CommitId {
                    value: commit.0.to_vec(),
                }),
                change: Some(q::ChangeId {
                    value: node.change_id.0.to_vec(),
                }),
                edges,
                canonical: if i.projection == q::Projection::Canonical {
                    Some(tinytree_proto::v1::ObjectData {
                        descriptor: Some(tinytree_proto::v1::ObjectDescriptor {
                            key: Some(tinytree_proto::v1::ObjectKey {
                                kind: 3,
                                native_id: encoded.descriptor.key.id.0.to_vec(),
                            }),
                            content_blake3: encoded.descriptor.key.id.0.to_vec(),
                            size_bytes: encoded.descriptor.size_bytes,
                            object_format_version: u32::from(encoded.descriptor.format_version),
                        }),
                        canonical_envelope: encoded.bytes,
                    })
                } else {
                    None
                },
            });
        }
        if set.contains(&m::CommitId::ROOT) {
            rows.push(q::GraphRow {
                commit: Some(q::CommitId { value: vec![0; 32] }),
                change: Some(q::ChangeId { value: vec![0; 16] }),
                edges: vec![],
                canonical: None,
            });
        }
        let handle = random();
        self.queries.lock().unwrap().insert(handle, (rows, 0, 0));
        Ok(reply(
            QueryHandle {
                handle,
                expires_at_unix_seconds: now().unwrap() + 300,
            },
            o,
        ))
    }
    async fn page(
        &self,
        h: [u8; 32],
        seq: u64,
        o: QueryOwner,
    ) -> Result<QueryReply<QueryPage>, tonic::Status> {
        if self.pause_pages.load(Ordering::SeqCst) {
            let (send, recv) = futures::channel::oneshot::channel();
            self.page_waiters.lock().unwrap().push(send);
            let _ = recv.await;
        }
        let mut queries = self.queries.lock().unwrap();
        let (rows, offset, expected) = queries.get_mut(&h).unwrap();
        assert_eq!(seq, *expected);
        *expected += 1;
        if seq == 0 {
            return Ok(reply(
                QueryPage {
                    sequence: seq,
                    rows: vec![],
                    exhausted: false,
                },
                o,
            ));
        }
        let page = rows.get(*offset).cloned().into_iter().collect();
        *offset += 1;
        Ok(reply(
            QueryPage {
                sequence: seq,
                rows: page,
                exhausted: *offset >= rows.len(),
            },
            o,
        ))
    }
    async fn read_commit(
        &self,
        scope: [u8; 32],
        id: m::CommitId,
        owner: QueryOwner,
    ) -> Result<QueryReply<jj_native_backend::VerifiedMetadata>, tonic::Status> {
        let scopes = self.scopes.lock().unwrap();
        let nodes = scopes
            .get(&scope)
            .ok_or_else(|| tonic::Status::failed_precondition("fixture scope expired"))?;
        let node = nodes
            .iter()
            .find(|n| model_commit_id(n) == id)
            .ok_or_else(|| tonic::Status::invalid_argument("fixture commit outside scope"))?;
        let encoded = m::encode(&m::Object::Commit(node.clone()), Default::default()).unwrap();
        let verified = jj_native_backend::VerifiedMetadata::verify(
            encoded.descriptor,
            encoded.bytes,
            Default::default(),
        )
        .unwrap();
        let proof = Arc::new(CanonicalReplyOwner);
        self.reply_owners
            .lock()
            .unwrap()
            .push(Arc::downgrade(&proof));
        Ok(reply(verified, Arc::new((owner, proof))))
    }
    async fn close_query(&self, h: [u8; 32], _: QueryOwner) -> Result<(), tonic::Status> {
        self.queries.lock().unwrap().remove(&h);
        self.closes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn close_scope(&self, h: [u8; 32], _: QueryOwner) -> Result<(), tonic::Status> {
        self.scopes.lock().unwrap().remove(&h);
        Ok(())
    }
}
fn private() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    dir
}
fn fixture() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    NativeIndexStore,
    Arc<FixtureIo>,
    Arc<dyn NativeObjectIo>,
    Config,
) {
    let directory = private();
    let objects_directory = private();
    let objects: Arc<dyn NativeObjectIo> = Arc::new(
        jj_native_backend::DirectObjectIo::init(objects_directory.path(), Default::default())
            .unwrap(),
    );
    let io = Arc::new(FixtureIo::default());
    let config = Config {
        endpoint: Endpoint::UnixSocket(directory.path().join("broker.sock")),
        store_alias: "example".into(),
        provider_fingerprint: [7; 32],
    };
    let store = NativeIndexStore::init(
        &directory.path().join("index"),
        config.clone(),
        Limits::default(),
        io.clone(),
        objects.clone(),
        vec![],
    )
    .unwrap();
    (directory, objects_directory, store, io, objects, config)
}
#[test]
fn immutable_operation_associations_reopen_and_reject_missing_transplanted_or_foreign_state() {
    let (directory, _objects_directory, store, io, objects, config) = fixture();
    let foreign_dir =
        private_fs::PrivateDir::create(directory.path().join("foreign-index")).unwrap();
    std::fs::write(foreign_dir.path().join("sentinel"), b"untouched").unwrap();
    assert!(matches!(
        NativeIndexStore::init(
            foreign_dir.path(),
            config.clone(),
            Default::default(),
            io.clone(),
            objects.clone(),
            vec![]
        ),
        Err(Error::Corrupt)
    ));
    let names = std::fs::read_dir(foreign_dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect::<Vec<_>>();
    assert_eq!(names, vec![std::ffi::OsString::from("sentinel")]);
    assert_eq!(
        std::fs::read(foreign_dir.path().join("sentinel")).unwrap(),
        b"untouched"
    );
    let index = store.seed_index().unwrap();
    assert!(block_on(index.has_id(&CommitId::from_bytes(&[0; 32]))).unwrap());
    assert!(matches!(
        index.all_heads_for_gc(),
        Err(IndexError::AllHeadsForGcUnsupported)
    ));
    assert!(matches!(
        store.index_at_operation(&[3; 64]),
        Err(Error::MissingOperation)
    ));
    store.associate_operation(&[1; 64], &index).unwrap();
    store.associate_operation(&[1; 64], &index).unwrap();
    let reopened = NativeIndexStore::open(
        &directory.path().join("index"),
        &config,
        Default::default(),
        io.clone(),
        objects.clone(),
    )
    .unwrap();
    assert!(
        block_on(
            reopened
                .index_at_operation(&[1; 64])
                .unwrap()
                .has_id(&CommitId::from_bytes(&[0; 32]))
        )
        .unwrap()
    );
    let operations = directory.path().join("index/operations");
    std::fs::copy(
        operations.join("01".repeat(64)),
        operations.join("02".repeat(64)),
    )
    .unwrap();
    assert!(matches!(
        reopened.index_at_operation(&[2; 64]),
        Err(Error::Corrupt)
    ));
    // A copied directory retains the UUID but is not the same physical index.
    // Its later segment must never be ACKed by a destination lacking that tip.
    let copied_path = directory.path().join("copied-index");
    let copied = private_fs::PrivateDir::create(&copied_path).unwrap();
    for file in ["manifest", "seed", "writer.lock"] {
        std::fs::copy(
            directory.path().join("index").join(file),
            copied.path().join(file),
        )
        .unwrap();
    }
    for child in ["segments", "operations"] {
        let target = private_fs::PrivateDir::create(copied.path().join(child)).unwrap();
        for entry in std::fs::read_dir(directory.path().join("index").join(child)).unwrap() {
            let entry = entry.unwrap();
            std::fs::copy(entry.path(), target.path().join(entry.file_name())).unwrap();
        }
    }
    let copied_store = NativeIndexStore::open(
        &copied_path,
        &config,
        Default::default(),
        io.clone(),
        objects.clone(),
    )
    .unwrap();
    let divergent = copied_store
        .persist_index(copied_store.seed_index().unwrap().start_modification())
        .unwrap();
    assert!(matches!(
        store.associate_operation(&[9; 64], &divergent),
        Err(Error::Foreign)
    ));
    assert!(matches!(
        store.index_at_operation(&[9; 64]),
        Err(Error::MissingOperation)
    ));
    let mut foreign = config;
    foreign.provider_fingerprint = [8; 32];
    assert!(matches!(
        NativeIndexStore::open(
            &directory.path().join("index"),
            &foreign,
            Default::default(),
            io,
            objects
        ),
        Err(Error::Foreign)
    ));
}
#[test]
fn public_mutable_traits_keep_local_drafts_hidden_siblings_and_lazy_streams_across_restart() {
    let _sync_test = SYNC_TEST.lock().unwrap();
    block_on(async {
        let (directory, _objects_directory, store, io, objects, config) = fixture();
        let backend =
            jj_native_backend::NativeBackend::from_io(objects.clone(), Default::default())
                .await
                .unwrap();
        let jj_store = Store::new(
            Box::new(backend),
            jj_lib::signing::Signer::new(None, vec![]),
            jj_lib::tree_merge::MergeOptions {
                hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
                same_change: jj_lib::merge::SameChange::Accept,
            },
        );
        let root = jj_store.root_commit();
        let mut small = Limits::default();
        small.max_query_rows = 2;
        let bounded = NativeIndexStore::open(
            &directory.path().join("index"),
            &config,
            small,
            io.clone(),
            objects.clone(),
        )
        .unwrap();
        let bounded = bounded.seed_index().unwrap();
        let before = io.opens.load(Ordering::SeqCst);
        let allowed = ResolvedExpression::Union(
            Box::new(ResolvedExpression::Commits(vec![root.id().clone()])),
            Box::new(ResolvedExpression::Commits(vec![root.id().clone()])),
        );
        assert!(bounded.evaluate_revset(&allowed, &jj_store).is_ok());
        let rejected = ResolvedExpression::Union(
            Box::new(ResolvedExpression::Commits(vec![root.id().clone(); 2])),
            Box::new(ResolvedExpression::Commits(vec![root.id().clone()])),
        );
        let error = bounded.evaluate_revset(&rejected, &jj_store).unwrap_err();
        let RevsetEvaluationError::Other(source) = error else {
            panic!("wrong compiler limit category")
        };
        assert!(matches!(source.downcast_ref::<Error>(), Some(Error::Limit)));
        assert_eq!(
            io.opens.load(Ordering::SeqCst),
            before,
            "AST validation must not perform query I/O"
        );
        let mut mutable = store.seed_index().unwrap().start_modification();
        let mut commits = Vec::new();
        for n in [1, 2] {
            let mut value = root.store_commit().as_ref().clone();
            value.parents = vec![root.id().clone()];
            value.change_id = ChangeId::from_bytes(&[9; 16]);
            value.description = format!("private sibling {n}");
            let commit = jj_store.write_commit(value, None).await.unwrap();
            assert!(
                !mutable.as_index().has_id(commit.id()).await.unwrap(),
                "a canonical Store object is not yet a member of this logical index"
            );
            let all = mutable
                .as_index()
                .evaluate_revset(
                    &ResolvedExpression::Ancestors {
                        heads: Box::new(ResolvedExpression::Commits(vec![root.id().clone()])),
                        generation: 0..u64::MAX,
                        parents_range: 0..u32::MAX,
                    },
                    &jj_store,
                )
                .unwrap();
            assert!(
                !all.containing_fn()(commit.id()).await.unwrap(),
                "a Store-only ID is outside the frozen logical index"
            );
            drop(all);
            mutable.add_commit(&commit).await.unwrap();
            let view = mutable
                .change_id_index(&mut std::iter::once(root.id()))
                .unwrap();
            let PrefixResolution::SingleMatch(changes) = view
                .resolve_prefix(&HexPrefix::try_from_hex("09".repeat(16)).unwrap())
                .await
                .unwrap()
            else {
                panic!("draft change missing")
            };
            assert_eq!(
                changes.targets.len(),
                n as usize,
                "pending checkpoints with identical persisted tips must not share stale answers"
            );
            commits.push(commit);
        }
        let readonly = mutable.as_index();
        for commit in &commits {
            assert!(readonly.has_id(commit.id()).await.unwrap());
        }
        let expression = ResolvedExpression::Ancestors {
            heads: Box::new(ResolvedExpression::Commits(
                commits.iter().map(|c| c.id().clone()).collect(),
            )),
            generation: 0..u64::MAX,
            parents_range: 0..u32::MAX,
        };
        let revset = readonly.evaluate_revset(&expression, &jj_store).unwrap();
        assert!(!revset.is_empty().unwrap());
        let rows = revset
            .commit_change_ids()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, *commits[1].id());
        assert_eq!(rows[1].0, *commits[0].id());
        assert_eq!(rows[2].0, *root.id());
        let graph = revset.stream_graph().try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            graph[0].1[0],
            jj_lib::graph::GraphEdge::direct(root.id().clone())
        );
        let contains = revset.containing_fn();
        let before_queries = io.begins.load(Ordering::SeqCst);
        assert!(contains(commits[0].id()).await.unwrap());
        assert!(contains(root.id()).await.unwrap());
        let before_facts = io.facts_count.load(Ordering::SeqCst);
        assert!(contains(commits[0].id()).await.unwrap());
        assert!(contains(root.id()).await.unwrap());
        assert_eq!(
            io.facts_count.load(Ordering::SeqCst),
            before_facts,
            "immutable membership answers are reused without opening cursors"
        );
        let other = readonly
            .evaluate_revset(
                &ResolvedExpression::Commits(vec![root.id().clone()]),
                &jj_store,
            )
            .unwrap();
        assert!(
            !other.containing_fn()(commits[0].id()).await.unwrap(),
            "membership cache binds the whole expression"
        );
        assert_eq!(
            io.begins.load(Ordering::SeqCst),
            before_queries,
            "membership checks must not open resumable evaluations"
        );
        assert!(io.opens.load(Ordering::SeqCst) >= 2);
        drop(contains);
        drop(revset);
        // Candidate-relative boolean predicates must not accidentally complement
        // against the whole logical universe (the intrinsic root is excluded).
        use jj_lib::revset::ResolvedPredicateExpression as P;
        let a = || {
            P::Set(Box::new(ResolvedExpression::Commits(vec![
                commits[0].id().clone(),
            ])))
        };
        let b = || {
            P::Set(Box::new(ResolvedExpression::Commits(vec![
                commits[1].id().clone(),
            ])))
        };
        struct PredicateCase {
            name: &'static str,
            predicate: P,
            expected: Vec<CommitId>,
        }
        let cases = [
            PredicateCase {
                name: "union",
                predicate: P::Union(Box::new(a()), Box::new(b())),
                expected: vec![commits[1].id().clone(), commits[0].id().clone()],
            },
            PredicateCase {
                name: "disjoint intersection",
                predicate: P::Intersection(Box::new(a()), Box::new(b())),
                expected: vec![],
            },
            PredicateCase {
                name: "nested complement",
                predicate: P::NotIn(Box::new(P::Union(
                    Box::new(P::NotIn(Box::new(a()))),
                    Box::new(b()),
                ))),
                expected: vec![commits[0].id().clone()],
            },
            PredicateCase {
                name: "intersection of complements",
                predicate: P::Intersection(
                    Box::new(P::NotIn(Box::new(a()))),
                    Box::new(P::NotIn(Box::new(b()))),
                ),
                expected: vec![],
            },
            PredicateCase {
                name: "union of complements",
                predicate: P::Union(
                    Box::new(P::NotIn(Box::new(a()))),
                    Box::new(P::NotIn(Box::new(b()))),
                ),
                expected: vec![commits[1].id().clone(), commits[0].id().clone()],
            },
            PredicateCase {
                name: "empty set complement",
                predicate: P::NotIn(Box::new(P::Set(Box::new(ResolvedExpression::Commits(
                    vec![],
                ))))),
                expected: vec![commits[1].id().clone(), commits[0].id().clone()],
            },
        ];
        for case in cases {
            let filtered = ResolvedExpression::FilterWithin {
                candidates: Box::new(ResolvedExpression::Ancestors {
                    heads: Box::new(ResolvedExpression::Commits(
                        commits.iter().map(|c| c.id().clone()).collect(),
                    )),
                    generation: 0..1,
                    parents_range: 0..u32::MAX,
                }),
                predicate: case.predicate,
            };
            let result = readonly
                .evaluate_revset(&filtered, &jj_store)
                .unwrap()
                .stream()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(result, case.expected, "{}", case.name);
        }
        let persisted = store.persist_index(mutable).unwrap();
        store.associate_operation(&[4; 64], &persisted).unwrap();
        let reopened = NativeIndexStore::open(
            &directory.path().join("index"),
            &config,
            Default::default(),
            io,
            objects,
        )
        .unwrap();
        let restored = reopened.index_at_operation(&[4; 64]).unwrap();
        for commit in &commits {
            assert!(restored.has_id(commit.id()).await.unwrap());
        }
        let restored_rows = restored
            .evaluate_revset(&expression, &jj_store)
            .unwrap()
            .stream()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            restored_rows,
            rows.into_iter().map(|(id, _)| id).collect::<Vec<_>>()
        );
    });
}

// Tests that intentionally occupy the global bounded bridge serialize with
// other synchronous-query assertions, not with ordinary adapter/storage tests.
static SYNC_TEST: Mutex<()> = Mutex::new(());
fn jj_store(objects: Arc<dyn NativeObjectIo>) -> Arc<Store> {
    let backend = block_on(jj_native_backend::NativeBackend::from_io(
        objects,
        Default::default(),
    ))
    .unwrap();
    Store::new(
        Box::new(backend),
        jj_lib::signing::Signer::new(None, vec![]),
        jj_lib::tree_merge::MergeOptions {
            hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
            same_change: jj_lib::merge::SameChange::Accept,
        },
    )
}
#[test]
fn synchronous_boolean_bridge_works_inside_current_thread_runtime() {
    let _sync_test = SYNC_TEST.lock().unwrap();
    let (_directory, _objects_directory, store, _io, objects, _config) = fixture();
    let jj_store = jj_store(objects);
    let index = store.seed_index().unwrap();
    let expression = ResolvedExpression::Commits(vec![jj_store.root_commit_id().clone()]);
    let revset = index.evaluate_revset(&expression, &jj_store).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        assert!(!revset.is_empty().unwrap());
    });
}
#[test]
fn synchronous_query_timeout_keeps_physical_jobs_bounded_and_recovers_after_release() {
    let _sync_test = SYNC_TEST.lock().unwrap();
    let (directory, _objects_directory, store, io, objects, config) = fixture();
    let mut limits = Limits::default();
    limits.sync_query_timeout_ms = 50;
    let bounded = NativeIndexStore::open(
        &directory.path().join("index"),
        &config,
        limits,
        io.clone(),
        objects.clone(),
    )
    .unwrap();
    let jj_store = jj_store(objects);
    let index = bounded.seed_index().unwrap();
    let revset = index
        .evaluate_revset(
            &ResolvedExpression::Commits(vec![jj_store.root_commit_id().clone()]),
            &jj_store,
        )
        .unwrap();
    io.pause_pages.store(true, Ordering::SeqCst);
    for _ in 0..4 {
        let error = revset.is_empty().unwrap_err();
        let RevsetEvaluationError::Other(source) = error else {
            panic!("wrong bridge error category")
        };
        assert!(matches!(
            source.downcast_ref::<Error>(),
            Some(Error::Query(tonic::Code::DeadlineExceeded))
        ));
    }
    let error = revset.is_empty().unwrap_err();
    let RevsetEvaluationError::Other(source) = error else {
        panic!("wrong bridge error category")
    };
    assert!(
        matches!(source.downcast_ref::<Error>(), Some(Error::Limit)),
        "timed-out physical jobs must keep admission until done"
    );
    io.pause_pages.store(false, Ordering::SeqCst);
    for waiter in io.page_waiters.lock().unwrap().drain(..) {
        let _ = waiter.send(());
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match revset.is_empty() {
            Ok(empty) => {
                assert!(!empty);
                break;
            }
            Err(_) if std::time::Instant::now() < deadline => std::thread::yield_now(),
            Err(e) => panic!("bridge did not recover: {e}"),
        }
    }
    drop(store);
}

#[derive(Debug)]
struct CountingObjects {
    inner: Arc<dyn NativeObjectIo>,
    reads: Mutex<Vec<m::ObjectKey>>,
    files: AtomicUsize,
    file_writes: AtomicUsize,
    reply_owner_probe: Option<Arc<FixtureIo>>,
    pause_metadata: AtomicBool,
    metadata_waiters: Mutex<Vec<futures::channel::oneshot::Sender<()>>>,
}
impl CountingObjects {
    async fn metadata_gate(&self) {
        if self.pause_metadata.load(Ordering::SeqCst) {
            let (send, recv) = futures::channel::oneshot::channel();
            self.metadata_waiters.lock().unwrap().push(send);
            let _ = recv.await;
        }
    }
    fn resume_metadata(&self) {
        self.pause_metadata.store(false, Ordering::SeqCst);
        for send in self.metadata_waiters.lock().unwrap().drain(..) {
            let _ = send.send(());
        }
    }
}
#[async_trait]
impl NativeObjectIo for CountingObjects {
    async fn descriptor_with_owner(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.inner.descriptor_with_owner(key, owner).await
    }
    async fn read_file_with_owner(
        &self,
        id: m::FileId,
        owner: QueryOwner,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        self.files.fetch_add(1, Ordering::SeqCst);
        if let Some(query) = &self.reply_owner_probe {
            let live = query
                .reply_owners
                .lock()
                .unwrap()
                .iter()
                .filter(|owner| owner.upgrade().is_some())
                .count();
            assert!(
                live >= 4,
                "canonical selected/parents/base response owners must outlive decoded Store caches until file resolution: {live}"
            );
        }
        self.inner.read_file_with_owner(id, owner).await
    }

    async fn descriptor(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<Option<m::Descriptor>> {
        self.inner.descriptor(key).await
    }
    async fn read_metadata(
        &self,
        key: m::ObjectKey,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.reads.lock().unwrap().push(key);
        self.metadata_gate().await;
        self.inner.read_metadata(key).await
    }
    async fn read_metadata_with_owner(
        &self,
        key: m::ObjectKey,
        owner: QueryOwner,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::VerifiedMetadata> {
        self.reads.lock().unwrap().push(key);
        self.metadata_gate().await;
        self.inner.read_metadata_with_owner(key, owner).await
    }
    async fn write_metadata(
        &self,
        value: m::Object,
    ) -> jj_lib::backend::BackendResult<m::Descriptor> {
        self.inner.write_metadata(value).await
    }
    async fn read_file(
        &self,
        id: m::FileId,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::FileRead> {
        self.files.fetch_add(1, Ordering::SeqCst);
        self.inner.read_file(id).await
    }
    async fn write_file(
        &self,
        input: &mut (dyn futures::io::AsyncRead + Send + Unpin),
        max: u64,
    ) -> jj_lib::backend::BackendResult<m::FileId> {
        self.file_writes.fetch_add(1, Ordering::SeqCst);
        self.inner.write_file(input, max).await
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: jj_native_backend::CopyCatalogLimits,
    ) -> jj_lib::backend::BackendResult<jj_native_backend::CopyCatalogSnapshot> {
        self.inner.copy_catalog_snapshot(limits).await
    }
}
#[test]
fn admitted_root_advance_promotes_overlay_without_losing_hidden_drafts_or_old_operations() {
    let _sync_test = SYNC_TEST.lock().unwrap();
    block_on(async {
        let (directory, _objects_directory, store, io, objects, config) = fixture();
        let counted = Arc::new(CountingObjects {
            inner: objects,
            reads: Mutex::new(Vec::new()),
            files: AtomicUsize::new(0),
            file_writes: AtomicUsize::new(0),
            reply_owner_probe: None,
            pause_metadata: AtomicBool::new(false),
            metadata_waiters: Mutex::new(Vec::new()),
        });
        let store = NativeIndexStore::open(
            &directory.path().join("index"),
            &config,
            Default::default(),
            io.clone(),
            counted.clone(),
        )
        .unwrap();
        // This fixture is already inside an executor: await provider assembly
        // instead of nesting the synchronous setup helper's LocalPool.
        let backend =
            jj_native_backend::NativeBackend::from_io(counted.clone(), Default::default())
                .await
                .unwrap();
        let jj_store = Store::new(
            Box::new(backend),
            jj_lib::signing::Signer::new(None, vec![]),
            jj_lib::tree_merge::MergeOptions {
                hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
                same_change: jj_lib::merge::SameChange::Accept,
            },
        );
        let root = jj_store.root_commit();
        let mut mutable = store.seed_index().unwrap().start_modification();
        let mut local = Vec::new();
        for description in ["published sibling", "hidden divergent sibling"] {
            let mut value = root.store_commit().as_ref().clone();
            value.parents = vec![root.id().clone()];
            value.change_id = ChangeId::from_bytes(&[9; 16]);
            value.description = description.into();
            let commit = jj_store.write_commit(value, None).await.unwrap();
            mutable.add_commit(&commit).await.unwrap();
            local.push(commit);
        }
        let old = store.persist_index(mutable).unwrap();
        store.associate_operation(&[10; 64], &old).unwrap();
        let published = local[0].id();
        let hidden = local[1].id();
        let metadata = counted
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(model_id(published).unwrap().0),
            })
            .await
            .unwrap();
        let m::Object::Commit(published_model) = metadata.into_object() else {
            panic!()
        };
        let mut advanced = published_model.clone();
        advanced.parents = vec![model_id(published).unwrap()];
        advanced.change_id = m::ChangeId([17; 16]);
        advanced.description = "new remote root".into();
        let advanced_id = model_commit_id(&advanced);
        io.remote
            .lock()
            .unwrap()
            .extend([published_model, advanced]);
        io.admitted.lock().unwrap().insert(advanced_id);
        assert!(!old.has_id(&jj_id(advanced_id)).await.unwrap());
        let old_association = std::fs::read(
            directory
                .path()
                .join("index/operations")
                .join("0a".repeat(64)),
        )
        .unwrap();
        counted.reads.lock().unwrap().clear();
        io.fork_overlays.lock().unwrap().clear();
        let extended = old.with_remote_roots(vec![advanced_id]).await.unwrap();
        assert!(extended.has_id(&jj_id(advanced_id)).await.unwrap());
        assert!(extended.has_id(published).await.unwrap());
        assert!(extended.has_id(hidden).await.unwrap());
        assert_eq!(
            io.fork_overlays.lock().unwrap().as_slice(),
            &[vec![model_id(hidden).unwrap()]],
            "only the not-remotely-covered sibling is uploaded"
        );
        let advanced_jj = jj_id(advanced_id);
        let change_index = extended
            .change_id_index(&mut std::iter::once(&advanced_jj))
            .unwrap();
        let PrefixResolution::SingleMatch(changes) = change_index
            .resolve_prefix(&HexPrefix::try_from_hex("09".repeat(16)).unwrap())
            .await
            .unwrap()
        else {
            panic!("divergent change lost")
        };
        assert!(
            changes
                .targets
                .contains(&(published.clone(), ResolvedChangeState::Visible))
        );
        assert!(
            changes
                .targets
                .contains(&(hidden.clone(), ResolvedChangeState::Hidden))
        );
        let prefix = HexPrefix::try_from_hex("09".repeat(16)).unwrap();
        let before_prefixes = io.prefix_count.load(Ordering::SeqCst);
        let again = change_index.resolve_prefix(&prefix).await.unwrap();
        assert!(matches!(again, PrefixResolution::SingleMatch(_)));
        assert_eq!(io.prefix_count.load(Ordering::SeqCst), before_prefixes);
        let hidden_view = extended
            .change_id_index(&mut std::iter::once(hidden))
            .unwrap();
        let PrefixResolution::SingleMatch(hidden_changes) =
            hidden_view.resolve_prefix(&prefix).await.unwrap()
        else {
            panic!("change missing")
        };
        assert!(
            hidden_changes
                .targets
                .contains(&(published.clone(), ResolvedChangeState::Hidden))
        );
        assert!(
            hidden_changes
                .targets
                .contains(&(hidden.clone(), ResolvedChangeState::Visible))
        );
        assert_eq!(
            io.prefix_count.load(Ordering::SeqCst),
            before_prefixes + 1,
            "visible heads are part of the cache key"
        );
        let expression = ResolvedExpression::Ancestors {
            heads: Box::new(ResolvedExpression::Commits(vec![
                advanced_jj.clone(),
                hidden.clone(),
            ])),
            generation: 0..u64::MAX,
            parents_range: 0..u32::MAX,
        };
        let revset = extended.evaluate_revset(&expression, &jj_store).unwrap();
        let rows = revset.stream().try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(
            rows,
            vec![
                hidden.clone(),
                advanced_jj.clone(),
                published.clone(),
                root.id().clone()
            ]
        );
        assert!(!old.has_id(&advanced_jj).await.unwrap());
        assert!(
            !store
                .index_at_operation(&[10; 64])
                .unwrap()
                .has_id(&advanced_jj)
                .await
                .unwrap()
        );
        assert!(matches!(
            store.associate_operation(&[10; 64], &extended),
            Err(Error::Corrupt)
        ));
        assert_eq!(
            std::fs::read(
                directory
                    .path()
                    .join("index/operations")
                    .join("0a".repeat(64))
            )
            .unwrap(),
            old_association
        );
        let mut merged = old.start_modification();
        merged.merge_in(&extended).unwrap();
        assert!(merged.as_index().has_id(&advanced_jj).await.unwrap());
        let merged = store.persist_index(merged).unwrap();
        store.associate_operation(&[11; 64], &merged).unwrap();
        let restored = store.index_at_operation(&[11; 64]).unwrap();
        assert!(restored.has_id(&advanced_jj).await.unwrap());
        assert!(restored.has_id(hidden).await.unwrap());
        let allowed: Vec<_> = local
            .iter()
            .map(|c| m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(model_id(c.id()).unwrap().0),
            })
            .collect();
        assert!(
            counted
                .reads
                .lock()
                .unwrap()
                .iter()
                .all(|key| allowed.contains(key)),
            "root advance must not read new public canonical ancestry through ObjectIo"
        );
        assert_eq!(counted.files.load(Ordering::SeqCst), 0);
        let segments = std::fs::read_dir(directory.path().join("index/segments"))
            .unwrap()
            .count();
        for (root, code, admitted, root_error) in [
            (
                m::CommitId([71; 32]),
                tonic::Code::Unimplemented,
                false,
                Some(tonic::Code::Unimplemented),
            ),
            (m::CommitId([72; 32]), tonic::Code::NotFound, true, None),
            (
                m::CommitId([73; 32]),
                tonic::Code::PermissionDenied,
                false,
                None,
            ),
        ] {
            if admitted {
                io.admitted.lock().unwrap().insert(root);
            }
            if let Some(root_error) = root_error {
                io.root_errors.lock().unwrap().insert(root, root_error);
            }
            assert!(
                matches!(old.with_remote_roots(vec![root]).await,Err(Error::Query(actual))if actual==code)
            );
            assert_eq!(
                std::fs::read_dir(directory.path().join("index/segments"))
                    .unwrap()
                    .count(),
                segments,
                "unadmitted roots must not publish index segments"
            );
        }
    });
}
#[test]
fn fresh_initializer_seeds_actual_root_operation_without_repairing_foreign_directories() {
    use std::os::unix::fs::PermissionsExt;
    let (directory, _objects_directory, _store, io, objects, config) = fixture();
    let path = directory.path().join("fresh-index");
    std::fs::create_dir(&path).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    let initialized = NativeIndexStore::init_fresh_for_root_operation(
        &path,
        config.clone(),
        Default::default(),
        io.clone(),
        objects.clone(),
        vec![],
        &[81; 64],
    )
    .unwrap();
    assert!(initialized.index_at_operation(&[81; 64]).is_ok());
    assert!(matches!(
        initialized.index_at_operation(&[0; 64]),
        Err(Error::MissingOperation)
    ));
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert!(matches!(
        NativeIndexStore::init_fresh_for_root_operation(
            &path,
            config.clone(),
            Default::default(),
            io.clone(),
            objects.clone(),
            vec![],
            &[81; 64]
        ),
        Err(Error::Corrupt)
    ));
    let foreign = directory.path().join("foreign-fresh");
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("sentinel"), b"unchanged").unwrap();
    std::fs::set_permissions(&foreign, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        NativeIndexStore::init_fresh_for_root_operation(
            &foreign,
            config,
            Default::default(),
            io,
            objects,
            vec![],
            &[81; 64]
        ),
        Err(Error::Corrupt)
    ));
    assert_eq!(
        std::fs::metadata(&foreign).unwrap().permissions().mode() & 0o777,
        0o755
    );
    assert_eq!(
        std::fs::read(foreign.join("sentinel")).unwrap(),
        b"unchanged"
    );
    assert!(!foreign.join("writer.lock").exists());
}
#[test]
fn explicit_cleanup_driver_drains_logical_scopes_after_repositories_are_dropped() {
    let _sync_test = SYNC_TEST.lock().unwrap();
    let (directory, _objects_directory, initial, io, objects, config) = fixture();
    let jj_store = jj_store(objects.clone());
    drop(initial);
    for _ in 0..40 {
        let store = NativeIndexStore::open(
            &directory.path().join("index"),
            &config,
            Default::default(),
            io.clone(),
            objects.clone(),
        )
        .unwrap();
        let cleanup = store.cleanup_handle();
        let index = store.seed_index().unwrap();
        let revset = index
            .evaluate_revset(
                &ResolvedExpression::Commits(vec![jj_store.root_commit_id().clone()]),
                &jj_store,
            )
            .unwrap();
        assert!(!revset.is_empty().unwrap());
        drop(revset);
        drop(index);
        drop(store);
        cleanup.wait(Duration::from_secs(2)).unwrap();
        assert!(io.scopes.lock().unwrap().is_empty());
        assert!(io.queries.lock().unwrap().is_empty());
    }
}

#[test]
fn conflict_report_filters_selected_nonempty_changes_not_empty_children() {
    let _sync_test = SYNC_TEST.lock().unwrap();
    let (directory, _objects_directory, index_store, io, objects, config) = fixture();
    let jj_store = jj_store(objects.clone());
    block_on(async {
        use jj_lib::{
            backend::{Tree, TreeValue},
            fileset::FilesetExpression,
            merge::Merge,
            repo_path::{RepoPath, RepoPathComponentBuf},
            revset::{ResolvedPredicateExpression as P, RevsetFilterPredicate as F},
        };
        let root = jj_store.root_commit();
        let backend = jj_store.backend();
        let mut terms = Vec::new();
        for target in ["left", "base", "right"] {
            let link = backend
                .write_symlink(RepoPath::root(), target)
                .await
                .unwrap();
            terms.push(
                backend
                    .write_tree(
                        RepoPath::root(),
                        &Tree::from_sorted_entries(vec![(
                            RepoPathComponentBuf::new("link").unwrap(),
                            TreeValue::Symlink(link),
                        )]),
                    )
                    .await
                    .unwrap(),
            );
        }
        let mut value = root.store_commit().as_ref().clone();
        value.parents = vec![root.id().clone()];
        value.root_tree = Merge::from_vec(terms);
        value.change_id = ChangeId::from_bytes(&[31; 16]);
        value.description = "nonempty conflicting change".into();
        let conflict = jj_store.write_commit(value, None).await.unwrap();
        let mut empty_value = conflict.store_commit().as_ref().clone();
        empty_value.parents = vec![conflict.id().clone()];
        empty_value.change_id = ChangeId::from_bytes(&[32; 16]);
        empty_value.description = "empty child preserving conflict".into();
        let empty = jj_store.write_commit(empty_value, None).await.unwrap();
        let mut merge_value = empty.store_commit().as_ref().clone();
        merge_value.parents = vec![conflict.id().clone(), empty.id().clone()];
        merge_value.change_id = ChangeId::from_bytes(&[33; 16]);
        merge_value.description = "merge preserving its merged parent tree".into();
        let empty_merge = jj_store.write_commit(merge_value, None).await.unwrap();
        let mut mutable = index_store.seed_index().unwrap().start_modification();
        mutable.add_commit(&conflict).await.unwrap();
        mutable.add_commit(&empty).await.unwrap();
        mutable.add_commit(&empty_merge).await.unwrap();
        // Two independent line edits auto-merge to the known combined content.
        // The selected tree is syntactically conflicted but its file value may
        // be exactly that resolved parent result, so HasConflict alone is not
        // enough and the resolver's synthesized file must not be published.
        let mut file_trees = Vec::new();
        for contents in [
            "one\nmiddle\nthree\n",
            "ONE\nmiddle\nthree\n",
            "one\nmiddle\nTHREE\n",
            "ONE\nmiddle\nTHREE\n",
            "OTHER\nmiddle\nTHREE\n",
        ] {
            let file = backend
                .write_file(
                    RepoPath::root(),
                    &mut futures::io::Cursor::new(contents.as_bytes()),
                )
                .await
                .unwrap();
            file_trees.push(
                backend
                    .write_tree(
                        RepoPath::root(),
                        &Tree::from_sorted_entries(vec![(
                            RepoPathComponentBuf::new("file").unwrap(),
                            TreeValue::File {
                                id: file,
                                executable: false,
                                copy_id: jj_lib::backend::CopyId::placeholder(),
                            },
                        )]),
                    )
                    .await
                    .unwrap(),
            );
        }
        let mut file_commits: Vec<Commit> = Vec::new();
        struct FileCase {
            name: &'static str,
            tree: usize,
            parents: Vec<usize>,
            conflicted: bool,
        }
        for case in [
            FileCase {
                name: "shared base",
                tree: 0,
                parents: vec![],
                conflicted: false,
            },
            FileCase {
                name: "left line edit",
                tree: 1,
                parents: vec![0],
                conflicted: false,
            },
            FileCase {
                name: "right line edit",
                tree: 2,
                parents: vec![0],
                conflicted: false,
            },
            FileCase {
                name: "equal automatic parent merge",
                tree: 3,
                parents: vec![1, 2],
                conflicted: true,
            },
            FileCase {
                name: "changed automatic parent merge",
                tree: 4,
                parents: vec![1, 2],
                conflicted: true,
            },
        ] {
            let mut value = root.store_commit().as_ref().clone();
            value.parents = if case.parents.is_empty() {
                vec![root.id().clone()]
            } else {
                case.parents
                    .iter()
                    .map(|n| file_commits[*n].id().clone())
                    .collect()
            };
            value.change_id = ChangeId::from_bytes(&[41 + case.tree as u8; 16]);
            value.description = case.name.into();
            value.root_tree = if !case.conflicted {
                Merge::resolved(file_trees[case.tree].clone())
            } else {
                Merge::from_vec(vec![
                    file_trees[case.tree].clone(),
                    jj_store.empty_tree_id().clone(),
                    jj_store.empty_tree_id().clone(),
                ])
            };
            let commit = jj_store.write_commit(value, None).await.unwrap();
            mutable.add_commit(&commit).await.unwrap();
            file_commits.push(commit);
        }
        let readonly = index_store.persist_index(mutable).unwrap();
        index_store
            .associate_operation(&[71; 64], &readonly)
            .unwrap();
        for file_first in [false, true] {
            let conflict_predicate = P::Filter(F::HasConflict);
            let file = P::Filter(F::File(FilesetExpression::all()));
            let (a, b) = if file_first {
                (file, conflict_predicate)
            } else {
                (conflict_predicate, file)
            };
            let expression = ResolvedExpression::FilterWithin {
                candidates: Box::new(ResolvedExpression::Commits(vec![
                    root.id().clone(),
                    conflict.id().clone(),
                    empty.id().clone(),
                    empty_merge.id().clone(),
                ])),
                predicate: P::Intersection(Box::new(a), Box::new(b)),
            };
            let revset = readonly.evaluate_revset(&expression, &jj_store).unwrap();
            assert!(!revset.is_empty().unwrap());
            assert_eq!(
                revset.stream().try_collect::<Vec<_>>().await.unwrap(),
                vec![conflict.id().clone()]
            );
            assert!(revset.containing_fn()(conflict.id()).await.unwrap());
            assert!(!revset.containing_fn()(empty.id()).await.unwrap());
            assert!(!revset.containing_fn()(empty_merge.id()).await.unwrap());
            assert!(!revset.containing_fn()(root.id()).await.unwrap());
            assert!(
                revset.stream_graph().try_collect::<Vec<_>>().await.is_err(),
                "selected file filtering must not return pre-filter graph edges"
            );
            let only_empty = ResolvedExpression::FilterWithin {
                candidates: Box::new(ResolvedExpression::Commits(vec![empty.id().clone()])),
                predicate: if let ResolvedExpression::FilterWithin { predicate, .. } = expression {
                    predicate
                } else {
                    unreachable!()
                },
            };
            assert!(
                readonly
                    .evaluate_revset(&only_empty, &jj_store)
                    .unwrap()
                    .is_empty()
                    .unwrap()
            );
        }
        let observed = Arc::new(CountingObjects {
            inner: objects.clone(),
            reads: Mutex::new(Vec::new()),
            files: AtomicUsize::new(0),
            file_writes: AtomicUsize::new(0),
            reply_owner_probe: Some(io.clone()),
            pause_metadata: AtomicBool::new(false),
            metadata_waiters: Mutex::new(Vec::new()),
        });
        let observed_store = NativeIndexStore::open(
            &directory.path().join("index"),
            &config,
            Limits::default(),
            io.clone(),
            observed.clone(),
        )
        .unwrap();
        let observed_index = observed_store.index_at_operation(&[71; 64]).unwrap();
        let regular = ResolvedExpression::FilterWithin {
            candidates: Box::new(ResolvedExpression::Commits(vec![
                file_commits[3].id().clone(),
                file_commits[4].id().clone(),
            ])),
            predicate: P::Intersection(
                Box::new(P::Filter(F::HasConflict)),
                Box::new(P::Filter(F::File(FilesetExpression::all()))),
            ),
        };
        let regular_revset = observed_index.evaluate_revset(&regular, &jj_store).unwrap();
        assert_eq!(
            regular_revset
                .stream()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![file_commits[4].id().clone()]
        );
        assert!(
            !regular_revset.containing_fn()(file_commits[3].id())
                .await
                .unwrap()
        );
        assert!(
            regular_revset.containing_fn()(file_commits[4].id())
                .await
                .unwrap()
        );
        assert!(
            observed.files.load(Ordering::SeqCst) > 0,
            "actual file contents must be resolved, not merely compared by tree metadata"
        );
        assert_eq!(
            observed.file_writes.load(Ordering::SeqCst),
            0,
            "synthesized merge files are private scratch, never authoritative provider writes"
        );
        assert!(
            io.reply_owners
                .lock()
                .unwrap()
                .iter()
                .all(|owner| owner.upgrade().is_none()),
            "completed selected work releases independent canonical reply owners after cache teardown"
        );
        let filtered = ResolvedExpression::FilterWithin {
            candidates: Box::new(ResolvedExpression::Commits(vec![conflict.id().clone()])),
            predicate: P::Intersection(
                Box::new(P::Filter(F::HasConflict)),
                Box::new(P::Filter(F::File(FilesetExpression::all()))),
            ),
        };
        let paused = Arc::new(CountingObjects {
            inner: objects.clone(),
            reads: Mutex::new(Vec::new()),
            files: AtomicUsize::new(0),
            file_writes: AtomicUsize::new(0),
            reply_owner_probe: None,
            pause_metadata: AtomicBool::new(false),
            metadata_waiters: Mutex::new(Vec::new()),
        });
        let mut concurrent_limits = Limits::default();
        concurrent_limits.sync_query_timeout_ms = 5000;
        let concurrent_store = NativeIndexStore::open(
            &directory.path().join("index"),
            &config,
            concurrent_limits,
            io.clone(),
            paused.clone(),
        )
        .unwrap();
        let concurrent_index = concurrent_store.index_at_operation(&[71; 64]).unwrap();
        assert_eq!(
            concurrent_index
                .evaluate_revset(
                    &ResolvedExpression::Commits(vec![conflict.id().clone()]),
                    &jj_store
                )
                .unwrap()
                .stream()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![conflict.id().clone()],
            "warm the actual scoped query path, not local has_id's exact-membership shortcut"
        );
        paused.pause_metadata.store(true, Ordering::SeqCst);
        // All four outer sync workers must be able to await distinct selected
        // workers. Sharing their blocking pool would starve every inner job.
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..4 {
                let index = concurrent_index.clone();
                let store = jj_store.clone();
                let expression = filtered.clone();
                workers.push(scope.spawn(move || {
                    index
                        .evaluate_revset(&expression, &store)
                        .unwrap()
                        .is_empty()
                }));
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while paused.metadata_waiters.lock().unwrap().len() < 4
                && std::time::Instant::now() < deadline
            {
                std::thread::yield_now();
            }
            let reached = paused.metadata_waiters.lock().unwrap().len();
            paused.resume_metadata();
            let results: Vec<_> = workers
                .into_iter()
                .map(|worker| worker.join().unwrap())
                .collect();
            assert!(
                reached == 4 && results.iter().all(|result| matches!(result, Ok(false))),
                "four selected jobs must run and complete while sync workers are occupied: reached={reached}, results={results:?}"
            );
        });
        let mut short = Limits::default();
        short.sync_query_timeout_ms = 50;
        let limited_store = NativeIndexStore::open(
            &directory.path().join("index"),
            &config,
            short,
            io,
            paused.clone(),
        )
        .unwrap();
        let limited = limited_store.index_at_operation(&[71; 64]).unwrap();
        assert_eq!(
            limited
                .evaluate_revset(
                    &ResolvedExpression::Commits(vec![conflict.id().clone()]),
                    &jj_store
                )
                .unwrap()
                .stream()
                .try_collect::<Vec<_>>()
                .await
                .unwrap(),
            vec![conflict.id().clone()]
        );
        let revset = limited.evaluate_revset(&filtered, &jj_store).unwrap();
        paused.pause_metadata.store(true, Ordering::SeqCst);
        // Cancellation drops only the waiter, not its in-progress selected job.
        let cancellation_revset = concurrent_index
            .evaluate_revset(&filtered, &jj_store)
            .unwrap();
        let mut canceled = cancellation_revset.stream();
        let waiting = canceled.next();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let registered = futures::future::poll_fn(|cx| {
            if !paused.metadata_waiters.lock().unwrap().is_empty() {
                return std::task::Poll::Ready(());
            }
            assert!(
                std::time::Instant::now() < deadline,
                "selected read never started"
            );
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        });
        match futures::future::select(waiting, registered).await {
            futures::future::Either::Right(((), waiting)) => drop(waiting),
            futures::future::Either::Left(_) => {
                panic!("paused selected stream completed before cancellation")
            }
        }
        drop(canceled);
        for _ in 0..3 {
            let error = revset.stream().try_collect::<Vec<_>>().await.unwrap_err();
            let RevsetEvaluationError::Other(source) = error else {
                panic!("wrong selected timeout category")
            };
            assert!(matches!(
                source.downcast_ref::<Error>(),
                Some(Error::Query(tonic::Code::DeadlineExceeded))
            ));
        }
        let error = revset.stream().try_collect::<Vec<_>>().await.unwrap_err();
        let RevsetEvaluationError::Other(source) = error else {
            panic!("wrong selected admission category")
        };
        assert!(
            matches!(source.downcast_ref::<Error>(), Some(Error::Limit)),
            "timed-out physical selected jobs retain all four permits"
        );
        paused.resume_metadata();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match revset.stream().try_collect::<Vec<_>>().await {
                Ok(rows) => {
                    assert_eq!(rows, vec![conflict.id().clone()]);
                    break;
                }
                Err(_) if std::time::Instant::now() < deadline => std::thread::yield_now(),
                Err(e) => panic!("selected workers did not recover: {e}"),
            }
        }
    });
}

#[test]
fn view_only_operations_reuse_segments_without_losing_old_associations() {
    let (directory, _object_directory, store, _io, _objects, _config) = fixture();
    let mut index = store.seed_index().unwrap();
    for operation in 1..=1024 {
        index = store.persist_index(index.start_modification()).unwrap();
        store
            .associate_operation(&lifecycle_bench_operation(operation), &index)
            .unwrap();
    }
    let path = directory.path().join("index");
    assert_eq!(std::fs::read_dir(path.join("segments")).unwrap().count(), 1);
    assert_eq!(
        std::fs::read_dir(path.join("operations")).unwrap().count(),
        1024
    );
    for operation in [1, 512, 1024] {
        let index = store
            .index_at_operation(&lifecycle_bench_operation(operation))
            .unwrap();
        assert!(block_on(index.has_id(&CommitId::from_bytes(&[0; 32]))).unwrap());
    }
    assert!(matches!(
        store.index_at_operation(&lifecycle_bench_operation(1025)),
        Err(Error::MissingOperation)
    ));
}

#[test]
fn changed_checkpoints_flatten_preserving_old_branches_and_bounded_shards() {
    let _sync_test = SYNC_TEST.lock().unwrap();
    block_on(async {
        for (name, count, limits) in [
            ("long_local_chain", 1024usize, Limits::default()),
            (
                "small_parentless_shards",
                12usize,
                Limits {
                    max_segment_bytes: 512,
                    max_depth: 2,
                    ..Limits::default()
                },
            ),
        ] {
            let (directory, _objects_directory, initial, io, objects, config) = fixture();
            drop(initial);
            let counted = Arc::new(CountingObjects {
                inner: objects,
                reads: Mutex::new(vec![]),
                files: AtomicUsize::new(0),
                file_writes: AtomicUsize::new(0),
                reply_owner_probe: None,
                pause_metadata: AtomicBool::new(false),
                metadata_waiters: Mutex::new(Vec::new()),
            });
            let path = directory.path().join("index");
            let store = NativeIndexStore::open(&path, &config, limits, io.clone(), counted.clone())
                .unwrap();
            let backend =
                jj_native_backend::NativeBackend::from_io(counted.clone(), Default::default())
                    .await
                    .unwrap();
            let jj_store = Store::new(
                Box::new(backend),
                jj_lib::signing::Signer::new(None, vec![]),
                jj_lib::tree_merge::MergeOptions {
                    hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
                    same_change: jj_lib::merge::SameChange::Accept,
                },
            );
            let mut index = store.seed_index().unwrap();
            let root = jj_store.root_commit();
            let mut parent = root.clone();
            let mut commits = Vec::new();
            for number in 1..=count {
                let mut value = root.store_commit().as_ref().clone();
                value.parents = vec![parent.id().clone()];
                value.change_id = ChangeId::from_bytes(&[9; 16]);
                value.description = format!("lifecycle {name} {number}");
                let commit = jj_store.write_commit(value, None).await.unwrap();
                let mut mutable = index.start_modification();
                mutable.add_commit(&commit).await.unwrap();
                let before = counted.reads.lock().unwrap().len();
                index = store.persist_index(mutable).unwrap();
                store
                    .associate_operation(&lifecycle_bench_operation(number), &index)
                    .unwrap();
                assert_eq!(
                    counted.reads.lock().unwrap().len(),
                    before,
                    "persistence must not hydrate canonical objects"
                );
                parent = commit.clone();
                commits.push(commit);
            }
            for number in [1, count / 2, count] {
                let prior = store
                    .index_at_operation(&lifecycle_bench_operation(number))
                    .unwrap();
                assert!(prior.has_id(commits[number - 1].id()).await.unwrap());
                assert!(
                    !prior
                        .has_id(&CommitId::from_bytes(&[99; 32]))
                        .await
                        .unwrap()
                );
            }
            // Branch from a historical checkpoint, then merge into the latest.
            let branch_base = store
                .index_at_operation(&lifecycle_bench_operation(count / 2))
                .unwrap();
            let old_association = std::fs::read(
                path.join("operations").join(
                    lifecycle_bench_operation(count / 2)
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>(),
                ),
            )
            .unwrap();
            let mut value = root.store_commit().as_ref().clone();
            value.parents = vec![commits[count / 2 - 1].id().clone()];
            value.change_id = ChangeId::from_bytes(&[9; 16]);
            value.description = "hidden divergent historical branch".into();
            let hidden = jj_store.write_commit(value, None).await.unwrap();
            let mut branch = branch_base.start_modification();
            branch.add_commit(&hidden).await.unwrap();
            let branch = store.persist_index(branch).unwrap();
            let mut merged = index.start_modification();
            merged.merge_in(&branch).unwrap();
            let merged = store.persist_index(merged).unwrap();
            assert!(merged.has_id(hidden.id()).await.unwrap());
            assert!(merged.has_id(commits.last().unwrap().id()).await.unwrap());
            assert_eq!(
                std::fs::read(
                    path.join("operations").join(
                        lifecycle_bench_operation(count / 2)
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>()
                    )
                )
                .unwrap(),
                old_association
            );
            assert_eq!(counted.files.load(Ordering::SeqCst), 0);
            // Promote an ancestor after compaction without discarding the
            // original canonical draft record or the hidden historical branch.
            let promoted = model_id(commits[0].id()).unwrap();
            let metadata = counted
                .read_metadata(m::ObjectKey {
                    kind: m::Kind::Commit,
                    id: m::ObjectId(promoted.0),
                })
                .await
                .unwrap();
            let m::Object::Commit(public) = metadata.into_object() else {
                panic!()
            };
            io.remote.lock().unwrap().push(public);
            io.admitted.lock().unwrap().insert(promoted);
            let promoted_index = merged.with_remote_roots(vec![promoted]).await.unwrap();
            assert!(promoted_index.has_id(hidden.id()).await.unwrap());
            assert!(
                promoted_index
                    .has_id(commits.last().unwrap().id())
                    .await
                    .unwrap()
            );
            assert!(
                !promoted_index
                    .has_id(&CommitId::from_bytes(&[98; 32]))
                    .await
                    .unwrap()
            );
            assert!(
                !io.fork_overlays
                    .lock()
                    .unwrap()
                    .last()
                    .unwrap()
                    .contains(&promoted)
            );
            assert_eq!(counted.files.load(Ordering::SeqCst), 0);
            if count > 512 {
                // A remote root lets a fresh branch assign order1 to a child
                // of a long historical local chain. Merge the old records back;
                // this forces a real deep draft DFS, unlike parent-first append.
                for commit in commits.iter().skip(1) {
                    let metadata = counted
                        .read_metadata(m::ObjectKey {
                            kind: m::Kind::Commit,
                            id: m::ObjectId(model_id(commit.id()).unwrap().0),
                        })
                        .await
                        .unwrap();
                    let m::Object::Commit(value) = metadata.into_object() else {
                        panic!()
                    };
                    io.remote.lock().unwrap().push(value);
                }
                let remote_tip = model_id(commits.last().unwrap().id()).unwrap();
                io.admitted.lock().unwrap().insert(remote_tip);
                let fresh = store
                    .seed_index()
                    .unwrap()
                    .with_remote_roots(vec![remote_tip])
                    .await
                    .unwrap();
                let mut chosen = None;
                for attempt in 0..64 {
                    let mut value = root.store_commit().as_ref().clone();
                    value.parents = vec![commits.last().unwrap().id().clone()];
                    value.description = format!("low ordinal child {attempt}");
                    let child = jj_store.write_commit(value, None).await.unwrap();
                    if child.id().as_bytes() < commits[0].id().as_bytes() {
                        chosen = Some(child);
                        break;
                    }
                }
                let child = chosen
                    .expect("bounded deterministic fixture child must sort before first draft");
                let mut branch = fresh.start_modification();
                branch.add_commit(&child).await.unwrap();
                branch.merge_in(&merged).unwrap();
                let deep = store.persist_index(branch).unwrap();
                assert!(deep.has_id(child.id()).await.unwrap());
                assert!(deep.has_id(hidden.id()).await.unwrap());
                assert!(deep.has_id(commits.last().unwrap().id()).await.unwrap());
            }
            // A destination missing an authoritative parent cannot use cached
            // index records to conceal corruption by publishing a new snapshot.
            let parent_tip = merged.checkpoint.tips[0];
            let segment = path.join("segments").join(
                parent_tip
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
            );
            std::fs::remove_file(segment).unwrap();
            assert!(store.persist_index(merged.start_modification()).is_err());
        }
    });
}

#[test]
fn rejected_snapshot_plans_leave_existing_operation_associations_unchanged() {
    let _sync_test = SYNC_TEST.lock().unwrap();
    block_on(async {
        for (name, limits) in [
            (
                "indivisible_record",
                Limits {
                    max_segment_bytes: 128,
                    max_depth: 2,
                    ..Limits::default()
                },
            ),
            (
                "complete_encoded_bytes",
                Limits {
                    max_segment_bytes: 256,
                    max_checkpoint_bytes: 512,
                    max_depth: 2,
                    ..Limits::default()
                },
            ),
            (
                "shard_count",
                Limits {
                    max_segment_bytes: 512,
                    max_segments: 1,
                    max_depth: 2,
                    ..Limits::default()
                },
            ),
        ] {
            let (directory, _objects_directory, initial, io, objects, config) = fixture();
            drop(initial);
            let path = directory.path().join("index");
            let wide = NativeIndexStore::open(
                &path,
                &config,
                Limits::default(),
                io.clone(),
                objects.clone(),
            )
            .unwrap();
            let store =
                NativeIndexStore::open(&path, &config, limits, io, objects.clone()).unwrap();
            let backend = jj_native_backend::NativeBackend::from_io(objects, Default::default())
                .await
                .unwrap();
            let jj_store = Store::new(
                Box::new(backend),
                jj_lib::signing::Signer::new(None, vec![]),
                jj_lib::tree_merge::MergeOptions {
                    hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
                    same_change: jj_lib::merge::SameChange::Accept,
                },
            );
            let root = jj_store.root_commit();
            let mut parent = root.clone();
            let mut index = store.seed_index().unwrap();
            store.associate_operation(&[0; 64], &index).unwrap();
            let seed_path = path.join("operations").join("0".repeat(128));
            let original = std::fs::read(&seed_path).unwrap();
            let mut rejected = false;
            for number in 1..=12 {
                let mut value = root.store_commit().as_ref().clone();
                value.parents = vec![parent.id().clone()];
                value.description = format!("bounded {name} {number}");
                let commit = jj_store.write_commit(value, None).await.unwrap();
                // Same repository identity, independently configured read
                // context: exercise persistence limits, not overlay RPC limits.
                let source = wide
                    .index_at_operation(&lifecycle_bench_operation(number - 1))
                    .unwrap();
                let mut mutable = source.start_modification();
                mutable.add_commit(&commit).await.unwrap();
                match store.persist_index(mutable) {
                    Ok(next) => {
                        store
                            .associate_operation(&lifecycle_bench_operation(number), &next)
                            .unwrap();
                        index = next;
                        parent = commit;
                    }
                    Err(Error::Limit) => {
                        rejected = true;
                        assert!(matches!(
                            store.index_at_operation(&lifecycle_bench_operation(number)),
                            Err(Error::MissingOperation)
                        ));
                        break;
                    }
                    Err(error) => panic!("unexpected snapshot failure for {name}: {error}"),
                }
            }
            assert!(rejected, "{name} must reject rather than enlarge limits");
            assert_eq!(std::fs::read(seed_path).unwrap(), original);
            store.index_at_operation(&[0; 64]).unwrap();
            store.persist_index(index.start_modification()).unwrap();
        }
    });
}

fn lifecycle_bench_operation(n: usize) -> [u8; 64] {
    let mut id = [0; 64];
    id[..8].copy_from_slice(&(n as u64).to_le_bytes());
    id
}

#[test]
fn immutable_fact_memo_is_optional_bounded_and_never_remembers_errors() {
    block_on(async {
        let (directory, _objects_directory, _store, io, objects, config) = fixture();
        struct Case {
            name: &'static str,
            bytes: usize,
            entries: usize,
            repeat_calls: usize,
        }
        for case in [
            Case {
                name: "disabled bytes",
                bytes: 0,
                entries: 8,
                repeat_calls: 2,
            },
            Case {
                name: "disabled entries",
                bytes: 8192,
                entries: 0,
                repeat_calls: 2,
            },
            Case {
                name: "single live entry",
                bytes: 8192,
                entries: 1,
                repeat_calls: 1,
            },
            Case {
                name: "too small to retain",
                bytes: 128,
                entries: 8,
                repeat_calls: 2,
            },
        ] {
            let limits = Limits {
                fact_cache_bytes: case.bytes,
                fact_cache_entries: case.entries,
                ..Limits::default()
            };
            let store = NativeIndexStore::open(
                &directory.path().join("index"),
                &config,
                limits,
                io.clone(),
                objects.clone(),
            )
            .unwrap();
            let index = store.seed_index().unwrap();
            let a = CommitId::from_bytes(&[71; 32]);
            let b = CommitId::from_bytes(&[72; 32]);
            io.fail_fact.store(true, Ordering::SeqCst);
            assert!(index.has_id(&a).await.is_err(), "{}", case.name);
            let before = io.facts_count.load(Ordering::SeqCst);
            assert!(!index.has_id(&a).await.unwrap());
            assert!(!index.has_id(&a).await.unwrap());
            assert_eq!(
                io.facts_count.load(Ordering::SeqCst) - before,
                case.repeat_calls,
                "{}",
                case.name
            );
            assert!(!index.has_id(&b).await.unwrap());
            let before = io.facts_count.load(Ordering::SeqCst);
            assert!(!index.has_id(&a).await.unwrap());
            assert_eq!(
                io.facts_count.load(Ordering::SeqCst),
                before + 1,
                "{}: eviction preserves exact negatives",
                case.name
            );
        }
    });
}

#[derive(Default)]
struct PortableArtifactSink {
    values: Vec<tinytree_cloud::VerifiedIndexArtifact>,
    emitted: BTreeSet<tinytree_cloud::IndexArtifactId>,
}
#[async_trait]
impl IndexArtifactSink for PortableArtifactSink {
    async fn write(&mut self, artifact: ExportArtifact) -> Result<(), Error> {
        assert!(
            self.emitted.insert(artifact.descriptor().id),
            "shared artifacts are emitted once"
        );
        self.values.push(
            tinytree_cloud::decode_index_artifact(
                artifact.bytes(),
                artifact.descriptor(),
                Default::default(),
            )
            .unwrap(),
        );
        Ok(())
    }
}
#[test]
fn portable_export_preserves_exact_historical_indexes_and_never_invents_root_operation() {
    block_on(async {
        let (_directory, _objects_directory, store, io, objects, _config) = fixture();
        let mut sink = PortableArtifactSink::default();
        assert!(matches!(
            store
                .export_operation_indexes(&[[0; 64]], Default::default(), &mut sink, Arc::new(()))
                .await,
            Err(Error::MissingOperation)
        ));
        assert!(sink.values.is_empty());
        let seed = store.seed_index().unwrap();
        store.associate_operation(&[0; 64], &seed).unwrap();
        let backend =
            jj_native_backend::NativeBackend::from_io(objects.clone(), Default::default())
                .await
                .unwrap();
        let jj_store = Store::new(
            Box::new(backend),
            jj_lib::signing::Signer::new(None, vec![]),
            jj_lib::tree_merge::MergeOptions {
                hunk_level: jj_lib::files::FileMergeHunkLevel::Line,
                same_change: jj_lib::merge::SameChange::Accept,
            },
        );
        let root = jj_store.root_commit();
        let mut branches = Vec::new();
        let mut commits = Vec::new();
        for description in ["initial working-copy draft", "hidden divergent draft"] {
            let mut value = root.store_commit().as_ref().clone();
            value.parents = vec![root.id().clone()];
            value.change_id = ChangeId::from_bytes(&[19; 16]);
            value.description = description.into();
            let commit = jj_store.write_commit(value, None).await.unwrap();
            let mut mutable = seed.start_modification();
            mutable.add_commit(&commit).await.unwrap();
            branches.push(store.persist_index(mutable).unwrap());
            commits.push(commit);
        }
        store.associate_operation(&[1; 64], &branches[0]).unwrap();
        let mut merged = branches[0].start_modification();
        merged.merge_in(&branches[1]).unwrap();
        let merged = store.persist_index(merged).unwrap();
        store.associate_operation(&[2; 64], &merged).unwrap();
        store.associate_operation(&[3; 64], &merged).unwrap();
        // Promotion changes only anchors/overlay classification, not preserved
        // local records. It must not discard a published draft during export.
        let verified = objects
            .read_metadata(m::ObjectKey {
                kind: m::Kind::Commit,
                id: m::ObjectId(model_id(commits[0].id()).unwrap().0),
            })
            .await
            .unwrap();
        let m::Object::Commit(published) = verified.into_object() else {
            panic!()
        };
        let published_id = model_commit_id(&published);
        io.remote.lock().unwrap().push(published);
        io.admitted.lock().unwrap().insert(published_id);
        let promoted = merged.with_remote_roots(vec![published_id]).await.unwrap();
        store.associate_operation(&[4; 64], &promoted).unwrap();
        let before_opens = io.opens.load(Ordering::SeqCst);
        let before_facts = io.facts_count.load(Ordering::SeqCst);
        let result = store
            .export_operation_indexes(
                &[[0; 64], [1; 64], [2; 64], [3; 64], [4; 64]],
                Default::default(),
                &mut sink,
                Arc::new(()),
            )
            .await
            .unwrap();
        assert_eq!(
            io.opens.load(Ordering::SeqCst),
            before_opens,
            "export does not create remote scopes"
        );
        assert_eq!(io.facts_count.load(Ordering::SeqCst), before_facts);
        assert_eq!(result.operation_indexes().len(), 5);
        assert_eq!(
            result.operation_indexes()[2].root,
            result.operation_indexes()[3].root
        );
        assert_eq!(result.work().unique_artifacts, sink.values.len());
        assert!(result.work().source_reads >= 10);
        let graph = tinytree_cloud::verify_index_artifact_graph(
            result.index_roots(),
            &sink.values,
            Default::default(),
        )
        .unwrap();
        let snapshot = |n: usize| graph.snapshot(result.operation_indexes()[n].root).unwrap();
        assert_eq!(snapshot(0).roots, vec![m::CommitId::ROOT]);
        assert!(snapshot(0).drafts.is_empty());
        assert_eq!(snapshot(1).drafts.len(), 1);
        let all = snapshot(2);
        assert_eq!(all.drafts.len(), 2);
        assert!(
            all.drafts.iter().all(|d| d.order == 1),
            "independent branches preserve repeated original ordinals"
        );
        for commit in &commits {
            let record = all
                .drafts
                .iter()
                .find(|d| d.commit() == model_id(commit.id()).unwrap())
                .unwrap();
            assert_eq!(record.change, m::ChangeId([19; 16]));
            assert_eq!(record.parents, vec![m::CommitId::ROOT]);
        }
        let after = snapshot(4);
        assert!(after.roots.contains(&published_id));
        assert_eq!(
            after.drafts, all.drafts,
            "hidden/promoted records survive exactly"
        );
        assert_eq!(result.artifacts().len(), sink.emitted.len());
        drop(graph);
        drop(result);
        drop(sink);
        struct HoldSink(Vec<ExportArtifact>);
        #[async_trait]
        impl IndexArtifactSink for HoldSink {
            async fn write(&mut self, value: ExportArtifact) -> Result<(), Error> {
                self.0.push(value);
                Ok(())
            }
        }
        let mut held = HoldSink(vec![]);
        let artifact_owner: QueryOwner = Arc::new(CanonicalReplyOwner);
        let owner_probe = Arc::downgrade(&artifact_owner);
        assert!(
            matches!(
                store
                    .export_operation_indexes(
                        &[[0; 64], [1; 64]],
                        Default::default(),
                        &mut held,
                        artifact_owner
                    )
                    .await,
                Err(Error::Limit)
            ),
            "retained artifacts consume the same peak quota; never wait for self-release"
        );
        assert!(!held.0.is_empty());
        assert!(
            owner_probe.upgrade().is_some(),
            "escaped artifact retains caller guard after export error"
        );
        drop(held);
        assert!(owner_probe.upgrade().is_none());
        for limits in [
            PortableExportLimits {
                max_source_reads: 1,
                ..Default::default()
            },
            PortableExportLimits {
                max_source_bytes: 1,
                ..Default::default()
            },
            PortableExportLimits {
                max_work_units: 1,
                ..Default::default()
            },
            PortableExportLimits {
                max_working_bytes: 1024,
                ..Default::default()
            },
        ] {
            let mut sink = PortableArtifactSink::default();
            assert!(matches!(
                store
                    .export_operation_indexes(&[[0; 64]], limits, &mut sink, Arc::new(()))
                    .await,
                Err(Error::Limit)
            ));
            assert!(sink.values.is_empty());
        }
        let mut sink = PortableArtifactSink::default();
        assert!(matches!(
            store
                .export_operation_indexes(
                    &[[0; 64], [0; 64]],
                    Default::default(),
                    &mut sink,
                    Arc::new(())
                )
                .await,
            Err(Error::Invalid)
        ));
        assert!(sink.values.is_empty());
        let retried = store
            .export_operation_indexes(
                &[[0; 64], [1; 64]],
                Default::default(),
                &mut sink,
                Arc::new(()),
            )
            .await
            .unwrap();
        assert_eq!(retried.operation_indexes().len(), 2);
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_portable_export_keeps_accepted_sink_artifact_owned_until_physical_teardown() {
    struct PhysicalSink {
        entered: Option<tokio::sync::oneshot::Sender<()>>,
        release: Option<tokio::sync::oneshot::Receiver<()>>,
        completed: Option<tokio::sync::oneshot::Sender<usize>>,
    }
    #[async_trait]
    impl IndexArtifactSink for PhysicalSink {
        async fn write(&mut self, artifact: ExportArtifact) -> Result<(), Error> {
            let entered = self.entered.take().unwrap();
            let release = self.release.take().unwrap();
            let completed = self.completed.take().unwrap();
            // Model an admitted physical sink whose lifetime is independent of
            // the caller's export future. The ENTIRE opaque artifact is moved,
            // not just the source guard or a borrowed/copied payload.
            tokio::spawn(async move {
                let _ = entered.send(());
                let _ = release.await;
                let bytes = artifact.bytes().len();
                assert!(bytes > 0);
                drop(artifact);
                let _ = completed.send(bytes);
            });
            futures::future::pending::<Result<(), Error>>().await
        }
    }
    let (_directory, _objects_directory, store, _io, _objects, _config) = fixture();
    let seed = store.seed_index().unwrap();
    store.associate_operation(&[0; 64], &seed).unwrap();
    let owner: QueryOwner = Arc::new(CanonicalReplyOwner);
    let probe = Arc::downgrade(&owner);
    let (entered, accepted) = tokio::sync::oneshot::channel();
    let (release, blocked) = tokio::sync::oneshot::channel();
    let (completed, physical_done) = tokio::sync::oneshot::channel();
    let exporting = store.clone();
    let caller = tokio::spawn(async move {
        let mut sink = PhysicalSink {
            entered: Some(entered),
            release: Some(blocked),
            completed: Some(completed),
        };
        exporting
            .export_operation_indexes(&[[0; 64]], Default::default(), &mut sink, owner)
            .await
            .map(|_| ())
    });
    tokio::time::timeout(Duration::from_secs(5), accepted)
        .await
        .unwrap()
        .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(
        probe.upgrade().is_some(),
        "accepted physical artifact must retain source ownership after caller cancellation"
    );
    release.send(()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), physical_done)
            .await
            .unwrap()
            .unwrap()
            > 0
    );
    assert!(
        probe.upgrade().is_none(),
        "physical payload teardown releases its last source/quota owner"
    );
    let mut sink = PortableArtifactSink::default();
    let retry = store
        .export_operation_indexes(&[[0; 64]], Default::default(), &mut sink, Arc::new(()))
        .await
        .unwrap();
    assert_eq!(retry.operation_indexes().len(), 1);
    assert_eq!(retry.artifacts().len(), sink.values.len());
}
