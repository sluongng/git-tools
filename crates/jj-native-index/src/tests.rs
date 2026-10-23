//! Public adapter/ledger tests with an executor-independent, small reference
//! QueryIo fixture. These are not broker/server or native CLI acceptance.
use super::*;
use futures::{TryStreamExt as _, executor::block_on};
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
    next: AtomicUsize,
    closes: AtomicUsize,
    pause_pages: AtomicBool,
    page_waiters: Mutex<Vec<futures::channel::oneshot::Sender<()>>>,
}
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
            let mut out = selected(v.heads.as_deref().unwrap(), nodes);
            loop {
                let prior = out.clone();
                for node in nodes {
                    let encoded =
                        m::encode(&m::Object::Commit(node.clone()), Default::default()).unwrap();
                    if out.contains(&m::CommitId(encoded.descriptor.key.id.0)) {
                        out.extend(node.parents.iter().copied());
                    }
                }
                if prior == out {
                    return out;
                }
            }
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
        assert_eq!(i.roots, vec![m::CommitId::ROOT]);
        let mut nodes = Vec::new();
        for bytes in i.overlay {
            let m::Object::Commit(commit) = m::decode(&bytes, Default::default()).unwrap() else {
                panic!()
            };
            nodes.push(commit)
        }
        let mut handle = [0; 32];
        handle[..8]
            .copy_from_slice(&(self.next.fetch_add(1, Ordering::SeqCst) as u64 + 1).to_le_bytes());
        self.scopes.lock().unwrap().insert(handle, nodes);
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(reply(
            QueryScope {
                handle,
                expires_at_unix_seconds: now().unwrap() + 300,
            },
            o,
        ))
    }
    async fn fork_scope(
        &self,
        _: ForkQueryScope,
        _: QueryOwner,
    ) -> Result<QueryReply<QueryScope>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
    }
    async fn facts(
        &self,
        s: [u8; 32],
        facts: Vec<q::GraphFact>,
        o: QueryOwner,
    ) -> Result<QueryReply<q::GraphFacts>, tonic::Status> {
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
        _: [u8; 32],
        _: String,
        _: Vec<m::CommitId>,
        _: QueryOwner,
    ) -> Result<QueryReply<q::ChangePrefixResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
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
                q::graph_expr::Expression::Intersection(v) => pending.extend(v.inputs.iter()),
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
        _: [u8; 32],
        _: m::CommitId,
        _: QueryOwner,
    ) -> Result<QueryReply<jj_native_backend::VerifiedMetadata>, tonic::Status> {
        Err(tonic::Status::unimplemented("fixture"))
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
        assert!(contains(commits[0].id()).await.unwrap());
        assert!(contains(root.id()).await.unwrap());
        assert!(io.opens.load(Ordering::SeqCst) >= 2);
        drop(contains);
        drop(revset);
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
