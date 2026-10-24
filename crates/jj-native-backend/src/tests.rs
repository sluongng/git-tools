use super::*;
use futures::{AsyncReadExt, TryStreamExt, executor::block_on, io::Cursor};
use jj_lib::{
    backend::{MillisSinceEpoch, Signature, Timestamp},
    conflict_labels::ConflictLabels,
    merge::Merge,
};

fn path(value: &str) -> RepoPathBuf {
    RepoPathBuf::from_internal_string(value).unwrap()
}
fn tree_entry(name: &str, value: TreeValue) -> (RepoPathComponentBuf, TreeValue) {
    (RepoPathComponentBuf::new(name).unwrap(), value)
}
fn commit(backend: &NativeBackend, parent: CommitId, tree: TreeId) -> Commit {
    let author = Signature {
        name: "Native Author".into(),
        email: "author@example.invalid".into(),
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(-123456),
            tz_offset: 90,
        },
    };
    Commit {
        parents: vec![parent],
        predecessors: vec![],
        root_tree: Merge::resolved(tree),
        conflict_labels: ConflictLabels::unlabeled().into_merge(),
        change_id: ChangeId::from_bytes(&[7; 16]),
        description: "native fixture\n".into(),
        author: author.clone(),
        committer: Signature {
            timestamp: Timestamp {
                timestamp: MillisSinceEpoch(234567),
                tz_offset: -90,
            },
            ..author
        },
        secure_sig: None,
    }
}
#[test]
fn actual_backend_files_trees_signatures_copy_and_restart_without_git() {
    let directory = tempfile::tempdir().unwrap();
    let config = Config::default();
    let (file, tree, copy, commit_id, expected) = block_on(async {
        let backend = NativeBackend::init(directory.path(), config.clone()).unwrap();
        assert_eq!(backend.commit_id_length(), 32);
        assert_eq!(backend.change_id_length(), 16);
        let bytes = (0..2 * 1024 * 1024)
            .map(|n| (n % 251) as u8)
            .collect::<Vec<_>>();
        let file = backend
            .write_file(RepoPath::root(), &mut Cursor::new(bytes.clone()))
            .await
            .unwrap();
        assert_eq!(file.as_bytes(), blake3::hash(&bytes).as_bytes());
        let copy = backend
            .write_copy(&CopyHistory {
                current_path: path("a"),
                parents: vec![],
                salt: vec![0, 255],
            })
            .await
            .unwrap();
        let link = backend
            .write_symlink(RepoPath::root(), "../λ")
            .await
            .unwrap();
        let tree = backend
            .write_tree(
                RepoPath::root(),
                &Tree::from_sorted_entries(vec![
                    tree_entry(
                        "a",
                        TreeValue::File {
                            id: file.clone(),
                            executable: true,
                            copy_id: copy.clone(),
                        },
                    ),
                    tree_entry("link", TreeValue::Symlink(link.clone())),
                ]),
            )
            .await
            .unwrap();
        assert_eq!(
            backend.read_symlink(RepoPath::root(), &link).await.unwrap(),
            "../λ"
        );
        let value = commit(&backend, backend.root_commit_id().clone(), tree.clone());
        let mut sign = |data: &[u8]| Ok(data.iter().take(32).copied().rev().collect());
        let (id, signed) = backend.write_commit(value, Some(&mut sign)).await.unwrap();
        assert_eq!(backend.read_commit(&id).await.unwrap(), signed);
        assert!(signed.secure_sig.is_some());
        (file, tree, copy, id, bytes)
    });
    block_on(async {
        let backend = NativeBackend::load(directory.path(), config).unwrap();
        let mut reader = backend.read_file(RepoPath::root(), &file).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
        let tree = backend.read_tree(RepoPath::root(), &tree).await.unwrap();
        assert!(matches!(
            tree.entry(RepoPathComponentBuf::new("a").unwrap().as_ref())
                .unwrap()
                .value(),
            TreeValue::File {
                executable: true,
                ..
            }
        ));
        assert_eq!(
            backend.read_copy(&copy).await.unwrap().current_path,
            path("a")
        );
        assert!(
            backend
                .read_commit(&commit_id)
                .await
                .unwrap()
                .secure_sig
                .is_some()
        );
    });
    assert!(!directory.path().join(".git").exists());
}
#[test]
fn full_negative_tree_terms_labels_and_legacy_edges_round_trip() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let backend = NativeBackend::init(directory.path(), Config::default()).unwrap();
        let empty = backend.empty_tree_id().clone();
        let mut value = commit(&backend, backend.root_commit_id().clone(), empty.clone());
        value.root_tree = Merge::from_vec(vec![empty.clone(), empty.clone(), empty]);
        value.conflict_labels =
            ConflictLabels::from_vec(vec!["left".into(), "base".into(), "right".into()])
                .into_merge();
        value.predecessors = vec![backend.root_commit_id().clone()];
        let (id, stored) = backend.write_commit(value.clone(), None).await.unwrap();
        assert_eq!(stored, value);
        assert_eq!(backend.read_commit(&id).await.unwrap(), value);
        value.conflict_labels = ConflictLabels::unlabeled().into_merge();
        let (id, _) = backend.write_commit(value.clone(), None).await.unwrap();
        assert_eq!(backend.read_commit(&id).await.unwrap(), value);
        value.conflict_labels = Merge::from_vec(vec![String::new(); 3]);
        let mut sign = |data: &[u8]| Ok(data.iter().take(32).copied().collect());
        let (id, stored) = backend
            .write_commit(value.clone(), Some(&mut sign))
            .await
            .unwrap();
        assert!(
            stored.conflict_labels.is_resolved(),
            "all-empty labels normalize to unlabeled"
        );
        assert_eq!(
            stored.root_tree, value.root_tree,
            "raw tree terms must stay intact"
        );
        assert_eq!(stored.predecessors, value.predecessors);
        assert!(stored.secure_sig.is_some());
        assert_eq!(
            backend.read_commit(&id).await.unwrap(),
            stored,
            "write must return the actually stored commit"
        );
    });
}
#[test]
fn copy_queries_recover_descendants_and_derive_real_ancestor_file_provenance() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let backend = NativeBackend::init(directory.path(), Config::default()).unwrap();
        let file = backend
            .write_file(RepoPath::root(), &mut Cursor::new(b"copied bytes"))
            .await
            .unwrap();
        let original = backend
            .write_copy(&CopyHistory {
                current_path: path("a"),
                parents: vec![],
                salt: vec![1],
            })
            .await
            .unwrap();
        let tree = backend
            .write_tree(
                RepoPath::root(),
                &Tree::from_sorted_entries(vec![tree_entry(
                    "a",
                    TreeValue::File {
                        id: file.clone(),
                        executable: false,
                        copy_id: original.clone(),
                    },
                )]),
            )
            .await
            .unwrap();
        let (base, _) = backend
            .write_commit(
                commit(&backend, backend.root_commit_id().clone(), tree),
                None,
            )
            .await
            .unwrap();
        let copied = backend
            .write_copy(&CopyHistory {
                current_path: path("b"),
                parents: vec![original.clone()],
                salt: vec![2],
            })
            .await
            .unwrap();
        let tree = backend
            .write_tree(
                RepoPath::root(),
                &Tree::from_sorted_entries(vec![tree_entry(
                    "b",
                    TreeValue::File {
                        id: file.clone(),
                        executable: false,
                        copy_id: copied.clone(),
                    },
                )]),
            )
            .await
            .unwrap();
        let (head, _) = backend
            .write_commit(commit(&backend, base.clone(), tree), None)
            .await
            .unwrap();
        let empty = backend.empty_tree_id().clone();
        let copied_tree = backend
            .read_commit(&head)
            .await
            .unwrap()
            .root_tree
            .as_resolved()
            .unwrap()
            .clone();
        let mut deleted = commit(&backend, base.clone(), empty.clone());
        deleted.root_tree = Merge::from_vec(vec![empty.clone(), copied_tree.clone(), empty]);
        deleted.conflict_labels =
            ConflictLabels::from_vec(vec!["left".into(), "base".into(), "right".into()])
                .into_merge();
        let (deleted_id, stored) = backend.write_commit(deleted.clone(), None).await.unwrap();
        assert_eq!(
            stored.root_tree, deleted.root_tree,
            "stored negative terms must remain intact"
        );
        assert!(
            backend
                .get_copy_records(None, &base, &deleted_id)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty(),
            "both positive sides delete b; negative term must not invent a copy"
        );
        let (reintroduced, _) = backend
            .write_commit(commit(&backend, deleted_id.clone(), copied_tree), None)
            .await
            .unwrap();
        let reintroduced_records = backend
            .get_copy_records(None, &deleted_id, &reintroduced)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            reintroduced_records.len(),
            1,
            "negative-only parent file must not hide a new copy event"
        );
        assert_eq!(reintroduced_records[0].target_commit, reintroduced);
        let related = backend.get_related_copies(&original).await.unwrap();
        assert_eq!(related[0].id, copied);
        assert_eq!(related[1].id, original);
        let records = backend
            .get_copy_records(None, &base, &head)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0],
            CopyRecord {
                target: path("b"),
                target_commit: head.clone(),
                source: path("a"),
                source_file: file,
                source_commit: base.clone()
            }
        );
        assert!(
            backend
                .get_copy_records(Some(&[path("other")]), &base, &head)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .is_empty()
        );
    });
}
#[test]
fn intrinsic_root_invalid_ids_limits_and_submodules_are_explicit() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let backend = NativeBackend::init(directory.path(), Config::default()).unwrap();
        let root = backend.read_commit(backend.root_commit_id()).await.unwrap();
        assert!(root.parents.is_empty());
        assert_eq!(root.change_id, *backend.root_change_id());
        assert!(
            backend
                .read_tree(RepoPath::root(), backend.empty_tree_id())
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            backend
                .read_commit(&CommitId::from_bytes(&[0; 31]))
                .await
                .is_err()
        );
        assert!(
            backend
                .write_tree(
                    RepoPath::root(),
                    &Tree::from_sorted_entries(vec![tree_entry(
                        "module",
                        TreeValue::GitSubmodule(CommitId::from_bytes(&[1; 20]))
                    )])
                )
                .await
                .is_err()
        );
        assert!(
            NativeBackend::load(directory.path(), Config::default()).is_err(),
            "exclusive owner is an explicit prototype limitation"
        );
        assert!(
            backend
                .get_related_copies(&CopyId::from_bytes(&[1; 32]))
                .await
                .is_err()
        );
    });
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.store.max_file_bytes = 1;
        let backend = NativeBackend::init(directory.path(), config).unwrap();
        assert!(
            backend
                .write_file(RepoPath::root(), &mut Cursor::new(b"too big"))
                .await
                .is_err()
        );
    });
}
#[test]
fn real_jj_materialized_workspace_initializes_and_snapshots_with_native_backend() {
    use jj_lib::{
        config::{ConfigLayer, ConfigSource, StackedConfig},
        gitignore::GitIgnoreFile,
        matchers::{EverythingMatcher, NothingMatcher},
        repo::Repo,
        settings::UserSettings,
        signing::Signer,
        working_copy::SnapshotOptions,
        workspace::Workspace,
    };
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let mut config = StackedConfig::with_defaults();
        config.add_layer(ConfigLayer::parse(ConfigSource::User,"[user]\nname='Native Author'\nemail='author@example.invalid'\n[operation]\nhostname='test'\nusername='test'\n").unwrap());
        let settings = UserSettings::from_config_and_home_dir(config, None).unwrap();
        let initializer =
            |_: &UserSettings, path: &Path| -> Result<Box<dyn Backend>, BackendInitError> {
                Ok(Box::new(NativeBackend::init(path, Config::default())?))
            };
        let (mut workspace, repo) = Workspace::init_with_backend(
            &settings,
            directory.path(),
            &initializer,
            Signer::new(None, vec![]),
        )
        .await
        .unwrap();
        assert_eq!(repo.store().backend().name(), NativeBackend::NAME);
        assert!(!directory.path().join(".git").exists());
        assert!(repo.index().supports_exact_commit_membership());
        let mut transaction = repo.start_transaction();
        assert!(
            transaction
                .repo_mut()
                .mutable_index()
                .supports_rewrite_inference()
        );
        assert!(
            transaction
                .repo_mut()
                .index()
                .supports_exact_commit_membership()
        );
        let root = repo.store().root_commit();
        let signature = root.author().clone();
        let change = ChangeId::from_bytes(&[57; 16]);
        let builder = transaction
            .repo_mut()
            .new_commit(vec![root.id().clone()], root.tree())
            .set_change_id(change.clone())
            .set_description("indexed duplicate protection")
            .set_author(signature.clone())
            .set_committer(signature.clone())
            .detach();
        let stored = builder.write_hidden().await.unwrap();
        assert!(
            !transaction
                .repo_mut()
                .index()
                .has_id(stored.id())
                .await
                .unwrap(),
            "writing an object alone must not admit it to the index"
        );
        let admitted = builder.write(transaction.repo_mut()).await.unwrap();
        assert_eq!(stored.id(), admitted.id());
        assert!(
            transaction
                .repo_mut()
                .index()
                .has_id(admitted.id())
                .await
                .unwrap()
        );
        let duplicate = transaction
            .repo_mut()
            .new_commit(vec![root.id().clone()], root.tree())
            .set_change_id(change)
            .set_description("indexed duplicate protection")
            .set_author(signature.clone())
            .set_committer(signature)
            .write()
            .await
            .unwrap_err();
        assert!(duplicate.to_string().contains("already exists"));
        drop(transaction);
        std::fs::write(
            directory.path().join("hello"),
            b"native materialized fixture",
        )
        .unwrap();
        let mut locked = workspace.start_working_copy_mutation().await.unwrap();
        let (tree, _) = locked
            .locked_wc()
            .snapshot(&SnapshotOptions {
                base_ignores: GitIgnoreFile::empty(),
                progress: None,
                start_tracking_matcher: &EverythingMatcher,
                force_tracking_matcher: &NothingMatcher,
                max_new_file_size: 1024,
            })
            .await
            .unwrap();
        let tree_id = tree.tree_ids().as_resolved().unwrap();
        let actual = repo
            .store()
            .backend()
            .read_tree(RepoPath::root(), tree_id)
            .await
            .unwrap();
        assert!(
            actual
                .entry(RepoPathComponentBuf::new("hello").unwrap().as_ref())
                .is_some()
        );
        // Snapshot object I/O is accepted here; full operation/checkout/undo
        // transaction acceptance is a later phase, not inferred from this case.
    });
}

#[test]
fn missing_native_storage_load_never_initializes_or_repairs_it() {
    let directory = tempfile::tempdir().unwrap();
    for child in ["native-objects", "native-file-staging"] {
        std::fs::create_dir(directory.path().join(child)).unwrap();
    }
    assert!(NativeBackend::load(directory.path(), Config::default()).is_err());
    for child in ["native-objects", "native-file-staging"] {
        assert_eq!(
            std::fs::read_dir(directory.path().join(child))
                .unwrap()
                .count(),
            0
        );
    }
}
#[test]
fn aggregate_staging_admission_precedes_input_and_cancellation_releases_it() {
    use futures::{
        channel::oneshot,
        future::{Either, select},
    };
    use std::{
        future::Future,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };
    struct Blocked {
        entered: Option<oneshot::Sender<()>>,
        release: oneshot::Receiver<()>,
    }
    impl AsyncRead for Blocked {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            _: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            if let Some(tx) = self.entered.take() {
                let _ = tx.send(());
            }
            match Pin::new(&mut self.release).poll(cx) {
                Poll::Ready(_) => Poll::Ready(Ok(0)),
                Poll::Pending => Poll::Pending,
            }
        }
    }
    struct Counted {
        polls: Arc<AtomicUsize>,
        cursor: Cursor<Vec<u8>>,
    }
    impl AsyncRead for Counted {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &mut [u8],
        ) -> Poll<std::io::Result<usize>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Pin::new(&mut self.cursor).poll_read(cx, bytes)
        }
    }
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.store.max_file_bytes = 8;
        config.store.max_staging_bytes = 16;
        config.store.max_staging_uploads = 1;
        let backend = NativeBackend::init(directory.path(), config).unwrap();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (_release_tx, release_rx) = oneshot::channel();
        let mut blocked = Blocked {
            entered: Some(entered_tx),
            release: release_rx,
        };
        let mut first = Box::pin(backend.write_file(RepoPath::root(), &mut blocked));
        match select(first.as_mut(), entered_rx).await {
            Either::Right((Ok(()), _)) => {}
            _ => panic!("first input did not block"),
        }
        assert_eq!(
            std::fs::read_dir(directory.path().join("native-file-staging"))
                .unwrap()
                .count(),
            1
        );
        let polls = Arc::new(AtomicUsize::new(0));
        let mut counted = Counted {
            polls: polls.clone(),
            cursor: Cursor::new(b"second".to_vec()),
        };
        let mut second = Box::pin(backend.write_file(RepoPath::root(), &mut counted));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(second.as_mut().poll(cx).is_pending())).await
        );
        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "queued writer must not read or spool input"
        );
        assert_eq!(
            std::fs::read_dir(directory.path().join("native-file-staging"))
                .unwrap()
                .count(),
            1
        );
        drop(first);
        second.await.unwrap();
        assert!(polls.load(Ordering::SeqCst) > 0);
        assert_eq!(
            std::fs::read_dir(directory.path().join("native-file-staging"))
                .unwrap()
                .count(),
            0
        );
    });
}
#[test]
fn copy_query_frontier_bytes_and_duplicate_edges_fail_with_explicit_budgets() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.store.max_path_context_bytes = 2048;
        let backend = NativeBackend::init(directory.path(), config).unwrap();
        let empty = backend.empty_tree_id().clone();
        let children = (0..10)
            .map(|n| tree_entry(&format!("child{n}"), TreeValue::Tree(empty.clone())))
            .collect();
        let child = backend
            .write_tree(RepoPath::root(), &Tree::from_sorted_entries(children))
            .await
            .unwrap();
        let top = backend
            .write_tree(
                RepoPath::root(),
                &Tree::from_sorted_entries(vec![tree_entry(
                    &"p".repeat(500),
                    TreeValue::Tree(child),
                )]),
            )
            .await
            .unwrap();
        let (head, _) = backend
            .write_commit(
                commit(&backend, backend.root_commit_id().clone(), top),
                None,
            )
            .await
            .unwrap();
        let error = backend
            .get_copy_records(None, backend.root_commit_id(), &head)
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap_err();
        assert!(error.to_string().contains("path bytes") || error.to_string().contains("context"));
        drop(backend);
        let mut config = Config::default();
        config.store.max_closure_edges = 3;
        let backend = NativeBackend::load(directory.path(), config).unwrap();
        let mut duplicate = commit(
            &backend,
            backend.root_commit_id().clone(),
            backend.empty_tree_id().clone(),
        );
        duplicate.parents = vec![backend.root_commit_id().clone(); 5];
        let (head, _) = backend.write_commit(duplicate, None).await.unwrap();
        assert!(
            backend
                .get_copy_records(None, backend.root_commit_id(), &head)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
                .to_string()
                .contains("edge budget")
        );
        let mut wide = commit(
            &backend,
            backend.root_commit_id().clone(),
            backend.empty_tree_id().clone(),
        );
        wide.root_tree = Merge::from_vec(vec![backend.empty_tree_id().clone(); 5]);
        let (head, _) = backend.write_commit(wide, None).await.unwrap();
        assert!(
            backend
                .get_copy_records(None, backend.root_commit_id(), &head)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
                .to_string()
                .contains("budget"),
            "merge preprocessing must be admitted before quadratic work"
        );
        drop(backend);
        let mut config = Config::default();
        config.store.max_closure_edges = 50;
        let backend = NativeBackend::load(directory.path(), config).unwrap();
        let link = backend
            .write_symlink(RepoPath::root(), "target")
            .await
            .unwrap();
        let mut trees = Vec::new();
        for group in 0..3 {
            let entries = (0..3)
                .map(|index| {
                    tree_entry(
                        &format!("n{group}-{index}"),
                        TreeValue::Symlink(link.clone()),
                    )
                })
                .collect();
            trees.push(
                backend
                    .write_tree(RepoPath::root(), &Tree::from_sorted_entries(entries))
                    .await
                    .unwrap(),
            );
        }
        let mut value = commit(&backend, backend.root_commit_id().clone(), trees[0].clone());
        value.root_tree = Merge::from_vec(trees);
        let (head, _) = backend.write_commit(value, None).await.unwrap();
        assert!(
            backend
                .get_copy_records(None, backend.root_commit_id(), &head)
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err()
                .to_string()
                .contains("edge budget"),
            "per-name merge work must count even though there are no file records"
        );
    });
}

#[test]
fn shared_provider_backends_use_one_authoritative_owner_and_copy_snapshot() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let config = Config::default();
        let io = Arc::new(DirectObjectIo::init(directory.path(), config.clone()).unwrap());
        let first = NativeBackend::from_io(io.clone(), config.clone())
            .await
            .unwrap();
        let second = NativeBackend::from_io(io.clone(), config.clone())
            .await
            .unwrap();
        let id = first
            .write_file(RepoPath::root(), &mut Cursor::new(b"provider bytes"))
            .await
            .unwrap();
        let mut bytes = Vec::new();
        second
            .read_file(RepoPath::root(), &id)
            .await
            .unwrap()
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert_eq!(bytes, b"provider bytes");
        let parent = first
            .write_copy(&CopyHistory {
                current_path: path("a"),
                parents: vec![],
                salt: vec![1],
            })
            .await
            .unwrap();
        let child = second
            .write_copy(&CopyHistory {
                current_path: path("b"),
                parents: vec![parent.clone()],
                salt: vec![2],
            })
            .await
            .unwrap();
        let related = first.get_related_copies(&parent).await.unwrap();
        assert_eq!(related[0].id, child);
        assert_eq!(related[1].id, parent);
        let snapshot = io
            .copy_catalog_snapshot(CopyCatalogLimits {
                max_objects: 2,
                max_metadata_bytes: 4096,
                max_edges: 2,
            })
            .await
            .unwrap();
        assert_eq!(snapshot.into_entries().len(), 2);
        assert!(
            io.copy_catalog_snapshot(CopyCatalogLimits {
                max_objects: 1,
                max_metadata_bytes: 4096,
                max_edges: 2
            })
            .await
            .is_err()
        );
        assert!(
            NativeBackend::load(directory.path(), config).is_err(),
            "Direct still owns one process-exclusive store"
        );
    });
}
#[test]
fn provider_attachment_requires_verified_bytes_not_a_descriptor_promise() {
    #[derive(Debug)]
    struct PromiseOnly;
    #[async_trait]
    impl NativeObjectIo for PromiseOnly {
        async fn descriptor(&self, _: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
            Ok(Some(m::empty_tree().descriptor))
        }
        async fn read_metadata(&self, _: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
            Err(invalid("descriptor promise has no resident verified bytes"))
        }
        async fn write_metadata(&self, _: m::Object) -> BackendResult<m::Descriptor> {
            Err(invalid("not implemented by fixture"))
        }
        async fn read_file(&self, _: m::FileId) -> BackendResult<FileRead> {
            Err(invalid("not implemented by fixture"))
        }
        async fn write_file(
            &self,
            _: &mut (dyn AsyncRead + Send + Unpin),
            _: u64,
        ) -> BackendResult<m::FileId> {
            Err(invalid("not implemented by fixture"))
        }
        async fn copy_catalog_snapshot(
            &self,
            _: CopyCatalogLimits,
        ) -> BackendResult<CopyCatalogSnapshot> {
            Err(invalid("not implemented by fixture"))
        }
    }
    block_on(async {
        let promise = Arc::new(PromiseOnly);
        assert!(
            promise
                .descriptor(m::empty_tree().descriptor.key)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            NativeBackend::from_io(promise, Config::default())
                .await
                .is_err()
        );
    });
    let encoded = m::empty_tree();
    VerifiedMetadata::verify(
        encoded.descriptor,
        encoded.bytes.clone(),
        m::Limits::default(),
    )
    .unwrap();
    let mut wrong = encoded.descriptor;
    wrong.key.id.0[0] ^= 1;
    assert!(VerifiedMetadata::verify(wrong, encoded.bytes.clone(), m::Limits::default()).is_err());
    let mut trailing = encoded.bytes;
    trailing.push(0);
    let mut descriptor = m::describe_file(&trailing);
    descriptor.key.kind = m::Kind::Tree;
    assert!(VerifiedMetadata::verify(descriptor, trailing, m::Limits::default()).is_err());
}

#[test]
fn cancelled_metadata_install_keeps_the_physical_put_owner_and_admission() {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};
    let directory = tempfile::tempdir().unwrap();
    let mut provider = DirectObjectIo::init(directory.path(), Default::default()).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_gate = gate.clone();
    let (entered, started) = std::sync::mpsc::channel();
    provider.set_metadata_put_hook(Arc::new(move || {
        entered.send(()).unwrap();
        let (lock, changed) = &*worker_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
    }));
    let provider = Arc::new(provider);
    let pool = Arc::new(tokio::sync::Semaphore::new(1));
    let owner = Arc::new(pool.clone().try_acquire_owned().unwrap());
    let weak = Arc::downgrade(&owner);
    let mut commit = m::synthetic_root();
    commit.parents = vec![m::CommitId::ROOT];
    let object = m::Object::Commit(commit);
    let expected = m::encode(&object, m::Limits::default()).unwrap().descriptor;
    let worker: Arc<dyn NativeObjectIo> = provider.clone();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let task = runtime.spawn(async move { worker.write_metadata_with_owner(object, owner).await });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    task.abort();
    let _ = runtime.block_on(task);
    assert!(
        weak.upgrade().is_some(),
        "accepted Store.put must retain its caller after cancellation"
    );
    assert!(
        pool.clone().try_acquire_owned().is_err(),
        "another alias must not acquire physical work admission early"
    );
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    let deadline = Instant::now() + Duration::from_secs(5);
    while weak.upgrade().is_some() {
        assert!(
            Instant::now() < deadline,
            "physical put owner did not release"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(pool.clone().try_acquire_owned().is_ok());
    assert_eq!(
        block_on(provider.descriptor(expected.key)).unwrap(),
        Some(expected)
    );
}

#[test]
fn owned_file_read_keeps_admission_through_cancelled_physical_chunk_output() {
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant};
    let directory = tempfile::tempdir().unwrap();
    let mut provider = DirectObjectIo::init(directory.path(), Default::default()).unwrap();
    let bytes = vec![19; 65536];
    let id =
        block_on(provider.write_file(&mut Cursor::new(bytes.clone()), bytes.len() as u64)).unwrap();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let worker_gate = gate.clone();
    let (entered, started) = std::sync::mpsc::channel();
    provider.set_file_read_hook(Arc::new(move || {
        entered.send(()).unwrap();
        let (lock, changed) = &*worker_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
    }));
    let pool = Arc::new(tokio::sync::Semaphore::new(1));
    let owner = Arc::new(pool.clone().try_acquire_owned().unwrap());
    let weak = Arc::downgrade(&owner);
    let file = block_on(provider.read_file_with_owner(id, owner)).unwrap();
    let (_, mut reader) = file.into_parts();
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let task = runtime.spawn(async move {
        let mut output = vec![0; 4096];
        reader.read_exact(&mut output).await.unwrap();
        output
    });
    // The hook runs after a real read has populated the owned chunk, before
    // returning it to the waiter. Dropping the waiter must not free its owner.
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    task.abort();
    let _ = runtime.block_on(task);
    assert!(weak.upgrade().is_some());
    assert!(pool.clone().try_acquire_owned().is_err());
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    let deadline = Instant::now() + Duration::from_secs(5);
    while weak.upgrade().is_some() {
        assert!(
            Instant::now() < deadline,
            "physical file-output owner did not release"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    let next = Arc::new(pool.clone().try_acquire_owned().unwrap());
    let eof_owner = Arc::downgrade(&next);
    let file = block_on(provider.read_file_with_owner(id, next)).unwrap();
    let (_, mut reader) = file.into_parts();
    let mut output = Vec::new();
    block_on(reader.read_to_end(&mut output)).unwrap();
    assert_eq!(output, bytes);
    assert!(
        eof_owner.upgrade().is_some(),
        "an application-held EOF reader retains its owned delivery"
    );
    assert!(pool.clone().try_acquire_owned().is_err());
    drop(reader);
    assert!(eof_owner.upgrade().is_none());
    assert!(pool.clone().try_acquire_owned().is_ok());
}

// The maintenance adapter intentionally has one process-wide, nonqueued slot.
static CLIENT_GC_TEST: std::sync::Mutex<()> = std::sync::Mutex::new(());
fn client_gc_options(roots: Vec<m::ObjectKey>) -> NativeClientGcOptions {
    NativeClientGcOptions {
        complete_roots: roots,
        transient_roots: Vec::new(),
        now: now().unwrap() + 7200,
        grace_seconds: 0,
        limits: tinytree_store::ClientGcLimits::default(),
        deadline: std::time::Instant::now() + std::time::Duration::from_secs(5),
    }
}

#[test]
fn owned_client_gc_keeps_resident_closure_and_blocks_missing_file_without_deletion() {
    let _serial = CLIENT_GC_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let io = DirectObjectIo::init(directory.path(), Default::default()).unwrap();
    block_on(async {
        let keep = io.write_file(&mut Cursor::new(b"keep"), 4).await.unwrap();
        let spare = io.write_file(&mut Cursor::new(b"spare"), 5).await.unwrap();
        let tree = io
            .write_metadata(m::Object::Tree(m::Tree {
                entries: vec![m::TreeEntry {
                    name: "file".into(),
                    value: m::TreeValue::File {
                        id: keep,
                        executable: false,
                        size_bytes: 4,
                        copy_id: None,
                    },
                }],
            }))
            .await
            .unwrap();
        let missing = m::describe_file(b"not resident");
        let incomplete = io
            .write_metadata(m::Object::Tree(m::Tree {
                entries: vec![m::TreeEntry {
                    name: "missing".into(),
                    value: m::TreeValue::File {
                        id: m::FileId(missing.key.id.0),
                        executable: false,
                        size_bytes: missing.size_bytes,
                        copy_id: None,
                    },
                }],
            }))
            .await
            .unwrap();
        assert!(
            matches!(io.client_gc_with_owner(client_gc_options(vec![incomplete.key]), Arc::new(())).await,
            Err(NativeClientGcError::Store(tinytree_store::Error::MissingObject(key))) if key == missing.key)
        );
        let spare_key = m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(spare.0),
        };
        assert!(io.descriptor(spare_key).await.unwrap().is_some());
        let plan = io
            .plan_client_gc_with_owner(client_gc_options(vec![tree.key]), Arc::new(()))
            .await
            .unwrap();
        assert_eq!(plan.would_delete_objects, 2);
        assert_eq!(
            plan,
            io.plan_client_gc_with_owner(client_gc_options(vec![tree.key]), Arc::new(()))
                .await
                .unwrap()
        );
        assert!(io.descriptor(spare_key).await.unwrap().is_some());
        assert_eq!(
            io.descriptor(incomplete.key).await.unwrap(),
            Some(incomplete)
        );
        let report = io
            .client_gc_with_owner(client_gc_options(vec![tree.key]), Arc::new(()))
            .await
            .unwrap();
        assert_eq!(report.deleted_objects, plan.would_delete_objects);
        assert_eq!(report.deleted_bytes, plan.would_delete_bytes);
        assert!(io.descriptor(spare_key).await.unwrap().is_none());
        assert_eq!(io.descriptor(tree.key).await.unwrap(), Some(tree));
        let (_, mut reader) = io.read_file(keep).await.unwrap().into_parts();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"keep");
        let mut expired = client_gc_options(vec![tree.key]);
        expired.deadline = std::time::Instant::now();
        assert!(matches!(
            io.client_gc_with_owner(expired, Arc::new(())).await,
            Err(NativeClientGcError::Deadline)
        ));
        let mut excessive = client_gc_options(vec![tree.key]);
        excessive.limits.max_catalog_records += 1;
        assert!(matches!(
            io.client_gc_with_owner(excessive, Arc::new(())).await,
            Err(NativeClientGcError::InvalidLimits)
        ));
    });
}

#[test]
fn owned_client_gc_rejects_catalog_change_after_same_store_epoch() {
    let _serial = CLIENT_GC_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut io = DirectObjectIo::init(directory.path(), Default::default()).unwrap();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker_gate = gate.clone();
    let (entered, started) = std::sync::mpsc::channel();
    io.set_client_gc_hook(Arc::new(move || {
        entered.send(()).unwrap();
        let (lock, changed) = &*worker_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
    }));
    let io = Arc::new(io);
    let spare = block_on(io.write_file(&mut Cursor::new(b"old"), 3)).unwrap();
    let worker = io.clone();
    let task = std::thread::spawn(move || {
        block_on(worker.client_gc_with_owner(client_gc_options(Vec::new()), Arc::new(())))
    });
    started
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    block_on(io.write_file(&mut Cursor::new(b"new"), 3)).unwrap();
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    assert!(matches!(
        task.join().unwrap(),
        Err(NativeClientGcError::Store(tinytree_store::Error::Invalid(
            "stale client GC epoch"
        )))
    ));
    assert!(
        block_on(io.descriptor(m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(spare.0)
        }))
        .unwrap()
        .is_some()
    );
}

#[test]
fn timed_out_client_gc_keeps_exclusive_owner_and_nonqueued_slot_until_physical_completion() {
    use std::time::{Duration, Instant};
    let _serial = CLIENT_GC_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut io = DirectObjectIo::init(directory.path(), Default::default()).unwrap();
    let spare = block_on(io.write_file(&mut Cursor::new(b"orphan"), 6)).unwrap();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker_gate = gate.clone();
    let (entered, started) = std::sync::mpsc::channel();
    io.set_client_gc_hook(Arc::new(move || {
        entered.send(()).unwrap();
        let (lock, changed) = &*worker_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
    }));
    let io = Arc::new(io);
    let pool = Arc::new(tokio::sync::Semaphore::new(1));
    let owner = Arc::new(pool.clone().try_acquire_owned().unwrap());
    let weak = Arc::downgrade(&owner);
    let worker = io.clone();
    let mut options = client_gc_options(Vec::new());
    options.deadline = Instant::now() + Duration::from_secs(1);
    let task = std::thread::spawn(move || block_on(worker.client_gc_with_owner(options, owner)));
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(matches!(
        task.join().unwrap(),
        Err(NativeClientGcError::Deadline)
    ));
    assert!(weak.upgrade().is_some());
    assert!(pool.clone().try_acquire_owned().is_err());
    assert!(matches!(
        block_on(io.client_gc_with_owner(client_gc_options(Vec::new()), Arc::new(()))),
        Err(NativeClientGcError::Busy)
    ));
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    let deadline = Instant::now() + Duration::from_secs(5);
    // Weak::upgrade may fail before the last Arc's payload destructor has
    // finished returning its permit. Observe the actual admission boundary.
    let _released = loop {
        if let Ok(permit) = pool.clone().try_acquire_owned() {
            break permit;
        }
        assert!(
            Instant::now() < deadline,
            "physical collector admission did not release"
        );
        std::thread::sleep(Duration::from_millis(5));
    };
    assert!(
        block_on(io.descriptor(m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(spare.0)
        }))
        .unwrap()
        .is_some()
    );
    let report =
        block_on(io.client_gc_with_owner(client_gc_options(Vec::new()), Arc::new(()))).unwrap();
    assert_eq!(report.deleted_objects, 1);
}

#[test]
fn dropped_client_gc_waiter_does_not_release_its_physical_owner() {
    use std::time::{Duration, Instant};
    let _serial = CLIENT_GC_TEST.lock().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut io = DirectObjectIo::init(directory.path(), Default::default()).unwrap();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let worker_gate = gate.clone();
    let (entered, started) = std::sync::mpsc::channel();
    io.set_client_gc_hook(Arc::new(move || {
        entered.send(()).unwrap();
        let (lock, changed) = &*worker_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
    }));
    let spare = block_on(io.write_file(&mut Cursor::new(b"cancelled orphan"), 16)).unwrap();
    let io = Arc::new(io);
    let observer = io.clone();
    let owner = Arc::new(());
    let weak = Arc::downgrade(&owner);
    let (abort, registration) = futures::future::AbortHandle::new_pair();
    let task = std::thread::spawn(move || {
        block_on(futures::future::Abortable::new(
            io.client_gc_with_owner(client_gc_options(Vec::new()), owner),
            registration,
        ))
    });
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    abort.abort();
    assert!(task.join().unwrap().is_err());
    // The caller is gone, but the accepted physical job still owns the same
    // Store and collector fence independently.
    assert!(weak.upgrade().is_some());
    let (lock, changed) = &*gate;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    let deadline = Instant::now() + Duration::from_secs(5);
    while weak.upgrade().is_some() {
        assert!(
            Instant::now() < deadline,
            "cancelled collector owner did not release"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        block_on(observer.descriptor(m::ObjectKey {
            kind: m::Kind::File,
            id: m::ObjectId(spare.0)
        }))
        .unwrap()
        .is_none()
    );
}

#[test]
fn external_stack_rewrite_keeps_server_ids_and_operation_evolution() {
    use jj_lib::{
        config::{ConfigLayer, ConfigSource, StackedConfig},
        evolution::walk_predecessors,
        op_store::RefTarget,
        ref_name::RefName,
        repo::{ReadonlyRepo, Repo},
        settings::UserSettings,
        signing::Signer,
    };
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let mut config = StackedConfig::with_defaults();
        config.add_layer(ConfigLayer::parse(ConfigSource::User,
            "[user]\nname='Native Author'\nemail='author@example.invalid'\n[operation]\nhostname='test'\nusername='test'\n[experimental]\nrecord-predecessors-in-commit=false\n").unwrap());
        let settings = UserSettings::from_config_and_home_dir(config, None).unwrap();
        // Metadata-only repository: no materialized or mounted working copy.
        let repo = ReadonlyRepo::init(
            &settings,
            directory.path(),
            &|_, path| Ok(Box::new(NativeBackend::init(path, Config::default())?)),
            Signer::new(None, vec![]),
            ReadonlyRepo::default_workspace_store_initializer(),
            ReadonlyRepo::default_op_store_initializer(),
            ReadonlyRepo::default_op_heads_store_initializer(),
            ReadonlyRepo::default_index_store_initializer(),
            ReadonlyRepo::default_submodule_store_initializer(),
        )
        .await
        .unwrap();
        let root = repo.store().root_commit();
        let mut tx = repo.start_transaction();
        let a = tx
            .repo_mut()
            .new_commit(vec![root.id().clone()], root.tree())
            .set_description("stack base")
            .write()
            .await
            .unwrap();
        let b = tx
            .repo_mut()
            .new_commit(vec![a.id().clone()], root.tree())
            .set_description("stack tip")
            .write()
            .await
            .unwrap();
        let child = tx
            .repo_mut()
            .new_commit(vec![b.id().clone()], root.tree())
            .set_description("unsubmitted child")
            .write()
            .await
            .unwrap();
        tx.repo_mut()
            .set_local_bookmark_target(RefName::new("landed"), RefTarget::normal(b.id().clone()));
        tx.repo_mut().set_local_bookmark_target(
            RefName::new("child"),
            RefTarget::normal(child.id().clone()),
        );
        let repo = tx.commit("create local stack").await.unwrap();

        // Model exact objects returned by a server. Do not use CommitBuilder:
        // its committer update would change these canonical IDs.
        let mut canonical_a = a.store_commit().as_ref().clone();
        canonical_a.description = "canonical stack base".into();
        canonical_a.committer.timestamp.timestamp = MillisSinceEpoch(42);
        let new_a = repo.store().write_commit(canonical_a, None).await.unwrap();
        let mut canonical_b = b.store_commit().as_ref().clone();
        canonical_b.parents = vec![new_a.id().clone()];
        canonical_b.committer.timestamp.timestamp = MillisSinceEpoch(43);
        let new_b = repo.store().write_commit(canonical_b, None).await.unwrap();
        assert!(new_a.store_commit().predecessors.is_empty());
        assert!(new_b.store_commit().predecessors.is_empty());
        let pairs = vec![
            (a.id().clone(), new_a.id().clone()),
            (b.id().clone(), new_b.id().clone()),
        ];

        let mut mismatch = b.store_commit().as_ref().clone();
        mismatch.change_id = ChangeId::from_bytes(&[99; 16]);
        let mismatch = repo.store().write_commit(mismatch, None).await.unwrap();
        let mut obsolete = b.store_commit().as_ref().clone();
        obsolete.parents = vec![a.id().clone()];
        obsolete.description = "invalid canonical parent".into();
        let obsolete = repo.store().write_commit(obsolete, None).await.unwrap();
        let invalid_batches = [
            vec![pairs[1].clone(), pairs[0].clone()],
            vec![pairs[0].clone(), (b.id().clone(), mismatch.id().clone())],
            vec![(a.id().clone(), a.id().clone())],
            vec![pairs[0].clone(), pairs[0].clone()],
            vec![pairs[0].clone(), (b.id().clone(), obsolete.id().clone())],
            vec![pairs[0].clone(), (new_a.id().clone(), a.id().clone())],
        ];
        for batch in invalid_batches {
            let mut tx = repo.start_transaction();
            tx.repo_mut()
                .index_commits(&[
                    new_a.clone(),
                    new_b.clone(),
                    mismatch.clone(),
                    obsolete.clone(),
                ])
                .await
                .unwrap();
            assert!(
                tx.repo_mut()
                    .record_external_rewrites(&batch)
                    .await
                    .is_err()
            );
            assert!(!tx.repo_mut().has_rewrites());
            assert!(
                !tx.repo_mut().has_changes(),
                "invalid batch must not record operation evolution"
            );
            drop(tx);
        }
        for limits in [
            jj_lib::repo::ExternalRewriteValidationLimits {
                max_rewrites: 1,
                ..Default::default()
            },
            jj_lib::repo::ExternalRewriteValidationLimits {
                max_operation_steps: 1,
                ..Default::default()
            },
            jj_lib::repo::ExternalRewriteValidationLimits {
                max_evolution_steps: 1,
                ..Default::default()
            },
        ] {
            let mut tx = repo.start_transaction();
            tx.repo_mut().index_commits(&[new_b.clone()]).await.unwrap();
            assert!(matches!(
                tx.repo_mut()
                    .record_external_rewrites_with_limits(&pairs, limits)
                    .await,
                Err(jj_lib::repo::ExternalRewriteError::ValidationLimitExceeded(
                    _
                ))
            ));
            assert!(!tx.repo_mut().has_changes());
        }
        // No-op operations have no relevant evolution rows. The preflight
        // must still stop before scanning an arbitrarily long operation chain.
        let mut long_history = repo.clone();
        for _ in 0..16 {
            long_history = long_history
                .start_transaction()
                .commit("unrelated operation")
                .await
                .unwrap();
        }
        let mut limited = long_history.start_transaction();
        limited
            .repo_mut()
            .index_commits(&[new_b.clone()])
            .await
            .unwrap();
        assert!(matches!(
            limited
                .repo_mut()
                .record_external_rewrites_with_limits(
                    &pairs,
                    jj_lib::repo::ExternalRewriteValidationLimits {
                        max_operation_steps: 4,
                        ..Default::default()
                    }
                )
                .await,
            Err(jj_lib::repo::ExternalRewriteError::ValidationLimitExceeded(
                "operation history"
            ))
        ));
        assert!(!limited.repo_mut().has_changes());
        assert!(matches!(
            limited
                .repo_mut()
                .record_external_rewrites_with_limits(
                    &pairs,
                    jj_lib::repo::ExternalRewriteValidationLimits {
                        max_rewrites: 0,
                        ..Default::default()
                    }
                )
                .await,
            Err(jj_lib::repo::ExternalRewriteError::InvalidLimits)
        ));
        assert!(!limited.repo_mut().has_changes());

        // A legacy operation can carry commit-embedded predecessor edges that
        // modern operation maps cannot prove absent. Do not silently skip it.
        let mut legacy_data = repo.operation().store_operation().clone();
        legacy_data.parents = vec![repo.op_id().clone()];
        legacy_data.commit_predecessors = None;
        let op_store = repo.operation().op_store();
        let legacy_id = op_store.write_operation(&legacy_data).await.unwrap();
        let legacy_op = jj_lib::operation::Operation::new(op_store, legacy_id, legacy_data);
        let legacy_repo = repo.loader().load_at(&legacy_op).await.unwrap();
        let mut legacy = legacy_repo.start_transaction();
        legacy
            .repo_mut()
            .index_commits(&[new_b.clone()])
            .await
            .unwrap();
        assert!(matches!(
            legacy.repo_mut().record_external_rewrites(&pairs).await,
            Err(jj_lib::repo::ExternalRewriteError::IncompleteEvolutionHistory)
        ));
        assert!(!legacy.repo_mut().has_changes());

        let mut empty = repo.start_transaction();
        empty
            .repo_mut()
            .record_external_rewrites_with_limits(
                &[],
                jj_lib::repo::ExternalRewriteValidationLimits {
                    max_rewrites: 0,
                    max_operation_steps: 0,
                    max_evolution_steps: 0,
                },
            )
            .await
            .unwrap();
        assert!(!empty.repo_mut().has_changes());
        let mut tx = repo.start_transaction();
        tx.repo_mut().add_head(&new_b).await.unwrap();
        tx.repo_mut()
            .record_external_rewrites(&pairs)
            .await
            .unwrap();
        // Duplicate invocation in one transaction is rejected without replacing
        // the successful mapping or introducing duplicate evolution edges.
        assert!(
            tx.repo_mut()
                .record_external_rewrites(&pairs)
                .await
                .is_err()
        );
        assert_eq!(tx.repo_mut().rebase_descendants().await.unwrap(), 1);
        let landed = tx.commit("import canonical stack rewrite").await.unwrap();
        assert_eq!(
            landed
                .view()
                .get_local_bookmark(RefName::new("landed"))
                .as_normal(),
            Some(new_b.id())
        );
        let rebased_child = landed
            .view()
            .get_local_bookmark(RefName::new("child"))
            .as_normal()
            .unwrap();
        assert_ne!(rebased_child, child.id());
        assert_eq!(
            landed
                .store()
                .get_commit_async(rebased_child)
                .await
                .unwrap()
                .parent_ids(),
            &[new_b.id().clone()]
        );
        for (old, new) in &pairs {
            // This is the same operation-history walker used by jj evolog.
            let history: Vec<_> = walk_predecessors(&landed, std::slice::from_ref(new))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(history[0].commit.id(), new);
            assert_eq!(history[0].predecessor_ids(), &[old.clone()]);
            assert!(history.iter().any(|entry| entry.commit.id() == old));
            assert!(
                landed
                    .store()
                    .get_commit_async(new)
                    .await
                    .unwrap()
                    .store_commit()
                    .predecessors
                    .is_empty()
            );
        }
        // Replay after publication cannot overwrite history or reverse an edge.
        let mut replay = landed.start_transaction();
        assert!(matches!(
            replay.repo_mut().record_external_rewrites(&pairs).await,
            Err(jj_lib::repo::ExternalRewriteError::DestinationAlreadyRecorded(_))
        ));
        assert!(!replay.repo_mut().has_changes());
        let reverse = vec![(new_b.id().clone(), b.id().clone())];
        assert!(
            replay
                .repo_mut()
                .record_external_rewrites(&reverse)
                .await
                .is_err()
        );
        assert!(!replay.repo_mut().has_changes());

        // An operation predecessor need not be indexed. Base index freshness
        // alone must not permit X -> hidden Y to become Y -> X on import.
        let mut hidden = a.store_commit().as_ref().clone();
        hidden.parents = vec![root.id().clone()];
        hidden.change_id = ChangeId::from_bytes(&[101; 16]);
        hidden.description = "hidden predecessor-only object".into();
        let hidden = landed.store().write_commit(hidden, None).await.unwrap();
        let mut tx = landed.start_transaction();
        let successor = tx
            .repo_mut()
            .new_commit(vec![root.id().clone()], root.tree())
            .set_change_id(hidden.change_id().clone())
            .set_description("indexed successor of hidden object")
            .set_predecessors(vec![hidden.id().clone()])
            .write()
            .await
            .unwrap();
        let with_hidden_evolution = tx
            .commit("record predecessor-only evolution")
            .await
            .unwrap();
        assert!(
            !with_hidden_evolution
                .index()
                .has_id(hidden.id())
                .await
                .unwrap()
        );
        let mut tx = with_hidden_evolution.start_transaction();
        tx.repo_mut()
            .index_commits(&[hidden.clone()])
            .await
            .unwrap();
        assert!(
            matches!(tx.repo_mut().record_external_rewrites(&[(successor.id().clone(), hidden.id().clone())]).await,
            Err(jj_lib::repo::ExternalRewriteError::EvolutionCycle(id)) if id == *hidden.id())
        );
        assert!(!tx.repo_mut().has_rewrites());
        assert!(!tx.repo_mut().has_changes());

        // A pending predecessor edge can bridge two historical chains through
        // hidden intermediate commits: X -> Z (history), Z -> W (pending),
        // W -> Y (history). Importing X as Y must not close that cycle.
        let mut hidden_y = a.store_commit().as_ref().clone();
        hidden_y.parents = vec![root.id().clone()];
        hidden_y.change_id = ChangeId::from_bytes(&[105; 16]);
        hidden_y.description = "hidden pending-bridge target".into();
        let hidden_y = landed.store().write_commit(hidden_y, None).await.unwrap();
        let mut hidden_z = a.store_commit().as_ref().clone();
        hidden_z.parents = vec![root.id().clone()];
        hidden_z.change_id = ChangeId::from_bytes(&[106; 16]);
        hidden_z.description = "hidden pending-bridge intermediate".into();
        let hidden_z = landed.store().write_commit(hidden_z, None).await.unwrap();
        let mut tx = with_hidden_evolution.start_transaction();
        let x = tx
            .repo_mut()
            .new_commit(vec![root.id().clone()], root.tree())
            .set_change_id(hidden_y.change_id().clone())
            .set_description("bridge source X")
            .set_predecessors(vec![hidden_z.id().clone()])
            .write()
            .await
            .unwrap();
        let w = tx
            .repo_mut()
            .new_commit(vec![root.id().clone()], root.tree())
            .set_description("bridge source W")
            .set_predecessors(vec![hidden_y.id().clone()])
            .write()
            .await
            .unwrap();
        let bridge_base = tx
            .commit("record independent hidden predecessor chains")
            .await
            .unwrap();
        assert!(!bridge_base.index().has_id(hidden_y.id()).await.unwrap());
        assert!(!bridge_base.index().has_id(hidden_z.id()).await.unwrap());
        let mut tx = bridge_base.start_transaction();
        // This exact object is not indexed yet, so the public builder can
        // legitimately attach a new operation evolution edge without rewriting
        // an existing source or changing the canonical object bytes.
        let z = tx
            .repo_mut()
            .new_commit(vec![root.id().clone()], root.tree())
            .set_change_id(hidden_z.change_id().clone())
            .set_description(hidden_z.description())
            .set_author(hidden_z.author().clone())
            .set_committer(hidden_z.committer().clone())
            .set_predecessors(vec![w.id().clone()])
            .write()
            .await
            .unwrap();
        assert_eq!(z.id(), hidden_z.id());
        tx.repo_mut()
            .index_commits(&[hidden_y.clone()])
            .await
            .unwrap();
        assert!(!tx.repo_mut().has_rewrites());
        assert!(
            matches!(tx.repo_mut().record_external_rewrites(&[(x.id().clone(), hidden_y.id().clone())]).await,
            Err(jj_lib::repo::ExternalRewriteError::EvolutionCycle(id)) if id == *hidden_y.id())
        );
        assert!(!tx.repo_mut().has_rewrites());
        let after_rejection = tx
            .commit("preserve original pending evolution")
            .await
            .unwrap();
        assert_eq!(
            after_rejection.operation().predecessors_for_commit(z.id()),
            Some(&[w.id().clone()][..])
        );
        assert!(
            after_rejection
                .operation()
                .predecessors_for_commit(hidden_y.id())
                .is_none()
        );
    });
}
