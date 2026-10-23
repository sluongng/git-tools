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
    let worker = provider.clone();
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
