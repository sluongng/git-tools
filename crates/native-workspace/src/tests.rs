use super::*;
use futures::{TryStreamExt, io::Cursor};
use jj_lib::backend::{BackendError, BackendResult};
use jj_native_backend::{CopyCatalogLimits, CopyCatalogSnapshot, FileRead, VerifiedMetadata};
use output_core::{CacheConfig, LocalCas, SourceLimits, Tree};
use std::sync::{Mutex, atomic::AtomicUsize};

fn error() -> BackendError {
    BackendError::Other(Box::new(std::io::Error::other("fixture unavailable")))
}
#[derive(Debug, Default)]
struct Provider {
    metadata: Mutex<HashMap<m::ObjectKey, (m::Descriptor, Vec<u8>)>>,
    files: Mutex<HashMap<m::FileId, (m::Descriptor, Vec<u8>)>>,
    file_reads: AtomicUsize,
    metadata_reads: AtomicUsize,
    read_gate: Mutex<Option<Arc<Semaphore>>>,
    copy_gate: Mutex<Option<Arc<CopyGate>>>,
}
#[derive(Debug)]
struct CopyGate {
    entered: tokio::sync::Notify,
    release: Semaphore,
}
impl Default for CopyGate {
    fn default() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            release: Semaphore::new(0),
        }
    }
}

impl Provider {
    fn object(&self, object: m::Object) -> m::Descriptor {
        let encoded = m::encode(&object, m::Limits::default()).unwrap();
        self.metadata
            .lock()
            .unwrap()
            .insert(encoded.descriptor.key, (encoded.descriptor, encoded.bytes));
        encoded.descriptor
    }
    fn file(&self, bytes: &[u8]) -> m::Descriptor {
        let d = m::describe_file(bytes);
        self.files
            .lock()
            .unwrap()
            .insert(m::FileId(d.key.id.0), (d, bytes.to_vec()));
        d
    }
}
#[async_trait]
impl NativeObjectIo for Provider {
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        Ok(if key.kind == m::Kind::File {
            self.files
                .lock()
                .unwrap()
                .get(&m::FileId(key.id.0))
                .map(|v| v.0)
        } else {
            self.metadata.lock().unwrap().get(&key).map(|v| v.0)
        })
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.metadata_reads.fetch_add(1, Ordering::SeqCst);
        let (d, b) = self
            .metadata
            .lock()
            .unwrap()
            .get(&key)
            .cloned()
            .ok_or_else(error)?;
        VerifiedMetadata::verify(d, b, m::Limits::default())
    }
    async fn write_metadata(&self, _: m::Object) -> BackendResult<m::Descriptor> {
        Err(error())
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        self.file_reads.fetch_add(1, Ordering::SeqCst);
        let (d, b) = self
            .files
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(error)?;
        let gate = self.read_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            let _permit = gate.acquire_owned().await.map_err(|_| error())?;
        }
        let copy_gate = self.copy_gate.lock().unwrap().clone();
        if let Some(gate) = copy_gate {
            let stream = futures::stream::once(async move {
                gate.entered.notify_one();
                let _permit = gate
                    .release
                    .acquire()
                    .await
                    .map_err(|_| std::io::Error::other("copy gate closed"))?;
                Ok::<_, std::io::Error>(b)
            });
            FileRead::from_preverified_pinned_source(d, Box::pin(stream.into_async_read()))
        } else {
            FileRead::from_preverified_pinned_source(d, Box::pin(Cursor::new(b)))
        }
    }
    async fn read_file_for_cache(
        &self,
        id: m::FileId,
        _owner: Arc<dyn std::any::Any + Send + Sync>,
    ) -> BackendResult<FileRead> {
        self.read_file(id).await
    }
    async fn write_file(
        &self,
        _: &mut (dyn futures::AsyncRead + Unpin + Send),
        _: u64,
    ) -> BackendResult<m::FileId> {
        Err(error())
    }
    async fn copy_catalog_snapshot(
        &self,
        l: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        CopyCatalogSnapshot::from_verified(vec![], l)
    }
}
fn scope() -> Scope {
    Scope::from_configured_identity("repository", b"configured-auth-profile").unwrap()
}
fn file_entry(
    name: &str,
    d: m::Descriptor,
    executable: bool,
    copy_id: Option<m::CopyId>,
) -> m::TreeEntry {
    m::TreeEntry {
        name: name.into(),
        value: m::TreeValue::File {
            id: m::FileId(d.key.id.0),
            executable,
            size_bytes: d.size_bytes,
            copy_id,
        },
    }
}
struct Fixture {
    provider: Arc<Provider>,
    root: m::TreeId,
    executable: m::Descriptor,
    unused: m::Descriptor,
    copy: m::CopyId,
}
fn fixture() -> Fixture {
    let provider = Arc::new(Provider::default());
    let executable = provider.file(b"#!/bin/sh\necho native\n");
    let unused = provider.file(b"not accessed during startup");
    let link = provider.object(m::Object::Symlink("bin/tool".into()));
    let copy = m::CopyId([8; 32]);
    let child = provider.object(m::Object::Tree(m::Tree {
        entries: vec![file_entry("tool", executable, true, Some(copy))],
    }));
    let root = provider.object(m::Object::Tree(m::Tree {
        entries: vec![
            m::TreeEntry {
                name: "bin".into(),
                value: m::TreeValue::Tree(m::TreeId(child.key.id.0)),
            },
            m::TreeEntry {
                name: "link".into(),
                value: m::TreeValue::Symlink {
                    id: m::SymlinkId(link.key.id.0),
                    target_bytes: 8,
                },
            },
            file_entry("unused", unused, false, None),
        ],
    }));
    Fixture {
        provider,
        root: m::TreeId(root.key.id.0),
        executable,
        unused,
        copy,
    }
}
async fn install(
    directory: &std::path::Path,
    scope: &Scope,
    compiled: &CompiledWorkspace,
) -> Arc<Tree> {
    let cache = LocalCas::open(
        directory.join("cache"),
        compiled.source(),
        CacheConfig::default(),
    )
    .unwrap();
    let tree = Tree::open_with_cache_and_source(
        directory.join("namespace"),
        scope.backend(),
        cache,
        compiled.source(),
    )
    .unwrap();
    tree.install_source(
        compiled.manifest(tree.identity()),
        None,
        "initial",
        &SourceLimits::default(),
    )
    .unwrap();
    tree
}

#[tokio::test]
async fn metadata_only_install_stat_modes_symlinks_then_one_demand_read() {
    let f = fixture();
    let scope = scope();
    let compiler = Compiler::new(f.provider.clone(), scope.clone(), Limits::default()).unwrap();
    let compiled = compiler.compile_tree(f.root).await.unwrap();
    assert_eq!(compiled.file_provenance()["bin/tool"].copy_id, Some(f.copy));
    assert_eq!(compiled.root_merge().terms, vec![f.root]);
    assert_eq!(compiled.tree_provenance()[""], f.root);
    assert!(compiled.tree_provenance().contains_key("bin"));
    assert_eq!(f.provider.file_reads.load(Ordering::SeqCst), 0);
    let directory = tempfile::tempdir().unwrap();
    let tree = install(directory.path(), &scope, &compiled).await;
    let tool = tree.resolve("bin/tool").unwrap();
    let unused = tree.resolve("unused").unwrap();
    assert_eq!(tool.mode & 0o777, 0o755);
    assert_eq!(unused.mode & 0o777, 0o644);
    assert_eq!(tool.size, f.executable.size_bytes);
    assert_eq!(
        tree.readlink(tree.resolve("link").unwrap().id).unwrap(),
        "bin/tool"
    );
    assert!(tree.stat("bin/tool", 9).await.unwrap().is_some());
    assert_eq!(f.provider.file_reads.load(Ordering::SeqCst), 0);
    assert_eq!(
        tree.read(tool.id, 0, 100).await.unwrap(),
        b"#!/bin/sh\necho native\n"
    );
    assert_eq!(f.provider.file_reads.load(Ordering::SeqCst), 1);
    assert_eq!(
        tree.read(tool.id, 0, 100).await.unwrap(),
        b"#!/bin/sh\necho native\n"
    );
    assert_eq!(f.provider.file_reads.load(Ordering::SeqCst), 1);
    assert_eq!(unused.remote.as_ref().unwrap().size, f.unused.size_bytes);
}

#[tokio::test]
async fn wrong_scope_size_or_nonmanifest_artifact_is_rejected_before_provider_read() {
    let f = fixture();
    let compiled = Compiler::new(f.provider.clone(), scope(), Limits::default())
        .unwrap()
        .compile_tree(f.root)
        .await
        .unwrap();
    let source = compiled.source();
    let good = scope().artifact(f.executable);
    let mut cases = vec![];
    let mut changed = good.clone();
    changed.backend.push('x');
    cases.push(changed);
    let mut changed = good.clone();
    changed.instance.push('x');
    cases.push(changed);
    let mut changed = good.clone();
    changed.size += 1;
    cases.push(changed);
    let mut changed = good.clone();
    changed.digest_function = 1;
    cases.push(changed);
    cases.push(scope().artifact(m::describe_file(b"not a manifest member")));
    for artifact in cases {
        let mut target = tempfile::tempfile().unwrap();
        assert!(source.fetch(&artifact, &mut target).await.is_err());
        assert_eq!(target.metadata().unwrap().len(), 0);
        assert!(source.missing(&[artifact]).await.is_err());
    }
    assert_eq!(f.provider.file_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn corrupt_file_source_never_publishes_a_successful_cache_hit() {
    let f = fixture();
    let compiled = Compiler::new(f.provider.clone(), scope(), Limits::default())
        .unwrap()
        .compile_tree(f.root)
        .await
        .unwrap();
    // Deliberate malicious-provider compatibility fixture: FileRead's constructor
    // attests verification but cannot itself verify a streaming source.
    f.provider
        .files
        .lock()
        .unwrap()
        .get_mut(&m::FileId(f.executable.key.id.0))
        .unwrap()
        .1[0] ^= 1;
    let directory = tempfile::tempdir().unwrap();
    let cache = LocalCas::open(
        directory.path().join("cache"),
        compiled.source(),
        CacheConfig::default(),
    )
    .unwrap();
    let artifact = scope().artifact(f.executable);
    assert!(cache.clone().content(artifact.clone()).await.is_err());
    cache.wait_for_idle().await.unwrap();
    assert!(cache.cached_content(artifact).await.unwrap().is_none());
}

#[tokio::test]
async fn limits_and_corrupt_metadata_fail_before_changing_an_installed_namespace() {
    let f = fixture();
    let scope = scope();
    let compiled = Compiler::new(f.provider.clone(), scope.clone(), Limits::default())
        .unwrap()
        .compile_tree(f.root)
        .await
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let tree = install(directory.path(), &scope, &compiled).await;
    let original = tree.resolve("bin/tool").unwrap();
    for limits in [
        Limits {
            max_entries: 1,
            ..Limits::default()
        },
        Limits {
            max_metadata_objects: 1,
            ..Limits::default()
        },
        Limits {
            max_metadata_bytes: 1,
            ..Limits::default()
        },
        Limits {
            max_path_bytes: 3,
            ..Limits::default()
        },
        Limits {
            max_total_path_bytes: 3,
            ..Limits::default()
        },
        Limits {
            max_file_bytes: 1,
            ..Limits::default()
        },
        Limits {
            max_symlink_bytes: 1,
            ..Limits::default()
        },
    ] {
        assert!(
            Compiler::new(f.provider.clone(), scope.clone(), limits)
                .unwrap()
                .compile_tree(f.root)
                .await
                .is_err()
        );
        assert_eq!(tree.resolve("bin/tool").unwrap(), original);
    }
    let root_key = m::ObjectKey {
        kind: m::Kind::Tree,
        id: m::ObjectId(f.root.0),
    };
    f.provider
        .metadata
        .lock()
        .unwrap()
        .get_mut(&root_key)
        .unwrap()
        .1[0] ^= 1;
    assert!(
        Compiler::new(f.provider.clone(), scope, Limits::default())
            .unwrap()
            .compile_tree(f.root)
            .await
            .is_err()
    );
    assert_eq!(tree.resolve("bin/tool").unwrap(), original);
    assert_eq!(f.provider.file_reads.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn unresolved_root_merge_is_explicit_and_preserves_every_term() {
    let f = fixture();
    let compiler = Compiler::new(f.provider.clone(), scope(), Limits::default()).unwrap();
    let merge = m::TreeMerge {
        terms: vec![f.root, m::TreeId([4; 32]), f.root],
        labels: vec!["left".into(), "base".into(), "right".into()],
    };
    let error = compiler.compile_merge(&merge).await.err().unwrap();
    assert_eq!(
        error.downcast_ref::<UnresolvedMerge>().unwrap().merge,
        merge
    );
    assert_eq!(f.provider.metadata_reads.load(Ordering::SeqCst), 0);
    assert_eq!(f.provider.file_reads.load(Ordering::SeqCst), 0);
}

#[test]
fn configured_security_identity_uses_length_delimited_scopes() {
    let first = Scope::from_configured_identity("a", b"bc").unwrap();
    let second = Scope::from_configured_identity("ab", b"c").unwrap();
    assert_ne!(first.backend(), second.backend());
    assert_ne!(
        first.backend(),
        Scope::from_configured_identity("a", b"different-principal")
            .unwrap()
            .backend()
    );
}

#[tokio::test]
async fn later_compilations_share_one_source_and_failed_registration_is_atomic() {
    let provider = Arc::new(Provider::default());
    let first = provider.file(b"first generation");
    let second = provider.file(b"second generation");
    let third = provider.file(b"not admitted past catalog limit");
    let make_root = |name: &str, d| {
        m::TreeId(
            provider
                .object(m::Object::Tree(m::Tree {
                    entries: vec![file_entry(name, d, false, None)],
                }))
                .key
                .id
                .0,
        )
    };
    let root1 = make_root("first", first);
    let root2 = make_root("second", second);
    let root3 = make_root("third", third);
    let compiler = Compiler::new(
        provider.clone(),
        scope(),
        Limits {
            max_authorized_files: 2,
            ..Limits::default()
        },
    )
    .unwrap();
    let compiled1 = compiler.compile_tree(root1).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let tree = install(directory.path(), &scope(), &compiled1).await;
    let old_node = tree.resolve("first").unwrap();
    let old_pin = tree.pin(old_node.id).unwrap();
    let snapshot = tree
        .freeze_source(
            output_core::SourceFrontendFence::TreeOnly,
            &SourceLimits::default(),
        )
        .unwrap();
    let compiled2 = compiler.compile_tree(root2).await.unwrap();
    assert!(Arc::ptr_eq(&compiled1.source(), &compiled2.source()));
    snapshot
        .install(
            compiled2.manifest(tree.identity()),
            "next",
            &SourceLimits::default(),
        )
        .unwrap();
    assert_eq!(
        tree.read(tree.resolve("second").unwrap().id, 0, 100)
            .await
            .unwrap(),
        b"second generation"
    );
    assert_eq!(
        tree.read(old_node.id, 0, 100).await.unwrap(),
        b"first generation"
    );
    drop(old_pin);
    assert!(compiler.compile_tree(root3).await.is_err());
    assert!(
        compiler
            .source()
            .missing(&[scope().artifact(third)])
            .await
            .is_err()
    );
    assert_eq!(
        tree.resolve("second").unwrap().remote.unwrap(),
        scope().artifact(second)
    );
    assert_eq!(provider.file_reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelled_provider_read_keeps_physical_guard_and_transfer_admission_until_drained() {
    let f = fixture();
    let gate = Arc::new(Semaphore::new(0));
    *f.provider.read_gate.lock().unwrap() = Some(gate.clone());
    let guard = Arc::new(());
    let weak = Arc::downgrade(&guard);
    let compiler = Compiler::new(
        f.provider.clone(),
        scope(),
        Limits {
            max_file_transfers: 1,
            ..Limits::default()
        },
    )
    .unwrap()
    .with_lifetime_guard(guard);
    let compiled = compiler.compile_tree(f.root).await.unwrap();
    let source = compiled.source();
    let artifact = scope().artifact(f.executable);
    let source_for_request = source.clone();
    let request = tokio::spawn(async move {
        let mut target = tempfile::tempfile().unwrap();
        source_for_request.fetch(&artifact, &mut target).await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.provider.file_reads.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert_eq!(source.transfers.available_permits(), 0);
    drop(compiled);
    drop(compiler);
    drop(source);
    assert!(
        weak.upgrade().is_some(),
        "cancelled provider task must still own the physical guard"
    );
    gate.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_timeout_during_physical_copy_retains_temp_reservation_and_ownership_until_drain() {
    let f = fixture();
    let gate = Arc::new(CopyGate::default());
    *f.provider.copy_gate.lock().unwrap() = Some(gate.clone());
    let compiler = Compiler::new(
        f.provider.clone(),
        scope(),
        Limits {
            operation_timeout_seconds: 1,
            ..Limits::default()
        },
    )
    .unwrap();
    let compiled = compiler.compile_tree(f.root).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("cache");
    let config = CacheConfig {
        fetch_timeout: Duration::from_millis(50),
        operation_timeout: Duration::from_millis(100),
        ..CacheConfig::default()
    };
    let source = compiled.source();
    let cache = LocalCas::open(&path, source.clone(), config.clone()).unwrap();
    let weak = Arc::downgrade(&cache);
    let artifact = scope().artifact(f.executable);
    let request = tokio::spawn(cache.clone().content(artifact.clone()));
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), request)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    // Both the source deadline and cache deadline expire while the actual
    // pinned reader is blocked inside the owned physical copy job.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(cache.metrics().reserved_bytes, f.executable.size_bytes);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), cache.wait_for_idle())
            .await
            .is_err()
    );
    assert!(
        std::fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.path().extension().is_some_and(|v| v == "tmp"))
    );
    drop(cache);
    assert!(
        LocalCas::open(&path, source.clone(), config.clone()).is_err(),
        "a detached physical copy must retain the original cache owner"
    );
    gate.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let cache = LocalCas::open(&path, source, config).unwrap();
    assert_eq!(cache.metrics().reserved_bytes, 0);
    assert!(
        cache
            .clone()
            .cached_content(artifact)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !std::fs::read_dir(&path)
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.path().extension().is_some_and(|v| v == "tmp"))
    );
}
