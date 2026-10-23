use super::*;

#[derive(Debug)]
struct HeldUpload {
    delegate: Arc<CountingIo>,
    completed_physical_write: tokio::sync::Notify,
    release_return: tokio::sync::Semaphore,
    metadata_writes: AtomicUsize,
    fail_return: bool,
}
#[async_trait]
impl NativeObjectIo for HeldUpload {
    async fn descriptor(&self, key: m::ObjectKey) -> BackendResult<Option<m::Descriptor>> {
        self.delegate.descriptor(key).await
    }
    async fn read_metadata(&self, key: m::ObjectKey) -> BackendResult<VerifiedMetadata> {
        self.delegate.read_metadata(key).await
    }
    async fn write_metadata(&self, object: m::Object) -> BackendResult<m::Descriptor> {
        self.metadata_writes.fetch_add(1, Ordering::SeqCst);
        self.delegate.write_metadata(object).await
    }
    async fn read_file(&self, id: m::FileId) -> BackendResult<FileRead> {
        self.delegate.read_file(id).await
    }
    async fn write_file(
        &self,
        input: &mut (dyn AsyncRead + Send + Unpin),
        max: u64,
    ) -> BackendResult<m::FileId> {
        // A real native write has completed, but its owned provider future has
        // not returned. Admission revocation must not cancel this future.
        let result = self.delegate.write_file(input, max).await;
        self.completed_physical_write.notify_one();
        let _permit = self
            .release_return
            .acquire()
            .await
            .map_err(|error| jj_lib::backend::BackendError::Other(Box::new(error)))?;
        if self.fail_return {
            return Err(jj_lib::backend::BackendError::Other(Box::new(
                std::io::Error::other("injected completed-upload response failure"),
            )));
        }
        result
    }
    async fn copy_catalog_snapshot(
        &self,
        limits: CopyCatalogLimits,
    ) -> BackendResult<CopyCatalogSnapshot> {
        self.delegate.copy_catalog_snapshot(limits).await
    }
}

async fn open_with_provider(
    fixture: &Fixture,
    provider: Arc<dyn NativeObjectIo>,
) -> Result<Controller> {
    Controller::open_host(
        Host::Protocol {
            tree: fixture.tree.clone(),
            compiler: fixture.compiler.clone(),
            root: fixture.root.clone(),
            state: fixture.state.clone(),
            identity: [7; 32],
            backend: fixture.scope.backend().into(),
        },
        provider,
        Config::new("fixture".into(), fixture.working_copy.clone()),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_admission_does_not_prepare_or_publish_and_keeps_the_original_deadline() {
    let fixture = Fixture::new(false).await;
    let controller = fixture.open(None).await;
    let original = controller.inspect().unwrap();
    let revoked = Arc::new(AtomicBool::new(false));
    let deadline = tokio::time::Instant::now();
    let scope = ActionScope::new(deadline, revoked.clone());
    assert_eq!(
        scope.bounded(deadline + Duration::from_secs(10)).deadline(),
        deadline
    );
    let mut mutation = controller.begin_session(&original).await.unwrap();
    mutation.set_admission_scope(scope);
    assert!(matches!(
        mutation.snapshot("expired", &options()).await,
        Err(ProjectionFailure::Rejected(_))
    ));
    assert!(controller.inner.ledger.lock().unwrap().requests.is_empty());
    assert_eq!(fixture.tree.source_baseline().unwrap().0, fixture.base.0);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
    revoked.store(true, Ordering::Release);
    assert_eq!(
        mutation
            .action_scope()
            .check()
            .unwrap_err()
            .downcast_ref::<ActionScopeError>(),
        Some(&ActionScopeError::Revoked)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_revocation_drains_the_admitted_upload_before_stopping_later_logical_writes() {
    let fixture = Fixture::new(false).await;
    fixture.local("new", b"owned physical payload").await;
    let provider = Arc::new(HeldUpload {
        delegate: fixture.provider.clone(),
        completed_physical_write: tokio::sync::Notify::new(),
        release_return: tokio::sync::Semaphore::new(0),
        metadata_writes: AtomicUsize::new(0),
        fail_return: false,
    });
    let controller = open_with_provider(&fixture, provider.clone())
        .await
        .unwrap();
    let before = controller.inspect().unwrap();
    let revoked = Arc::new(AtomicBool::new(false));
    let owned_revoked = revoked.clone();
    let owned_controller = controller.clone();
    let task = tokio::spawn(async move {
        let mut mutation = owned_controller.begin_session(&before).await.unwrap();
        mutation.set_admission_scope(ActionScope::new(
            tokio::time::Instant::now() + Duration::from_secs(30),
            owned_revoked,
        ));
        mutation.snapshot("revoked-after-upload", &options()).await
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        provider.completed_physical_write.notified(),
    )
    .await
    .unwrap();
    revoked.store(true, Ordering::Release);
    assert!(
        !task.is_finished(),
        "EOF must not abort a provider's owned physical future"
    );
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            controller.receipt("revoked-after-upload")
        )
        .await
        .is_err(),
        "recovery must wait for the command/physical session to drain"
    );
    provider.release_return.add_permits(1);
    assert!(matches!(
        task.await.unwrap(),
        Err(ProjectionFailure::Ambiguous(_))
    ));
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider.metadata_writes.load(Ordering::SeqCst),
        0,
        "no tree/metadata stage may be admitted after revocation"
    );
    assert_eq!(fixture.tree.source_baseline().unwrap().0, fixture.base.0);
    assert!(controller.requires_reconciliation());
    {
        let ledger = controller.inner.ledger.lock().unwrap();
        let intent = ledger.requests.get("revoked-after-upload").unwrap();
        assert!(
            !intent.rejected && intent.receipt.is_none(),
            "expired prepared work must retain an uncertain durable intent"
        );
    }
    assert!(
        matches!(
            controller.receipt("revoked-after-upload").await.unwrap(),
            ProjectionOutcome::NotApplied
        ),
        "only drained, fenced, unchanged-baseline recovery may seal nonapplication"
    );
    assert!(!controller.requires_reconciliation());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_rejected_receipt_does_not_wait_for_its_own_live_mutation() {
    let fixture = Fixture::new(false).await;
    fixture
        .local("new", b"uploaded before response failure")
        .await;
    let provider = Arc::new(HeldUpload {
        delegate: fixture.provider.clone(),
        completed_physical_write: tokio::sync::Notify::new(),
        release_return: tokio::sync::Semaphore::new(1),
        metadata_writes: AtomicUsize::new(0),
        fail_return: true,
    });
    let controller = open_with_provider(&fixture, provider).await.unwrap();
    let before = controller.inspect().unwrap();
    let mut mutation = controller.begin_session(&before).await.unwrap();
    assert!(matches!(
        mutation.snapshot("terminal-rejected", &options()).await,
        Err(ProjectionFailure::Rejected(_))
    ));
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 1);
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(2),
            controller.receipt("terminal-rejected")
        )
        .await
        .unwrap()
        .unwrap(),
        ProjectionOutcome::NotApplied
    ));
    // Keep the command guard alive through the metadata query.
    drop(mutation);
    assert_eq!(fixture.tree.source_baseline().unwrap().0, fixture.base.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn prepared_only_restart_has_a_safe_sealed_abort_instead_of_a_permanent_unknown_trap() {
    let fixture = Fixture::new(false).await;
    let controller = fixture.open(None).await;
    let before = controller.inspect().unwrap();
    let frozen = fixture
        .tree
        .freeze_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    controller
        .prepare(
            "prepared-only",
            &before,
            Action::Snapshot { policy: [1; 32] },
            frozen.version().clone(),
        )
        .unwrap();
    drop(frozen);
    drop(controller);
    // Models process loss after the durable intent, before any namespace call.
    // It does not claim hardware power-loss acceptance.
    let controller = fixture.open(None).await;
    assert!(matches!(
        controller.receipt("prepared-only").await.unwrap(),
        ProjectionOutcome::NotApplied
    ));
    assert_eq!(controller.inspect().unwrap(), before);
    assert!(controller.begin_session(&before).await.is_ok());
    assert!(matches!(
        controller.receipt("never-admitted").await.unwrap(),
        ProjectionOutcome::Unknown
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_receipt_with_a_changed_generation_cannot_be_sealed_as_not_applied() {
    let fixture = Fixture::new(false).await;
    let controller = fixture.open(None).await;
    let before = controller.inspect().unwrap();
    let frozen = fixture
        .tree
        .freeze_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
        .unwrap();
    let expected = frozen.version().clone();
    controller
        .prepare(
            "missing-proof",
            &before,
            Action::Snapshot { policy: [2; 32] },
            expected.clone(),
        )
        .unwrap();
    // An explicitly artificial concurrent-writer boundary: even an unchanged
    // root with a different control generation is not the pre-intent state.
    frozen
        .acknowledge_root(
            fixture.base.0,
            expected,
            "other-writer",
            &SourceLimits::default(),
        )
        .unwrap();
    let ledger = fs::read(fixture.state.join("jj-controller/ledger.json")).unwrap();
    drop(controller);
    assert!(
        open_with_provider(&fixture, fixture.provider.clone())
            .await
            .is_err()
    );
    assert_eq!(
        fs::read(fixture.state.join("jj-controller/ledger.json")).unwrap(),
        ledger,
        "an unavailable proof must never repair or overwrite the retained intent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_sealed_noops_are_acknowledged_even_when_root_and_operation_do_not_change() {
    let fixture = Fixture::new(false).await;
    let mut config = Config::new("fixture".into(), fixture.working_copy.clone());
    config.max_pending_requests = 1;
    let controller = fixture.open(Some(config)).await;
    let before = controller.inspect().unwrap();
    for index in 0..4 {
        let id = format!("sealed-noop-{index}");
        let frozen = fixture
            .tree
            .freeze_source(SourceFrontendFence::TreeOnly, &SourceLimits::default())
            .unwrap();
        controller
            .prepare(
                &id,
                &before,
                Action::Snapshot { policy: [4; 32] },
                frozen.version().clone(),
            )
            .unwrap();
        drop(frozen);
        assert!(matches!(
            controller.receipt(&id).await.unwrap(),
            ProjectionOutcome::NotApplied
        ));
        controller
            .acknowledge(&before, &operation(), None)
            .await
            .unwrap();
        assert!(
            controller.inner.ledger.lock().unwrap().requests.is_empty(),
            "a prior identical ACK must not hide newly sealed no-ops"
        );
    }
    assert_eq!(controller.inspect().unwrap(), before);
    assert_eq!(fixture.provider.uploads.load(Ordering::SeqCst), 0);
}
