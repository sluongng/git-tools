//! Private broker adapter. Never calls back into broker RPCs or substitutes a
//! host-wide last-seen generation for the workspace's selected snapshot/intent.
use crate::{PublishRequest, RemoteRepositoryControl};
use async_trait::async_trait;
use native_broker::{
    AdmittedCommit, ConfirmedPublication, ControlResources, NativeRepositoryControl,
    ReferenceSnapshot,
};
use tinytree_model::CommitId;
use tonic::Status;
fn status(error: anyhow::Error) -> Status {
    if let Some(status) = error.downcast_ref::<Status>() {
        return Status::new(status.code(), "native repository control operation failed");
    }
    Status::internal("native repository control operation failed")
}
fn publication(value: crate::ConfirmedPublication) -> ConfirmedPublication {
    ConfirmedPublication {
        idempotency_key: value.idempotency_key,
        name: value.reference_name,
        generation: value.generation,
        head: value.head,
    }
}
async fn drained<T>(
    control: &RemoteRepositoryControl,
    result: anyhow::Result<T>,
) -> Result<T, Status> {
    // A reentrant real-control caller already owns the drain barrier. On a
    // rejected foreign-control call no work was accepted by this adapter. In
    // either case waiting here could self-deadlock or create a cross-alias cycle.
    if !crate::io::scope().is_some_and(|scope| scope.has_control()) {
        control.drain_control().await;
    }
    result.map_err(status)
}
#[async_trait]
impl NativeRepositoryControl for RemoteRepositoryControl {
    fn resources(&self) -> ControlResources {
        self.inner.config.control_work_budget()
    }
    fn landing_resources(&self) -> Option<ControlResources> {
        let mut r = self.inner.config.control_work_budget();
        // Local unary request decoding completes before handler dispatch; thin
        // staging then upstream status run sequentially. Response encoding owns
        // only the compact retained result, not the completed staging frontier
        // or upstream RPC decoder. Keep all three peaks under one host owner.
        r.memory_bytes = r
            .memory_bytes
            .max(256 * 1024 * 1024)
            .saturating_add(2 * 1024 * 1024);
        Some(r)
    }
    async fn landing_with_owner(
        &self,
        intent: native_broker::LandingIntent,
        submit: bool,
        owner: native_broker::CommandGuard,
    ) -> Result<native_broker::LandingReport, Status> {
        self.landing_owned(intent, submit, owner)
            .await
            .map_err(status)
    }

    async fn observe_reference_with_owner(
        &self,
        name: &str,
        owner: native_broker::CommandGuard,
    ) -> Result<ReferenceSnapshot, Status> {
        let result = self
            .run_owned(
                self.inner.deadline(),
                owner,
                RemoteRepositoryControl::observe_reference(self, name),
            )
            .await;
        let observed = drained(self, result).await?;
        Ok(ReferenceSnapshot {
            name: observed.name,
            generation: observed.generation,
            head: observed.head.map(|head| AdmittedCommit {
                head: head.head,
                metadata_objects: head.metadata_objects as u64,
            }),
        })
    }
    async fn admit_commit_with_owner(
        &self,
        head: CommitId,
        owner: native_broker::CommandGuard,
    ) -> Result<AdmittedCommit, Status> {
        if head == CommitId::ROOT {
            return Err(Status::invalid_argument(
                "intrinsic root requires no native remote admission",
            ));
        }
        let result = self
            .run_owned(
                self.inner.deadline(),
                owner,
                RemoteRepositoryControl::admit_commit(self, head),
            )
            .await;
        let admitted = drained(self, result).await?;
        Ok(AdmittedCommit {
            head: admitted.head,
            metadata_objects: admitted.metadata_objects as u64,
        })
    }
    async fn publish_with_owner(
        &self,
        request: native_broker::PublishRequest,
        owner: native_broker::CommandGuard,
    ) -> Result<ConfirmedPublication, Status> {
        let result = self
            .run_owned(
                self.inner.deadline(),
                owner,
                RemoteRepositoryControl::publish(
                    self,
                    PublishRequest {
                        head: request.head,
                        reference_name: request.name,
                        expected_generation: request.expected_generation,
                        idempotency_key: request.idempotency_key,
                    },
                ),
            )
            .await;
        drained(self, result).await.map(publication)
    }
    async fn publication_with_owner(
        &self,
        key: &str,
        owner: native_broker::CommandGuard,
    ) -> Result<Option<ConfirmedPublication>, Status> {
        let result = self
            .run_owned(
                self.inner.deadline(),
                owner,
                RemoteRepositoryControl::publication(self, key),
            )
            .await;
        drained(self, result)
            .await
            .map(|value| value.map(publication))
    }

    async fn observe_reference(&self, name: &str) -> Result<ReferenceSnapshot, Status> {
        let result = RemoteRepositoryControl::observe_reference(self, name).await;
        let observed = drained(self, result).await?;
        Ok(ReferenceSnapshot {
            name: observed.name,
            generation: observed.generation,
            head: observed.head.map(|head| AdmittedCommit {
                head: head.head,
                metadata_objects: head.metadata_objects as u64,
            }),
        })
    }
    async fn admit_commit(&self, head: CommitId) -> Result<AdmittedCommit, Status> {
        if head == CommitId::ROOT {
            return Err(Status::invalid_argument(
                "intrinsic root requires no native remote admission",
            ));
        }
        let result = RemoteRepositoryControl::admit_commit(self, head).await;
        let admitted = drained(self, result).await?;
        Ok(AdmittedCommit {
            head: admitted.head,
            metadata_objects: admitted.metadata_objects as u64,
        })
    }
    async fn publish(
        &self,
        request: native_broker::PublishRequest,
    ) -> Result<ConfirmedPublication, Status> {
        let result = RemoteRepositoryControl::publish(
            self,
            PublishRequest {
                head: request.head,
                reference_name: request.name,
                expected_generation: request.expected_generation,
                idempotency_key: request.idempotency_key,
            },
        )
        .await;
        drained(self, result).await.map(publication)
    }
    async fn publication(&self, id: &str) -> Result<Option<ConfirmedPublication>, Status> {
        let result = RemoteRepositoryControl::publication(self, id).await;
        drained(self, result)
            .await
            .map(|value| value.map(publication))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, RemoteRepository};
    use jj_native_backend::DirectObjectIo;
    use local_cas::{Artifact, BlobSource, LocalCas};
    use std::{
        sync::{Arc, Condvar, Mutex},
        time::Duration,
    };
    struct UnusedSource;
    #[async_trait]
    impl BlobSource for UnusedSource {
        async fn fetch(&self, _: &Artifact, _: &mut std::fs::File) -> anyhow::Result<()> {
            anyhow::bail!("unexpected test fetch")
        }
        async fn missing(&self, a: &[Artifact]) -> anyhow::Result<Vec<Artifact>> {
            Ok(a.to_vec())
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_physical_worker_keeps_adapter_and_other_alias_budget_draining() {
        let root = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let local = root.path().join("local");
        std::fs::create_dir(&local).unwrap();
        let direct = Arc::new(DirectObjectIo::init(&local, Default::default()).unwrap());
        let cache = LocalCas::open(
            root.path().join("cache"),
            Arc::new(UnusedSource),
            Default::default(),
        )
        .unwrap();
        let repo = RemoteRepository::open(
            direct,
            cache,
            &root.path().join("ledger"),
            &root.path().join("staging"),
            Config::default(),
        )
        .unwrap();
        // One permit represents the entire broker work pool reserved by control.
        let global = Arc::new(tokio::sync::Semaphore::new(1));
        let held = global.clone().acquire_owned().await.unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let worker_gate = gate.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let control = repo.control.clone();
        let adapter = tokio::spawn(async move {
            let _global = held;
            let deadline = tokio::time::Instant::now() + Duration::from_millis(20);
            let result: Result<(), anyhow::Error> = control
                .run(deadline, async move {
                    let scope = crate::io::scope().unwrap();
                    let worker = tokio::spawn(crate::io::control_scope(scope, async move {
                        crate::io::blocking(move || {
                            let _ = entered.send(());
                            let (lock, notify) = &*worker_gate;
                            let mut released = lock.lock().unwrap();
                            while !*released {
                                released = notify.wait(released).unwrap();
                            }
                            Ok(())
                        })
                        .await
                    }));
                    started.await.unwrap();
                    worker.abort();
                    let _ = worker.await;
                    Err(Status::deadline_exceeded("controlled native operation timeout").into())
                })
                .await;
            drained(&control, result).await
        });
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            !adapter.is_finished(),
            "trait must not return/release global admission while a cancelled worker is physically alive"
        );
        assert!(
            global.clone().try_acquire_owned().is_err(),
            "another alias must remain blocked by the shared broker pool"
        );
        let (lock, notify) = &*gate;
        *lock.lock().unwrap() = true;
        notify.notify_all();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), adapter)
                .await
                .unwrap()
                .unwrap()
                .unwrap_err()
                .code(),
            tonic::Code::DeadlineExceeded
        );
        assert!(global.try_acquire_owned().is_ok());
    }
}
