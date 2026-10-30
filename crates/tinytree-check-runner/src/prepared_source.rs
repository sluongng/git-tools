//! Explicit prepared execution sources. Snapshot materialization is not itself
//! read-only enforcement; the pinned namespace helper owns that proof.
use crate::candidate_http::CandidateClient;
use crate::snapshot_linux::{HelperPlan, read_completed_proof};
use crate::source::{MaterializedSnapshot, materialize};
use crate::{Config, Request};
use anyhow::{Context, Result, ensure};
use std::path::{Path, PathBuf};
use tinytree_forge_core::checker::CheckerLease;
use tokio::{sync::watch, time::Instant};

pub(crate) enum PreparedSource {
    NativeFuse {
        root: PathBuf,
    },
    HttpsSnapshot {
        snapshot: MaterializedSnapshot,
        plan: HelperPlan,
    },
}
impl PreparedSource {
    pub(crate) fn native(root: PathBuf) -> Self {
        Self::NativeFuse { root }
    }
    pub(crate) async fn https(
        config: &Config,
        request: &Request,
        lease: &CheckerLease,
        client: &CandidateClient,
        attempt: &Path,
        cancelled: &mut watch::Receiver<bool>,
        deadline: Instant,
        arguments: Vec<String>,
    ) -> Result<Self> {
        let source = config
            .https_snapshot
            .as_ref()
            .context("snapshot source is unavailable")?;
        lease.validate()?;
        ensure!(&lease.request == request, "snapshot lease request differs");
        let snapshot = materialize(
            client,
            lease,
            &attempt.join("source-staging"),
            &attempt.join("snapshot.json"),
            &source.limits,
            cancelled,
            deadline,
        )
        .await?;
        ensure!(
            snapshot.root == attempt.join("source-staging")
                && snapshot.manifest_path == attempt.join("snapshot.json")
                && snapshot.binding == snapshot.manifest.immutable_binding()?
                && snapshot.manifest.lease.immutable_binding()? == lease.immutable_binding()?,
            "materialized source binding differs"
        );
        let plan = HelperPlan {
            manifest: snapshot.manifest_path.clone(),
            staging: snapshot.root.clone(),
            source: attempt.join("source"),
            proof: attempt.join("snapshot-proof.json"),
            invocation: request.workspace_id(),
            bazel: attempt.join("tools/bazel"),
            args: arguments,
        };
        Ok(Self::HttpsSnapshot { snapshot, plan })
    }
    /// Program, arguments and working directory. No transport credentials enter
    /// this command; the runner applies its common fresh execution environment.
    pub(crate) fn execution(
        &self,
        attempt: &Path,
        native_arguments: Vec<String>,
    ) -> Result<(PathBuf, Vec<String>, PathBuf)> {
        match self {
            Self::NativeFuse { root } => {
                Ok((attempt.join("tools/bazel"), native_arguments, root.clone()))
            }
            Self::HttpsSnapshot { plan, .. } => {
                let mut args = Vec::new();
                for (flag, path) in [
                    ("--manifest", &plan.manifest),
                    ("--staging", &plan.staging),
                    ("--source", &plan.source),
                    ("--proof", &plan.proof),
                ] {
                    args.push(flag.into());
                    args.push(path.to_str().context("invalid helper path")?.into());
                }
                args.extend([
                    "--invocation".into(),
                    plan.invocation.clone(),
                    "--bazel".into(),
                    plan.bazel.to_str().context("invalid Bazel path")?.into(),
                    "--".into(),
                ]);
                args.extend(plan.args.clone());
                Ok((
                    attempt.join("tools/readonly-snapshot"),
                    args,
                    attempt.to_owned(),
                ))
            }
        }
    }
    pub(crate) fn verify_completed(&self, status: i32) -> Result<()> {
        if let Self::HttpsSnapshot { snapshot, plan } = self {
            read_completed_proof(plan, &snapshot.manifest, status)?;
        }
        Ok(())
    }
}

/// Call only after process ownership is quiescent. The snapshot helper has no
/// daemon or persistent Bazel server; its private namespace dies with its group.
pub(crate) fn cleanup_snapshot(attempt: &Path) -> Result<()> {
    for child in ["source", "source-staging"] {
        let path = attempt.join(child);
        match std::fs::symlink_metadata(&path) {
            Ok(metadata) => {
                ensure!(
                    metadata.is_dir() && !metadata.file_type().is_symlink(),
                    "unexpected snapshot directory"
                );
                std::fs::remove_dir_all(path)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
