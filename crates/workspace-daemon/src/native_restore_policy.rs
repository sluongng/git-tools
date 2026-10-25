//! Host-selected automatic restore eligibility, never a public-root grant.
use crate::{
    config::{NativeCapturePolicy, NativeCaptureUploadPolicy, NativeIndexMode, NativeWorkspaces},
    control::NativeRestoreSelection,
};
use anyhow::{Context as _, Result, ensure};
use native_workspace_admin::restore::RestoreSourceIdentity;
use serde::{Deserialize, Serialize};
use std::path::Path;

pub(crate) struct Binding<'a> {
    pub root: &'a Path,
    pub store: &'a str,
    pub workspace_id: [u8; 32],
    pub source: &'a NativeRestoreSelection,
    pub provider_identity: [u8; 32],
}
impl Binding<'_> {
    fn digest(&self) -> Result<[u8; 32]> {
        ensure!(
            self.root.is_absolute()
                && self.root.as_os_str().len() <= 4096
                && self.store.len() <= 64
                && !self.store.is_empty()
                && self.workspace_id != [0; 32]
                && self.provider_identity != [0; 32],
            "invalid automatic restore binding"
        );
        let bytes = serde_json::to_vec(&(
            self.root,
            self.store,
            self.workspace_id,
            self.source,
            self.provider_identity,
        ))?;
        ensure!(
            bytes.len() <= 16 * 1024,
            "automatic restore binding exceeds bound"
        );
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-restore-automatic-binding-v1\0");
        hash.update(&bytes);
        Ok(*hash.finalize().as_bytes())
    }
}
/// Saved before any per-ID/destination effects. Missing on old intents means no
/// enrollment, not permission to resolve the current configuration again.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Requested {
    version: u32,
    binding: [u8; 32],
    require_lazy_queries: bool,
    policy_digest: [u8; 32],
    capture_enabled: bool,
    upload_enabled: bool,
    auto_capture: bool,
    auto_upload: bool,
}
impl Requested {
    pub fn resolve(
        settings: &NativeWorkspaces,
        binding: &Binding<'_>,
        remote: bool,
    ) -> Result<Self> {
        let policy = settings
            .creation_description(binding.provider_identity, remote, true)
            .creation_policy
            .context("automatic restore policy unavailable")?;
        let mut requested = Self {
            version: 1,
            binding: [0; 32],
            require_lazy_queries: remote && settings.index_mode != NativeIndexMode::Legacy,
            policy_digest: policy.digest,
            capture_enabled: policy.capture_enabled,
            upload_enabled: policy.upload_enabled,
            auto_capture: policy.auto_capture,
            auto_upload: policy.auto_upload,
        };
        requested.binding = requested.bound_digest(binding)?;
        requested.validate(binding)?;
        Ok(requested)
    }
    pub fn policy(&self) -> Result<native_broker::CreationPolicy> {
        native_broker::CreationPolicy::checked(
            native_broker::CreationPolicy {
                digest: self.policy_digest,
                capture_enabled: self.capture_enabled,
                upload_enabled: self.upload_enabled,
                auto_capture: self.auto_capture,
                auto_upload: self.auto_upload,
            }
            .wire(),
        )
        .map_err(Into::into)
    }
    fn bound_digest(&self, binding: &Binding<'_>) -> Result<[u8; 32]> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-restore-request-policy-v1\0");
        hash.update(&binding.digest()?);
        hash.update(&self.version.to_le_bytes());
        hash.update(&self.policy_digest);
        hash.update(&[
            self.require_lazy_queries as u8,
            self.capture_enabled as u8,
            self.upload_enabled as u8,
            self.auto_capture as u8,
            self.auto_upload as u8,
        ]);
        Ok(*hash.finalize().as_bytes())
    }
    pub fn validate(&self, binding: &Binding<'_>) -> Result<()> {
        ensure!(
            self.version == 1 && self.binding == self.bound_digest(binding)?,
            "automatic restore policy binding differs"
        );
        self.policy()?;
        Ok(())
    }
    /// A saved automatic contract is immutable. Current Disabled policies can
    /// fence its backup effects, but never change its root or lazy-query choice.
    pub async fn check_features(
        &self,
        client: &tinytree_client::NativeClient,
        current: &NativeWorkspaces,
    ) -> Result<()> {
        use tinytree_client::CapabilityFeature as F;
        let mut features = Vec::with_capacity(5);
        if self.require_lazy_queries {
            features.extend([F::FederatedScopeLog, F::PathHistory, F::Blame]);
        }
        if self.auto_upload
            && current.capture.policy != NativeCapturePolicy::Disabled
            && current.upload.policy != NativeCaptureUploadPolicy::Disabled
        {
            features.extend([F::CapturedIndexedCloudRead, F::CapturedIndexedCloudWrite]);
        }
        // Explicit compatibility policies and old captures keep the existing
        // authenticated collector; they do not acquire a new capability gate.
        if features.is_empty() {
            return Ok(());
        }
        let configured = client
            .capabilities(&features)
            .await
            .context("automatic restore feature probe failed")?;
        ensure!(
            configured
                .features
                .iter()
                .all(|feature| feature.supported && feature.authorized),
            "automatic restore requires supported and authorized query/cloud features"
        );
        Ok(())
    }
    pub fn needs_feature_probe(&self, current: &NativeWorkspaces) -> bool {
        self.require_lazy_queries
            || (self.auto_upload
                && current.capture.policy != NativeCapturePolicy::Disabled
                && current.upload.policy != NativeCaptureUploadPolicy::Disabled)
    }
}
/// Only the verified collector result can be bound to the saved host request.
/// This records eligibility, not a continuing retention or availability proof.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Verified {
    version: u32,
    pub requested: Requested,
    source: RestoreSourceIdentity,
}
impl Verified {
    pub fn seal(
        binding: &Binding<'_>,
        requested: Requested,
        source: RestoreSourceIdentity,
    ) -> Result<Self> {
        let verified = Self {
            version: 1,
            requested,
            source,
        };
        verified.validate(binding, &verified.source)?;
        Ok(verified)
    }
    pub fn validate(&self, binding: &Binding<'_>, source: &RestoreSourceIdentity) -> Result<()> {
        let selected = NativeRestoreSelection {
            workspace_id: source.workspace_id(),
            generation: source.generation(),
            manifest_id: source.manifest_id(),
            manifest_blake3: source.manifest_blake3(),
            manifest_size_bytes: source.manifest_size_bytes(),
        };
        ensure!(
            self.version == 1
                && self.source == *source
                && *binding.source == selected
                && binding.provider_identity == source.provider_identity(),
            "verified automatic restore source differs"
        );
        self.requested.validate(binding)
    }
}
