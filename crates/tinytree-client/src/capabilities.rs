//! Authenticated configured support. This does not check immutable-root readiness.
use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i32)]
pub enum CapabilityFeature {
    FederatedScopeLog = 1,
    PathHistory = 2,
    Blame = 3,
    CapturedIndexedCloudRead = 4,
    CapturedIndexedCloudWrite = 5,
    LandingRecoveryCloudRead = 6,
    LandingRecoveryCloudWrite = 7,
}
impl CapabilityFeature {
    pub const ALL: [Self; 5] = [
        Self::FederatedScopeLog,
        Self::PathHistory,
        Self::Blame,
        Self::CapturedIndexedCloudRead,
        Self::CapturedIndexedCloudWrite,
    ];
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RootReadiness {
    NotChecked,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FeatureCapability {
    pub feature: CapabilityFeature,
    pub supported: bool,
    pub authorized: bool,
}
/// Require both support and grant before using a feature. Query consumers must
/// still OpenScope on their chosen immutable roots; this result is not a lease,
/// index-coverage proof, storage quota promise or retained-version authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Capabilities {
    pub features: Vec<FeatureCapability>,
    pub root_readiness: RootReadiness,
}
impl NativeClient {
    /// Request a nonempty ascending feature set. Old servers return explicit
    /// UNIMPLEMENTED; this method never substitutes an eager/legacy workflow.
    pub async fn capabilities(
        &self,
        features: &[CapabilityFeature],
    ) -> Result<Capabilities, Status> {
        if features.is_empty()
            || features.len() > 7
            || features.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(Status::invalid_argument(
                "invalid requested capability features",
            ));
        }
        let request = p::GetCapabilitiesRequest {
            repository_id: self.inner.config.repository_id.clone(),
            features: features.iter().map(|f| *f as i32).collect(),
        };
        self.bounded(&request)?;
        let deadline = self.deadline();
        let budget = self.admit(deadline, 16 * 1024).await?;
        self.retry(deadline, false, |allowance| {
            let mut grpc = self
                .rpc_client(budget.clone())
                .max_decoding_message_size(4096);
            let request = request.clone();
            async move {
                let value = grpc
                    .get_capabilities(self.request(request, allowance))
                    .await?
                    .into_inner();
                if value.schema_version != 1
                    || value.repository_id != self.inner.config.repository_id
                    || value.root_readiness != p::RootReadiness::NotChecked as i32
                    || value.features.len() != features.len()
                    || value
                        .features
                        .iter()
                        .zip(features)
                        .any(|(state, expected)| state.feature != *expected as i32)
                {
                    return Err(integrity());
                }
                Ok(Capabilities {
                    features: value
                        .features
                        .into_iter()
                        .zip(features)
                        .map(|(state, feature)| FeatureCapability {
                            feature: *feature,
                            supported: state.supported,
                            authorized: state.authorized,
                        })
                        .collect(),
                    root_readiness: RootReadiness::NotChecked,
                })
            }
        })
        .await
    }
}
