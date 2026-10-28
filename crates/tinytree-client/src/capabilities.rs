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
    ExactPathGraphV1 = 8,
    ExactCloudRepresentationRead = 9,
    ExactCloudRepresentationWrite = 10,
    JjResolvedBlameV1 = 11,
    AnnotationFrontierV1 = 12,
    DurableRetainedOffloadV1 = 13,
    AnnotationFrontierPagingV1 = 14,
    MergedPathGraphV2 = 15,
    StructuredCloudPublicationV2 = 16,
    DurableCloudVersionPinV1 = 17,
    FilesetGraphV3 = 18,
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
    /// Probe only the additive resolved-blame feature. Servers implementing an
    /// older capability vocabulary reject this exact singleton as InvalidArgument;
    /// pre-capability servers return Unimplemented. Neither proves a grant or
    /// selected-root readiness. All other failures remain failures.
    pub async fn resolved_blame_capability(&self) -> Result<Option<FeatureCapability>, Status> {
        match self
            .capabilities(&[CapabilityFeature::JjResolvedBlameV1])
            .await
        {
            Ok(capabilities) => Ok(Some(capabilities.features[0])),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument | tonic::Code::Unimplemented
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Exact singleton feature12 negotiation. Legacy vocabulary absence is not
    /// authorization, selected-source readiness, or a failed-query fallback.
    pub async fn annotation_frontier_capability(
        &self,
    ) -> Result<Option<FeatureCapability>, Status> {
        match self
            .capabilities(&[CapabilityFeature::AnnotationFrontierV1])
            .await
        {
            Ok(capabilities) => Ok(Some(capabilities.features[0])),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument | tonic::Code::Unimplemented
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Negotiate bounded frontier continuation, not legacy single-reply support.
    pub async fn annotation_frontier_paging_capability(
        &self,
    ) -> Result<Option<FeatureCapability>, Status> {
        match self
            .capabilities(&[CapabilityFeature::AnnotationFrontierPagingV1])
            .await
        {
            Ok(capabilities) => Ok(Some(capabilities.features[0])),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument | tonic::Code::Unimplemented
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Negotiate the full retained backing contract, independently of exact
    /// representation reads (feature9). Legacy singleton vocabulary absence is
    /// unsupported, not a grant. Denial, corruption, quota and transport errors
    /// remain errors. No result is cached across clients, selections or restarts.
    pub async fn durable_retained_offload_capability(
        &self,
    ) -> Result<Option<FeatureCapability>, Status> {
        match self
            .capabilities(&[CapabilityFeature::DurableRetainedOffloadV1])
            .await
        {
            Ok(capabilities) => Ok(Some(capabilities.features[0])),
            Err(error)
                if matches!(
                    error.code(),
                    tonic::Code::InvalidArgument | tonic::Code::Unimplemented
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Request a nonempty ascending feature set. Old servers return explicit
    /// UNIMPLEMENTED; this method never substitutes an eager/legacy workflow.
    pub async fn capabilities(
        &self,
        features: &[CapabilityFeature],
    ) -> Result<Capabilities, Status> {
        if features.is_empty()
            || features.len() > 18
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
