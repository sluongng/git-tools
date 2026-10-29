//! Experimental native repository transport, independent of Git serving.
//!
//! Protobuf messages describe requests and canonical object envelopes; protobuf
//! serialization is not the canonical native object encoding or hash domain.
//! These bindings provide identity/zstd negotiation, not server transport policy,
//! authentication, canonical-form validation, or durable publication by themselves.

pub mod fileset;
pub use fileset::{FilesetStats, FilesetValidationError, validate_fileset};

// Preserve protobuf package nesting for generated cross-package type paths.
pub mod tinytree {
    pub mod forge {
        pub mod v1 {
            tonic::include_proto!("tinytree.forge.v1");
        }
    }
    pub mod landing {
        pub mod v1 {
            tonic::include_proto!("tinytree.landing.v1");
        }
    }
    pub mod cloud {
        pub mod v1 {
            tonic::include_proto!("tinytree.cloud.v1");
        }
    }
    pub mod v1 {
        tonic::include_proto!("tinytree.v1");
    }
    pub mod query {
        pub mod v1 {
            tonic::include_proto!("tinytree.query.v1");
        }
    }
}
pub use tinytree::{cloud::v1 as cloud_v1, landing::v1 as landing_v1, query::v1 as query_v1, v1};
pub use tinytree::forge::v1 as forge_v1;

/// Shared encoded envelope caps, excluding the gRPC framing header.
pub const FORGE_MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
pub const FORGE_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

/// Optional query admission metadata. This requests heavier resource accounting,
/// not a query capability or permission.
pub const QUERY_WORK_CLASS_HEADER: &str = "x-tinytree-query-work-class";
/// Complete predecode reservation for merged-path graph queries.
pub const MERGED_PATH_V2_WORK_CLASS: &str = "merged-path-v2";

/// Descriptor set for the native-only protocol, including streaming cardinality.
pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("tinytree_descriptor");

/// Fixed resource diagnostics, never raw Store/RPC text or publication identity.
/// These categories carry no retention, retry or publication authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationFailureReason {
    ManifestBytes,
    CaptureBytes,
    StagingLeaseCount,
    ClientManifestBytes,
    ClientCaptureBytes,
    ClientStagingLeaseCount,
    ClientStagingLeaseFormat,
    ClientRequestSize,
    ClientPublicationMemory,
    ServerAdmissionCall,
    ServerAdmissionMemory,
    ServerAdmissionRepository,
    ServerMemoryBound,
    StorePublicationRequest,
    StoreCaptureAttachmentBytes,
    StoreValidationCatalogBudget,
    StoreOwnerQuota,
    StoreRepositoryQuota,
    StoreVersionDecodedObjectBudget,
    StoreCaptureInputMemory,
    StoreCaptureValidationBudget,
    StoreCaptureValidationMetadata,
    StoreIndexValidationBudget,
    StoreIndexNativeProofMemory,
    StoreIndexNativeProofSteps,
    StoreIndexProofWork,
    StoreIndexArtifactAllocation,
    StoreIndexArtifactCount,
    StoreIndexLoaderMemory,
    StoreIndexDecodedAllocation,
    StoreIndexGraphMemory,
    StoreFrozenIndexMemory,
    StoreFrozenSnapshotMemory,
    StoreCaptureGraphMemory,
    StoreCaptureBindingMemory,
    StoreSelectedAnchorsMemory,
    StoreVersionMemberAllocation,
    StoreVersionRawBytes,
    StoreCatalogScan,
    StorePayloadValidation,
    StoreStagingClosure,
    StoreCatalogReferences,
    StoreRetainedVersionQuota,
    StoreVersionValidationMetadata,
    StoreVersionValidationBytes,
    StoreWorkspaceGeneration,
    StoreValidationCatalogRecords,
    StoreValidationCatalogMetadata,
    StoreValidationCatalogRecordsMetadata,
    StoreValidationCatalogPayload,
    StoreValidationCatalogRecordsPayload,
    StoreValidationCatalogMetadataPayload,
    StoreValidationCatalogRecordsMetadataPayload,
    StoreIndexProofRecords,
    StoreIndexProofMetadata,
    StoreIndexProofRecordsMetadata,
    StoreIndexProofPayload,
    StoreIndexProofRecordsPayload,
    StoreIndexProofMetadataPayload,
    StoreIndexProofRecordsMetadataPayload,
    StoreResidentMemory,
    StoreIndexGraphWork,
    StoreIndexAggregatePayload,
    StoreIndexAnchorProof,
    StoreObjectClosure,
    StoreClosureEdges,
    StoreClosureMetadataBytes,
    StoreTreePathContexts,
    StoreTreePathLength,
    StoreTreePathContextBytes,
    StorePromisedRootQuotaScan,
    StorePrincipalClosureBytes,
    StoreLogicalHeadBinding,
    UnknownStoreLimit,
}
impl PublicationFailureReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ManifestBytes => "manifest_bytes",
            Self::CaptureBytes => "capture_bytes",
            Self::StagingLeaseCount => "staging_lease_count",
            Self::ClientManifestBytes => "client_manifest_bytes",
            Self::ClientCaptureBytes => "client_capture_bytes",
            Self::ClientStagingLeaseCount => "client_staging_lease_count",
            Self::ClientStagingLeaseFormat => "client_staging_lease_format",
            Self::ClientRequestSize => "client_request_size",
            Self::ClientPublicationMemory => "client_publication_memory",
            Self::ServerAdmissionCall => "server_admission_call",
            Self::ServerAdmissionMemory => "server_admission_memory",
            Self::ServerAdmissionRepository => "server_admission_repository",
            Self::ServerMemoryBound => "server_memory_bound",
            Self::StorePublicationRequest => "store_publication_request",
            Self::StoreCaptureAttachmentBytes => "store_capture_attachment_bytes",
            Self::StoreValidationCatalogBudget => "store_validation_catalog_budget",
            Self::StoreOwnerQuota => "store_owner_quota",
            Self::StoreRepositoryQuota => "store_repository_quota",
            Self::StoreVersionDecodedObjectBudget => "store_version_decoded_object_budget",
            Self::StoreCaptureInputMemory => "store_capture_input_memory",
            Self::StoreCaptureValidationBudget => "store_capture_validation_budget",
            Self::StoreCaptureValidationMetadata => "store_capture_validation_metadata",
            Self::StoreIndexValidationBudget => "store_index_validation_budget",
            Self::StoreIndexNativeProofMemory => "store_index_native_proof_memory",
            Self::StoreIndexNativeProofSteps => "store_index_native_proof_steps",
            Self::StoreIndexProofWork => "store_index_proof_work",
            Self::StoreIndexArtifactAllocation => "store_index_artifact_allocation",
            Self::StoreIndexArtifactCount => "store_index_artifact_count",
            Self::StoreIndexLoaderMemory => "store_index_loader_memory",
            Self::StoreIndexDecodedAllocation => "store_index_decoded_allocation",
            Self::StoreIndexGraphMemory => "store_index_graph_memory",
            Self::StoreFrozenIndexMemory => "store_frozen_index_memory",
            Self::StoreFrozenSnapshotMemory => "store_frozen_snapshot_memory",
            Self::StoreCaptureGraphMemory => "store_capture_graph_memory",
            Self::StoreCaptureBindingMemory => "store_capture_binding_memory",
            Self::StoreSelectedAnchorsMemory => "store_selected_anchors_memory",
            Self::StoreVersionMemberAllocation => "store_version_member_allocation",
            Self::StoreVersionRawBytes => "store_version_raw_bytes",
            Self::StoreCatalogScan => "store_catalog_scan",
            Self::StorePayloadValidation => "store_payload_validation",
            Self::StoreStagingClosure => "store_staging_closure",
            Self::StoreCatalogReferences => "store_catalog_references",
            Self::StoreRetainedVersionQuota => "store_retained_version_quota",
            Self::StoreVersionValidationMetadata => "store_version_validation_metadata",
            Self::StoreVersionValidationBytes => "store_version_validation_bytes",
            Self::StoreWorkspaceGeneration => "store_workspace_generation",
            Self::StoreValidationCatalogRecords => "store_validation_catalog_records",
            Self::StoreValidationCatalogMetadata => "store_validation_catalog_metadata",
            Self::StoreValidationCatalogRecordsMetadata => "store_validation_catalog_records_metadata",
            Self::StoreValidationCatalogPayload => "store_validation_catalog_payload",
            Self::StoreValidationCatalogRecordsPayload => "store_validation_catalog_records_payload",
            Self::StoreValidationCatalogMetadataPayload => "store_validation_catalog_metadata_payload",
            Self::StoreValidationCatalogRecordsMetadataPayload => "store_validation_catalog_records_metadata_payload",
            Self::StoreIndexProofRecords => "store_index_proof_records",
            Self::StoreIndexProofMetadata => "store_index_proof_metadata",
            Self::StoreIndexProofRecordsMetadata => "store_index_proof_records_metadata",
            Self::StoreIndexProofPayload => "store_index_proof_payload",
            Self::StoreIndexProofRecordsPayload => "store_index_proof_records_payload",
            Self::StoreIndexProofMetadataPayload => "store_index_proof_metadata_payload",
            Self::StoreIndexProofRecordsMetadataPayload => "store_index_proof_records_metadata_payload",
            Self::StoreResidentMemory => "store_resident_memory",
            Self::StoreIndexGraphWork => "store_index_graph_work",
            Self::StoreIndexAggregatePayload => "store_index_aggregate_payload",
            Self::StoreIndexAnchorProof => "store_index_anchor_proof",
            Self::StoreObjectClosure => "store_object_closure",
            Self::StoreClosureEdges => "store_closure_edges",
            Self::StoreClosureMetadataBytes => "store_closure_metadata_bytes",
            Self::StoreTreePathContexts => "store_tree_path_contexts",
            Self::StoreTreePathLength => "store_tree_path_length",
            Self::StoreTreePathContextBytes => "store_tree_path_context_bytes",
            Self::StorePromisedRootQuotaScan => "store_promised_root_quota_scan",
            Self::StorePrincipalClosureBytes => "store_principal_closure_bytes",
            Self::StoreLogicalHeadBinding => "store_logical_head_binding",
            Self::UnknownStoreLimit => "unknown_store_limit",
        }
    }
    pub fn message(self) -> &'static str {
        match self {
            Self::ManifestBytes => "workspace-cloud limit: manifest_bytes",
            Self::CaptureBytes => "workspace-cloud limit: capture_bytes",
            Self::StagingLeaseCount => "workspace-cloud limit: staging_lease_count",
            Self::ClientManifestBytes => "workspace-cloud limit: client_manifest_bytes",
            Self::ClientCaptureBytes => "workspace-cloud limit: client_capture_bytes",
            Self::ClientStagingLeaseCount => "workspace-cloud limit: client_staging_lease_count",
            Self::ClientStagingLeaseFormat => "workspace-cloud limit: client_staging_lease_format",
            Self::ClientRequestSize => "workspace-cloud limit: client_request_size",
            Self::ClientPublicationMemory => "workspace-cloud limit: client_publication_memory",
            Self::ServerAdmissionCall => "workspace-cloud limit: server_admission_call",
            Self::ServerAdmissionMemory => "workspace-cloud limit: server_admission_memory",
            Self::ServerAdmissionRepository => "workspace-cloud limit: server_admission_repository",
            Self::ServerMemoryBound => "workspace-cloud limit: server_memory_bound",
            Self::StorePublicationRequest => "workspace-cloud limit: store_publication_request",
            Self::StoreCaptureAttachmentBytes => "workspace-cloud limit: store_capture_attachment_bytes",
            Self::StoreValidationCatalogBudget => "workspace-cloud limit: store_validation_catalog_budget",
            Self::StoreOwnerQuota => "workspace-cloud limit: store_owner_quota",
            Self::StoreRepositoryQuota => "workspace-cloud limit: store_repository_quota",
            Self::StoreVersionDecodedObjectBudget => "workspace-cloud limit: store_version_decoded_object_budget",
            Self::StoreCaptureInputMemory => "workspace-cloud limit: store_capture_input_memory",
            Self::StoreCaptureValidationBudget => "workspace-cloud limit: store_capture_validation_budget",
            Self::StoreCaptureValidationMetadata => "workspace-cloud limit: store_capture_validation_metadata",
            Self::StoreIndexValidationBudget => "workspace-cloud limit: store_index_validation_budget",
            Self::StoreIndexNativeProofMemory => "workspace-cloud limit: store_index_native_proof_memory",
            Self::StoreIndexNativeProofSteps => "workspace-cloud limit: store_index_native_proof_steps",
            Self::StoreIndexProofWork => "workspace-cloud limit: store_index_proof_work",
            Self::StoreIndexArtifactAllocation => "workspace-cloud limit: store_index_artifact_allocation",
            Self::StoreIndexArtifactCount => "workspace-cloud limit: store_index_artifact_count",
            Self::StoreIndexLoaderMemory => "workspace-cloud limit: store_index_loader_memory",
            Self::StoreIndexDecodedAllocation => "workspace-cloud limit: store_index_decoded_allocation",
            Self::StoreIndexGraphMemory => "workspace-cloud limit: store_index_graph_memory",
            Self::StoreFrozenIndexMemory => "workspace-cloud limit: store_frozen_index_memory",
            Self::StoreFrozenSnapshotMemory => "workspace-cloud limit: store_frozen_snapshot_memory",
            Self::StoreCaptureGraphMemory => "workspace-cloud limit: store_capture_graph_memory",
            Self::StoreCaptureBindingMemory => "workspace-cloud limit: store_capture_binding_memory",
            Self::StoreSelectedAnchorsMemory => "workspace-cloud limit: store_selected_anchors_memory",
            Self::StoreVersionMemberAllocation => "workspace-cloud limit: store_version_member_allocation",
            Self::StoreVersionRawBytes => "workspace-cloud limit: store_version_raw_bytes",
            Self::StoreCatalogScan => "workspace-cloud limit: store_catalog_scan",
            Self::StorePayloadValidation => "workspace-cloud limit: store_payload_validation",
            Self::StoreStagingClosure => "workspace-cloud limit: store_staging_closure",
            Self::StoreCatalogReferences => "workspace-cloud limit: store_catalog_references",
            Self::StoreRetainedVersionQuota => "workspace-cloud limit: store_retained_version_quota",
            Self::StoreVersionValidationMetadata => "workspace-cloud limit: store_version_validation_metadata",
            Self::StoreVersionValidationBytes => "workspace-cloud limit: store_version_validation_bytes",
            Self::StoreWorkspaceGeneration => "workspace-cloud limit: store_workspace_generation",
            Self::StoreValidationCatalogRecords => "workspace-cloud limit: store_validation_catalog_records",
            Self::StoreValidationCatalogMetadata => "workspace-cloud limit: store_validation_catalog_metadata",
            Self::StoreValidationCatalogRecordsMetadata => "workspace-cloud limit: store_validation_catalog_records_metadata",
            Self::StoreValidationCatalogPayload => "workspace-cloud limit: store_validation_catalog_payload",
            Self::StoreValidationCatalogRecordsPayload => "workspace-cloud limit: store_validation_catalog_records_payload",
            Self::StoreValidationCatalogMetadataPayload => "workspace-cloud limit: store_validation_catalog_metadata_payload",
            Self::StoreValidationCatalogRecordsMetadataPayload => "workspace-cloud limit: store_validation_catalog_records_metadata_payload",
            Self::StoreIndexProofRecords => "workspace-cloud limit: store_index_proof_records",
            Self::StoreIndexProofMetadata => "workspace-cloud limit: store_index_proof_metadata",
            Self::StoreIndexProofRecordsMetadata => "workspace-cloud limit: store_index_proof_records_metadata",
            Self::StoreIndexProofPayload => "workspace-cloud limit: store_index_proof_payload",
            Self::StoreIndexProofRecordsPayload => "workspace-cloud limit: store_index_proof_records_payload",
            Self::StoreIndexProofMetadataPayload => "workspace-cloud limit: store_index_proof_metadata_payload",
            Self::StoreIndexProofRecordsMetadataPayload => "workspace-cloud limit: store_index_proof_records_metadata_payload",
            Self::StoreResidentMemory => "workspace-cloud limit: store_resident_memory",
            Self::StoreIndexGraphWork => "workspace-cloud limit: store_index_graph_work",
            Self::StoreIndexAggregatePayload => "workspace-cloud limit: store_index_aggregate_payload",
            Self::StoreIndexAnchorProof => "workspace-cloud limit: store_index_anchor_proof",
            Self::StoreObjectClosure => "workspace-cloud limit: store_object_closure",
            Self::StoreClosureEdges => "workspace-cloud limit: store_closure_edges",
            Self::StoreClosureMetadataBytes => "workspace-cloud limit: store_closure_metadata_bytes",
            Self::StoreTreePathContexts => "workspace-cloud limit: store_tree_path_contexts",
            Self::StoreTreePathLength => "workspace-cloud limit: store_tree_path_length",
            Self::StoreTreePathContextBytes => "workspace-cloud limit: store_tree_path_context_bytes",
            Self::StorePromisedRootQuotaScan => "workspace-cloud limit: store_promised_root_quota_scan",
            Self::StorePrincipalClosureBytes => "workspace-cloud limit: store_principal_closure_bytes",
            Self::StoreLogicalHeadBinding => "workspace-cloud limit: store_logical_head_binding",
            Self::UnknownStoreLimit => "workspace-cloud limit: unknown_store_limit",
        }
    }
    /// Exact static vocabulary only; substrings and arbitrary provider text fail.
    pub fn from_message(message: &str) -> Option<Self> {
        match message {
            "workspace-cloud limit: manifest_bytes" => Some(Self::ManifestBytes),
            "workspace-cloud limit: capture_bytes" => Some(Self::CaptureBytes),
            "workspace-cloud limit: staging_lease_count" => Some(Self::StagingLeaseCount),
            "workspace-cloud limit: client_manifest_bytes" => Some(Self::ClientManifestBytes),
            "workspace-cloud limit: client_capture_bytes" => Some(Self::ClientCaptureBytes),
            "workspace-cloud limit: client_staging_lease_count" => Some(Self::ClientStagingLeaseCount),
            "workspace-cloud limit: client_staging_lease_format" => Some(Self::ClientStagingLeaseFormat),
            "workspace-cloud limit: client_request_size" => Some(Self::ClientRequestSize),
            "workspace-cloud limit: client_publication_memory" => Some(Self::ClientPublicationMemory),
            "workspace-cloud limit: server_admission_call" => Some(Self::ServerAdmissionCall),
            "workspace-cloud limit: server_admission_memory" => Some(Self::ServerAdmissionMemory),
            "workspace-cloud limit: server_admission_repository" => Some(Self::ServerAdmissionRepository),
            "workspace-cloud limit: server_memory_bound" => Some(Self::ServerMemoryBound),
            "workspace-cloud limit: store_publication_request" => Some(Self::StorePublicationRequest),
            "workspace-cloud limit: store_capture_attachment_bytes" => Some(Self::StoreCaptureAttachmentBytes),
            "workspace-cloud limit: store_validation_catalog_budget" => Some(Self::StoreValidationCatalogBudget),
            "workspace-cloud limit: store_owner_quota" => Some(Self::StoreOwnerQuota),
            "workspace-cloud limit: store_repository_quota" => Some(Self::StoreRepositoryQuota),
            "workspace-cloud limit: store_version_decoded_object_budget" => Some(Self::StoreVersionDecodedObjectBudget),
            "workspace-cloud limit: store_capture_input_memory" => Some(Self::StoreCaptureInputMemory),
            "workspace-cloud limit: store_capture_validation_budget" => Some(Self::StoreCaptureValidationBudget),
            "workspace-cloud limit: store_capture_validation_metadata" => Some(Self::StoreCaptureValidationMetadata),
            "workspace-cloud limit: store_index_validation_budget" => Some(Self::StoreIndexValidationBudget),
            "workspace-cloud limit: store_index_native_proof_memory" => Some(Self::StoreIndexNativeProofMemory),
            "workspace-cloud limit: store_index_native_proof_steps" => Some(Self::StoreIndexNativeProofSteps),
            "workspace-cloud limit: store_index_proof_work" => Some(Self::StoreIndexProofWork),
            "workspace-cloud limit: store_index_artifact_allocation" => Some(Self::StoreIndexArtifactAllocation),
            "workspace-cloud limit: store_index_artifact_count" => Some(Self::StoreIndexArtifactCount),
            "workspace-cloud limit: store_index_loader_memory" => Some(Self::StoreIndexLoaderMemory),
            "workspace-cloud limit: store_index_decoded_allocation" => Some(Self::StoreIndexDecodedAllocation),
            "workspace-cloud limit: store_index_graph_memory" => Some(Self::StoreIndexGraphMemory),
            "workspace-cloud limit: store_frozen_index_memory" => Some(Self::StoreFrozenIndexMemory),
            "workspace-cloud limit: store_frozen_snapshot_memory" => Some(Self::StoreFrozenSnapshotMemory),
            "workspace-cloud limit: store_capture_graph_memory" => Some(Self::StoreCaptureGraphMemory),
            "workspace-cloud limit: store_capture_binding_memory" => Some(Self::StoreCaptureBindingMemory),
            "workspace-cloud limit: store_selected_anchors_memory" => Some(Self::StoreSelectedAnchorsMemory),
            "workspace-cloud limit: store_version_member_allocation" => Some(Self::StoreVersionMemberAllocation),
            "workspace-cloud limit: store_version_raw_bytes" => Some(Self::StoreVersionRawBytes),
            "workspace-cloud limit: store_catalog_scan" => Some(Self::StoreCatalogScan),
            "workspace-cloud limit: store_payload_validation" => Some(Self::StorePayloadValidation),
            "workspace-cloud limit: store_staging_closure" => Some(Self::StoreStagingClosure),
            "workspace-cloud limit: store_catalog_references" => Some(Self::StoreCatalogReferences),
            "workspace-cloud limit: store_retained_version_quota" => Some(Self::StoreRetainedVersionQuota),
            "workspace-cloud limit: store_version_validation_metadata" => Some(Self::StoreVersionValidationMetadata),
            "workspace-cloud limit: store_version_validation_bytes" => Some(Self::StoreVersionValidationBytes),
            "workspace-cloud limit: store_workspace_generation" => Some(Self::StoreWorkspaceGeneration),
            "workspace-cloud limit: store_validation_catalog_records" => Some(Self::StoreValidationCatalogRecords),
            "workspace-cloud limit: store_validation_catalog_metadata" => Some(Self::StoreValidationCatalogMetadata),
            "workspace-cloud limit: store_validation_catalog_records_metadata" => Some(Self::StoreValidationCatalogRecordsMetadata),
            "workspace-cloud limit: store_validation_catalog_payload" => Some(Self::StoreValidationCatalogPayload),
            "workspace-cloud limit: store_validation_catalog_records_payload" => Some(Self::StoreValidationCatalogRecordsPayload),
            "workspace-cloud limit: store_validation_catalog_metadata_payload" => Some(Self::StoreValidationCatalogMetadataPayload),
            "workspace-cloud limit: store_validation_catalog_records_metadata_payload" => Some(Self::StoreValidationCatalogRecordsMetadataPayload),
            "workspace-cloud limit: store_index_proof_records" => Some(Self::StoreIndexProofRecords),
            "workspace-cloud limit: store_index_proof_metadata" => Some(Self::StoreIndexProofMetadata),
            "workspace-cloud limit: store_index_proof_records_metadata" => Some(Self::StoreIndexProofRecordsMetadata),
            "workspace-cloud limit: store_index_proof_payload" => Some(Self::StoreIndexProofPayload),
            "workspace-cloud limit: store_index_proof_records_payload" => Some(Self::StoreIndexProofRecordsPayload),
            "workspace-cloud limit: store_index_proof_metadata_payload" => Some(Self::StoreIndexProofMetadataPayload),
            "workspace-cloud limit: store_index_proof_records_metadata_payload" => Some(Self::StoreIndexProofRecordsMetadataPayload),
            "workspace-cloud limit: store_resident_memory" => Some(Self::StoreResidentMemory),
            "workspace-cloud limit: store_index_graph_work" => Some(Self::StoreIndexGraphWork),
            "workspace-cloud limit: store_index_aggregate_payload" => Some(Self::StoreIndexAggregatePayload),
            "workspace-cloud limit: store_index_anchor_proof" => Some(Self::StoreIndexAnchorProof),
            "workspace-cloud limit: store_object_closure" => Some(Self::StoreObjectClosure),
            "workspace-cloud limit: store_closure_edges" => Some(Self::StoreClosureEdges),
            "workspace-cloud limit: store_closure_metadata_bytes" => Some(Self::StoreClosureMetadataBytes),
            "workspace-cloud limit: store_tree_path_contexts" => Some(Self::StoreTreePathContexts),
            "workspace-cloud limit: store_tree_path_length" => Some(Self::StoreTreePathLength),
            "workspace-cloud limit: store_tree_path_context_bytes" => Some(Self::StoreTreePathContextBytes),
            "workspace-cloud limit: store_promised_root_quota_scan" => Some(Self::StorePromisedRootQuotaScan),
            "workspace-cloud limit: store_principal_closure_bytes" => Some(Self::StorePrincipalClosureBytes),
            "workspace-cloud limit: store_logical_head_binding" => Some(Self::StoreLogicalHeadBinding),
            "workspace-cloud limit: unknown_store_limit" => Some(Self::UnknownStoreLimit),
            "private workspace call budget exhausted" => Some(Self::ServerAdmissionCall),
            "private workspace memory budget exhausted" => Some(Self::ServerAdmissionMemory),
            "private workspace repository budget exhausted" => Some(Self::ServerAdmissionRepository),
            "private workspace memory bound" => Some(Self::ServerMemoryBound),
            _ => None,
        }
    }
}


#[cfg(test)]
mod tests {
    use super::{FILE_DESCRIPTOR_SET, query_v1, v1};
    use prost::Message;
    use tonic::{codec::CompressionEncoding, transport::Channel};

    #[test]
    fn finite_store_resource_reasons_round_trip_without_accepting_extra_text() {
        use super::PublicationFailureReason as Reason;
        for (reason, name) in [
            (Reason::StoreObjectClosure, "store_object_closure"),
            (Reason::StoreClosureEdges, "store_closure_edges"),
            (Reason::StoreClosureMetadataBytes, "store_closure_metadata_bytes"),
            (Reason::StoreTreePathContexts, "store_tree_path_contexts"),
            (Reason::StoreTreePathLength, "store_tree_path_length"),
            (Reason::StoreTreePathContextBytes, "store_tree_path_context_bytes"),
            (Reason::StorePromisedRootQuotaScan, "store_promised_root_quota_scan"),
            (Reason::StorePrincipalClosureBytes, "store_principal_closure_bytes"),
            (Reason::StoreLogicalHeadBinding, "store_logical_head_binding"),
        ] {
            assert_eq!(reason.as_str(), name);
            assert_eq!(Reason::from_message(reason.message()), Some(reason));
            assert_eq!(Reason::from_message(&format!("{} private-marker", reason.message())), None);
        }
    }

    #[test]
    fn durable_version_pins_have_explicit_unary_contracts() {
        let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
        let service = descriptors
            .file
            .iter()
            .find(|file| file.package.as_deref() == Some("tinytree.cloud.v1"))
            .unwrap()
            .service
            .iter()
            .find(|service| service.name.as_deref() == Some("WorkspaceCloud"))
            .unwrap();
        for (name, input, output) in [
            (
                "AcquireVersionPin",
                ".tinytree.cloud.v1.AcquireVersionPinRequest",
                ".tinytree.cloud.v1.VersionPin",
            ),
            (
                "ReleaseVersionPin",
                ".tinytree.cloud.v1.ReleaseVersionPinRequest",
                ".tinytree.cloud.v1.VersionPinReleased",
            ),
        ] {
            let method = service
                .method
                .iter()
                .find(|method| method.name.as_deref() == Some(name))
                .unwrap();
            assert_eq!(method.input_type.as_deref(), Some(input));
            assert_eq!(method.output_type.as_deref(), Some(output));
            assert!(!method.client_streaming.unwrap_or(false));
            assert!(!method.server_streaming.unwrap_or(false));
        }
    }

    #[test]
    fn latest_captured_version_has_an_explicit_unary_contract() {
        let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
        let cloud = descriptors
            .file
            .iter()
            .find(|file| file.package.as_deref() == Some("tinytree.cloud.v1"))
            .unwrap();
        let method = cloud
            .service
            .iter()
            .find(|service| service.name.as_deref() == Some("WorkspaceCloud"))
            .unwrap()
            .method
            .iter()
            .find(|method| method.name.as_deref() == Some("GetLatestCapturedVersion"))
            .unwrap();
        assert_eq!(
            method.input_type.as_deref(),
            Some(".tinytree.cloud.v1.GetLatestCapturedVersionRequest")
        );
        assert_eq!(
            method.output_type.as_deref(),
            Some(".tinytree.cloud.v1.Version")
        );
        assert!(!method.client_streaming.unwrap_or(false));
        assert!(!method.server_streaming.unwrap_or(false));
    }

    #[test]
    fn native_repository_contract_preserves_rpc_streaming_and_public_bindings() {
        let descriptors = prost_types::FileDescriptorSet::decode(FILE_DESCRIPTOR_SET).unwrap();
        assert_eq!(descriptors.file.len(), 4);
        let file = descriptors
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("tinytree/v1/repository.proto"))
            .unwrap();
        assert_eq!(file.package.as_deref(), Some("tinytree.v1"));
        assert!(file.dependency.is_empty());
        assert_eq!(file.service.len(), 1);
        let repository = file
            .service
            .iter()
            .find(|service| service.name.as_deref() == Some("Repository"))
            .unwrap();
        assert_eq!(repository.name.as_deref(), Some("Repository"));
        let methods: Vec<_> = repository
            .method
            .iter()
            .map(|method| {
                (
                    method.name.as_deref().unwrap(),
                    method.client_streaming.unwrap_or(false),
                    method.server_streaming.unwrap_or(false),
                )
            })
            .collect();
        assert_eq!(
            methods,
            [
                ("OpenRepository", false, false),
                ("GetCommitGraph", false, false),
                ("BatchReadObjects", false, true),
                ("FindMissingObjects", false, false),
                ("ReadFile", false, true),
                ("WriteObject", true, false),
                ("PublishReference", false, false),
                ("GetPublication", false, false),
            ]
        );

        let query_file = descriptors
            .file
            .iter()
            .find(|file| file.name.as_deref() == Some("tinytree/query/v1/query.proto"))
            .unwrap();
        assert_eq!(query_file.package.as_deref(), Some("tinytree.query.v1"));
        assert_eq!(
            query_file.dependency,
            [
                "tinytree/v1/repository.proto",
                "tinytree/cloud/v1/cloud.proto"
            ]
        );
        assert_eq!(query_file.service.len(), 1);
        let query = query_file
            .service
            .iter()
            .find(|service| service.name.as_deref() == Some("RepositoryQuery"))
            .unwrap();
        let query_methods: Vec<_> = query
            .method
            .iter()
            .map(|method| {
                (
                    method.name.as_deref().unwrap(),
                    method.client_streaming.unwrap_or(false),
                    method.server_streaming.unwrap_or(false),
                )
            })
            .collect();
        assert_eq!(
            query_methods,
            [
                ("OpenScope", true, false),
                ("OpenCloudScope", true, false),
                ("ForkScope", true, false),
                ("CloseScope", false, false),
                ("QueryFacts", false, false),
                ("ResolveCommitPrefix", false, false),
                ("ResolveChangePrefix", false, false),
                ("ShortestPrefix", false, false),
                ("Evaluate", false, false),
                ("CloseQuery", false, false),
                ("PathHistory", false, false),
                ("Blame", false, false),
                ("Search", false, false),
            ]
        );

        let landing_file = descriptors
            .file
            .iter()
            .find(|f| f.package.as_deref() == Some("tinytree.landing.v1"))
            .unwrap();
        assert_eq!(
            landing_file.service[0]
                .method
                .iter()
                .map(|m| (
                    m.name.as_deref().unwrap(),
                    m.client_streaming.unwrap_or(false),
                    m.server_streaming.unwrap_or(false)
                ))
                .collect::<Vec<_>>(),
            [("StartLanding", false, false), ("GetLanding", false, false)]
        );

        // Check the public generated builders without constructing a mock
        // Repository implementation or a runtime/network connection.
        fn client_compression(
            client: v1::repository_client::RepositoryClient<Channel>,
        ) -> v1::repository_client::RepositoryClient<Channel> {
            client
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
        }
        #[allow(dead_code)]
        fn server_compression<T: v1::repository_server::Repository>(
            service: T,
        ) -> v1::repository_server::RepositoryServer<T> {
            v1::repository_server::RepositoryServer::new(service)
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
        }
        fn query_client_compression(
            client: query_v1::repository_query_client::RepositoryQueryClient<Channel>,
        ) -> query_v1::repository_query_client::RepositoryQueryClient<Channel> {
            client
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
        }
        #[allow(dead_code)]
        fn query_server_compression<T: query_v1::repository_query_server::RepositoryQuery>(
            service: T,
        ) -> query_v1::repository_query_server::RepositoryQueryServer<T> {
            query_v1::repository_query_server::RepositoryQueryServer::new(service)
                .send_compressed(CompressionEncoding::Zstd)
                .accept_compressed(CompressionEncoding::Zstd)
        }
        let _ = (client_compression, query_client_compression);
    }
    #[test]
    fn query_bindings_preserve_false_answers_and_explicit_stream_completion() {
        let answer = query_v1::GraphFactAnswer {
            answer: Some(query_v1::graph_fact_answer::Answer::HasId(false)),
        };
        let bytes = answer.encode_to_vec();
        assert!(!bytes.is_empty());
        let decoded = query_v1::GraphFactAnswer::decode(bytes.as_slice()).unwrap();
        assert_eq!(decoded.answer, answer.answer);
        let page = query_v1::EvaluatePage {
            exhausted: Some(false),
            ..Default::default()
        };
        let decoded = query_v1::EvaluatePage::decode(page.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded.exhausted, Some(false));
        assert_ne!(
            decoded.exhausted,
            query_v1::EvaluatePage::default().exhausted
        );
        // Line zero and an unresolved origin are distinct from an attributed
        // intrinsic-root line, including when crossing a proxy boundary.
        let blame = query_v1::BlameResult {
            coverage: query_v1::HistoryCoverage::HistoryScanLimit as i32,
            ranges: vec![query_v1::BlameRange {
                start_line: 0,
                end_line: 1,
                origin: None,
                original_line: 0,
            }],
            ..Default::default()
        };
        let decoded = query_v1::BlameResult::decode(blame.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, blame);
        assert_eq!(decoded.ranges[0].origin, None);
        let marker = query_v1::OpenScopeFrame {
            frame: Some(query_v1::open_scope_frame::Frame::ScopeEnd(
                query_v1::ScopeEnd::default(),
            )),
        };
        let decoded = query_v1::OpenScopeFrame::decode(marker.encode_to_vec().as_slice()).unwrap();
        assert!(matches!(
            decoded.frame,
            Some(query_v1::open_scope_frame::Frame::ScopeEnd(_))
        ));

        // Explicit empty traversal intervals and Latest(0) must not become
        // unbounded intervals/counts while crossing the wire.
        use query_v1::graph_expr::Expression;
        let range = query_v1::GraphExpr {
            version: 1,
            expression: Some(Expression::Range(Box::new(query_v1::RangeExpr {
                parents: Some(query_v1::ParentRange {
                    start: 0,
                    end_exclusive: Some(0),
                }),
                ..Default::default()
            }))),
        };
        let latest = query_v1::GraphExpr {
            version: 1,
            expression: Some(Expression::Latest(Box::new(query_v1::LatestExpr {
                candidates: Some(Box::new(range)),
                count: 0,
            }))),
        };
        assert_eq!(
            query_v1::GraphExpr::decode(latest.encode_to_vec().as_slice()).unwrap(),
            latest
        );
        let conflict = query_v1::GraphExpr {
            version: 1,
            expression: Some(Expression::HasConflict(Box::new(query_v1::UnaryExpr {
                input: Some(Box::new(latest)),
            }))),
        };
        assert_eq!(
            query_v1::GraphExpr::decode(conflict.encode_to_vec().as_slice()).unwrap(),
            conflict
        );
        let dag_range = query_v1::DagRangeExpr {
            generations_from_roots: Some(query_v1::GenerationRange {
                start: 0,
                end_exclusive: Some(0),
            }),
            ..Default::default()
        };
        let decoded = query_v1::DagRangeExpr::decode(dag_range.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, dag_range);
        assert_ne!(
            decoded.generations_from_roots,
            query_v1::DagRangeExpr::default().generations_from_roots
        );
    }
}
