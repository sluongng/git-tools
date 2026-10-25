//! Verifying native repository client. Receipts and presence are not retention
//! leases. Git conversion, jj catalogs and mount residency are separate layers.
mod client;
mod config;
mod transport;
mod verified;
pub use client::NativeClient;
pub use client::cloud::{
    CloudCapture, CloudCaptureDescriptor, CloudCaptureId, CloudCapturedPublishIntent,
    CloudDescriptor, CloudIndexArtifact, CloudIndexArtifactDescriptor, CloudIndexArtifactId,
    CloudIndexWriteReceipt, CloudLimits, CloudManifestId, CloudObject, CloudObjectId,
    CloudPublication, CloudPublishIntent, CloudRawDigest, CloudVersion, CloudVersionPage,
    CloudVersionSummary, CloudWorkspace, CloudWorkspacePage, CloudWriteReceipt,
    RetainedVersionSelector, VerifiedCapturedVersion, VerifiedRetainedVersion,
};
pub use client::history::{CheckedBlame, CheckedPathHistory};
pub use client::query::{
    ChangeMatch, ChangePrefix, ChangeTarget, CheckedGraphPage, CheckedGraphQuery, CheckedGraphRows,
    CheckedIdsPage, CheckedIdsQuery, CheckedScopeHandle, CommitPrefix, FactAnswer, GraphEdge,
    GraphRow, IdRow, PrefixTarget, QueryFact,
};
pub use client::search::{CheckedSearch, CheckedSearchPage};
pub use config::{Config, ConfigError, Limits, Tls};
pub use tinytree_proto::cloud_v1 as cloud_protocol;
pub use tinytree_proto::query_v1 as query_protocol;
pub use tinytree_proto::v1 as protocol;
pub use verified::{
    CanonicalCommitSummaries, MetadataOutcome, UploadSource, VerifiedFile, VerifiedMetadata,
    VerifiedReader,
};

pub use client::landing::{
    CheckedLandingJob, LandingCandidate, LandingHandle, LandingLimits, LandingPhase,
    LandingPreparation, LandingReceipt, LandingRewrite, LandingStart,
};
