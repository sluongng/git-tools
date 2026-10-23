//! Verifying native repository client. Receipts and presence are not retention
//! leases. Git conversion, jj catalogs and mount residency are separate layers.
mod client;
mod config;
mod transport;
mod verified;
pub use client::NativeClient;
pub use client::query::{
    ChangeMatch, ChangePrefix, ChangeTarget, CheckedIdsPage, CheckedIdsQuery, CheckedScopeHandle,
    CommitPrefix, FactAnswer, IdRow, PrefixTarget, QueryFact,
};
pub use config::{Config, ConfigError, Limits, Tls};
pub use tinytree_proto::query_v1 as query_protocol;
pub use tinytree_proto::v1 as protocol;
pub use verified::{
    CanonicalCommitSummaries, MetadataOutcome, UploadSource, VerifiedFile, VerifiedMetadata,
    VerifiedReader,
};
