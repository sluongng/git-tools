//! Select the immutable remote baseline, not the index's local COMMIT overlays.
use crate::{broker::Locator, remote_state::LockedState};
use jj_lib::index::ReadonlyIndex;
use native_broker::{
    BrokerQueryClient, CloudQuerySource, NativeQueryIo, OpenCloudQueryScope, OpenQueryScope,
    QueryReply, QueryScope,
};
use std::{io, path::Path, sync::Arc};
use tinytree_model::CommitId;

pub(crate) enum Source {
    Cloud(CloudQuerySource),
    Live {
        roots: Vec<CommitId>,
        // Preserve the live observation's command lock through the query.
        _state: LockedState,
    },
}
impl Source {
    pub(crate) fn resolve(
        index: &dyn ReadonlyIndex,
        working_copy: &Path,
        locator: &Locator,
    ) -> io::Result<Self> {
        if let Some(source) = index
            .downcast_ref::<jj_native_index::NativeReadonlyIndex>()
            .and_then(|index| index.cloud_query_source())
        {
            // The loaded checkpoint is operation-specific. Do not substitute
            // restore-journal roots, moving refs or a scope with local drafts.
            return Ok(Self::Cloud(source));
        }
        let state = LockedState::open(working_copy, locator)?;
        let mut roots = state
            .state
            .observations
            .iter()
            .filter_map(|observation| observation.head.as_ref())
            .map(|head| {
                let mut bytes = [0; 32];
                for (n, byte) in bytes.iter_mut().enumerate() {
                    *byte = u8::from_str_radix(&head[n * 2..n * 2 + 2], 16)
                        .map_err(|_| io::Error::other("invalid observed native head"))?;
                }
                CommitId::from_slice(&bytes)
                    .map_err(|_| io::Error::other("invalid observed native head"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        if roots.is_empty() {
            return Err(io::Error::other(
                "native queries require an observed remote reference or an exact retained-cloud index source",
            ));
        }
        roots.sort();
        roots.dedup();
        Ok(Self::Live {
            roots,
            _state: state,
        })
    }

    pub(crate) async fn open(
        &self,
        query: &BrokerQueryClient,
        replay_key: [u8; 32],
    ) -> Result<QueryReply<QueryScope>, tonic::Status> {
        match self {
            Self::Cloud(source) => {
                query
                    .open_cloud_scope(
                        OpenCloudQueryScope {
                            source: source.clone(),
                            overlay: Vec::new(),
                            replay_key,
                        },
                        Arc::new(()),
                    )
                    .await
            }
            Self::Live { roots, .. } => {
                query
                    .open_scope(
                        OpenQueryScope {
                            roots: roots.clone(),
                            overlay: Vec::new(),
                            replay_key,
                            query_identity_version: 2,
                        },
                        Arc::new(()),
                    )
                    .await
            }
        }
    }
}
