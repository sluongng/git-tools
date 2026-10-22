use crate::*;
use jj_lib::{
    backend::{MillisSinceEpoch, SecureSig, Signature, Timestamp},
    conflict_labels::ConflictLabels,
    merge::Merge,
};

pub(crate) fn model_commit(value: &Commit) -> BackendResult<m::Commit> {
    let terms = value
        .root_tree
        .iter()
        .map(|id| model_id(id.as_bytes()).map(m::TreeId))
        .collect::<BackendResult<Vec<_>>>()?;
    if value
        .conflict_labels
        .as_resolved()
        .is_some_and(|label| !label.is_empty())
    {
        return Err(invalid("resolved conflict label must be empty"));
    }
    let conflict_labels = ConflictLabels::from_merge(value.conflict_labels.clone());
    let labels = if conflict_labels.has_labels() {
        conflict_labels.as_slice().to_vec()
    } else {
        vec![String::new(); terms.len()]
    };
    Ok(m::Commit {
        parents: value
            .parents
            .iter()
            .map(|id| model_id(id.as_bytes()).map(m::CommitId))
            .collect::<BackendResult<_>>()?,
        predecessors: value
            .predecessors
            .iter()
            .map(|id| model_id(id.as_bytes()).map(m::CommitId))
            .collect::<BackendResult<_>>()?,
        root_tree: m::TreeMerge { terms, labels },
        change_id: m::ChangeId::from_slice(value.change_id.as_bytes()).map_err(other)?,
        description: value.description.clone(),
        author: model_signature(&value.author),
        committer: model_signature(&value.committer),
        secure_signature: value.secure_sig.as_ref().map(|sig| m::SecureSignature {
            data: sig.data.clone(),
            signature: sig.sig.clone(),
        }),
    })
}
pub(crate) fn commit(value: m::Commit) -> Commit {
    Commit {
        parents: value
            .parents
            .into_iter()
            .map(|id| CommitId::from_bytes(&id.0))
            .collect(),
        predecessors: value
            .predecessors
            .into_iter()
            .map(|id| CommitId::from_bytes(&id.0))
            .collect(),
        root_tree: Merge::from_vec(
            value
                .root_tree
                .terms
                .into_iter()
                .map(|id| TreeId::from_bytes(&id.0))
                .collect::<Vec<_>>(),
        ),
        conflict_labels: ConflictLabels::from_vec(value.root_tree.labels).into_merge(),
        change_id: ChangeId::from_bytes(&value.change_id.0),
        description: value.description,
        author: signature(value.author),
        committer: signature(value.committer),
        secure_sig: value.secure_signature.map(|sig| SecureSig {
            data: sig.data,
            sig: sig.signature,
        }),
    }
}
fn model_signature(value: &Signature) -> m::Signature {
    m::Signature {
        name: value.name.clone(),
        email: value.email.clone(),
        timestamp: m::Timestamp {
            millis_since_epoch: value.timestamp.timestamp.0,
            tz_offset_minutes: value.timestamp.tz_offset,
        },
    }
}
fn signature(value: m::Signature) -> Signature {
    Signature {
        name: value.name,
        email: value.email,
        timestamp: Timestamp {
            timestamp: MillisSinceEpoch(value.timestamp.millis_since_epoch),
            tz_offset: value.timestamp.tz_offset_minutes,
        },
    }
}
pub(crate) fn model_copy(value: &CopyHistory) -> BackendResult<m::CopyHistory> {
    Ok(m::CopyHistory {
        current_path: value.current_path.as_internal_file_string().to_owned(),
        parents: value
            .parents
            .iter()
            .map(|id| model_id(id.as_bytes()).map(m::CopyId))
            .collect::<BackendResult<_>>()?,
        salt: value.salt.clone(),
    })
}
pub(crate) fn copy(value: m::CopyHistory) -> BackendResult<CopyHistory> {
    Ok(CopyHistory {
        current_path: RepoPathBuf::from_internal_string(&value.current_path).map_err(other)?,
        parents: value
            .parents
            .into_iter()
            .map(|id| CopyId::from_bytes(&id.0))
            .collect(),
        salt: value.salt,
    })
}
