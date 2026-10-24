//! Immutable checkpoint source pins, never portable host configuration or grants.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum CheckpointSource {
    LiveRoots,
    CloudSnapshot {
        pins: native_broker::CloudQuerySource,
        /// Exact frozen membership; later deltas never mutate these tips.
        baseline_tips: Vec<SegmentId>,
    },
}
impl CheckpointSource {
    /// Union exact frozen selections from ONE immutable authenticated version.
    /// Live roots and different versions remain separate authority domains.
    pub(super) fn merge(&self, other: &Self, limits: &Limits) -> Result<Self, Error> {
        if self == other {
            return Ok(self.clone());
        }
        let (
            Self::CloudSnapshot {
                pins: left,
                baseline_tips: left_tips,
            },
            Self::CloudSnapshot {
                pins: right,
                baseline_tips: right_tips,
            },
        ) = (self, other)
        else {
            return Err(Error::Unsupported);
        };
        if left.provider_identity != right.provider_identity
            || left.workspace_id != right.workspace_id
            || left.generation != right.generation
            || left.manifest_id != right.manifest_id
            || left.manifest_blake3 != right.manifest_blake3
            || left.manifest_size_bytes != right.manifest_size_bytes
        {
            return Err(Error::Unsupported);
        }
        self.validate(limits)?;
        other.validate(limits)?;
        let index_root_ids: BTreeSet<_> = left
            .index_root_ids
            .iter()
            .chain(&right.index_root_ids)
            .copied()
            .collect();
        let expected_anchors: BTreeSet<_> = left
            .expected_anchors
            .iter()
            .chain(&right.expected_anchors)
            .copied()
            .collect();
        let baseline_tips: BTreeSet<_> = left_tips.iter().chain(right_tips).copied().collect();
        if index_root_ids.len() > limits.max_roots
            || expected_anchors.len() > limits.max_roots
            || baseline_tips.len() > limits.max_segments
        {
            return Err(Error::Limit);
        }
        let mut pins = left.clone();
        pins.index_root_ids = index_root_ids.into_iter().collect();
        pins.expected_anchors = expected_anchors.into_iter().collect();
        let merged = Self::CloudSnapshot {
            pins,
            baseline_tips: baseline_tips.into_iter().collect(),
        };
        merged.validate(limits)?;
        // Check persisted source framing before installing any mutable state.
        merged.encode(limits)?;
        Ok(merged)
    }
    pub(super) fn memo_key(&self, graph: [u8; 32]) -> [u8; 32] {
        if matches!(self, Self::LiveRoots) {
            return graph;
        }
        let mut hash = blake3::Hasher::new();
        hash.update(b"jj-native-index-cloud-checkpoint-v1\0");
        hash.update(&graph);
        if let Self::CloudSnapshot {
            pins,
            baseline_tips,
        } = self
        {
            hash.update(&pins.digest());
            for tip in baseline_tips {
                hash.update(tip);
            }
        }
        *hash.finalize().as_bytes()
    }
    pub(super) fn validate_checkpoint(
        &self,
        storage: &Storage,
        roots: &[m::CommitId],
        drafts: &[Arc<Draft>],
        limits: &Limits,
    ) -> Result<BTreeSet<m::CommitId>, Error> {
        let Self::CloudSnapshot {
            pins,
            baseline_tips,
        } = self
        else {
            return Ok(BTreeSet::new());
        };
        self.validate(limits)?;
        if pins.provider_identity != storage.config().provider_fingerprint
            || pins.expected_anchors.as_slice() != roots
        {
            return Err(Error::Foreign);
        }
        let (baseline_roots, baseline) = storage.load(baseline_tips, limits)?;
        if baseline_roots != roots {
            return Err(Error::Corrupt);
        }
        let current: BTreeMap<_, _> = drafts.iter().map(|draft| (draft.id(), draft)).collect();
        let mut ids = BTreeSet::new();
        for draft in baseline {
            let Some(actual) = current.get(&draft.id()) else {
                return Err(Error::Corrupt);
            };
            if !actual.canonical_eq(&draft) || actual.order != draft.order {
                return Err(Error::Corrupt);
            }
            ids.insert(draft.id());
        }
        Ok(ids)
    }
    pub(super) fn validate(&self, limits: &Limits) -> Result<(), Error> {
        if let Self::CloudSnapshot {
            pins,
            baseline_tips,
        } = self
        {
            if pins.provider_identity == [0; 32]
                || pins.generation == 0
                || pins.manifest_size_bytes == 0
                || pins.index_root_ids.is_empty()
                || pins.index_root_ids.len() > limits.max_roots
                || !pins.index_root_ids.windows(2).all(|ids| ids[0] < ids[1])
                || pins.expected_anchors.is_empty()
                || pins.expected_anchors.len() > limits.max_roots
                || !pins.expected_anchors.windows(2).all(|ids| ids[0] < ids[1])
                || baseline_tips.is_empty()
                || baseline_tips.len() > limits.max_segments
            {
                return Err(Error::Invalid);
            }
        }
        Ok(())
    }
    pub(super) fn encode(&self, limits: &Limits) -> Result<Vec<u8>, Error> {
        self.validate(limits)?;
        let Self::CloudSnapshot {
            pins,
            baseline_tips,
        } = self
        else {
            return Ok(vec![0]);
        };
        let multi = pins.index_root_ids.len() > 1;
        let size = 185usize
            .checked_add(
                pins.index_root_ids
                    .len()
                    .checked_sub(1)
                    .and_then(|n| n.checked_mul(32))
                    .ok_or(Error::Limit)?,
            )
            .and_then(|n| n.checked_add(if multi { 4 } else { 0 }))
            .ok_or(Error::Limit)?
            .checked_add(
                pins.expected_anchors
                    .len()
                    .checked_mul(32)
                    .ok_or(Error::Limit)?,
            )
            .and_then(|n| n.checked_add(baseline_tips.len().checked_mul(32)?))
            .ok_or(Error::Limit)?;
        if size
            .checked_add(45)
            .is_none_or(|n| n > limits.max_segment_bytes)
        {
            return Err(Error::Limit);
        }
        let mut out = Vec::with_capacity(size);
        // Tag1 stays byte-identical. Old readers reject tag2 instead of
        // interpreting a multi-selection as live roots or a smaller snapshot.
        out.push(if multi { 2 } else { 1 });
        out.extend_from_slice(&pins.provider_identity);
        out.extend_from_slice(&pins.workspace_id);
        out.extend_from_slice(&pins.generation.to_le_bytes());
        out.extend_from_slice(&pins.manifest_id);
        out.extend_from_slice(&pins.manifest_blake3);
        out.extend_from_slice(&pins.manifest_size_bytes.to_le_bytes());
        if multi {
            out.extend_from_slice(&(pins.index_root_ids.len() as u32).to_le_bytes());
        }
        for root in &pins.index_root_ids {
            out.extend_from_slice(root);
        }
        out.extend_from_slice(&(pins.expected_anchors.len() as u32).to_le_bytes());
        for id in &pins.expected_anchors {
            out.extend_from_slice(&id.0);
        }
        out.extend_from_slice(&(baseline_tips.len() as u32).to_le_bytes());
        for tip in baseline_tips {
            out.extend_from_slice(tip);
        }
        if out.len() > limits.max_segment_bytes {
            return Err(Error::Limit);
        }
        Ok(out)
    }
    pub(super) fn decode(bytes: &[u8], limits: &Limits) -> Result<Self, Error> {
        let mut r = persistence::Reader(bytes);
        let [tag] = r.array()?;
        if tag == 0 {
            r.end()?;
            return Ok(Self::LiveRoots);
        }
        if tag != 1 && tag != 2 {
            return Err(Error::Unsupported);
        }
        let provider_identity = r.array()?;
        let workspace_id = r.array()?;
        let generation = u64::from_le_bytes(r.array()?);
        let manifest_id = r.array()?;
        let manifest_blake3 = r.array()?;
        let manifest_size_bytes = u64::from_le_bytes(r.array()?);
        let index_root_ids = if tag == 1 {
            vec![r.array()?]
        } else {
            let n = r.count_width(limits.max_roots, 32)?;
            if n < 2 {
                return Err(Error::Corrupt);
            }
            let mut roots = Vec::with_capacity(n);
            for _ in 0..n {
                roots.push(r.array()?);
            }
            roots
        };
        let n = r.count_width(limits.max_roots, 32)?;
        let mut expected_anchors = Vec::with_capacity(n);
        for _ in 0..n {
            expected_anchors.push(m::CommitId(r.array()?));
        }
        let n = r.count_width(limits.max_segments, 32)?;
        let mut baseline_tips = Vec::with_capacity(n);
        for _ in 0..n {
            baseline_tips.push(r.array()?);
        }
        r.end()?;
        let result = Self::CloudSnapshot {
            pins: native_broker::CloudQuerySource {
                provider_identity,
                workspace_id,
                generation,
                manifest_id,
                manifest_blake3,
                manifest_size_bytes,
                index_root_ids,
                expected_anchors,
            },
            baseline_tips,
        };
        result.validate(limits)?;
        Ok(result)
    }
}
