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
                || pins.index_root_ids.len() != 1
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
        let size = 185usize
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
        out.push(1);
        out.extend_from_slice(&pins.provider_identity);
        out.extend_from_slice(&pins.workspace_id);
        out.extend_from_slice(&pins.generation.to_le_bytes());
        out.extend_from_slice(&pins.manifest_id);
        out.extend_from_slice(&pins.manifest_blake3);
        out.extend_from_slice(&pins.manifest_size_bytes.to_le_bytes());
        out.extend_from_slice(&pins.index_root_ids[0]);
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
        if tag != 1 {
            return Err(Error::Unsupported);
        }
        let provider_identity = r.array()?;
        let workspace_id = r.array()?;
        let generation = u64::from_le_bytes(r.array()?);
        let manifest_id = r.array()?;
        let manifest_blake3 = r.array()?;
        let manifest_size_bytes = u64::from_le_bytes(r.array()?);
        let root = r.array()?;
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
                index_root_ids: vec![root],
                expected_anchors,
            },
            baseline_tips,
        };
        result.validate(limits)?;
        Ok(result)
    }
}
