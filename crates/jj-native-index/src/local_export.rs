//! Existing local checkpoint export without providers, factories or cleanup.
use super::*;

pub struct LocalIndexExportReader {
    storage: Arc<Storage>,
    limits: Limits,
}
impl LocalIndexExportReader {
    /// Conservative local loader/source-codec admission ceiling, not RSS.
    pub fn required_read_bytes(limits: &Limits) -> Result<usize, Error> {
        portable::local_read_peak(limits)
    }
    /// Opens only existing, identity-checked storage. No index rebuilding, live
    /// queries, canonical reads, directory creation or broker connection occurs.
    /// The caller owns this independent read reservation through synchronous
    /// open/fingerprint work. Reject an insufficient pool before any source I/O.
    pub fn open_existing(
        path: &Path,
        expected: &Config,
        limits: Limits,
        reserved_bytes: usize,
    ) -> Result<Self, Error> {
        limits.validate()?;
        expected.validate()?;
        if Self::required_read_bytes(&limits)? > reserved_bytes {
            return Err(Error::Limit);
        }
        Ok(Self {
            storage: Arc::new(Storage::open(path, expected, &limits)?),
            limits,
        })
    }
    pub fn association_fingerprint(
        &self,
        ids: &[[u8; 64]],
        max_reads: usize,
        max_bytes: usize,
    ) -> Result<[u8; 32], Error> {
        if ids.is_empty() || ids.len() > 65_537 {
            return Err(Error::Limit);
        }
        let mut reads = persistence::ExportReadBudget {
            reads: 0,
            bytes: 0,
            max_reads,
            max_bytes,
        };
        let mut hash = blake3::Hasher::new();
        hash.update(b"native-local-export-associations-v1\0");
        hash.update(&self.storage.identity());
        for id in ids {
            let (tips, source) =
                self.storage
                    .operation_export(id, &self.limits, Some(&mut reads))?;
            hash.update(id);
            hash.update(&(tips.len() as u64).to_be_bytes());
            for tip in tips {
                hash.update(&tip);
            }
            let bytes = source.encode(&self.limits)?;
            hash.update(&(bytes.len() as u64).to_be_bytes());
            hash.update(&bytes);
        }
        Ok(*hash.finalize().as_bytes())
    }
    pub async fn export_operation_indexes(
        &self,
        operation_ids: &[[u8; 64]],
        limits: PortableExportLimits,
        sink: &mut dyn IndexArtifactSink,
        owner: QueryOwner,
    ) -> Result<PortableIndexExport, Error> {
        portable::export_local(
            &self.storage,
            &self.limits,
            self.storage.clone(),
            operation_ids,
            limits,
            sink,
            owner,
        )
        .await
    }
}
