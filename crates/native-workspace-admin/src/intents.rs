use super::*;

/// Trusted explicit-resume information. Unlike ordinary workspace loading,
/// recovery can identify a stopped mount without consulting its control marker.
#[derive(Clone, Debug)]
pub struct ResumeInfo {
    pub binding: Binding,
    pub initial_commit: CommitId,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntentRecord {
    version: u32,
    root: PathBuf,
    repository: String,
    endpoint: Endpoint,
    workspace_id: [u8; 32],
    initial_commit: Vec<u8>,
    completed: bool,
}

/// An exclusive per-root provisioning lease. Keep this handle alive across the
/// mount RPC and jj initialization. Dropping it releases only the lock: pending
/// intent is durable and never expires or resets a partial initialization.
pub struct ProvisioningIntent {
    registry: Arc<Registry>,
    directory: PrivateDir,
    _lock: File,
    key: String,
    record: IntentRecord,
}
impl ProvisioningIntent {
    pub fn workspace_id(&self) -> [u8; 32] {
        self.record.workspace_id
    }
    pub fn initial_commit(&self) -> CommitId {
        CommitId::new(self.record.initial_commit.clone())
    }
    pub fn root(&self) -> &Path {
        &self.record.root
    }
    pub fn repository(&self) -> &str {
        &self.record.repository
    }
    pub fn endpoint(&self) -> LocalEndpoint {
        self.record.endpoint.local()
    }
    /// Mark terminal only after the exact workspace is privately registered as
    /// Ready. A crash before this write simply leaves an exact retry available.
    pub fn complete(&mut self, binding: &Binding) -> Result<()> {
        self.registry.validate(binding)?;
        ensure!(
            binding.root == self.record.root
                && binding.repository == self.record.repository
                && binding.endpoint == self.record.endpoint
                && binding.workspace_id == self.record.workspace_id,
            "provisioning completion differs from durable intent"
        );
        let ready = self.registry.resume_info(&self.record.root)?;
        ensure!(
            ready.binding == *binding && ready.initial_commit == self.initial_commit(),
            "provisioning completion requires the exact Ready registration"
        );
        let mut completed = self.record.clone();
        completed.completed = true;
        self.directory
            .replace(&self.key, &bounded_encode(&completed)?)?;
        self.record = completed;
        Ok(())
    }
}

impl Registry {
    /// Explicit recovery only. Never use this marker-free lookup to load a
    /// working copy or infer that a provider is mounted.
    pub fn resume_info(&self, root: &Path) -> Result<ResumeInfo> {
        normalized(root)?;
        let root = root.canonicalize()?;
        let _lock = self.lock()?;
        let mut found = None;
        for (count, entry) in fs::read_dir(self.records.path())?.enumerate() {
            ensure!(
                count < MAX_WORKSPACES,
                "native registry workspace limit exceeded"
            );
            let entry = entry?;
            let name = entry.file_name();
            let binding: Binding = serde_json::from_slice(&self.records.read(&name, MAX_RECORD)?)?;
            self.validate(&binding)?;
            ensure!(
                name == std::ffi::OsStr::new(&record_name(&binding.workspace_id)),
                "native registry record identity mismatch"
            );
            if binding.root != root {
                continue;
            }
            self.validate_ready(&binding)?;
            ensure!(
                found.is_none(),
                "multiple Ready registrations reserve this root"
            );
            let admin = PrivateDir::open_existing(&binding.admin)?;
            let journal: Initialization =
                serde_json::from_slice(&admin.read(JOURNAL, MAX_RECORD)?)?;
            ensure!(
                crate::restore::ready_attachment(&binding, &journal)?.is_none(),
                "restored origin requires typed current-state resume, not clone admission"
            );
            found = Some(ResumeInfo {
                binding,
                initial_commit: CommitId::new(journal.initial_commit),
            });
        }
        found.context("no Ready native workspace registration for this root; preserve pending initialization and reconcile explicitly")
    }

    /// Allocate and SyncAll-persist a random opaque ID before sending any mount
    /// RPC. The root lock also rejects competing aliases, endpoints, and heads.
    pub fn begin_provisioning(
        self: &Arc<Self>,
        root: &Path,
        repository: String,
        endpoint: LocalEndpoint,
        initial_commit: &CommitId,
    ) -> Result<ProvisioningIntent> {
        normalized(root)?;
        let root = root.canonicalize()?;
        let endpoint: Endpoint = endpoint.into();
        // Reuse the full binding validator without provisioning any state.
        self.binding(
            [1; 32],
            &root,
            repository.clone(),
            endpoint.local(),
            "intent".into(),
        )?;
        ensure!(
            initial_commit.as_bytes().len() == 32,
            "invalid native bootstrap commit identity"
        );
        let (directory, lock, key) = self
            .intent_lease(&root, true)?
            .context("client intent directory unavailable")?;
        if let Some(record) = self.read_intent(&directory, &key, &root)? {
            ensure!(
                record.repository == repository
                    && record.endpoint == endpoint
                    && record.initial_commit == initial_commit.as_bytes(),
                "another durable provisioning request reserves this root; recover the original intent explicitly"
            );
            ensure!(
                !record.completed,
                "workspace provisioning is already complete; use explicit resume"
            );
            return Ok(ProvisioningIntent {
                registry: self.clone(),
                directory,
                _lock: lock,
                key,
                record,
            });
        }
        let mut workspace_id = [0; 32];
        getrandom::fill(&mut workspace_id)
            .map_err(|error| anyhow::anyhow!("workspace identity entropy unavailable: {error}"))?;
        ensure!(workspace_id != [0; 32], "invalid random workspace identity");
        let record = IntentRecord {
            version: 1,
            root,
            repository,
            endpoint,
            workspace_id,
            initial_commit: initial_commit.as_bytes().to_vec(),
            completed: false,
        };
        {
            let _global = self.lock()?;
            for (count, entry) in fs::read_dir(self.records.path())?.enumerate() {
                ensure!(
                    count < MAX_WORKSPACES,
                    "native registry workspace limit exceeded"
                );
                let entry = entry?;
                let existing: Binding =
                    serde_json::from_slice(&self.records.read(entry.file_name(), MAX_RECORD)?)?;
                self.validate(&existing)?;
                ensure!(
                    existing.root != record.root,
                    "this root already has a registered workspace; use explicit resume"
                );
            }
            for (count, entry) in fs::read_dir(directory.path())?.enumerate() {
                let _entry = entry?;
                ensure!(count < MAX_WORKSPACES, "native client intent limit reached");
            }
            directory.replace(&key, &bounded_encode(&record)?)?;
        }
        Ok(ProvisioningIntent {
            registry: self.clone(),
            directory,
            _lock: lock,
            key,
            record,
        })
    }

    /// Recover the original pending request without inventing a new identity or
    /// treating an interrupted jj initialization as safe to reset.
    pub fn pending_provisioning(
        self: &Arc<Self>,
        root: &Path,
    ) -> Result<Option<ProvisioningIntent>> {
        normalized(root)?;
        let root = root.canonicalize()?;
        let Some((directory, lock, key)) = self.intent_lease(&root, false)? else {
            return Ok(None);
        };
        let Some(record) = self.read_intent(&directory, &key, &root)? else {
            return Ok(None);
        };
        if record.completed {
            return Ok(None);
        }
        Ok(Some(ProvisioningIntent {
            registry: self.clone(),
            directory,
            _lock: lock,
            key,
            record,
        }))
    }

    fn read_intent(
        &self,
        directory: &PrivateDir,
        key: &str,
        root: &Path,
    ) -> Result<Option<IntentRecord>> {
        let bytes = match directory.read(key, MAX_RECORD) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let record: IntentRecord = serde_json::from_slice(&bytes)?;
        ensure!(
            record.version == 1
                && record.root == root
                && record.workspace_id != [0; 32]
                && record.initial_commit.len() == 32,
            "invalid durable provisioning intent; preserve and reconcile explicitly"
        );
        self.binding(
            record.workspace_id,
            root,
            record.repository.clone(),
            record.endpoint.local(),
            "intent".into(),
        )?;
        Ok(Some(record))
    }

    fn intent_lease(
        &self,
        root: &Path,
        create: bool,
    ) -> Result<Option<(PrivateDir, File, String)>> {
        let key = format!("{}.json", blake3::hash(&serde_json::to_vec(root)?).to_hex());
        let lock = self
            .root
            .open_lock(format!("provision-{}.lock", &key[..64]))?;
        lock.lock_exclusive()?;
        let _global = self.lock()?;
        let mut header: RegistryHeader =
            serde_json::from_slice(&self.root.read("registry.json", 1024)?)?;
        ensure!(
            header.version == 1 && header.ready,
            "native registry is incomplete"
        );
        if !header.client_intents {
            if !create {
                return Ok(None);
            }
            let path = self.root.path().join("client-intents");
            // A previous interrupted capability publication may have created
            // only this empty directory. Nonempty unrecognized state is kept.
            let directory = PrivateDir::create(&path)?;
            ensure!(
                fs::read_dir(directory.path())?.next().is_none(),
                "unrecognized client intent state; preserve and reconcile explicitly"
            );
            header.client_intents = true;
            self.root
                .replace("registry.json", &serde_json::to_vec(&header)?)?;
            return Ok(Some((directory, lock, key)));
        }
        Ok(Some((
            PrivateDir::open_existing(self.root.path().join("client-intents"))?,
            lock,
            key,
        )))
    }
}
