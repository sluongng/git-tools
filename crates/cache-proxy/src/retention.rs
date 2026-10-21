//! Best-effort retention touches, never a substitute for authoritative presence.
//! One bounded worker has its own connection and never holds local blob leases.
use super::*;
use std::{collections::HashSet, sync::Weak};
use tokio::sync::{Notify, watch};

const BATCH: usize = 256;
const MAX_DIGESTS: usize = 65536;
const MAX_AC_BYTES: usize = 64 * 1024 * 1024;
const MAX_AC_KEY: usize = 64 * 1024;
struct ActionWork {
    key: Vec<u8>,
    version: u64,
    generation: u64,
    refresh_metadata: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Metrics {
    pub action_refreshes: u64,
    pub output_refreshes: u64,
    pub blob_refreshes: u64,
    pub failures: u64,
    pub missing_blobs: u64,
    pub dropped: u64,
}
#[derive(Default)]
struct Counters {
    actions: AtomicU64,
    outputs: AtomicU64,
    blobs: AtomicU64,
    failures: AtomicU64,
    missing: AtomicU64,
    dropped: AtomicU64,
}
pub(super) struct ActionEntry {
    pub expires: Instant,
    pub value: ActionResult,
    confirmed: Instant,
    outputs_due: Instant,
    accessed: Instant,
    used: bool,
    attempted: Option<Instant>,
    busy: bool,
    version: u64,
}
#[derive(Default)]
pub(super) struct Actions {
    pub entries: HashMap<Vec<u8>, ActionEntry>,
    pub generation: u64,
    serial: u64,
}
struct Touch {
    confirmed: Option<Instant>,
    accessed: Instant,
    attempted: Option<Instant>,
    used: bool,
    busy: bool,
}
pub(super) struct Retention {
    owner: Weak<Upstream>,
    pub actions: Arc<Mutex<Actions>>,
    blobs: Mutex<HashMap<Artifact, Touch>>,
    notify: Notify,
    stop: watch::Sender<bool>,
    enabled: bool,
    interval: Duration,
    window: Duration,
    capacity: usize,
    counters: Counters,
}
impl Drop for Upstream {
    fn drop(&mut self) {
        self.retention.stop();
    }
}
impl Retention {
    pub fn new(owner: Weak<Upstream>, config: &ProxyConfig) -> Arc<Self> {
        Arc::new(Self {
            owner,
            actions: Default::default(),
            blobs: Default::default(),
            notify: Notify::new(),
            stop: watch::channel(false).0,
            enabled: !config.upstream_retention.is_zero(),
            interval: config.retention_refresh_interval,
            window: config.upstream_retention,
            capacity: config.retention_max_entries,
            counters: Counters::default(),
        })
    }
    pub fn metrics(&self) -> Metrics {
        Metrics {
            action_refreshes: self.counters.actions.load(Ordering::Relaxed),
            output_refreshes: self.counters.outputs.load(Ordering::Relaxed),
            blob_refreshes: self.counters.blobs.load(Ordering::Relaxed),
            failures: self.counters.failures.load(Ordering::Relaxed),
            missing_blobs: self.counters.missing.load(Ordering::Relaxed),
            dropped: self.counters.dropped.load(Ordering::Relaxed),
        }
    }
    pub fn stop(&self) {
        self.stop.send_replace(true);
    }
    fn due(&self, confirmed: Option<Instant>, attempted: Option<Instant>, now: Instant) -> bool {
        confirmed.is_none_or(|at| now.duration_since(at) >= self.interval)
            && attempted.is_none_or(|at| {
                now.duration_since(at) >= self.interval.min(Duration::from_secs(60))
            })
    }
    pub fn accessed(&self, artifact: &Artifact) {
        if !self.enabled || *self.stop.borrow() {
            return;
        }
        let Some(upstream) = self.owner.upgrade() else {
            return;
        };
        if artifact.backend != upstream.config.backend_identity
            || artifact.instance != upstream.config.instance_name
        {
            return;
        }
        let mut blobs = self.blobs.lock().unwrap();
        let now = Instant::now();
        if !blobs.contains_key(artifact) && blobs.len() >= self.capacity {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let touch = blobs.entry(artifact.clone()).or_insert(Touch {
            confirmed: None,
            accessed: now,
            attempted: None,
            used: true,
            busy: false,
        });
        touch.accessed = now;
        touch.used = true;
        if !touch.busy && self.due(touch.confirmed, touch.attempted, now) {
            self.notify.notify_one();
        }
    }
    pub fn confirm(&self, artifact: &Artifact, started: Instant, register: bool) {
        if !self.enabled {
            return;
        }
        let mut blobs = self.blobs.lock().unwrap();
        if !blobs.contains_key(artifact) && (!register || blobs.len() >= self.capacity) {
            return;
        }
        let touch = blobs.entry(artifact.clone()).or_insert(Touch {
            confirmed: Some(started),
            accessed: started,
            attempted: None,
            used: false,
            busy: false,
        });
        if touch.confirmed.is_none_or(|at| at <= started) {
            touch.confirmed = Some(started);
            touch.used = touch.accessed > started;
        }
    }
    pub fn observe_presence(&self, artifacts: &[Artifact], missing: &[Digest], started: Instant) {
        if !self.enabled {
            return;
        }
        let missing: HashSet<_> = missing
            .iter()
            .map(|d| (d.hash.as_str(), d.size_bytes))
            .collect();
        for artifact in artifacts {
            if !missing.contains(&(artifact.hash.as_str(), artifact.size as i64)) {
                self.confirm(artifact, started, false);
            } else if let Some(touch) = self.blobs.lock().unwrap().get_mut(artifact)
                && touch.confirmed.is_none_or(|at| at <= started)
            {
                touch.confirmed = None;
                touch.attempted = Some(started);
            }
        }
    }
    pub fn action_hit(&self, entry: &mut ActionEntry) {
        if self.enabled {
            entry.used = true;
            entry.accessed = Instant::now();
            if !entry.busy && self.action_due(entry, Instant::now()) {
                self.notify.notify_one();
            }
        }
    }
    fn action_due(&self, entry: &ActionEntry, now: Instant) -> bool {
        (now >= entry.outputs_due || now.duration_since(entry.confirmed) >= self.interval)
            && entry.attempted.is_none_or(|at| {
                now.duration_since(at) >= self.interval.min(Duration::from_secs(60))
            })
    }
    pub fn clear_actions(&self) {
        let mut actions = self.actions.lock().unwrap();
        actions.generation = actions.generation.wrapping_add(1);
        actions.entries.clear();
    }
    pub fn store_action(
        &self,
        key: Vec<u8>,
        value: ActionResult,
        started: Instant,
        generation: u64,
        expected_version: Option<u64>,
    ) {
        let Some(upstream) = self.owner.upgrade() else {
            return;
        };
        let config = &upstream.config;
        if config.action_cache_ttl.is_zero()
            || config.max_action_cache_entries == 0
            || key.len() > MAX_AC_KEY
            || key.len() + value.encoded_len() > MAX_AC_BYTES
        {
            return;
        }
        let mut actions = self.actions.lock().unwrap();
        if actions.generation != generation
            || expected_version
                .is_some_and(|v| actions.entries.get(&key).is_none_or(|e| e.version != v))
        {
            return;
        }
        let now = Instant::now();
        let previous = actions.entries.get(&key);
        let outputs_due = if expected_version.is_some() {
            started + self.interval
        } else {
            previous.map_or(started, |e| e.outputs_due)
        };
        let was_used = expected_version.is_none() || previous.is_some_and(|e| e.accessed > started);
        let accessed = previous.map_or(started, |e| e.accessed.max(started));
        let attempted = if expected_version.is_some() {
            None
        } else {
            previous.and_then(|e| e.attempted)
        };
        // Expired AC values are not served. A bounded recently-used entry may
        // remain eligible for background renewal until its retention window ends.
        actions.entries.retain(|_, entry| {
            entry.expires > now
                || (self.enabled && entry.used && now.duration_since(entry.confirmed) < self.window)
        });
        actions.entries.remove(&key);
        let bytes = key.len() + value.encoded_len();
        let mut used: usize = actions
            .entries
            .iter()
            .map(|(k, e)| k.len() + e.value.encoded_len())
            .sum();
        while !actions.entries.is_empty()
            && (actions.entries.len() >= config.max_action_cache_entries
                || used + bytes > MAX_AC_BYTES)
        {
            let old = actions
                .entries
                .iter()
                .min_by_key(|(_, e)| e.expires)
                .map(|(k, _)| k.clone())
                .unwrap();
            let entry = actions.entries.remove(&old).unwrap();
            used -= old.len() + entry.value.encoded_len();
        }
        actions.serial = actions.serial.wrapping_add(1);
        let version = actions.serial;
        actions.entries.insert(
            key,
            ActionEntry {
                expires: started + config.action_cache_ttl,
                value,
                confirmed: started,
                outputs_due,
                accessed,
                used: was_used,
                attempted,
                busy: false,
                version,
            },
        );
        if self.enabled {
            self.notify.notify_one();
        }
    }
    pub fn start(self: &Arc<Self>) {
        if !self.enabled {
            return;
        }
        let worker = self.clone();
        tokio::spawn(async move { worker.run().await });
    }
    async fn run(self: Arc<Self>) {
        let mut stop = self.stop.subscribe();
        let tick = self
            .interval
            .div_f64(4.0)
            .min(Duration::from_secs(60))
            .max(Duration::from_millis(1));
        loop {
            if *stop.borrow() {
                break;
            }
            tokio::select! {
                biased;
                _ = stop.changed() => break,
                _ = self.notify.notified() => {},
                _ = tokio::time::sleep(tick) => {},
            }
            // One operation at a time, on a separate channel. Foreground reads
            // never queue behind this worker or wait for retention maintenance.
            loop {
                let Some(upstream) = self.owner.upgrade() else {
                    return;
                };
                let (blobs, action) = self.take_work();
                if blobs.is_empty() && action.is_none() {
                    break;
                }
                let operation = async {
                    if !blobs.is_empty() {
                        self.refresh_blobs(&upstream, &blobs).await;
                    }
                    if let Some(action) = action {
                        self.refresh_action(&upstream, action).await;
                    }
                };
                tokio::select! {
                    biased;
                    _ = stop.changed() => return,
                    _ = operation => {},
                }
                drop(upstream);
                tokio::task::yield_now().await;
            }
        }
    }
    fn take_work(&self) -> (Vec<Artifact>, Option<ActionWork>) {
        let now = Instant::now();
        let mut blobs = self.blobs.lock().unwrap();
        blobs.retain(|_, t| t.busy || now.duration_since(t.accessed) < self.window);
        let mut batch = Vec::new();
        for (artifact, touch) in blobs.iter_mut() {
            if batch.len() == BATCH {
                break;
            }
            if touch.used
                && !touch.busy
                && self.due(touch.confirmed, touch.attempted, now)
                && batch
                    .first()
                    .is_none_or(|a: &Artifact| a.digest_function == artifact.digest_function)
            {
                touch.busy = true;
                touch.attempted = Some(now);
                batch.push(artifact.clone());
            }
        }
        drop(blobs);
        let mut actions = self.actions.lock().unwrap();
        actions.entries.retain(|_, e| {
            e.expires > now || (e.used && now.duration_since(e.confirmed) < self.window)
        });
        let generation = actions.generation;
        let action = actions.entries.iter_mut().find_map(|(key, e)| {
            if e.used && !e.busy && self.action_due(e, now) {
                e.busy = true;
                e.attempted = Some(now);
                Some(ActionWork {
                    key: key.clone(),
                    version: e.version,
                    generation,
                    refresh_metadata: e.expires <= now
                        || now.duration_since(e.confirmed) >= self.interval,
                })
            } else {
                None
            }
        });
        (batch, action)
    }
    fn budget(&self, upstream: &Upstream) -> tokio::time::Instant {
        tokio::time::Instant::now()
            + upstream
                .config
                .policy
                .background_timeout()
                .min(Duration::from_secs(30))
                .min(self.window - self.interval)
    }
    async fn refresh_blobs(&self, upstream: &Upstream, artifacts: &[Artifact]) {
        let started = Instant::now();
        let deadline = self.budget(upstream);
        let result = tokio::time::timeout_at(
            deadline,
            upstream.config.policy.retry(
                deadline,
                upstream
                    .config
                    .policy
                    .rpc_timeout()
                    .min(Duration::from_secs(10)),
                |_| upstream.touch(artifacts),
            ),
        )
        .await;
        let mut blobs = self.blobs.lock().unwrap();
        match result {
            Ok(Ok(missing)) => {
                self.counters
                    .blobs
                    .fetch_add((artifacts.len() - missing.len()) as u64, Ordering::Relaxed);
                self.counters
                    .missing
                    .fetch_add(missing.len() as u64, Ordering::Relaxed);
                for a in artifacts {
                    if let Some(t) = blobs.get_mut(a) {
                        t.busy = false;
                        // Do not let an older background result overwrite a
                        // newer foreground confirmation.
                        if t.confirmed.is_none_or(|at| at <= started) {
                            t.confirmed = (!missing.contains(a)).then_some(started);
                            t.used = t.accessed > started;
                        }
                    }
                }
                if !missing.is_empty() {
                    drop(blobs);
                    self.clear_actions();
                }
            }
            _ => {
                self.counters.failures.fetch_add(1, Ordering::Relaxed);
                for a in artifacts {
                    if let Some(t) = blobs.get_mut(a) {
                        t.busy = false;
                    }
                }
            }
        }
    }
    async fn refresh_action(
        &self,
        upstream: &Upstream,
        ActionWork {
            key,
            version,
            generation,
            refresh_metadata,
        }: ActionWork,
    ) {
        let started = Instant::now();
        let cached = if refresh_metadata {
            None
        } else {
            let actions = self.actions.lock().unwrap();
            if actions.generation != generation {
                return;
            }
            let Some(entry) = actions.entries.get(&key).filter(|e| e.version == version) else {
                return;
            };
            Some(entry.value.clone())
        };
        let deadline = self.budget(upstream);
        let result = tokio::time::timeout_at(
            deadline,
            upstream.config.policy.retry(
                deadline,
                upstream
                    .config
                    .policy
                    .rpc_timeout()
                    .min(Duration::from_secs(10)),
                |_| async {
                    let request = GetActionResultRequest::decode(key.as_slice())
                        .map_err(|_| Status::internal("invalid cached request"))?;
                    let function = if request.digest_function == 0 {
                        1
                    } else {
                        request.digest_function
                    };
                    let value = if let Some(value) = &cached {
                        value.clone()
                    } else {
                        upstream.retained_action(request).await?
                    };
                    upstream.touch_outputs(&value, function).await?;
                    Ok::<_, Status>(value)
                },
            ),
        )
        .await;
        if matches!(result, Ok(Ok(_))) {
            self.counters.outputs.fetch_add(1, Ordering::Relaxed);
        }
        let mut actions = self.actions.lock().unwrap();
        if actions.generation != generation {
            return;
        }
        if matches!(result, Ok(Err(ref s)) if matches!(s.code(), tonic::Code::NotFound | tonic::Code::PermissionDenied | tonic::Code::Unauthenticated))
        {
            self.counters.failures.fetch_add(1, Ordering::Relaxed);
            actions.entries.remove(&key);
            return;
        }
        let Some(entry) = actions.entries.get_mut(&key) else {
            return;
        };
        if entry.version != version {
            // A foreground AC read may win the race without changing the
            // result. Keep its newer AC timestamp, but do not forget completed
            // output maintenance and immediately repeat the same closure walk.
            if let Ok(Ok(value)) = &result
                && entry.value == *value
            {
                entry.outputs_due = started + self.interval;
                entry.used = entry.accessed > started;
            }
            return;
        }
        match result {
            Ok(Ok(_)) if !refresh_metadata => {
                entry.outputs_due = started + self.interval;
                entry.used = entry.accessed > started;
                entry.busy = false;
                entry.attempted = None;
            }
            Ok(Ok(value)) => {
                // Reuse insertion's byte/count admission and fixed timestamp.
                drop(actions);
                self.store_action(key, value, started, generation, Some(version));
                self.counters.actions.fetch_add(1, Ordering::Relaxed);
            }
            _ => {
                self.counters.failures.fetch_add(1, Ordering::Relaxed);
                if let Some(e) = actions.entries.get_mut(&key) {
                    e.busy = false;
                }
            }
        }
    }
}

impl Upstream {
    async fn retained_action(
        &self,
        request: GetActionResultRequest,
    ) -> Result<ActionResult, Status> {
        let mut client =
            action_cache_client::ActionCacheClient::new(self.maintenance_channel.clone())
                .accept_compressed(CompressionEncoding::Gzip);
        if self.send_gzip(RpcMethod::GetActionResult) {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }
        let response = client.get_action_result(self.request(request)).await?;
        self.observe_gzip(RpcMethod::GetActionResult, &response);
        Ok(response.into_inner())
    }
    async fn touch(&self, artifacts: &[Artifact]) -> Result<Vec<Artifact>, Status> {
        if artifacts.is_empty() {
            return Ok(vec![]);
        }
        let mut missing = Vec::new();
        for batch in artifacts.chunks(BATCH) {
            let mut client =
                content_addressable_storage_client::ContentAddressableStorageClient::new(
                    self.maintenance_channel.clone(),
                )
                .accept_compressed(CompressionEncoding::Gzip);
            if self.send_gzip(RpcMethod::FindMissingBlobs) {
                client = client.send_compressed(CompressionEncoding::Gzip);
            }
            let response = client
                .find_missing_blobs(
                    self.request(FindMissingBlobsRequest {
                        instance_name: self.config.instance_name.clone(),
                        digest_function: batch[0].digest_function,
                        blob_digests: batch
                            .iter()
                            .map(|a| Digest {
                                hash: a.hash.clone(),
                                size_bytes: a.size as i64,
                            })
                            .collect(),
                    }),
                )
                .await?;
            self.observe_gzip(RpcMethod::FindMissingBlobs, &response);
            for digest in response.into_inner().missing_blob_digests {
                let artifact = batch
                    .iter()
                    .find(|a| a.hash == digest.hash && a.size as i64 == digest.size_bytes)
                    .ok_or_else(|| Status::data_loss("unrequested missing digest"))?;
                if !missing.contains(artifact) {
                    missing.push(artifact.clone());
                }
            }
        }
        Ok(missing)
    }
    async fn retention_metadata(
        &self,
        digest: &Digest,
        function: i32,
        remaining: &mut usize,
    ) -> Result<Vec<u8>, Status> {
        let artifact = self.artifact(digest, function)?;
        if artifact.size > *remaining as u64 {
            return Err(Status::resource_exhausted(
                "retention metadata budget exceeded",
            ));
        }
        let mut stream = ByteStreamClient::new(self.maintenance_channel.clone())
            .accept_compressed(CompressionEncoding::Gzip)
            .read(self.request(bs::ReadRequest {
                resource_name: resource(&artifact),
                read_offset: 0,
                read_limit: 0,
            }))
            .await?
            .into_inner();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.message().await? {
            if bytes.len().saturating_add(chunk.data.len()) > artifact.size as usize {
                return Err(Status::data_loss("overlong retention metadata"));
            }
            bytes.extend(chunk.data);
        }
        verify(&bytes, &artifact)?;
        *remaining -= bytes.len();
        Ok(bytes)
    }
    async fn touch_outputs(&self, value: &ActionResult, function: i32) -> Result<(), Status> {
        let mut digests = HashMap::new();
        let mut add = |d: &Digest| -> Result<(), Status> {
            let a = self.artifact(d, function)?;
            if digests.len() >= MAX_DIGESTS {
                return Err(Status::resource_exhausted(
                    "retention digest budget exceeded",
                ));
            }
            digests.insert((a.hash.clone(), a.size), a);
            Ok(())
        };
        for f in &value.output_files {
            if let Some(d) = &f.digest {
                add(d)?;
            }
        }
        for d in [&value.stdout_digest, &value.stderr_digest]
            .into_iter()
            .flatten()
        {
            add(d)?;
        }
        let mut remaining = self.config.max_batch_bytes;
        for output in &value.output_directories {
            if let Some(d) = &output.tree_digest {
                add(d)?;
                let bytes = self.retention_metadata(d, function, &mut remaining).await?;
                let tree = Tree::decode(bytes.as_slice())
                    .map_err(|_| Status::data_loss("invalid output Tree"))?;
                if tree.root.is_none() {
                    return Err(Status::data_loss("missing output Tree root"));
                }
                for directory in tree.root.iter().chain(tree.children.iter()) {
                    for f in &directory.files {
                        if let Some(d) = &f.digest {
                            add(d)?;
                        }
                    }
                }
            }
            if let Some(root) = &output.root_directory_digest {
                let mut pending = vec![root.clone()];
                let mut seen = HashSet::new();
                while let Some(d) = pending.pop() {
                    if !seen.insert((d.hash.clone(), d.size_bytes)) {
                        continue;
                    }
                    if seen.len() > MAX_DIGESTS {
                        return Err(Status::resource_exhausted(
                            "retention directory budget exceeded",
                        ));
                    }
                    add(&d)?;
                    let bytes = self
                        .retention_metadata(&d, function, &mut remaining)
                        .await?;
                    let directory = Directory::decode(bytes.as_slice())
                        .map_err(|_| Status::data_loss("invalid output Directory"))?;
                    for f in &directory.files {
                        if let Some(d) = &f.digest {
                            add(d)?;
                        }
                    }
                    for child in directory.directories {
                        let d = child
                            .digest
                            .ok_or_else(|| Status::data_loss("missing child Directory digest"))?;
                        if pending.len() >= MAX_DIGESTS {
                            return Err(Status::resource_exhausted(
                                "retention directory budget exceeded",
                            ));
                        }
                        pending.push(d);
                    }
                }
            }
        }
        let artifacts: Vec<_> = digests.into_values().collect();
        let started = Instant::now();
        if !self.touch(&artifacts).await?.is_empty() {
            return Err(Status::not_found("output no longer present upstream"));
        }
        for artifact in artifacts {
            self.retention.confirm(&artifact, started, false);
        }
        Ok(())
    }
}
