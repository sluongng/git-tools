//! Remote Asset mappings are disposable metadata, not a second blob store.
//! Every reused mapping needs verified local bytes and upstream CAS presence.
use super::execution::Budget;
use super::*;
use reapi::asset::{self, fetch_server::Fetch};

struct Entry {
    expires: Instant,
    used: u64,
    bytes: usize,
    value: asset::FetchBlobResponse,
}
#[derive(Default)]
pub(super) struct Mappings {
    entries: HashMap<Vec<u8>, Entry>,
    bytes: usize,
    clock: u64,
}
impl Mappings {
    fn remove(&mut self, key: &[u8]) {
        if let Some(entry) = self.entries.remove(key) {
            self.bytes -= entry.bytes;
        }
    }
    fn get(&mut self, key: &[u8]) -> Option<(Instant, asset::FetchBlobResponse)> {
        if self
            .entries
            .get(key)
            .is_some_and(|entry| entry.expires <= Instant::now())
        {
            self.remove(key);
        }
        self.clock = self.clock.wrapping_add(1);
        let entry = self.entries.get_mut(key)?;
        entry.used = self.clock;
        Some((entry.expires, entry.value.clone()))
    }
    fn insert(
        &mut self,
        key: Vec<u8>,
        value: asset::FetchBlobResponse,
        ttl: Duration,
        config: &ProxyConfig,
    ) {
        let bytes = key.len() + value.encoded_len();
        if ttl.is_zero()
            || config.max_asset_cache_entries == 0
            || bytes > config.max_asset_cache_bytes
        {
            return;
        }
        let expired = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.expires <= Instant::now())
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in expired {
            self.remove(&key);
        }
        self.remove(&key);
        while self.entries.len() >= config.max_asset_cache_entries
            || self.bytes + bytes > config.max_asset_cache_bytes
        {
            let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            self.remove(&oldest);
        }
        self.clock = self.clock.wrapping_add(1);
        self.entries.insert(
            key,
            Entry {
                expires: Instant::now() + ttl,
                used: self.clock,
                bytes,
                value,
            },
        );
        self.bytes += bytes;
    }
}
impl CacheProxy {
    fn asset_client(&self, method: RpcMethod) -> asset::fetch_client::FetchClient<Channel> {
        let client = asset::fetch_client::FetchClient::new(self.upstream.asset_channel.clone())
            .accept_compressed(CompressionEncoding::Gzip)
            .max_encoding_message_size(self.upstream.config.max_batch_bytes + 1024 * 1024)
            .max_decoding_message_size(self.upstream.config.max_batch_bytes + 1024 * 1024);
        if self.upstream.send_gzip(method) {
            client.send_compressed(CompressionEncoding::Gzip)
        } else {
            client
        }
    }
    fn asset_artifact(&self, value: &asset::FetchBlobResponse) -> Option<Artifact> {
        if value.status.as_ref().is_some_and(|s| s.code != 0) {
            return None;
        }
        // Unspecified response function means SHA256, even if BLAKE3 was requested.
        self.upstream
            .artifact(
                value.blob_digest.as_ref()?,
                if value.digest_function == 0 {
                    1
                } else {
                    value.digest_function
                },
            )
            .ok()
    }
    async fn fetch_blob_inner(
        &self,
        request: asset::FetchBlobRequest,
        metadata: CallerMetadata,
        budget: Budget,
    ) -> Result<Response<asset::FetchBlobResponse>, Status> {
        // Full request identity preserves URI order and indexed header qualifiers.
        // The proxy's immutable upstream owns the credential/instance scope.
        let key = Sha256::digest(request.encode_to_vec()).to_vec();
        let cacheable = request.oldest_content_accepted.is_none()
            && !self.upstream.config.asset_cache_ttl.is_zero();
        if cacheable {
            let cached = { self.assets.lock().unwrap().get(&key) };
            if let Some((expires, value)) = cached {
                if let Some(artifact) = self.asset_artifact(&value) {
                    let local = self.cache.clone().cached_content(artifact.clone()).await;
                    if matches!(local, Ok(Some(_)))
                        && self
                            .upstream
                            .missing(&[artifact])
                            .await
                            .is_ok_and(|missing| missing.is_empty())
                    {
                        if expires <= Instant::now() {
                            self.assets.lock().unwrap().remove(&key);
                        } else {
                            self.metrics.asset_hits.fetch_add(1, Ordering::Relaxed);
                            return Ok(Response::new(value));
                        }
                    }
                }
                self.assets.lock().unwrap().remove(&key);
            }
        }
        self.metrics.asset_upstream.fetch_add(1, Ordering::Relaxed);
        let response = self
            .asset_client(RpcMethod::FetchBlob)
            .fetch_blob(budget.request(&self.upstream, request, &metadata)?)
            .await?;
        self.upstream.observe_gzip(RpcMethod::FetchBlob, &response);
        let value = response.get_ref();
        if cacheable && self.asset_artifact(value).is_some() {
            let mut ttl = self.upstream.config.asset_cache_ttl;
            if let Some(expires) = value.expires_at.as_ref() {
                use std::time::{SystemTime, UNIX_EPOCH};
                ttl = if expires.seconds < 0 || !(0..1_000_000_000).contains(&expires.nanos) {
                    Duration::ZERO
                } else {
                    UNIX_EPOCH
                        .checked_add(Duration::new(expires.seconds as u64, expires.nanos as u32))
                        .and_then(|time| time.duration_since(SystemTime::now()).ok())
                        .map_or(Duration::ZERO, |remaining| ttl.min(remaining))
                };
            }
            self.assets
                .lock()
                .unwrap()
                .insert(key, value.clone(), ttl, &self.upstream.config);
        }
        Ok(clean_response(response))
    }
}
#[tonic::async_trait]
impl Fetch for CacheProxy {
    async fn fetch_blob(
        &self,
        request: Request<asset::FetchBlobRequest>,
    ) -> Result<Response<asset::FetchBlobResponse>, Status> {
        self.upstream.scope(&request.get_ref().instance_name)?;
        let metadata = CallerMetadata::capture(&request)?;
        let budget = Budget::capture(
            &request,
            Some(self.upstream.config.policy.operation_timeout()),
        )?;
        budget
            .run(self.fetch_blob_inner(request.into_inner(), metadata, budget))
            .await
    }
    async fn fetch_directory(
        &self,
        request: Request<asset::FetchDirectoryRequest>,
    ) -> Result<Response<asset::FetchDirectoryResponse>, Status> {
        self.upstream.scope(&request.get_ref().instance_name)?;
        let metadata = CallerMetadata::capture(&request)?;
        let budget = Budget::capture(
            &request,
            Some(self.upstream.config.policy.operation_timeout()),
        )?;
        self.metrics.asset_upstream.fetch_add(1, Ordering::Relaxed);
        // Root residence cannot establish availability of the complete tree.
        let response = budget
            .run(
                self.asset_client(RpcMethod::FetchDirectory)
                    .fetch_directory(budget.request(
                        &self.upstream,
                        request.into_inner(),
                        &metadata,
                    )?),
            )
            .await?;
        self.upstream
            .observe_gzip(RpcMethod::FetchDirectory, &response);
        Ok(clean_response(response))
    }
}
