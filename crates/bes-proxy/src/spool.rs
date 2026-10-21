use crate::{Config, Metrics};
use anyhow::{Context, Result, ensure};
use fjall::Readable;
use prost::Message;
use reapi::bes::{
    self, PublishBuildToolEventStreamRequest as Event, PublishBuildToolEventStreamResponse as Ack,
    PublishLifecycleEventRequest, StreamId,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use tonic::Status;

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct State {
    stream: Vec<u8>,
    pub accepted: i64,
    pub acked: i64,
    pub terminal: Option<i64>,
    pub attribution: Option<Vec<u8>>,
    completed_at: Option<u64>,
}
impl State {
    pub fn complete(&self) -> bool {
        self.terminal.is_some_and(|seq| seq == self.acked)
    }
}
#[derive(Serialize, Deserialize)]
struct Lifecycle {
    payload: Vec<u8>,
    attribution: Option<Vec<u8>>,
}
type LifecycleDelivery = (Vec<u8>, PublishLifecycleEventRequest, Option<Vec<u8>>);
pub(crate) struct Admission {
    pub acks: Vec<Ack>,
    pub error: Option<Status>,
}
pub(crate) struct Spool {
    streams: fjall::SingleWriterTxKeyspace,
    events: fjall::SingleWriterTxKeyspace,
    lifecycle: fjall::SingleWriterTxKeyspace,
    meta: fjall::SingleWriterTxKeyspace,
    owner: fjall::SingleWriterTxDatabase,
    config: Config,
    metrics: Metrics,
}
fn internal(_: impl std::fmt::Display) -> Status {
    Status::internal("BES durable spool operation failed")
}
fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn stream_key(stream: &StreamId) -> String {
    hex(&Sha256::digest(stream.encode_to_vec()))
}
pub(crate) fn event_id(event: &Event) -> Result<String, Status> {
    let ordered = event
        .ordered_build_event
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("BES event lacks ordered event"))?;
    let stream = ordered
        .stream_id
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("BES event lacks stream ID"))?;
    if stream.build_id.is_empty()
        || stream.invocation_id.is_empty()
        || stream.build_id.len() > 4096
        || stream.invocation_id.len() > 4096
    {
        return Err(Status::invalid_argument(
            "BES tool stream requires bounded build and invocation IDs",
        ));
    }
    if ordered.sequence_number <= 0 || ordered.event.is_none() {
        return Err(Status::invalid_argument(
            "BES event requires positive sequence and event",
        ));
    }
    Ok(stream_key(stream))
}
fn key(id: &str, sequence: i64) -> Vec<u8> {
    let mut key = id.as_bytes().to_vec();
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}
pub(crate) fn terminal(event: &Event) -> bool {
    matches!(event.ordered_build_event.as_ref().and_then(|e| e.event.as_ref()).and_then(|e| e.event.as_ref()),
        Some(bes::build_event::Event::ComponentStreamFinished(finish)) if finish.r#type == 1)
}
impl Spool {
    pub fn open(path: &Path, config: &Config) -> Result<Self> {
        if !path.exists() {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(path)?;
        }
        let metadata = std::fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "BES spool must be a real directory"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            ensure!(
                metadata.uid() == unsafe { libc::geteuid() },
                "BES spool must belong to current user"
            );
            ensure!(
                metadata.permissions().mode() & 0o077 == 0,
                "BES spool must be private (mode0700)"
            );
        }
        let owner = fjall::SingleWriterTxDatabase::builder(path)
            .open()
            .context("opening BES spool")?;
        let streams = owner.keyspace("streams", fjall::KeyspaceCreateOptions::default)?;
        let events = owner.keyspace("events", fjall::KeyspaceCreateOptions::default)?;
        let lifecycle = owner.keyspace("lifecycle", fjall::KeyspaceCreateOptions::default)?;
        let meta = owner.keyspace("meta", fjall::KeyspaceCreateOptions::default)?;
        let mut spool = Self {
            streams,
            events,
            lifecycle,
            meta,
            owner,
            config: config.clone(),
            metrics: Metrics::default(),
        };
        spool.recount()?;
        let binding =
            Sha256::digest(serde_json::to_vec(&(&config.upstream, &config.api_key))?).to_vec();
        let mut tx = spool
            .owner
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        if let Some(existing) = tx.get(&spool.meta, b"binding")?
            && existing.as_ref() != binding.as_slice()
        {
            ensure!(
                spool.metrics.pending_streams == 0
                    && spool.metrics.pending_events == 0
                    && spool.metrics.pending_lifecycle_events == 0,
                "BES pending spool belongs to a different upstream or credential; drain it before changing backend identity"
            );
            for item in tx.iter(&spool.streams) {
                let (key, _) = item.into_inner()?;
                tx.remove(&spool.streams, key);
            }
            spool.metrics.completed_streams = 0;
        }
        tx.insert(&spool.meta, b"binding", binding);
        tx.commit()?;
        spool.trim(&std::collections::HashSet::new())?;
        #[cfg(unix)]
        {
            std::fs::File::open(path)?.sync_all()?;
            if let Some(parent) = path.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
        }
        Ok(spool)
    }
    fn recount(&mut self) -> Result<()> {
        let tx = self.owner.read_tx();
        let mut metrics = Metrics::default();
        for item in tx.iter(&self.streams) {
            let (_, value) = item.into_inner()?;
            let state: State = serde_json::from_slice(&value)?;
            if state.complete() {
                metrics.completed_streams += 1;
            } else {
                metrics.pending_streams += 1;
            }
        }
        for item in tx.iter(&self.events) {
            let (_, value) = item.into_inner()?;
            metrics.pending_events += 1;
            metrics.pending_bytes += value.len() as u64;
        }
        for item in tx.iter(&self.lifecycle) {
            let (_, value) = item.into_inner()?;
            metrics.pending_lifecycle_events += 1;
            metrics.pending_bytes += value.len() as u64;
        }
        self.metrics = metrics;
        Ok(())
    }
    pub fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }
    pub fn pending_streams(&self) -> Result<Vec<String>, Status> {
        let tx = self.owner.read_tx();
        let mut pending = Vec::new();
        for item in tx.iter(&self.streams) {
            let (key, value) = item.into_inner().map_err(internal)?;
            let state: State = serde_json::from_slice(&value).map_err(internal)?;
            if !state.complete() {
                pending.push(String::from_utf8(key.to_vec()).map_err(internal)?);
            }
        }
        Ok(pending)
    }
    pub fn stream(&self, id: &str) -> Result<State, Status> {
        let value = self
            .owner
            .read_tx()
            .get(&self.streams, id)
            .map_err(internal)?
            .ok_or_else(|| Status::internal("BES stream checkpoint missing"))?;
        serde_json::from_slice(&value).map_err(internal)
    }
    pub fn acknowledged(&self, id: &str) -> Result<i64, Status> {
        Ok(self.stream(id)?.acked)
    }
    pub fn append_batch(
        &mut self,
        events: Vec<Event>,
        attribution: Option<Vec<u8>>,
    ) -> Result<Admission, Status> {
        let mut metrics = self.metrics.clone();
        let mut tx = self
            .owner
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let mut acks = Vec::with_capacity(events.len());
        let mut error = None;
        for event in events {
            let result = (|| -> Result<Ack, Status> {
                let id = event_id(&event)?;
                let ordered = event.ordered_build_event.as_ref().unwrap();
                let sequence = ordered.sequence_number;
                let stream = ordered.stream_id.as_ref().unwrap();
                let payload = event.encode_to_vec();
                if payload.len() > self.config.max_event_bytes {
                    return Err(Status::resource_exhausted(
                        "BES event exceeds configured limit",
                    ));
                }
                let existing = tx.get(&self.streams, &id).map_err(internal)?;
                let mut state = if let Some(ref bytes) = existing {
                    serde_json::from_slice::<State>(bytes).map_err(internal)?
                } else {
                    if metrics.pending_streams >= self.config.max_streams {
                        return Err(Status::resource_exhausted(
                            "BES pending stream limit reached",
                        ));
                    }
                    State {
                        stream: stream.encode_to_vec(),
                        accepted: 0,
                        acked: 0,
                        terminal: None,
                        attribution: attribution.clone(),
                        completed_at: None,
                    }
                };
                if sequence <= state.accepted {
                    if sequence > state.acked {
                        let previous = tx
                            .get(&self.events, key(&id, sequence))
                            .map_err(internal)?
                            .ok_or_else(|| Status::internal("BES pending event missing"))?;
                        let previous = Event::decode(previous.as_ref()).map_err(internal)?;
                        // Retry attempt metadata may legitimately differ; event identity may not.
                        if previous.ordered_build_event != event.ordered_build_event {
                            return Err(Status::already_exists(
                                "conflicting BES event for existing sequence",
                            ));
                        }
                    }
                    return Ok(Ack {
                        stream_id: Some(stream.clone()),
                        sequence_number: sequence,
                    });
                }
                if state.terminal.is_some() || state.accepted.checked_add(1) != Some(sequence) {
                    return Err(Status::invalid_argument(
                        "BES events must be consecutive and cannot follow terminal",
                    ));
                }
                if metrics.pending_bytes.saturating_add(payload.len() as u64)
                    > self.config.max_pending_bytes
                {
                    return Err(Status::resource_exhausted(
                        "BES durable spool is full; pending events cannot be evicted",
                    ));
                }
                state.accepted = sequence;
                if terminal(&event) {
                    state.terminal = Some(sequence);
                }
                tx.insert(&self.events, key(&id, sequence), &payload);
                tx.insert(
                    &self.streams,
                    &id,
                    serde_json::to_vec(&state).map_err(internal)?,
                );
                metrics.pending_bytes += payload.len() as u64;
                metrics.pending_events += 1;
                if existing.is_none() {
                    metrics.pending_streams += 1;
                }
                Ok(Ack {
                    stream_id: Some(stream.clone()),
                    sequence_number: sequence,
                })
            })();
            match result {
                Ok(ack) => acks.push(ack),
                Err(status)
                    if matches!(
                        status.code(),
                        tonic::Code::InvalidArgument
                            | tonic::Code::AlreadyExists
                            | tonic::Code::ResourceExhausted
                    ) =>
                {
                    error = Some(status);
                    break;
                }
                Err(status) => return Err(status),
            }
        }
        if metrics.pending_events != self.metrics.pending_events {
            tx.commit().map_err(internal)?;
            metrics.admission_commits += 1;
            self.metrics = metrics;
        }
        // No ACK escapes until the whole accepted prefix is fsync-safe. A bad
        // later event cannot roll back events whose ACKs have already escaped.
        Ok(Admission { acks, error })
    }
    pub fn event(&self, id: &str, sequence: i64) -> Result<Option<(Event, bool)>, Status> {
        let value = self
            .owner
            .read_tx()
            .get(&self.events, key(id, sequence))
            .map_err(internal)?;
        value
            .map(|bytes| {
                let event = Event::decode(bytes.as_ref()).map_err(internal)?;
                let last = terminal(&event);
                Ok((event, last))
            })
            .transpose()
    }
    pub fn ack(&mut self, id: &str, ack: Ack) -> Result<(), Status> {
        let mut state = self.stream(id)?;
        let stream = StreamId::decode(state.stream.as_slice()).map_err(internal)?;
        if ack.stream_id.as_ref() != Some(&stream)
            || ack.sequence_number != state.acked + 1
            || ack.sequence_number > state.accepted
        {
            return Err(Status::data_loss(
                "BES upstream ACK stream or sequence mismatch",
            ));
        }
        let mut tx = self
            .owner
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let event_key = key(id, ack.sequence_number);
        let payload = tx
            .get(&self.events, &event_key)
            .map_err(internal)?
            .ok_or_else(|| Status::internal("BES acknowledged payload missing"))?;
        state.acked = ack.sequence_number;
        if state.complete() {
            state.completed_at = Some(now());
            state.attribution = None;
        }
        // The replay checkpoint and payload deletion are one fsync-safe transaction.
        tx.insert(
            &self.streams,
            id,
            serde_json::to_vec(&state).map_err(internal)?,
        );
        tx.remove(&self.events, event_key);
        tx.commit().map_err(internal)?;
        self.metrics.pending_events -= 1;
        self.metrics.pending_bytes -= payload.len() as u64;
        if state.complete() {
            self.metrics.pending_streams -= 1;
            self.metrics.completed_streams += 1;
        }
        Ok(())
    }
    pub fn trim(&mut self, protected: &std::collections::HashSet<String>) -> Result<()> {
        let read = self.owner.read_tx();
        let mut completed = Vec::new();
        for item in read.iter(&self.streams) {
            let (key, value) = item.into_inner()?;
            let state: State = serde_json::from_slice(&value)?;
            if state.complete() {
                completed.push((state.completed_at.unwrap_or(0), key.to_vec()));
            }
        }
        completed.sort_by_key(|(time, _)| *time);
        let cutoff = now().saturating_sub(self.config.tombstone_ttl_seconds);
        let remove_count = completed
            .len()
            .saturating_sub(self.config.max_completed_streams);
        let mut tx = self
            .owner
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let mut removed = 0;
        for (time, key) in completed {
            if protected.contains(&String::from_utf8_lossy(&key).into_owned()) {
                continue;
            }
            if removed < remove_count || time < cutoff {
                tx.remove(&self.streams, key);
                removed += 1;
            }
        }
        if removed > 0 {
            tx.commit()?;
        }
        self.metrics.completed_streams -= removed;
        Ok(())
    }
    pub fn append_lifecycle(
        &mut self,
        event: PublishLifecycleEventRequest,
        attribution: Option<Vec<u8>>,
    ) -> Result<(), Status> {
        let payload = event.encode_to_vec();
        if payload.len() > self.config.max_event_bytes {
            return Err(Status::resource_exhausted(
                "BES lifecycle event exceeds configured limit",
            ));
        }
        if event.build_event.is_none() {
            return Err(Status::invalid_argument("BES lifecycle event is missing"));
        }
        let digest = Sha256::digest(&payload);
        let record = serde_json::to_vec(&Lifecycle {
            payload,
            attribution,
        })
        .map_err(internal)?;
        let mut tx = self
            .owner
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        // De-duplicate pending retry requests while preserving FIFO admission order.
        for item in tx.iter(&self.lifecycle) {
            let (_, value) = item.into_inner().map_err(internal)?;
            let previous: Lifecycle = serde_json::from_slice(&value).map_err(internal)?;
            if Sha256::digest(&previous.payload) == digest {
                return Ok(());
            }
        }
        if self
            .metrics
            .pending_bytes
            .saturating_add(record.len() as u64)
            > self.config.max_pending_bytes
            || self.metrics.pending_lifecycle_events >= self.config.max_streams * 16
        {
            return Err(Status::resource_exhausted(
                "BES lifecycle spool limit reached",
            ));
        }
        let count = tx
            .get(&self.meta, b"lifecycle_counter")
            .map_err(internal)?
            .map(|value| {
                <[u8; 8]>::try_from(value.as_ref())
                    .map(u64::from_be_bytes)
                    .map_err(internal)
            })
            .transpose()?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("BES lifecycle counter exhausted"))?;
        tx.insert(&self.meta, b"lifecycle_counter", count.to_be_bytes());
        tx.insert(&self.lifecycle, count.to_be_bytes(), &record);
        tx.commit().map_err(internal)?;
        self.metrics.pending_bytes += record.len() as u64;
        self.metrics.pending_lifecycle_events += 1;
        Ok(())
    }
    pub fn has_lifecycle(&self) -> bool {
        self.metrics.pending_lifecycle_events > 0
    }
    pub fn next_lifecycle(&self) -> Result<Option<LifecycleDelivery>, Status> {
        let read = self.owner.read_tx();
        let Some(item) = read.iter(&self.lifecycle).next() else {
            return Ok(None);
        };
        let (key, value) = item.into_inner().map_err(internal)?;
        let record: Lifecycle = serde_json::from_slice(&value).map_err(internal)?;
        let event =
            PublishLifecycleEventRequest::decode(record.payload.as_slice()).map_err(internal)?;
        Ok(Some((key.to_vec(), event, record.attribution)))
    }
    pub fn ack_lifecycle(&mut self, key: Vec<u8>) -> Result<(), Status> {
        let mut tx = self
            .owner
            .write_tx()
            .durability(Some(fjall::PersistMode::SyncAll));
        let bytes = tx
            .get(&self.lifecycle, &key)
            .map_err(internal)?
            .ok_or_else(|| Status::internal("BES lifecycle checkpoint missing"))?;
        tx.remove(&self.lifecycle, key);
        tx.commit().map_err(internal)?;
        self.metrics.pending_bytes -= bytes.len() as u64;
        self.metrics.pending_lifecycle_events -= 1;
        Ok(())
    }
}
