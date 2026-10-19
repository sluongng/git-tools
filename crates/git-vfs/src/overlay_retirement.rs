//! Reference-free payload retirement; durable metadata is always removed first.
use super::{Handles, number};
use crate::overlay::{
    Payload,
    codec::decode_exact,
    payload::{Files, Intent},
};
use anyhow::{Context, Result, ensure};
use fjall::{PersistMode, Readable};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::JoinHandle;

const MAX_ITEMS: u64 = 256;
const MAX_BYTES: u64 = 32 * 1024 * 1024;
#[derive(Default)]
struct State {
    items: u64,
    bytes: u64,
    failed: bool,
    failures: u64,
    completed: u64,
    fallbacks: u64,
    stopping: bool,
    #[cfg(test)]
    paused: bool,
}
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    wake: Condvar,
}
struct WorkerExit(Arc<Shared>);
impl Drop for WorkerExit {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.items != 0 || !state.stopping {
            if !state.failed {
                state.failures += 1;
            }
            state.failed = true;
        }
        self.0.wake.notify_all();
    }
}
#[cfg(test)]
pub(super) struct Pause(Arc<Shared>);
#[cfg(test)]
impl Drop for Pause {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().paused = false;
        self.0.wake.notify_all();
    }
}
pub(super) struct Retirements {
    sender: Option<mpsc::SyncSender<(u64, u64)>>,
    thread: Option<JoinHandle<()>>,
    shared: Arc<Shared>,
}
pub(super) struct Admission {
    pub synchronous: bool,
    bytes: u64,
    shared: Arc<Shared>,
    transferred: bool,
}
impl Drop for Admission {
    fn drop(&mut self) {
        if !self.transferred {
            release(&self.shared, self.bytes, true);
        }
    }
}
fn release(shared: &Shared, bytes: u64, success: bool) {
    let mut state = shared.state.lock().unwrap();
    if success {
        state.items -= 1;
        state.bytes -= bytes;
        state.completed += 1;
    } else {
        state.failures += 1;
        state.failed = true;
    }
    shared.wake.notify_all();
}
fn remove_one(handles: &Handles, files: &Files, inode: u64, bytes: u64) -> Result<()> {
    let read = handles.db.read_tx();
    let recorded = read
        .get(&handles.retired_payloads, inode.to_be_bytes())?
        .context("payload retirement missing")?;
    ensure!(
        number(&recorded)? == bytes,
        "payload retirement size mismatch"
    );
    if let Some(raw) = read.get(&handles.entries, inode.to_be_bytes())? {
        let entry = decode_exact(&raw)?;
        ensure!(
            !entry.linked && !entry.control,
            "retirement references live or control inode"
        );
        ensure!(
            !matches!(entry.payload, Payload::File { .. }),
            "retirement still references payload"
        );
    }
    if let Some(raw) = read.get(&handles.meta, b"payload_intent")? {
        let intent: Intent = serde_json::from_slice(&raw)?;
        ensure!(
            decode_exact(&intent.after_entry)?.inode != inode,
            "retirement conflicts with pending redo"
        );
    }
    drop(read);
    files.remove(inode)?;
    let mut tx = handles.db.write_tx().durability(Some(PersistMode::SyncAll));
    tx.remove(&handles.retired_payloads, inode.to_be_bytes());
    tx.commit()?;
    Ok(())
}
impl Retirements {
    pub fn drain(handles: &Handles, files: &Files) -> Result<()> {
        loop {
            let tx = handles.db.read_tx();
            let batch: Vec<_> = tx
                .iter(&handles.retired_payloads)
                .take(128)
                .map(|item| {
                    let (key, value) = item.into_inner()?;
                    Ok((number(&key)?, number(&value)?))
                })
                .collect::<Result<_>>()?;
            drop(tx);
            if batch.is_empty() {
                return Ok(());
            }
            for (inode, bytes) in batch {
                remove_one(handles, files, inode, bytes)?;
            }
        }
    }
    pub fn start(handles: Handles, files: Files) -> Result<Self> {
        let shared = Arc::new(Shared::default());
        let worker_shared = shared.clone();
        let (sender, receiver) = mpsc::sync_channel(MAX_ITEMS as usize);
        let thread = std::thread::Builder::new()
            .name("git-vfs-retire".into())
            .spawn(move || {
                let _exit = WorkerExit(worker_shared.clone());
                while let Ok((inode, bytes)) = receiver.recv() {
                    #[cfg(test)]
                    {
                        let mut state = worker_shared.state.lock().unwrap();
                        while state.paused {
                            state = worker_shared.wake.wait(state).unwrap();
                        }
                    }
                    let success = remove_one(&handles, &files, inode, bytes).is_ok();
                    release(&worker_shared, bytes, success);
                }
            })?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
            shared,
        })
    }
    #[cfg(test)]
    pub fn pause(&self) -> Pause {
        self.shared.state.lock().unwrap().paused = true;
        Pause(self.shared.clone())
    }
    pub fn healthy(&self) -> bool {
        !self.shared.state.lock().unwrap().failed
    }
    pub fn reserve(&self, bytes: u64) -> Result<Admission> {
        let mut state = self.shared.state.lock().unwrap();
        ensure!(
            !state.failed,
            "payload retirement failed; reopen for recovery"
        );
        let synchronous = bytes > MAX_BYTES
            || state.items >= MAX_ITEMS
            || state.bytes.checked_add(bytes).is_none_or(|n| n > MAX_BYTES);
        if synchronous {
            state.fallbacks += 1;
            while state.items != 0 && !state.failed {
                state = self.shared.wake.wait(state).unwrap();
            }
            ensure!(
                !state.failed,
                "payload retirement failed while draining capacity"
            );
        }
        state.items += 1;
        state.bytes = state
            .bytes
            .checked_add(bytes)
            .context("retirement byte overflow")?;
        Ok(Admission {
            synchronous,
            bytes,
            shared: self.shared.clone(),
            transferred: false,
        })
    }
    pub fn submit(&self, mut admission: Admission, handles: &Handles, files: &Files, inode: u64) {
        admission.transferred = true;
        if admission.synchronous {
            release(
                &self.shared,
                admission.bytes,
                remove_one(handles, files, inode, admission.bytes).is_ok(),
            );
        } else if self
            .sender
            .as_ref()
            .unwrap()
            .send((inode, admission.bytes))
            .is_err()
        {
            release(&self.shared, admission.bytes, false);
        }
    }
    pub fn uncertain(&self, mut admission: Admission) {
        admission.transferred = true;
        release(&self.shared, admission.bytes, false);
    }
    pub fn wait(&self) -> Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        while state.items != 0 && !state.failed {
            state = self.shared.wake.wait(state).unwrap();
        }
        ensure!(
            !state.failed,
            "payload retirement failed; durable ledger retained"
        );
        Ok(())
    }
    pub fn stats(&self) -> (u64, u64, u64, u64, u64) {
        let state = self.shared.state.lock().unwrap();
        (
            state.items,
            state.bytes,
            state.completed,
            state.fallbacks,
            state.failures,
        )
    }
    pub fn stop(&mut self) {
        self.shared.state.lock().unwrap().stopping = true;
        drop(self.sender.take());
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
            if !state.failed {
                state.failures += 1;
            }
            state.failed = true;
            self.shared.wake.notify_all();
        }
    }
}
impl Drop for Retirements {
    fn drop(&mut self) {
        self.stop();
    }
}
