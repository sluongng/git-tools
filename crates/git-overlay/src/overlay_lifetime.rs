//! Kernel references and owned pins; accounting mistakes quarantine, never collect.
use anyhow::{Result, ensure};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
#[derive(Clone, Copy)]
pub enum PinKind {
    Open,
    Snapshot,
    Async,
}
#[derive(Default)]
struct Counts {
    lookups: u64,
    opens: u64,
    pins: u64,
    quarantined: bool,
}
#[derive(Default)]
struct State {
    counts: HashMap<u64, Counts>,
    permanent: HashSet<u64>,
    notifications: BTreeSet<u64>,
}
#[derive(Clone, Default)]
pub(super) struct Lifetime(Arc<Mutex<State>>);
pub struct Lease {
    lifetime: Lifetime,
    ids: Vec<u64>,
    kind: PinKind,
}
impl Lifetime {
    pub fn protect(&self, inode: u64) {
        self.0.lock().unwrap().permanent.insert(inode);
    }
    pub fn lookup(&self, inode: u64) -> Result<()> {
        let mut state = self.0.lock().unwrap();
        let refs = state.counts.entry(inode).or_default();
        refs.lookups = refs
            .lookups
            .checked_add(1)
            .ok_or_else(|| std::io::Error::from_raw_os_error(libc::EOVERFLOW))?;
        Ok(())
    }
    pub fn forget(&self, inode: u64, n: u64) {
        let mut state = self.0.lock().unwrap();
        let Some(refs) = state.counts.get_mut(&inode) else {
            return;
        };
        if n > refs.lookups {
            refs.quarantined = true;
        } else {
            refs.lookups -= n;
        }
        if refs.lookups == 0 && refs.opens == 0 && refs.pins == 0 && !refs.quarantined {
            state.counts.remove(&inode);
        }
        state.notifications.insert(inode);
    }
    pub fn take_notifications(&self) -> BTreeSet<u64> {
        std::mem::take(&mut self.0.lock().unwrap().notifications)
    }
    pub fn release_kernel(&self) {
        let mut state = self.0.lock().unwrap();
        let ids: Vec<_> = state.counts.keys().copied().collect();
        for refs in state.counts.values_mut() {
            refs.lookups = 0;
        }
        state.notifications.extend(ids);
        state
            .counts
            .retain(|_, r| r.opens != 0 || r.pins != 0 || r.quarantined);
    }
    pub fn eligible(&self, inode: u64) -> bool {
        let state = self.0.lock().unwrap();
        !state.permanent.contains(&inode)
            && state
                .counts
                .get(&inode)
                .is_none_or(|r| r.lookups == 0 && r.opens == 0 && r.pins == 0 && !r.quarantined)
    }
    pub fn pin(&self, ids: Vec<u64>, kind: PinKind) -> Result<Lease> {
        let mut unique = HashSet::new();
        let ids: Vec<_> = ids.into_iter().filter(|i| unique.insert(*i)).collect();
        let mut state = self.0.lock().unwrap();
        for id in &ids {
            let refs = state.counts.entry(*id).or_default();
            let count = match kind {
                PinKind::Open => refs.opens,
                _ => refs.pins,
            };
            ensure!(count < u64::MAX, "inode pin overflow");
        }
        for id in &ids {
            let refs = state.counts.get_mut(id).unwrap();
            match kind {
                PinKind::Open => refs.opens += 1,
                _ => refs.pins += 1,
            };
        }
        drop(state);
        Ok(Lease {
            lifetime: self.clone(),
            ids,
            kind,
        })
    }
    pub fn stats(&self) -> (u64, u64, u64, u64) {
        let state = self.0.lock().unwrap();
        let mut totals = (0, 0, 0, 0);
        for refs in state.counts.values() {
            totals.0 += refs.lookups;
            totals.1 += refs.opens;
            totals.2 += refs.pins;
            totals.3 += u64::from(refs.quarantined);
        }
        totals
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.lifetime.0.lock().unwrap();
        for id in &self.ids {
            let refs = state.counts.entry(*id).or_default();
            let count = match self.kind {
                PinKind::Open => &mut refs.opens,
                _ => &mut refs.pins,
            };
            if *count == 0 {
                refs.quarantined = true;
            } else {
                *count -= 1;
            }
            if refs.lookups == 0 && refs.opens == 0 && refs.pins == 0 && !refs.quarantined {
                state.counts.remove(id);
            }
            state.notifications.insert(*id);
        }
    }
}
