use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// `TransactionTracker` tracks the state of transactions to detect retransmissions.
pub struct TransactionTracker {
    retention_period: Duration,
    transactions: Mutex<HashMap<(u32, String), TransactionState>>,
}

impl TransactionTracker {
    pub fn new(retention_period: Duration) -> Self {
        Self {
            retention_period,
            transactions: Mutex::new(HashMap::new()),
        }
    }

    /// Checks if the transaction is a retransmission.
    /// If it's a new transaction, it is marked as `InProgress`.
    ///
    /// Returns true only for an existing transaction. At capacity, evict the
    /// oldest completed replay; never evict in-progress work. All-in-progress
    /// saturation is an explicit error, causing the caller to close the socket
    /// so a client can retry, rather than silently treating new work as duplicate.
    pub fn is_retransmission(&self, xid: u32, client_addr: &str) -> std::io::Result<bool> {
        let key = (xid, client_addr.to_string());
        let mut transactions = self.transactions.lock().map_err(|_| std::io::Error::other("RPC replay table poisoned"))?;
        housekeeping(&mut transactions, self.retention_period);
        if transactions.contains_key(&key) { return Ok(true); }
        while transactions.len() >= 4096 {
            let oldest = transactions.iter().filter_map(|(key,state)| match state {
                TransactionState::Completed(time,_) => Some((key.clone(),*time)),
                TransactionState::InProgress => None,
            }).min_by_key(|(_,time)|*time).map(|(key,_)|key);
            if let Some(key) = oldest { transactions.remove(&key); } else {
                return Err(std::io::Error::new(std::io::ErrorKind::WouldBlock,"RPC replay table has no completed entries to evict"));
            }
        }
        transactions.insert(key,TransactionState::InProgress);
        Ok(false)
    }

    /// Marks the transaction as processed.
    pub fn completed_response(&self, xid: u32, client_addr: &str) -> Option<Vec<u8>> {
        let mut transactions = self.transactions.lock().ok()?;
        housekeeping(&mut transactions, self.retention_period);
        match transactions.get(&(xid,client_addr.to_string())) { Some(TransactionState::Completed(_,reply)) => Some(reply.clone()), _ => None }
    }

    pub fn abandon(&self,xid:u32,client_addr:&str) {
        if let Ok(mut transactions) = self.transactions.lock() { transactions.remove(&(xid,client_addr.to_string())); }
    }

    pub fn mark_processed(&self, xid: u32, client_addr: &str, reply: Vec<u8>) {
        let key = (xid, client_addr.to_string());
        let completion_time = SystemTime::now();
        let mut transactions = self.transactions.lock().expect("unable to unlock transactions mutex");
        if let Some(tx) = transactions.get_mut(&key) {
            *tx = TransactionState::Completed(completion_time,reply);
        }
        // Bound replay storage independently of request rate and response size.
        while transactions.values().map(|state| match state { TransactionState::Completed(_,bytes) => bytes.len(), _ => 0 }).sum::<usize>() > 16 * 1024 * 1024 || transactions.len() > 4096 {
            let oldest = transactions.iter().filter_map(|(key,state)| match state { TransactionState::Completed(time,_) => Some((key.clone(),*time)),_=>None }).min_by_key(|(_,time)|*time).map(|(key,_)|key);
            if let Some(key) = oldest { transactions.remove(&key); } else { break; }
        }
    }
}

fn housekeeping(transactions: &mut HashMap<(u32, String), TransactionState>, max_age: Duration) {
    let mut cutoff = SystemTime::now() - max_age;
    transactions.retain(|_, v| match v {
        TransactionState::InProgress => true,
        TransactionState::Completed(completion_time, _) => completion_time >= &mut cutoff,
    });
}

pub enum TransactionState {
    InProgress,
    Completed(SystemTime, Vec<u8>),
}
