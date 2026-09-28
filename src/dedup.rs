//! Bounded trade replay suppression using the exchange's compound trade identity.

use crate::{primitives::wire::Trade, Error};
use std::collections::{HashSet, VecDeque};

type Key = (u64, String, u64);

/// FIFO deduplication window, retained by the consumer across reconnects.
/// Prints older than the window can reappear; this is not an exactly-once guarantee.
pub struct TradeDeduper {
    capacity: usize,
    seen: HashSet<Key>,
    order: VecDeque<Key>,
}

impl TradeDeduper {
    /// Construct a nonzero bounded window.
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error::Config("dedup capacity must be positive".into()));
        }
        Ok(Self {
            capacity,
            seen: HashSet::new(),
            order: VecDeque::new(),
        })
    }

    /// Return true for a new key, evicting the oldest key when full.
    pub fn accept(&mut self, trade: &Trade) -> bool {
        let key = (trade.time, trade.coin.as_str().to_owned(), trade.tid);
        if self.seen.contains(&key) {
            return false;
        }
        if self.order.len() == self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.seen.remove(&old);
            }
        }
        self.seen.insert(key.clone());
        self.order.push_back(key);
        true
    }
}
