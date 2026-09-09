use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;
use zakofish4_common::event::TimerId;

/// The deadlines the state machine has asked for.
///
/// A map plus "sleep until the earliest" rather than a task per timer: there
/// are only ever a handful, they are cancelled constantly, and a spawned task
/// per request would be far more machinery than the problem needs.
#[derive(Debug, Default)]
pub(crate) struct Timers {
    deadlines: HashMap<TimerId, Instant>,
}

impl Timers {
    pub fn start(&mut self, id: TimerId, after: Duration) {
        self.deadlines.insert(id, Instant::now() + after);
    }

    pub fn cancel(&mut self, id: TimerId) {
        self.deadlines.remove(&id);
    }

    /// When the next timer is due, if any.
    pub fn next_deadline(&self) -> Option<Instant> {
        self.deadlines.values().copied().min()
    }

    /// Remove and return every timer due at or before `now`.
    ///
    /// Returns all of them rather than one, because a single `sleep` wake can
    /// cover several deadlines and leaving the rest would delay them by a whole
    /// extra round of the loop.
    pub fn take_expired(&mut self, now: Instant) -> Vec<TimerId> {
        let fired: Vec<TimerId> = self
            .deadlines
            .iter()
            .filter(|(_, at)| **at <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in &fired {
            self.deadlines.remove(id);
        }
        fired
    }
}
