//! Min-heap scheduler for server polling.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

/// A scheduled item: (due_time, tie_breaker, server_key).
#[derive(Debug, Clone)]
struct ScheduleEntry {
    due: f64,
    seq: u64,
    key: String,
}

impl PartialEq for ScheduleEntry {
    fn eq(&self, other: &Self) -> bool {
        self.due == other.due && self.seq == other.seq
    }
}

impl Eq for ScheduleEntry {}

impl PartialOrd for ScheduleEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduleEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse for min-heap behavior.
        other
            .due
            .partial_cmp(&self.due)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

struct Inner {
    heap: BinaryHeap<ScheduleEntry>,
    counter: u64,
}

/// Shared handle to the scheduler, clonable across tasks.
#[derive(Clone)]
pub struct SchedulerHandle {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Notify>,
    epoch: std::time::Instant,
}

impl SchedulerHandle {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                heap: BinaryHeap::new(),
                counter: 0,
            })),
            notify: Arc::new(Notify::new()),
            epoch: std::time::Instant::now(),
        }
    }

    /// Current monotonic time as f64 seconds (relative to process start).
    pub fn now(&self) -> f64 {
        self.epoch.elapsed().as_secs_f64()
    }

    /// Schedule a server key at the given due time.
    pub fn schedule(&self, key: String, due: f64) {
        let inner = self.inner.clone();
        let notify = self.notify.clone();
        tokio::spawn(async move {
            let mut lock = inner.lock().await;
            lock.counter += 1;
            let seq = lock.counter;
            lock.heap.push(ScheduleEntry { due, seq, key });
            drop(lock);
            notify.notify_one();
        });
    }

    /// Pop the next due server key, waiting until it's time.
    /// Returns (key, due_time).
    pub async fn next_due(&self) -> (String, f64) {
        loop {
            let next = {
                let lock = self.inner.lock().await;
                lock.heap.peek().cloned()
            };

            match next {
                None => {
                    // Empty heap — wait for notification.
                    self.notify.notified().await;
                }
                Some(entry) => {
                    let now = self.now();
                    if entry.due > now {
                        let wait = std::time::Duration::from_secs_f64(entry.due - now);
                        tokio::select! {
                            _ = tokio::time::sleep(wait) => {}
                            _ = self.notify.notified() => { continue; }
                        }
                    }
                    // Try to pop — must re-check since another task may have added
                    // an earlier entry while we waited.
                    let mut lock = self.inner.lock().await;
                    if let Some(top) = lock.heap.peek() {
                        if top.due <= self.now() {
                            let entry = lock.heap.pop().unwrap();
                            return (entry.key, entry.due);
                        }
                    }
                    // Something changed, loop again.
                }
            }
        }
    }
}
