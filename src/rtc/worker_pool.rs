use std::sync::atomic::{AtomicUsize, Ordering};

use mediasoup::prelude::*;
use mediasoup::worker::WorkerLogLevel;

use crate::config::Config;

/// Owns the mediasoup C++ worker processes for this server instance.
///
/// Each `Worker` pins to a single CPU core inside mediasoup's own C++ thread, so the number of
/// workers is the hard ceiling on how many rooms can do CPU-bound RTP forwarding in parallel.
/// Rooms are handed out a worker round-robin at creation time (see `next_worker`), not created
/// fresh per room — reusing a fixed pool avoids the cost of spinning up a new OS thread/process
/// per meeting.
pub struct WorkerPool {
    _manager: WorkerManager,
    workers: Vec<Worker>,
    next: AtomicUsize,
}

impl WorkerPool {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let manager = WorkerManager::new();
        let mut workers = Vec::with_capacity(config.mediasoup_num_workers);

        for _ in 0..config.mediasoup_num_workers {
            let mut settings = WorkerSettings::default();
            settings.log_level = WorkerLogLevel::Warn;
            settings.rtc_port_range = config.mediasoup_min_port..=config.mediasoup_max_port;
            let worker = manager.create_worker(settings).await?;
            workers.push(worker);
        }

        tracing::info!(count = workers.len(), "mediasoup workers started");

        Ok(Self {
            _manager: manager,
            workers,
            next: AtomicUsize::new(0),
        })
    }

    /// Round-robin pick. `Relaxed` ordering is enough here: we only need eventual fairness
    /// across rooms, not a strict global order, so there's no reason to pay for a stronger
    /// memory fence on every room creation.
    pub fn next_worker(&self) -> &Worker {
        let i = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        &self.workers[i]
    }
}
