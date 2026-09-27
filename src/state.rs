use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use sqlx::PgPool;

use crate::config::Config;
use crate::db::models::Meeting;
use crate::rtc::room::Room;
use crate::rtc::worker_pool::WorkerPool;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub db: PgPool,
    pub rooms: Arc<RoomRegistry>,
}

/// How long an emptied room is kept around before its `Router` is torn down. Covers the common
/// case of a peer's app briefly reconnecting (network blip, backgrounding on mobile) without
/// paying the cost of a fresh mediasoup router for every reconnect.
const EMPTY_ROOM_GRACE_PERIOD: Duration = Duration::from_secs(15);

/// Live (in-memory) rooms, keyed by meeting join code. A room only exists here while at least
/// one peer is connected or was recently connected; the source of truth for a meeting's
/// existence is always Postgres.
pub struct RoomRegistry {
    worker_pool: WorkerPool,
    rooms: DashMap<String, Arc<Room>>,
}

impl RoomRegistry {
    pub async fn new(config: &Config) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            worker_pool: WorkerPool::new(config).await?,
            rooms: DashMap::new(),
        }))
    }

    pub async fn get_or_create(
        &self,
        meeting: &Meeting,
        db: PgPool,
        config: Arc<Config>,
    ) -> anyhow::Result<Arc<Room>> {
        if let Some(room) = self.rooms.get(&meeting.code) {
            return Ok(room.clone());
        }

        let worker = self.worker_pool.next_worker();
        let room = Room::new(meeting, worker, db, config).await?;
        self.rooms.insert(meeting.code.clone(), room.clone());
        Ok(room)
    }

    pub fn get(&self, code: &str) -> Option<Arc<Room>> {
        self.rooms.get(code).map(|r| r.clone())
    }

    pub fn schedule_cleanup(self: &Arc<Self>, code: String) {
        let registry = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(EMPTY_ROOM_GRACE_PERIOD).await;
            let should_remove = registry.rooms.get(&code).map(|r| r.is_empty()).unwrap_or(false);
            if should_remove {
                registry.rooms.remove(&code);
                tracing::info!(code, "closed empty room");
            }
        });
    }
}
