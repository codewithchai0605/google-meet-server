use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::extract::ws::{Message, Utf8Bytes};
use dashmap::{DashMap, DashSet};
use mediasoup::prelude::*;
use parking_lot::Mutex;
use tokio::sync::mpsc::UnboundedSender;
use uuid::Uuid;

use crate::rtc::protocol::PeerSummary;

/// Everything the server tracks for one connected participant.
///
/// Fields that are set exactly once (transports) use `Mutex<Option<T>>` rather than an async
/// lock: they're only ever locked for the instant it takes to clone an `Arc`-backed handle in
/// or out, never held across an `.await`. Fields that grow/shrink as producers and consumers
/// come and go use `DashMap`/`DashSet` for lock-free concurrent access instead of wrapping a
/// `HashMap` in a single coarse mutex, since a busy room mutates these from many peers' tasks
/// at once.
pub struct Peer {
    pub id: Uuid,
    pub user_id: Uuid,
    /// `Arc<str>` rather than `String`: this gets copied into every roster entry (`summary()`)
    /// built for every existing peer each time someone joins, plus every chat/event payload
    /// that echoes it back. An `Arc` clone is an atomic increment; a `String` clone is a fresh
    /// heap allocation and byte copy every time.
    pub display_name: Arc<str>,
    pub is_host: bool,
    /// `meeting_participants.id` for this join, used to stamp `left_at` when the peer leaves.
    pub participant_row_id: Uuid,

    pub ws_tx: UnboundedSender<Message>,

    pub rtp_capabilities: Mutex<Option<RtpCapabilities>>,
    pub send_transport: Mutex<Option<WebRtcTransport>>,
    pub recv_transport: Mutex<Option<WebRtcTransport>>,

    pub producers: DashMap<ProducerId, Producer>,
    pub consumers: DashMap<ConsumerId, Consumer>,
    /// Producers (owned by other peers) already turned into a consumer for this peer, so we
    /// never double-consume the same producer if both "peer just became ready" and "a new
    /// producer just appeared" fire close together.
    pub consumed_producer_ids: DashSet<ProducerId>,

    pub hand_raised: AtomicBool,
    pub audio_muted_by_host: AtomicBool,
}

impl Peer {
    pub fn summary(&self) -> PeerSummary {
        PeerSummary {
            peer_id: self.id,
            display_name: self.display_name.clone(),
            is_host: self.is_host,
            hand_raised: self.hand_raised.load(Ordering::Relaxed),
            muted: self.audio_muted_by_host.load(Ordering::Relaxed),
        }
    }

    pub fn is_ready_to_consume(&self) -> bool {
        self.rtp_capabilities.lock().is_some() && self.recv_transport.lock().is_some()
    }

    pub fn send(self: &Arc<Self>, msg: crate::rtc::protocol::WsOutbound) {
        if let Ok(text) = serde_json::to_string(&msg) {
            // Ignoring the send error: it only fails once the peer's socket task has already
            // exited, at which point there's nothing left to notify.
            let _ = self.ws_tx.send(Message::Text(text.into()));
        }
    }

    /// Send an already-encoded text frame.
    ///
    /// Takes `Utf8Bytes` — the same ref-counted buffer `Message::Text` stores internally —
    /// rather than a full `Message`. Callers that fan one frame out to many peers (see
    /// `Room::broadcast_event`) hand over just that cheaply-clonable payload and never
    /// construct the `Message` enum themselves; cloning it per recipient is an atomic refcount
    /// bump, not a fresh allocation. Wrapping it into the channel's actual message type happens
    /// only here, at the one place that needs to know it — if `ws_tx`'s type ever changes,
    /// every broadcaster doesn't need to change with it.
    pub fn send_text(&self, payload: Utf8Bytes) {
        let _ = self.ws_tx.send(Message::Text(payload));
    }
}
