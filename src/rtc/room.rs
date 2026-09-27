use std::sync::Arc;
use std::sync::atomic::Ordering;

use axum::extract::ws::Utf8Bytes;
use dashmap::DashMap;
use mediasoup::prelude::*;
use parking_lot::Mutex as SyncMutex;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::config::Config;
use crate::db;
use crate::rtc::codecs::media_codecs;
use crate::rtc::peer::Peer;
use crate::rtc::protocol::{ProducerSource, ServerEvent, TransportDirection, WsOutbound};
use crate::rtc::recording::RecordingSession;

/// One live meeting: a single mediasoup `Router` (all peers in a meeting share one router, so
/// mediasoup can route RTP between them without leaving the process) plus the peers currently
/// in the call or in the waiting room.
pub struct Room {
    pub id: Uuid,
    pub code: String,
    pub max_participants: i16,
    pub waiting_room_enabled: bool,
    pub router: Router,

    pub peers: DashMap<Uuid, Arc<Peer>>,
    pub waiting: DashMap<Uuid, Arc<Peer>>,

    pub recording: SyncMutex<Option<RecordingSession>>,

    db: PgPool,
    config: Arc<Config>,
}

impl Room {
    pub async fn new(
        meeting: &db::models::Meeting,
        worker: &Worker,
        db: PgPool,
        config: Arc<Config>,
    ) -> anyhow::Result<Arc<Self>> {
        let router = worker
            .create_router(RouterOptions::new(media_codecs()))
            .await
            .map_err(|e| anyhow::anyhow!("failed to create router: {e}"))?;

        Ok(Arc::new(Self {
            id: meeting.id,
            code: meeting.code.clone(),
            max_participants: meeting.max_participants,
            waiting_room_enabled: meeting.waiting_room_enabled,
            router,
            peers: DashMap::new(),
            waiting: DashMap::new(),
            recording: SyncMutex::new(None),
            db,
            config,
        }))
    }

    pub fn occupancy(&self) -> usize {
        self.peers.len()
    }

    pub fn is_full(&self) -> bool {
        self.occupancy() >= self.max_participants as usize
    }

    pub fn router_rtp_capabilities(&self) -> RtpCapabilitiesFinalized {
        self.router.rtp_capabilities().clone()
    }

    // -- roster / broadcast ------------------------------------------------

    /// Sends the same event to every peer in the room (minus `exclude`).
    ///
    /// Encodes the frame exactly once rather than once per recipient. The naive version of this
    /// — call `peer.send(...)` per peer, each re-serializing the payload — turns every
    /// broadcast (peer join/leave, chat, mute, hand raise, new/closed producer, recording
    /// start/stop, meeting end) into an O(peers) JSON encode plus O(peers) `Value` clones. In a
    /// large call that's real, repeated CPU and allocation work for byte-identical output.
    /// `Utf8Bytes` wraps ref-counted bytes, so cloning the already-encoded payload per recipient
    /// (`send_text`) costs an atomic increment instead — and `Peer`, not this method, is the one
    /// that knows how its channel actually frames it.
    pub fn broadcast_event(
        &self,
        event: ServerEvent,
        payload: serde_json::Value,
        exclude: Option<Uuid>,
    ) {
        let outbound = WsOutbound::event(event, payload);

        let text = match serde_json::to_string(&outbound) {
            Ok(text) => text,
            Err(err) => {
                tracing::warn!(?event, error = %err, "failed to serialize broadcast event");
                return;
            }
        };
        let payload: Utf8Bytes = text.into();

        for entry in self.peers.iter() {
            if Some(*entry.key()) == exclude {
                continue;
            }
            entry.value().send_text(payload.clone());
        }
    }

    fn find_host(&self) -> Option<Arc<Peer>> {
        self.peers
            .iter()
            .find(|e| e.value().is_host)
            .map(|e| e.value().clone())
    }

    // -- joining / waiting room ---------------------------------------------

    /// Entry point once a socket has authenticated and resolved which meeting it's for. Decides
    /// between the waiting room and immediate admission.
    pub async fn join(self: &Arc<Self>, peer: Arc<Peer>) {
        if self.waiting_room_enabled && !peer.is_host {
            self.waiting.insert(peer.id, peer.clone());
            peer.send(WsOutbound::event(
                ServerEvent::WaitingRoom,
                json!({ "message": "Waiting for the host to admit you" }),
            ));
            if let Some(host) = self.find_host() {
                host.send(WsOutbound::event(
                    ServerEvent::PeerWaiting,
                    json!({ "peerId": peer.id, "displayName": peer.display_name }),
                ));
            }
        } else {
            self.finalize_admit(peer).await;
        }
    }

    async fn finalize_admit(self: &Arc<Self>, peer: Arc<Peer>) {
        let roster: Vec<_> = self.peers.iter().map(|e| e.value().summary()).collect();

        peer.send(WsOutbound::event(
            ServerEvent::Welcome,
            json!({
                "selfId": peer.id,
                "isHost": peer.is_host,
                "routerRtpCapabilities": self.router_rtp_capabilities(),
                "peers": roster,
                // Lets a peer joining mid-recording show the "Recording" indicator immediately,
                // instead of only learning about it via a `recordingStarted` event they were
                // never around to receive.
                "recording": self.is_recording(),
            }),
        ));

        self.broadcast_event(
            ServerEvent::PeerJoined,
            json!(peer.summary()),
            Some(peer.id),
        );
        self.peers.insert(peer.id, peer);
    }

    pub async fn admit_waiting_peer(self: &Arc<Self>, peer_id: Uuid) {
        if let Some((_, peer)) = self.waiting.remove(&peer_id) {
            peer.send(WsOutbound::event(ServerEvent::Admitted, json!({})));
            self.finalize_admit(peer).await;
        }
    }

    pub fn deny_waiting_peer(&self, peer_id: Uuid) {
        if let Some((_, peer)) = self.waiting.remove(&peer_id) {
            peer.send(WsOutbound::event(
                ServerEvent::Denied,
                json!({ "message": "The host declined your request to join" }),
            ));
        }
    }

    // -- transports -----------------------------------------------------

    pub async fn create_webrtc_transport(
        &self,
        peer: &Arc<Peer>,
        direction: TransportDirection,
    ) -> anyhow::Result<WebRtcTransport> {
        let mut listen_infos = WebRtcTransportListenInfos::new(ListenInfo {
            protocol: Protocol::Udp,
            ip: self.config.mediasoup_listen_ip,
            announced_address: self
                .config
                .mediasoup_announced_ip
                .as_ref()
                .map(|s| s.to_string()),
            expose_internal_ip: false,
            port: None,
            port_range: Some(self.config.mediasoup_min_port..=self.config.mediasoup_max_port),
            flags: None,
            send_buffer_size: None,
            recv_buffer_size: None,
        });
        listen_infos = listen_infos.insert(ListenInfo {
            protocol: Protocol::Tcp,
            ip: self.config.mediasoup_listen_ip,
            announced_address: self
                .config
                .mediasoup_announced_ip
                .as_ref()
                .map(|s| s.to_string()),
            expose_internal_ip: false,
            port: None,
            port_range: Some(self.config.mediasoup_min_port..=self.config.mediasoup_max_port),
            flags: None,
            send_buffer_size: None,
            recv_buffer_size: None,
        });

        let mut options = WebRtcTransportOptions::new(listen_infos);
        options.enable_tcp = true;
        // Recv transports never send meaningful outgoing media of their own; a lower initial
        // bitrate ceiling here just controls congestion-control ramp-up, not a hard cap.
        if direction == TransportDirection::Recv {
            options.initial_available_outgoing_bitrate = 300_000;
        }

        let transport = self.router.create_webrtc_transport(options).await?;

        match direction {
            TransportDirection::Send => *peer.send_transport.lock() = Some(transport.clone()),
            TransportDirection::Recv => *peer.recv_transport.lock() = Some(transport.clone()),
        }

        Ok(transport)
    }

    pub async fn connect_webrtc_transport(
        self: &Arc<Self>,
        peer: &Arc<Peer>,
        transport_id: TransportId,
        dtls_parameters: DtlsParameters,
    ) -> anyhow::Result<()> {
        let transport = self
            .transport_by_id(peer, transport_id)
            .ok_or_else(|| anyhow::anyhow!("unknown transport"))?;
        transport
            .connect(WebRtcTransportRemoteParameters { dtls_parameters })
            .await?;

        // A recv transport just became usable: catch it up on every producer already in the
        // room that it hasn't consumed yet.
        let is_recv_transport = peer
            .recv_transport
            .lock()
            .as_ref()
            .map(|t| t.id() == transport_id)
            .unwrap_or(false);
        if is_recv_transport {
            self.try_consume_all_for(peer).await;
        }

        Ok(())
    }

    fn transport_by_id(&self, peer: &Arc<Peer>, id: TransportId) -> Option<WebRtcTransport> {
        if let Some(t) = peer.send_transport.lock().as_ref() {
            if t.id() == id {
                return Some(t.clone());
            }
        }
        if let Some(t) = peer.recv_transport.lock().as_ref() {
            if t.id() == id {
                return Some(t.clone());
            }
        }
        None
    }

    /// Regenerates ICE credentials for one of this peer's transports and returns them, so the
    /// client can restart its ICE gathering (typically after switching networks, e.g. Wi-Fi to
    /// cellular, where the old candidates are no longer reachable).
    pub async fn restart_ice(
        &self,
        peer: &Arc<Peer>,
        transport_id: TransportId,
    ) -> anyhow::Result<IceParameters> {
        let transport = self
            .transport_by_id(peer, transport_id)
            .ok_or_else(|| anyhow::anyhow!("unknown transport"))?;
        Ok(transport.restart_ice().await?)
    }

    // -- producing --------------------------------------------------------

    pub async fn produce(
        self: &Arc<Self>,
        peer: &Arc<Peer>,
        transport_id: TransportId,
        kind: MediaKind,
        rtp_parameters: RtpParameters,
        source: ProducerSource,
    ) -> anyhow::Result<Producer> {
        let transport = peer
            .send_transport
            .lock()
            .clone()
            .filter(|t| t.id() == transport_id)
            .ok_or_else(|| anyhow::anyhow!("send transport not found"))?;

        let mut options = ProducerOptions::new(kind, rtp_parameters);
        options.app_data = AppData::new(ProducerAppData { source });

        let producer = transport.produce(options).await?;
        peer.producers.insert(producer.id(), producer.clone());

        self.broadcast_event(
            ServerEvent::NewProducer,
            json!({
                "peerId": peer.id,
                "producerId": producer.id(),
                "kind": kind,
                "source": source,
            }),
            Some(peer.id),
        );

        self.consume_new_producer_for_others(peer.id, &producer, source)
            .await;

        if let Some(session) = {
            let lock = self.recording.lock();
            lock.clone()
        } {
            session
                .record_producer(peer, &producer, source, &self.router)
                .await;
        }

        Ok(producer)
    }

    pub fn producer_source(peer: &Arc<Peer>, producer_id: &ProducerId) -> Option<ProducerSource> {
        peer.producers.get(producer_id).and_then(|p| {
            p.app_data()
                .downcast_ref::<ProducerAppData>()
                .map(|d| d.source)
        })
    }

    // -- consuming --------------------------------------------------------

    async fn try_consume_all_for(self: &Arc<Self>, peer: &Arc<Peer>) {
        if !peer.is_ready_to_consume() {
            return;
        }
        let others: Vec<_> = self
            .peers
            .iter()
            .filter(|e| *e.key() != peer.id)
            .map(|e| e.value().clone())
            .collect();

        for owner in others {
            let producers: Vec<_> = owner
                .producers
                .iter()
                .map(|e| {
                    (
                        e.value().clone(),
                        Self::producer_source(&owner, e.key()).unwrap_or(ProducerSource::Camera),
                    )
                })
                .collect();
            for (producer, source) in producers {
                self.consume_one(peer, owner.id, &producer, source).await;
            }
        }
    }

    async fn consume_new_producer_for_others(
        &self,
        owner_id: Uuid,
        producer: &Producer,
        source: ProducerSource,
    ) {
        let targets: Vec<_> = self
            .peers
            .iter()
            .filter(|e| *e.key() != owner_id && e.value().is_ready_to_consume())
            .map(|e| e.value().clone())
            .collect();

        for target in targets {
            self.consume_one(&target, owner_id, producer, source).await;
        }
    }

    async fn consume_one(
        &self,
        target: &Arc<Peer>,
        owner_id: Uuid,
        producer: &Producer,
        source: ProducerSource,
    ) {
        if !target.consumed_producer_ids.insert(producer.id()) {
            return; // already consumed by this peer
        }

        let Some(recv_transport) = target.recv_transport.lock().clone() else {
            target.consumed_producer_ids.remove(&producer.id());
            return;
        };
        let Some(rtp_capabilities) = target.rtp_capabilities.lock().clone() else {
            target.consumed_producer_ids.remove(&producer.id());
            return;
        };

        let mut options = ConsumerOptions::new(producer.id(), rtp_capabilities);
        options.paused = true;

        match recv_transport.consume(options).await {
            Ok(consumer) => {
                let payload = json!({
                    "peerId": owner_id,
                    "producerId": producer.id(),
                    "consumerId": consumer.id(),
                    "kind": consumer.kind(),
                    "rtpParameters": consumer.rtp_parameters(),
                    "source": source,
                });
                target.consumers.insert(consumer.id(), consumer);
                target.send(WsOutbound::event(ServerEvent::NewConsumer, payload));
            }
            Err(err) => {
                target.consumed_producer_ids.remove(&producer.id());
                tracing::warn!(error = %err, "failed to create consumer");
            }
        }
    }

    // -- pause/resume/close ------------------------------------------------

    pub fn find_producer(&self, peer: &Arc<Peer>, id: ProducerId) -> Option<Producer> {
        peer.producers.get(&id).map(|p| p.value().clone())
    }

    pub fn find_consumer(&self, peer: &Arc<Peer>, id: ConsumerId) -> Option<Consumer> {
        peer.consumers.get(&id).map(|c| c.value().clone())
    }

    pub fn close_producer(self: &Arc<Self>, peer: &Arc<Peer>, id: ProducerId) {
        if peer.producers.remove(&id).is_some() {
            self.broadcast_event(
                ServerEvent::ProducerClosed,
                json!({ "producerId": id }),
                None,
            );
        }
    }

    // -- host moderation ----------------------------------------------------

    pub async fn mute_peer_audio(&self, target_peer_id: Uuid) -> bool {
        let Some(target) = self.peers.get(&target_peer_id).map(|e| e.value().clone()) else {
            return false;
        };
        let audio_producer = target
            .producers
            .iter()
            .find(|e| e.value().kind() == MediaKind::Audio)
            .map(|e| e.value().clone());

        if let Some(producer) = audio_producer {
            if producer.pause().await.is_ok() {
                target.audio_muted_by_host.store(true, Ordering::Relaxed);
                target.send(WsOutbound::event(ServerEvent::YouWereMuted, json!({})));
                self.broadcast_event(
                    ServerEvent::PeerMuted,
                    json!({ "peerId": target_peer_id }),
                    None,
                );
                return true;
            }
        }
        false
    }

    pub fn kick_peer(self: &Arc<Self>, target_peer_id: Uuid) {
        if let Some((_, peer)) = self.peers.remove(&target_peer_id) {
            peer.send(WsOutbound::event(ServerEvent::Kicked, json!({})));
            self.broadcast_event(
                ServerEvent::PeerLeft,
                json!({ "peerId": target_peer_id }),
                None,
            );
        }
    }

    /// Removes a peer from whichever map it's in (admitted or waiting), closes its transports
    /// (which cascades to close its producers/consumers via `Drop`), and tells everyone else.
    pub async fn remove_peer(self: &Arc<Self>, peer_id: Uuid) {
        if self.waiting.remove(&peer_id).is_some() {
            return;
        }
        if let Some((_, peer)) = self.peers.remove(&peer_id) {
            if let Err(err) =
                db::meetings::log_participant_leave(&self.db, peer.participant_row_id).await
            {
                tracing::warn!(error = %err, "failed to log participant leave");
            }
            self.broadcast_event(ServerEvent::PeerLeft, json!({ "peerId": peer_id }), None);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty() && self.waiting.is_empty()
    }

    // -- recording ----------------------------------------------------------

    pub async fn start_recording(self: &Arc<Self>) -> anyhow::Result<()> {
        if self.recording.lock().is_some() {
            return Ok(()); // already recording
        }
        let recordings_dir_str = self
            .config
            .recordings_dir
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("invalid UTF-8 in recordings_dir path"))?;

        let session = RecordingSession::new(self.id, recordings_dir_str, self.db.clone())?;

        // Capture everyone already producing when recording starts, not just future producers.
        let existing: Vec<_> = self
            .peers
            .iter()
            .flat_map(|entry| {
                let peer = entry.value().clone();
                entry
                    .value()
                    .producers
                    .iter()
                    .map(|p| (peer.clone(), p.value().clone()))
                    .collect::<Vec<_>>()
            })
            .collect();

        for (peer, producer) in existing {
            let source =
                Self::producer_source(&peer, &producer.id()).unwrap_or(ProducerSource::Camera);
            session
                .record_producer(&peer, &producer, source, &self.router)
                .await;
        }

        *self.recording.lock() = Some(session);
        self.broadcast_event(ServerEvent::RecordingStarted, json!({}), None);
        Ok(())
    }

    pub async fn stop_recording(&self) {
        let session = self.recording.lock().take();
        if let Some(session) = session {
            session.stop_all().await;
            self.broadcast_event(ServerEvent::RecordingStopped, json!({}), None);
        }
    }

    pub fn is_recording(&self) -> bool {
        self.recording.lock().is_some()
    }
}

/// What we stash in a `Producer`'s `app_data` so we can recover which kind of source it is
/// (mic / camera / screen-share) later without a separate side table.
#[derive(Clone, Copy)]
pub struct ProducerAppData {
    pub source: ProducerSource,
}
