use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use axum::extract::ws::{Message, WebSocket};
use dashmap::DashMap;
use dashmap::DashSet;
use mediasoup::prelude::*;
use parking_lot::Mutex;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::auth::jwt::verify_token;
use crate::db;
use crate::rtc::peer::Peer;
use crate::rtc::protocol::{ClientAction, ClientEnvelope, ServerEvent, WsOutbound};
use crate::state::AppState;

pub struct WsConnectParams {
    pub meeting_code: String,
    pub token: String,
}

/// Drives one WebSocket connection end to end: authenticate, resolve/create the room, register
/// the peer, then loop on incoming actions until the socket closes.
pub async fn handle_socket(socket: WebSocket, state: AppState, params: WsConnectParams) {
    let claims = match verify_token(&state.config.jwt_secret, &params.token) {
        Ok(c) => c,
        Err(_) => {
            let _ = socket; // drop; nothing to say to an unauthenticated caller
            return;
        }
    };

    let Ok(Some(meeting)) = db::meetings::find_by_code(&state.db, &params.meeting_code).await
    else {
        return;
    };
    if meeting.status == "ended" {
        return;
    }

    let is_host = claims.sub == meeting.host_id;

    let room = match state
        .rooms
        .get_or_create(&meeting, state.db.clone(), state.config.clone())
        .await
    {
        Ok(room) => room,
        Err(err) => {
            tracing::error!(error = %err, "failed to create room");
            return;
        }
    };

    if !is_host && room.is_full() {
        // Best-effort notice; the client should already prevent this via a pre-join capacity
        // check against the REST API, so reaching here is the race-condition fallback path.
        let (tx, _rx) = mpsc::unbounded_channel();
        let _ = tx.send(Message::Text(
            serde_json::to_string(&WsOutbound::event(
                ServerEvent::Error,
                json!({ "message": "Meeting is full" }),
            ))
            .unwrap()
            .into(),
        ));
        return;
    }

    let role = if is_host { "host" } else { "participant" };
    let participant_row_id =
        match db::meetings::log_participant_join(&state.db, meeting.id, claims.sub, role).await {
            Ok(id) => id,
            Err(err) => {
                tracing::error!(error = %err, "failed to log participant join");
                return;
            }
        };

    if is_host {
        let _ = db::meetings::set_status_live(&state.db, meeting.id).await;
    }

    let (mut ws_sink, mut ws_stream) = futures_util::StreamExt::split(socket);
    let (tx, mut rx) = mpsc::unbounded_channel::<Message>();

    // Outbound pump: anything queued on `tx` (from this connection's own responses, or pushed
    // by another peer's task via `Room::broadcast_event`) gets forwarded to the real socket.
    let pump = tokio::spawn(async move {
        use futures_util::SinkExt;
        while let Some(msg) = rx.recv().await {
            let is_close = matches!(msg, Message::Close(_));
            if ws_sink.send(msg).await.is_err() {
                break;
            }
            if is_close {
                break;
            }
        }
    });

    let peer = Arc::new(Peer {
        id: Uuid::new_v4(),
        user_id: claims.sub,
        // Moves rather than clones: `claims` isn't used again after this, and `Arc<str>` is
        // what `Peer` stores everywhere it needs to hand this name out cheaply (see peer.rs).
        display_name: Arc::from(claims.display_name),
        is_host,
        participant_row_id,
        ws_tx: tx,
        rtp_capabilities: Mutex::new(None),
        send_transport: Mutex::new(None),
        recv_transport: Mutex::new(None),
        producers: DashMap::new(),
        consumers: DashMap::new(),
        consumed_producer_ids: DashSet::new(),
        hand_raised: AtomicBool::new(false),
        audio_muted_by_host: AtomicBool::new(false),
    });

    room.join(peer.clone()).await;

    use futures_util::StreamExt;
    while let Some(Ok(msg)) = ws_stream.next().await {
        let Message::Text(text) = msg else {
            if matches!(msg, Message::Close(_)) {
                break;
            }
            continue;
        };

        // Log at the socket boundary, before deserializing into a concrete action. This keeps
        // the log useful for unknown/new actions and malformed client messages too.
        let raw_event: Value = match serde_json::from_str(&text) {
            Ok(event) => event,
            Err(err) => {
                tracing::warn!(
                    meeting_code = %room.code,
                    peer_id = %peer.id,
                    user_id = %peer.user_id,
                    raw_payload = %text,
                    error = %err,
                    "received invalid WebSocket JSON",
                );
                continue;
            }
        };

        let event_name = raw_event
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
            .to_owned();
        let request_id = raw_event.get("id").and_then(Value::as_u64);
        let payload = raw_event.as_object().map(|fields| {
            Value::Object(
                fields
                    .iter()
                    .filter(|(key, _)| key.as_str() != "id" && key.as_str() != "action")
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
            )
        });

        tracing::info!(
            meeting_code = %room.code,
            peer_id = %peer.id,
            user_id = %peer.user_id,
            event = %event_name,
            request_id,
            payload = ?payload,
            "received WebSocket event",
        );

        let envelope: ClientEnvelope = match serde_json::from_value(raw_event) {
            Ok(e) => e,
            Err(err) => {
                tracing::warn!(
                    meeting_code = %room.code,
                    peer_id = %peer.id,
                    event = %event_name,
                    error = %err,
                    "failed to parse WebSocket event",
                );
                continue;
            }
        };

        dispatch(&room, &peer, envelope).await;
    }

    room.remove_peer(peer.id).await;
    if room.is_empty() {
        state.rooms.schedule_cleanup(room.code.clone());
    }
    pump.abort();
}

async fn dispatch(room: &Arc<crate::rtc::room::Room>, peer: &Arc<Peer>, envelope: ClientEnvelope) {
    let id = envelope.id;

    macro_rules! reply_ok {
        ($payload:expr) => {
            peer.send(WsOutbound::ok(id, $payload))
        };
    }
    macro_rules! reply_err {
        ($msg:expr) => {
            peer.send(WsOutbound::err(id, $msg))
        };
    }
    macro_rules! require_host {
        () => {
            if !peer.is_host {
                reply_err!("host only");
                return;
            }
        };
    }

    match envelope.action {
        ClientAction::SetRtpCapabilities { rtp_capabilities } => {
            *peer.rtp_capabilities.lock() = Some(rtp_capabilities);
            reply_ok!(json!({}));
        }

        ClientAction::CreateWebRtcTransport { direction } => {
            match room.create_webrtc_transport(peer, direction).await {
                Ok(transport) => reply_ok!(json!({
                    "id": transport.id(),
                    "iceParameters": transport.ice_parameters(),
                    "iceCandidates": transport.ice_candidates(),
                    "dtlsParameters": transport.dtls_parameters(),
                })),
                Err(err) => reply_err!(err.to_string()),
            }
        }

        ClientAction::ConnectWebRtcTransport {
            transport_id,
            dtls_parameters,
        } => match room
            .connect_webrtc_transport(peer, transport_id, dtls_parameters)
            .await
        {
            Ok(()) => reply_ok!(json!({})),
            Err(err) => reply_err!(err.to_string()),
        },

        ClientAction::Produce {
            transport_id,
            kind,
            rtp_parameters,
            source,
        } => match room
            .produce(peer, transport_id, kind, rtp_parameters, source)
            .await
        {
            Ok(producer) => reply_ok!(json!({ "id": producer.id() })),
            Err(err) => reply_err!(err.to_string()),
        },

        ClientAction::PauseProducer { producer_id } => {
            match room.find_producer(peer, producer_id) {
                Some(p) => {
                    let _ = p.pause().await;
                    reply_ok!(json!({}));
                }
                None => reply_err!("producer not found"),
            }
        }

        ClientAction::ResumeProducer { producer_id } => {
            match room.find_producer(peer, producer_id) {
                Some(p) => {
                    let _ = p.resume().await;
                    reply_ok!(json!({}));
                }
                None => reply_err!("producer not found"),
            }
        }

        ClientAction::CloseProducer { producer_id } => {
            room.close_producer(peer, producer_id);
            reply_ok!(json!({}));
        }

        ClientAction::PauseConsumer { consumer_id } => {
            match room.find_consumer(peer, consumer_id) {
                Some(c) => {
                    let _ = c.pause().await;
                    reply_ok!(json!({}));
                }
                None => reply_err!("consumer not found"),
            }
        }

        ClientAction::ResumeConsumer { consumer_id } => match room.find_consumer(peer, consumer_id)
        {
            Some(c) => {
                let _ = c.resume().await;
                reply_ok!(json!({}));
            }
            None => reply_err!("consumer not found"),
        },

        ClientAction::RestartIce { transport_id } => {
            match room.restart_ice(peer, transport_id).await {
                Ok(ice_parameters) => reply_ok!(json!({ "iceParameters": ice_parameters })),
                Err(err) => reply_err!(err.to_string()),
            }
        }

        ClientAction::ChatMessage { text } => {
            let trimmed = text.trim();
            if !trimmed.is_empty() && trimmed.len() <= 2000 {
                room.broadcast_event(
                    ServerEvent::ChatMessage,
                    json!({
                        "peerId": peer.id,
                        "displayName": peer.display_name,
                        "text": trimmed,
                        "ts": chrono::Utc::now(),
                    }),
                    None,
                );
            }
            reply_ok!(json!({}));
        }

        ClientAction::RaiseHand => {
            peer.hand_raised
                .store(true, std::sync::atomic::Ordering::Relaxed);
            room.broadcast_event(ServerEvent::HandRaised, json!({ "peerId": peer.id }), None);
            reply_ok!(json!({}));
        }

        ClientAction::LowerHand => {
            peer.hand_raised
                .store(false, std::sync::atomic::Ordering::Relaxed);
            room.broadcast_event(ServerEvent::HandLowered, json!({ "peerId": peer.id }), None);
            reply_ok!(json!({}));
        }

        ClientAction::AdmitPeer { peer_id } => {
            require_host!();
            room.admit_waiting_peer(peer_id).await;
            reply_ok!(json!({}));
        }

        ClientAction::DenyPeer { peer_id } => {
            require_host!();
            room.deny_waiting_peer(peer_id);
            reply_ok!(json!({}));
        }

        ClientAction::KickPeer { peer_id } => {
            require_host!();
            room.kick_peer(peer_id);
            reply_ok!(json!({}));
        }

        ClientAction::MutePeerAudio { peer_id } => {
            require_host!();
            room.mute_peer_audio(peer_id).await;
            reply_ok!(json!({}));
        }

        ClientAction::StartRecording => {
            require_host!();
            match room.start_recording().await {
                Ok(()) => reply_ok!(json!({})),
                Err(err) => reply_err!(err.to_string()),
            }
        }

        ClientAction::StopRecording => {
            require_host!();
            room.stop_recording().await;
            reply_ok!(json!({}));
        }

        ClientAction::EndMeeting => {
            require_host!();
            room.broadcast_event(ServerEvent::MeetingEnded, json!({}), None);
            reply_ok!(json!({}));
        }

        ClientAction::Leave => {
            reply_ok!(json!({}));
        }
    }
}
