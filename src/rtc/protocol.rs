use std::sync::Arc;

use mediasoup::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// One incoming WebSocket frame from a client, decoded once at the edge of `signaling.rs`.
///
/// Request/response correlation mirrors plain JSON-RPC: every client action carries an `id`,
/// which the matching `WsOutbound::Response` echoes back. Server-initiated pushes (a new peer,
/// a chat line, a producer appearing) go out as `WsOutbound::Event` with no `id` to correlate.
#[derive(Debug, Deserialize)]
pub struct ClientEnvelope {
    pub id: u64,
    #[serde(flatten)]
    pub action: ClientAction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransportDirection {
    Send,
    Recv,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ProducerSource {
    Mic,
    Camera,
    Screen,
}

/// Every event the server can push to a client — a closed set, not a scattered pile of string
/// literals. The wire name is derived from the variant name via the same `rename_all =
/// "camelCase"` convention already used for `ClientAction` below, so there's exactly one place
/// that defines "what events exist" and "what they're called on the wire" — no second list to
/// keep in sync, and a typo'd event name is a compile error (unknown variant) instead of a
/// silent mismatch the Flutter client just never reacts to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum ServerEvent {
    Welcome,
    WaitingRoom,
    PeerWaiting,
    Admitted,
    Denied,
    PeerJoined,
    PeerLeft,
    NewProducer,
    NewConsumer,
    ProducerClosed,
    ChatMessage,
    HandRaised,
    HandLowered,
    PeerMuted,
    YouWereMuted,
    RecordingStarted,
    RecordingStopped,
    Kicked,
    MeetingEnded,
    Error,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "action", rename_all = "camelCase")]
pub enum ClientAction {
    /// Client sends the RTP capabilities of its local `Device` once it has loaded the router's
    /// capabilities from the `welcome` event. Required before the server will start pushing
    /// `newConsumer` events to this peer.
    SetRtpCapabilities {
        rtp_capabilities: RtpCapabilities,
    },
    CreateWebRtcTransport {
        direction: TransportDirection,
    },
    ConnectWebRtcTransport {
        transport_id: TransportId,
        dtls_parameters: DtlsParameters,
    },
    Produce {
        transport_id: TransportId,
        kind: MediaKind,
        rtp_parameters: RtpParameters,
        source: ProducerSource,
    },
    PauseProducer {
        producer_id: ProducerId,
    },
    ResumeProducer {
        producer_id: ProducerId,
    },
    CloseProducer {
        producer_id: ProducerId,
    },
    PauseConsumer {
        consumer_id: ConsumerId,
    },
    ResumeConsumer {
        consumer_id: ConsumerId,
    },
    RestartIce {
        transport_id: TransportId,
    },
    ChatMessage {
        text: String,
    },
    RaiseHand,
    LowerHand,
    /// Host-only. Server re-checks the requester is actually the host before acting.
    AdmitPeer {
        peer_id: Uuid,
    },
    DenyPeer {
        peer_id: Uuid,
    },
    KickPeer {
        peer_id: Uuid,
    },
    MutePeerAudio {
        peer_id: Uuid,
    },
    StartRecording,
    StopRecording,
    EndMeeting,
    Leave,
}

/// Every outbound frame. Internally tagged on `type` so the Flutter client can dispatch with a
/// single switch on one field instead of guessing the shape from which keys are present.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum WsOutbound {
    #[serde(rename = "response")]
    Response {
        id: u64,
        ok: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        payload: Option<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    #[serde(rename = "event")]
    Event { event: ServerEvent, payload: Value },
}

impl WsOutbound {
    pub fn ok(id: u64, payload: Value) -> Self {
        Self::Response {
            id,
            ok: true,
            payload: Some(payload),
            error: None,
        }
    }

    pub fn err(id: u64, message: impl Into<String>) -> Self {
        Self::Response {
            id,
            ok: false,
            payload: None,
            error: Some(message.into()),
        }
    }

    pub fn event(event: ServerEvent, payload: Value) -> Self {
        Self::Event { event, payload }
    }
}

/// Lightweight, JSON-facing description of a peer, used in roster/event payloads. Never carries
/// transport/producer internals — those stay server-side.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerSummary {
    pub peer_id: Uuid,
    /// `Arc<str>`, mirroring `Peer::display_name` — see the comment there. Keeping the same
    /// type here means building a roster of N peers clones N `Arc` pointers, not N strings.
    pub display_name: Arc<str>,
    pub is_host: bool,
    pub hand_raised: bool,
    pub muted: bool,
}
