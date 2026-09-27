use std::net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket};
use std::path::PathBuf;
use std::sync::Arc;

use dashmap::DashMap;
use mediasoup::prelude::*;
use mediasoup::supported_rtp_capabilities::get_supported_rtp_capabilities;
use sqlx::PgPool;
use tokio::io::AsyncWriteExt;
use tokio::process::{Child, Command};
use uuid::Uuid;

use crate::db;
use crate::rtc::peer::Peer;
use crate::rtc::protocol::ProducerSource;

const RECORDER_IP: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

struct RecordingLeg {
    /// Kept alive only so the PlainTransport (and the Consumer feeding it) aren't dropped —
    /// dropping either would stop mediasoup from forwarding RTP to ffmpeg.
    _plain_transport: PlainTransport,
    _consumer: Consumer,
    ffmpeg: Child,
    /// `recordings.id`, if the DB insert succeeded — `None` means we couldn't log this leg (DB
    /// hiccup), in which case there's nothing to update the status of later, but the recording
    /// itself still proceeds (the file is still useful even if this one row is missing).
    recording_row_id: Option<Uuid>,
}

/// One recording pass over a meeting: every participant's mic/camera producer gets its own
/// ffmpeg process writing a separate file, matching mediasoup's own recording example rather
/// than attempting server-side compositing (real layout/mixing is a substantial project in its
/// own right and out of scope for a first version).
#[derive(Clone)]
pub struct RecordingSession {
    inner: Arc<RecordingSessionInner>,
}

struct RecordingSessionInner {
    meeting_id: Uuid,
    recordings_dir: PathBuf,
    db: PgPool,
    legs: DashMap<ProducerId, RecordingLeg>,
}

impl RecordingSession {
    pub fn new(meeting_id: Uuid, recordings_dir: &str, db: PgPool) -> anyhow::Result<Self> {
        let dir = PathBuf::from(recordings_dir).join(meeting_id.to_string());
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            inner: Arc::new(RecordingSessionInner {
                meeting_id,
                recordings_dir: dir,
                db,
                legs: DashMap::new(),
            }),
        })
    }

    /// Start capturing one producer. Safe to call for a producer already being recorded (no-op).
    pub async fn record_producer(
        &self,
        peer: &Arc<Peer>,
        producer: &Producer,
        source: ProducerSource,
        router: &Router,
    ) {
        if self.inner.legs.contains_key(&producer.id()) {
            return;
        }
        if let Err(err) = self
            .try_record_producer(peer, producer, source, router)
            .await
        {
            tracing::warn!(error = %err, producer_id = %producer.id(), "failed to start recording producer");
        }
    }

    async fn try_record_producer(
        &self,
        peer: &Arc<Peer>,
        producer: &Producer,
        source: ProducerSource,
        router: &Router,
    ) -> anyhow::Result<()> {
        // Ask the OS for a free UDP port, then release it immediately: mediasoup's PlainTransport
        // and ffmpeg each bind their own socket, so we only need the port *number*, not the
        // socket itself.
        let rtp_port = {
            let socket = UdpSocket::bind(SocketAddr::new(RECORDER_IP, 0))?;
            socket.local_addr()?.port()
        };

        let plain_transport = router
            .create_plain_transport(PlainTransportOptions::new(ListenInfo {
                protocol: Protocol::Udp,
                ip: RECORDER_IP,
                announced_address: None,
                expose_internal_ip: false,
                port: None,
                port_range: None,
                flags: None,
                send_buffer_size: None,
                recv_buffer_size: None,
            }))
            .await?;

        plain_transport
            .connect(PlainTransportRemoteParameters {
                ip: Some(RECORDER_IP),
                port: Some(rtp_port),
                rtcp_port: None,
                srtp_parameters: None,
            })
            .await?;

        let consumer = plain_transport
            .consume(ConsumerOptions::new(
                producer.id(),
                get_supported_rtp_capabilities(),
            ))
            .await?;

        let kind = consumer.kind();
        let extension = match kind {
            MediaKind::Audio => "webm",
            MediaKind::Video => "webm", // VP8 -> webm; falls back to mp4 below for H264
        };
        let codec = consumer.rtp_parameters().codecs.first();
        let is_h264 = matches!(
            codec,
            Some(RtpCodecParameters::Video {
                mime_type: MimeTypeVideo::H264,
                ..
            })
        );
        let extension = if is_h264 { "mp4" } else { extension };

        let file_name = format!(
            "{}-{}-{:?}.{extension}",
            peer.id,
            format!("{source:?}").to_lowercase(),
            kind
        );
        let file_path = self.inner.recordings_dir.join(&file_name);
        let sdp_path = self
            .inner
            .recordings_dir
            .join(format!("{}.sdp", producer.id()));

        let sdp = build_sdp(rtp_port, consumer.rtp_parameters())?;
        tokio::fs::write(&sdp_path, sdp).await?;

        let mut cmd = Command::new("ffmpeg");
        cmd.args(["-y", "-protocol_whitelist", "file,udp,rtp", "-i"])
            .arg(&sdp_path)
            .args(["-c", "copy", "-f", if is_h264 { "mp4" } else { "webm" }]);
        if !is_h264 {
            // Nothing extra needed for webm.
        } else {
            cmd.args(["-movflags", "frag_keyframe+empty_moov"]);
        }
        cmd.arg(&file_path);
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::null());
        cmd.stderr(std::process::Stdio::null());
        cmd.kill_on_drop(true);

        let ffmpeg = cmd
            .spawn()
            .map_err(|e| anyhow::anyhow!("failed to spawn ffmpeg: {e}"))?;

        let kind_str = match kind {
            MediaKind::Audio => "audio",
            MediaKind::Video => "video",
        };
        let recording_row_id = match db::recordings::create(
            &self.inner.db,
            self.inner.meeting_id,
            Some(peer.user_id),
            kind_str,
            file_path.to_string_lossy().as_ref(),
        )
        .await
        {
            Ok(row) => Some(row.id),
            Err(err) => {
                tracing::warn!(error = %err, "failed to insert recording row");
                None
            }
        };

        self.inner.legs.insert(
            producer.id(),
            RecordingLeg {
                _plain_transport: plain_transport,
                _consumer: consumer,
                ffmpeg,
                recording_row_id,
            },
        );

        Ok(())
    }

    /// Gracefully stop every leg: writing `q` to ffmpeg's stdin asks it to finish the current
    /// frame and finalize the container (moov atom / cues), instead of leaving a broken file
    /// behind the way `SIGKILL` would. Updates each leg's `recordings` row to `ready` (or
    /// `failed` if ffmpeg didn't exit cleanly), so the host's recordings list reflects reality
    /// instead of staying stuck on `recording` forever.
    pub async fn stop_all(&self) {
        let producer_ids: Vec<_> = self.inner.legs.iter().map(|e| *e.key()).collect();
        for id in producer_ids {
            if let Some((_, mut leg)) = self.inner.legs.remove(&id) {
                if let Some(stdin) = leg.ffmpeg.stdin.as_mut() {
                    let _ = stdin.write_all(b"q").await;
                }
                let exit =
                    tokio::time::timeout(std::time::Duration::from_secs(5), leg.ffmpeg.wait())
                        .await;

                let Some(row_id) = leg.recording_row_id else {
                    continue; // nothing to update -- we never managed to log this leg
                };
                let status = match exit {
                    Ok(Ok(exit_status)) if exit_status.success() => "ready",
                    _ => "failed",
                };
                if let Err(err) = db::recordings::mark_status(&self.inner.db, row_id, status).await
                {
                    tracing::warn!(error = %err, recording_id = %row_id, "failed to update recording status");
                }
            }
        }
    }
}

/// A minimal SDP describing a single incoming RTP stream, just enough for ffmpeg to demux it.
/// mediasoup sends plain (unencrypted) RTP/RTCP over a `PlainTransport`, so no crypto lines are
/// needed here.
fn build_sdp(port: u16, rtp_parameters: &RtpParameters) -> anyhow::Result<String> {
    let codec = rtp_parameters
        .codecs
        .first()
        .ok_or_else(|| anyhow::anyhow!("consumer has no negotiated codec"))?;

    let (media_type, payload_type, encoding_name, clock_rate, channels) = match codec {
        RtpCodecParameters::Audio {
            mime_type,
            payload_type,
            clock_rate,
            channels,
            ..
        } => (
            "audio",
            *payload_type,
            mime_subtype(mime_type.as_str()),
            clock_rate.get(),
            Some(channels.get()),
        ),
        RtpCodecParameters::Video {
            mime_type,
            payload_type,
            clock_rate,
            ..
        } => (
            "video",
            *payload_type,
            mime_subtype(mime_type.as_str()),
            clock_rate.get(),
            None,
        ),
    };

    let rtpmap = match channels {
        Some(ch) if ch > 1 => format!("{payload_type} {encoding_name}/{clock_rate}/{ch}"),
        _ => format!("{payload_type} {encoding_name}/{clock_rate}"),
    };

    Ok(format!(
        "v=0\r\n\
         o=- 0 0 IN IP4 127.0.0.1\r\n\
         s=mediasoup-meet recording\r\n\
         c=IN IP4 127.0.0.1\r\n\
         t=0 0\r\n\
         m={media_type} {port} RTP/AVP {payload_type}\r\n\
         a=rtpmap:{rtpmap}\r\n\
         a=recvonly\r\n"
    ))
}

fn mime_subtype(mime: &str) -> &str {
    mime.split('/').nth(1).unwrap_or(mime)
}
