//! Delay release and multi-destination fan-out.
//!
//! * [`Releaser`] owns the [`DelayRing`]. It receives packets from the
//!   pipeline, sleeps until the oldest one is due, then hands it to every
//!   destination. It is the single point where "what goes out, and when" is
//!   decided, so all destinations are inherently in lock-step.
//! * [`destination_task`] owns one RTMP connection. It receives released
//!   packets through a bounded channel and pushes them with non-blocking
//!   sends. Any sign of falling behind (full channel, full socket queue)
//!   marks the destination as lagging, drops until the next keyframe and
//!   resumes there, so it rejoins the shared timeline instead of drifting.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use restream_rtmp::{ConnectOptions, PublishClient, PublishUrl, SendError, StreamMetadata};
use tokio::sync::{mpsc, watch};
use tokio::time::sleep;
use tracing::{debug, info, warn};

use crate::delay::DelayRing;
use crate::packet::{CodecConfig, Packet, PacketKind};
use crate::stats::{DestStatus, StatsBus, StatsEvent};

/// What the pipeline sends to the releaser.
#[derive(Debug)]
pub enum Inbound {
    Codec(CodecConfig),
    Packet(Packet, Instant),
    /// Input finished (drain mode): release what is buffered, then finish.
    Eos,
}

/// Operator commands to the releaser.
#[derive(Debug)]
pub enum ReleaseCommand {
    /// Discard the whole buffer now (panic button / hard stop).
    Cut,
    SetDelay(Duration),
    /// Treat the input as ended even though the pipeline never posted EOS.
    ForceEos,
    /// Attach a passive consumer (the On-Air monitor). It gets the codec
    /// config and every released packet; it is dropped when its queue fills.
    AttachTap(mpsc::Sender<Outbound>),
    DetachTap,
}

/// What destinations receive.
#[derive(Debug, Clone)]
pub enum Outbound {
    Codec(CodecConfig),
    /// First packet is imminent: connect now. Sent ~3 s before the first
    /// release so servers never see a long idle publisher (most drop one
    /// that is silent for 10 s).
    Prepare,
    Packet(Packet),
    /// No more packets will come; finish and disconnect.
    Finish,
}

/// How long before the first release destinations are told to connect.
pub const PRECONNECT: Duration = Duration::from_secs(3);

pub struct ReleaserConfig {
    pub delay: Duration,
    pub max_bytes: usize,
    pub expect_audio: bool,
}

/// Runs until the input is drained (after `Eos`) or the inbound channel closes.
pub async fn releaser_task(
    cfg: ReleaserConfig,
    mut inbound: mpsc::UnboundedReceiver<Inbound>,
    mut commands: mpsc::UnboundedReceiver<ReleaseCommand>,
    dests: Vec<mpsc::Sender<Outbound>>,
    dest_lag_flags: Vec<Arc<AtomicBool>>,
    stats: StatsBus,
) {
    let mut ring = DelayRing::new(cfg.delay, cfg.max_bytes);
    let mut codec: Option<CodecConfig> = None;
    let mut codec_sent = false;
    let mut tap: Option<mpsc::Sender<Outbound>> = None;
    let mut stats_tick = tokio::time::interval(Duration::from_millis(500));
    let mut first_release: Option<Instant> = None;
    // With a delay shorter than the pre-connect window there is no idle
    // period to avoid: connect right away so the first seconds are not lost.
    let mut armed = cfg.delay <= PRECONNECT;
    if armed {
        for d in &dests { let _ = d.try_send(Outbound::Prepare); }
    }
    // After a Cut nothing more may go out, including the encoder flush that
    // follows the EOS we send right after it.
    let mut cut = false;
    // Encoder-rate counters (reset every second).
    let mut enc_since = Instant::now();
    let (mut v_pkts, mut v_bytes, mut a_bytes, mut total_pkts) = (0u64, 0u64, 0u64, 0u64);

    loop {
        // Wake either to release the oldest packet, or (before the first
        // release) PRECONNECT earlier to tell destinations to connect.
        let due = ring.next_due_at();
        let wake = match due {
            Some(d) if !armed => Some(d.checked_sub(PRECONNECT).unwrap_or(d)),
            other => other,
        };
        let sleep_until = wake.map(tokio::time::Instant::from_std);

        tokio::select! {
            biased;

            cmd = commands.recv() => match cmd {
                Some(ReleaseCommand::Cut) => {
                    cut = true;
                    let n = ring.cut();
                    info!(target: "fanout", "cut: discarded {n} buffered packets");
                    // Discarded data means destinations must restart on a keyframe.
                    for f in &dest_lag_flags { f.store(true, Ordering::Relaxed); }
                    if ring.is_drained() { break; }
                }
                Some(ReleaseCommand::SetDelay(d)) => ring.set_delay(d),
                Some(ReleaseCommand::ForceEos) => {
                    ring.mark_eos();
                    if ring.is_drained() { break; }
                }
                Some(ReleaseCommand::AttachTap(t)) => {
                    if let Some(c) = &codec { let _ = t.try_send(Outbound::Codec(c.clone())); }
                    tap = Some(t);
                }
                Some(ReleaseCommand::DetachTap) => { tap = None; }
                None => {}
            },

            msg = inbound.recv() => match msg {
                Some(Inbound::Codec(c)) => {
                    if !codec_sent && c.is_complete(cfg.expect_audio) {
                        codec_sent = true;
                        for d in &dests { let _ = d.send(Outbound::Codec(c.clone())).await; }
                        if let Some(t) = &tap { let _ = t.try_send(Outbound::Codec(c.clone())); }
                    }
                    codec = Some(c);
                }
                Some(Inbound::Packet(_, _)) if cut => {}
                Some(Inbound::Packet(p, arrival)) => {
                    total_pkts += 1;
                    match p.kind {
                        PacketKind::Video => { v_pkts += 1; v_bytes += p.len() as u64; }
                        PacketKind::Audio => { a_bytes += p.len() as u64; }
                    }
                    ring.push(p, arrival);
                }
                Some(Inbound::Eos) => {
                    ring.mark_eos();
                    info!(target: "fanout", "input ended; draining {} buffered packets ({:?})", ring.len(), ring.fill());
                    if ring.is_drained() { break; }
                }
                None => {
                    // Pipeline side vanished without EOS (hard stop): nothing more will come.
                    warn!(target: "fanout", "packet source closed without EOS; draining {} packets", ring.len());
                    ring.mark_eos();
                    if ring.is_drained() { break; }
                }
            },

            _ = async { match sleep_until { Some(t) => tokio::time::sleep_until(t).await, None => std::future::pending::<()>().await } } => {
                let now = Instant::now();
                if !armed {
                    armed = true;
                    info!(target: "fanout", "first release in ≤{PRECONNECT:?}: destinations connecting");
                    for d in &dests { let _ = d.try_send(Outbound::Prepare); }
                }
                while let Some(p) = ring.pop_due(now) {
                    if first_release.is_none() {
                        first_release = Some(now);
                        info!(target: "fanout", "first packet released; on air");
                    }
                    if let Some(t) = &tap { let _ = t.try_send(Outbound::Packet(p.clone())); }
                    for (i, d) in dests.iter().enumerate() {
                        match d.try_send(Outbound::Packet(p.clone())) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => dest_lag_flags[i].store(true, Ordering::Relaxed),
                            Err(mpsc::error::TrySendError::Closed(_)) => {}
                        }
                    }
                }
                if ring.is_drained() { break; }
            },

            _ = stats_tick.tick() => {
                let now = Instant::now();
                stats.emit(StatsEvent::Delay {
                    fill: ring.fill(),
                    bytes: ring.bytes(),
                    packets: ring.len(),
                    on_air: first_release.is_some(),
                    on_air_in: if first_release.is_none() { ring.time_to_next_release(now) } else { None },
                    overflow_drops: ring.overflow_drops,
                });
                let el = enc_since.elapsed().as_secs_f64();
                if el >= 1.0 {
                    stats.emit(StatsEvent::Encoder {
                        video_fps: v_pkts as f64 / el,
                        video_kbps: v_bytes as f64 * 8.0 / 1000.0 / el,
                        audio_kbps: a_bytes as f64 * 8.0 / 1000.0 / el,
                        total_packets: total_pkts,
                    });
                    enc_since = now;
                    v_pkts = 0; v_bytes = 0; a_bytes = 0;
                }
            }
        }
    }

    for d in &dests {
        let _ = d.send(Outbound::Finish).await;
    }
    if let Some(t) = &tap { let _ = t.try_send(Outbound::Finish); }
    info!(target: "fanout", "releaser finished (eos={}, remaining={})", ring.is_eos(), ring.len());
}

/// Static description of one destination for the task.
#[derive(Debug, Clone)]
pub struct DestSpec {
    pub index: usize,
    pub name: String,
    pub url: PublishUrl,
    pub tls_insecure: bool,
    pub max_lag: Duration,
}

/// Per-connection timestamp rebasing and keyframe gating.
struct Stream {
    client: PublishClient,
    base_ms: Option<u64>,
    saw_keyframe: bool,
}

/// Runs for the whole session; reconnects with exponential backoff until
/// `Finish` arrives or `stop` flips.
pub async fn destination_task(
    spec: DestSpec,
    mut rx: mpsc::Receiver<Outbound>,
    lag_flag: Arc<AtomicBool>,
    stop: watch::Receiver<bool>,
    stats: StatsBus,
) {
    let mut codec: Option<CodecConfig> = None;
    let mut stream: Option<Stream> = None;
    let mut status = DestStatus::Standby;
    let mut reconnects: u32 = 0;
    let mut dropped: u64 = 0;
    let mut last_error: Option<String> = None;
    let mut backoff = Duration::from_secs(1);
    let mut next_attempt = Instant::now();
    let mut last_stats = Instant::now();
    // Do not open the connection until the releaser says data is imminent.
    let mut armed = false;
    let mut last_bytes: u64 = 0;

    let emit = |status: DestStatus, bytes: u64, kbps: f64, backlog: usize, reconnects: u32, dropped: u64, err: &Option<String>| {
        stats.emit(StatsEvent::Destination {
            index: spec.index,
            name: spec.name.clone(),
            status,
            bytes_sent: bytes,
            bitrate_kbps: kbps,
            backlog,
            reconnects,
            dropped_packets: dropped,
            last_error: err.clone(),
        });
    };
    emit(status, 0, 0.0, 0, 0, 0, &None);

    loop {
        if *stop.borrow() {
            break;
        }

        // ---- (re)connect when needed ----
        if stream.is_none() && armed && Instant::now() >= next_attempt {
            if status != DestStatus::Connecting {
                status = DestStatus::Connecting;
                emit(status, last_bytes, 0.0, 0, reconnects, dropped, &last_error);
            }
            let opts = ConnectOptions { tls_verify: !spec.tls_insecure, ..Default::default() };
            match PublishClient::connect(&spec.url, &opts).await {
                Ok(client) => {
                    info!(target: "fanout", dest = %spec.name, "connected to {}", spec.url);
                    backoff = Duration::from_secs(1);
                    last_error = None;
                    let mut s = Stream { client, base_ms: None, saw_keyframe: false };
                    if let Some(c) = &codec
                        && let Err(e) = send_config(&mut s, c) {
                            warn!(target: "fanout", dest = %spec.name, "config send failed: {e}");
                        }
                    stream = Some(s);
                    status = DestStatus::WaitingKeyframe;
                    emit(status, last_bytes, 0.0, 0, reconnects, dropped, &last_error);
                }
                Err(e) => {
                    let msg = e.to_string();
                    warn!(target: "fanout", dest = %spec.name, "connect failed: {msg}; retry in {backoff:?}");
                    last_error = Some(msg);
                    reconnects += 1;
                    next_attempt = Instant::now() + backoff;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    status = DestStatus::Backoff;
                    emit(status, last_bytes, 0.0, 0, reconnects, dropped, &last_error);
                }
            }
        }

        // ---- wait for work ----
        let retry_in = next_attempt.saturating_duration_since(Instant::now());
        let closed = async {
            match &stream {
                Some(s) => s.client.wait_closed().await,
                None => std::future::pending().await,
            }
        };
        let mut stop_rx = stop.clone();
        tokio::select! {
            msg = rx.recv() => match msg {
                Some(Outbound::Prepare) => { armed = true; }
                Some(Outbound::Codec(c)) => {
                    if let Some(s) = stream.as_mut()
                        && let Err(e) = send_config(s, &c) { warn!(target: "fanout", dest = %spec.name, "config: {e}"); }
                    codec = Some(c);
                }
                Some(Outbound::Packet(p)) => {
                    // Lag flagged by the releaser (our inbox was full): resync on keyframe.
                    if lag_flag.swap(false, Ordering::Relaxed)
                        && let Some(s) = stream.as_mut()
                            && s.saw_keyframe {
                                s.saw_keyframe = false;
                                dropped += 1;
                                if status != DestStatus::Lagging {
                                    status = DestStatus::Lagging;
                                    emit(status, last_bytes, 0.0, s.client.media_backlog(), reconnects, dropped, &last_error);
                                }
                            }
                    if let Some(s) = stream.as_mut() {
                        match forward(s, &p, &spec) {
                            Ok(true) => {
                                if status != DestStatus::Streaming {
                                    status = DestStatus::Streaming;
                                    emit(status, last_bytes, 0.0, s.client.media_backlog(), reconnects, dropped, &last_error);
                                }
                            }
                            Ok(false) => { dropped += 1; }
                            Err(SendError::Lagging) => {
                                dropped += 1;
                                s.saw_keyframe = false; // resync at next keyframe
                                if status != DestStatus::Lagging {
                                    status = DestStatus::Lagging;
                                    warn!(target: "fanout", dest = %spec.name, "lagging: send queue full, resyncing at next keyframe");
                                    emit(status, last_bytes, 0.0, s.client.media_backlog(), reconnects, dropped, &last_error);
                                }
                            }
                            Err(SendError::Closed) => {
                                let reason = s.client.closed_reason().unwrap_or_else(|| "closed".into());
                                schedule_reconnect(&spec, &mut stream, &mut status, &mut reconnects, &mut next_attempt, &mut backoff, &mut last_error, reason);
                                emit(status, last_bytes, 0.0, 0, reconnects, dropped, &last_error);
                            }
                        }
                    } else {
                        dropped += 1; // not connected: packet is lost for this destination
                    }
                }
                Some(Outbound::Finish) | None => break,
            },
            reason = closed => {
                schedule_reconnect(&spec, &mut stream, &mut status, &mut reconnects, &mut next_attempt, &mut backoff, &mut last_error, reason);
                emit(status, last_bytes, 0.0, 0, reconnects, dropped, &last_error);
            },
            _ = sleep(retry_in), if stream.is_none() && armed => {},
            _ = stop_rx.changed() => {},
        }

        // ---- periodic stats ----
        if last_stats.elapsed() >= Duration::from_secs(1) {
            let elapsed = last_stats.elapsed().as_secs_f64();
            let (bytes, backlog) = match &stream {
                Some(s) => (s.client.counters().bytes_sent.load(Ordering::Relaxed), s.client.media_backlog()),
                None => (last_bytes, 0),
            };
            let kbps = (bytes.saturating_sub(last_bytes) as f64 * 8.0 / 1000.0) / elapsed;
            emit(status, bytes, kbps, backlog, reconnects, dropped, &last_error);
            last_bytes = bytes;
            last_stats = Instant::now();
        }
    }

    if let Some(s) = stream.take() {
        s.client.close().await;
    }
    status = DestStatus::Stopped;
    emit(status, last_bytes, 0.0, 0, reconnects, dropped, &last_error);
    debug!(target: "fanout", dest = %spec.name, "destination task finished");
}

#[allow(clippy::too_many_arguments)]
fn schedule_reconnect(
    spec: &DestSpec,
    stream: &mut Option<Stream>,
    status: &mut DestStatus,
    reconnects: &mut u32,
    next_attempt: &mut Instant,
    backoff: &mut Duration,
    last_error: &mut Option<String>,
    reason: String,
) {
    warn!(target: "fanout", dest = %spec.name, "connection lost: {reason}; reconnecting in {backoff:?}");
    *stream = None;
    *last_error = Some(reason);
    *reconnects += 1;
    *next_attempt = Instant::now() + *backoff;
    *backoff = (*backoff * 2).min(Duration::from_secs(30));
    *status = DestStatus::Backoff;
}

fn send_config(s: &mut Stream, c: &CodecConfig) -> Result<(), SendError> {
    let m = &c.meta;
    let mut meta = StreamMetadata::new();
    meta.encoder = Some("restream".into());
    meta.video_width = Some(m.width);
    meta.video_height = Some(m.height);
    meta.video_codec_id = Some(7);
    meta.video_frame_rate = Some(m.fps as f32);
    meta.video_bitrate_kbps = Some(m.video_kbps);
    if c.audio_seq.is_some() {
        meta.audio_codec_id = Some(10);
        meta.audio_sample_rate = Some(m.sample_rate);
        meta.audio_channels = Some(m.channels);
        meta.audio_is_stereo = Some(m.channels == 2);
        meta.audio_bitrate_kbps = Some(m.audio_kbps);
    }
    s.client.send_metadata(&meta)?;
    if let Some(v) = &c.video_seq {
        s.client.send_video_config(v.clone(), 0)?;
    }
    if let Some(a) = &c.audio_seq {
        s.client.send_audio_config(a.clone(), 0)?;
    }
    Ok(())
}

/// Forward one packet. `Ok(false)` = intentionally skipped (waiting for keyframe).
fn forward(s: &mut Stream, p: &Packet, _spec: &DestSpec) -> Result<bool, SendError> {
    match p.kind {
        PacketKind::Video => {
            if !s.saw_keyframe {
                if !p.keyframe {
                    return Ok(false);
                }
                s.saw_keyframe = true;
                // Rebase timestamps on every (re)sync so the server sees a
                // stream starting at 0 with no backwards jumps.
                s.base_ms = Some(p.dts_ms());
            }
            let ts = rebase(s.base_ms, p.dts);
            s.client.try_send_video(p.body.clone(), ts, !p.keyframe)?;
            Ok(true)
        }
        PacketKind::Audio => {
            if !s.saw_keyframe {
                return Ok(false);
            }
            let ts = rebase(s.base_ms, p.dts);
            s.client.try_send_audio(p.body.clone(), ts)?;
            Ok(true)
        }
    }
}

fn rebase(base_ms: Option<u64>, t: Duration) -> u32 {
    let ms = t.as_millis() as u64;
    ms.saturating_sub(base_ms.unwrap_or(0)) as u32
}
