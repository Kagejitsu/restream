//! Statistics and status events, broadcast from the core to any observer
//! (GUI panels, the CLI's progress printer, tests).

use std::time::Duration;

use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestStatus {
    Disabled,
    /// Session running, delay still filling; will connect just before first data.
    Standby,
    Connecting,
    /// Connected, waiting for the next keyframe before sending video.
    WaitingKeyframe,
    Streaming,
    /// Streaming but the send queue is saturated; resync pending.
    Lagging,
    /// Disconnected; reconnect scheduled.
    Backoff,
    Stopped,
}

impl DestStatus {
    pub fn label(self) -> &'static str {
        match self {
            DestStatus::Disabled => "disabled",
            DestStatus::Standby => "standby",
            DestStatus::Connecting => "connecting",
            DestStatus::WaitingKeyframe => "waiting keyframe",
            DestStatus::Streaming => "streaming",
            DestStatus::Lagging => "LAGGING",
            DestStatus::Backoff => "reconnecting",
            DestStatus::Stopped => "stopped",
        }
    }
}

#[derive(Debug, Clone)]
pub enum StatsEvent {
    /// Per-destination health, emitted every second and on every state change.
    Destination {
        index: usize,
        name: String,
        status: DestStatus,
        bytes_sent: u64,
        bitrate_kbps: f64,
        /// Media packets queued for the socket (0 = keeping up).
        backlog: usize,
        reconnects: u32,
        dropped_packets: u64,
        last_error: Option<String>,
    },
    /// Delay buffer state, emitted every 500 ms while running.
    Delay {
        /// Wall-clock span of buffered output (release time of newest − oldest).
        fill: Duration,
        bytes: usize,
        packets: usize,
        /// True once the first packet has been released to destinations.
        on_air: bool,
        /// Time until the first buffered packet goes out (while filling).
        on_air_in: Option<Duration>,
        overflow_drops: u64,
    },
    /// Encoder output rate, emitted every second.
    Encoder { video_fps: f64, video_kbps: f64, audio_kbps: f64, total_packets: u64 },
    /// Audio meter from the `level` element (dBFS), per channel.
    AudioLevel { rms_db: Vec<f64>, peak_db: Vec<f64> },
    Recording { active: bool, path: Option<String>, bytes: u64, duration: Duration },
    /// Pipeline-level error (encoder/device failure). Session is stopping.
    Error(String),
    Warning(String),
    /// Something finished in the background (e.g. "recording saved as …").
    Notice(String),
    /// What the running session actually uses (emitted once at start).
    Session {
        video_encoder: String,
        video_encoder_desc: String,
        audio_encoder: Option<String>,
        width: u32,
        height: u32,
        fps: u32,
        bitrate_kbps: u32,
        audio_kbps: u32,
        delay_s: u32,
        destinations: Vec<String>,
        recording: bool,
        /// True when the configured encoder failed and this one was substituted.
        fallback: bool,
    },
}

#[derive(Clone)]
pub struct StatsBus {
    tx: broadcast::Sender<StatsEvent>,
}

impl StatsBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(1024);
        Self { tx }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<StatsEvent> {
        self.tx.subscribe()
    }
    pub fn emit(&self, ev: StatsEvent) {
        let _ = self.tx.send(ev);
    }
}

impl Default for StatsBus {
    fn default() -> Self {
        Self::new()
    }
}
