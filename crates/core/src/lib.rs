//! restream-core: capture → encode once → delay → fan out to N RTMP(S)
//! destinations, while recording the undelayed stream.
//!
//! Layering (nothing below `pipeline` knows about GStreamer):
//!
//! ```text
//!  pipeline (GStreamer)  ──Packet──▶  fanout::Releaser (DelayRing) ──▶ fanout::Destination ×N ──▶ restream-rtmp
//!       │                                     │
//!       └─ preview frames, audio levels ───▶ stats bus ◀──────────────┘
//! ```
//!
//! [`engine::Engine`] owns all of it and exposes the Go-Live / Stop / Cut
//! state machine that both the GUI and the CLI drive.

pub mod audio_monitor;
pub mod config;
pub mod delay;
pub mod devices;
pub mod engine;
pub mod fanout;
pub mod monitor;
pub mod packet;
pub mod pipeline;
pub mod preview;
pub mod record;
pub mod stats;

pub use config::Profile;
pub use engine::{Engine, EngineState};
pub use packet::{CodecConfig, Packet, PacketKind};
pub use stats::StatsEvent;

/// Initialise GStreamer. Safe to call more than once. Must run before any
/// pipeline or device enumeration.
pub fn init() -> anyhow::Result<()> {
    gstreamer::init()?;
    Ok(())
}
