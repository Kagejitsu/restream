//! Audio monitor: plays the program audio (post-gain, undelayed) to an
//! output device of the operator's choice, with its own volume. Attached to
//! and detached from a running pipeline (session or idle preview) at the
//! audio tee, like the recording branch.

use anyhow::{anyhow, Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use tracing::{info, warn};

pub struct MonitorBranch {
    pipeline: gst::Pipeline,
    bin: gst::Bin,
    tee: gst::Element,
    tee_pad: gst::Pad,
}

/// Sink description for `device` ("" = system default, "fake" = discard,
/// used by tests and headless runs).
fn sink_description(device: &str) -> String {
    if device == "fake" {
        return "fakesink sync=true".into();
    }
    if cfg!(windows) {
        if device.is_empty() {
            "wasapi2sink low-latency=true".into()
        } else {
            format!("wasapi2sink device=\"{}\" low-latency=true", esc(device))
        }
    } else if device.is_empty() {
        "autoaudiosink".into()
    } else {
        format!("pulsesink device=\"{}\"", esc(device))
    }
}

fn esc(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

pub fn db_to_linear(db: f32) -> f64 {
    10f64.powf(db as f64 / 20.0)
}

impl MonitorBranch {
    /// Attach to the pipeline's audio tee `at`.
    pub fn start(pipeline: &gst::Pipeline, device: &str, volume_db: f32) -> Result<Self> {
        let tee = pipeline.by_name("at").ok_or_else(|| anyhow!("no audio in this pipeline"))?;
        let desc = format!(
            "queue max-size-buffers=0 max-size-bytes=0 max-size-time=200000000 leaky=downstream ! audioconvert ! audioresample \
             ! volume name=monvol volume={} ! {}",
            db_to_linear(volume_db),
            sink_description(device)
        );
        let bin = gst::parse::bin_from_description(&desc, true).context("audio monitor branch")?;
        bin.set_property("name", "audio-monitor");
        pipeline.add(&bin).context("adding audio monitor")?;
        bin.sync_state_with_parent().context("starting audio monitor")?;
        let tee_pad = tee.request_pad_simple("src_%u").ok_or_else(|| anyhow!("audio tee pad"))?;
        let bin_sink = bin.static_pad("sink").ok_or_else(|| anyhow!("monitor bin sink pad"))?;
        if let Err(e) = tee_pad.link(&bin_sink) {
            let _ = bin.set_state(gst::State::Null);
            let _ = pipeline.remove(&bin);
            tee.release_request_pad(&tee_pad);
            return Err(anyhow!("linking audio monitor: {e:?}"));
        }
        info!(target: "monitor", "audio monitor on {}", if device.is_empty() { "default output" } else { device });
        Ok(Self { pipeline: pipeline.clone(), bin, tee, tee_pad })
    }

    pub fn set_volume_db(&self, db: f32) {
        if let Some(v) = self.bin.by_name("monvol") {
            v.set_property("volume", db_to_linear(db));
        }
    }

    /// Detach. Safe to call on a pipeline that has already stopped.
    pub fn stop(self) {
        let (_, current, _) = self.pipeline.state(gst::ClockTime::ZERO);
        if current >= gst::State::Paused {
            // Block the tee pad so nothing is mid-flight while we unlink.
            let bin_sink = self.bin.static_pad("sink");
            let tee_pad = self.tee_pad.clone();
            let (tx, rx) = std::sync::mpsc::channel::<()>();
            self.tee_pad.add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, move |_, _| {
                if let Some(s) = &bin_sink {
                    let _ = tee_pad.unlink(s);
                    s.send_event(gst::event::Eos::new());
                }
                let _ = tx.send(());
                gst::PadProbeReturn::Remove
            });
            let _ = rx.recv_timeout(std::time::Duration::from_secs(2));
        }
        let _ = self.bin.set_state(gst::State::Null);
        if self.pipeline.remove(&self.bin).is_err() {
            warn!(target: "monitor", "audio monitor bin was already gone");
        }
        self.tee.release_request_pad(&self.tee_pad);
        info!(target: "monitor", "audio monitor off");
    }
}
