//! Recording branch that can be attached to and detached from a running
//! pipeline, independent of streaming.
//!
//! Attach: request pads on the encoded tees (`et`, `eat`), link through
//! queues into `matroskamux ! filesink` (or `splitmuxsink` when splitting),
//! gate everything until the first video keyframe so the file starts clean.
//!
//! Detach: block the tee pads, unlink, push EOS into the branch so the muxer
//! finalises the file, wait for EOS to reach the sink, then tear the branch
//! down. Recording while the whole pipeline ends (drain/cut) needs none of
//! this: the pipeline's own EOS finalises the file.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use gstreamer as gst;
use gstreamer::prelude::*;
use tracing::{info, warn};

pub struct RecordBranch {
    pipeline: gst::Pipeline,
    elements: Vec<gst::Element>,
    /// (tee, requested src pad, downstream queue sink pad)
    links: Vec<(gst::Element, gst::Pad, gst::Pad)>,
    /// Sink whose EOS tells us the file is finalised.
    final_sink: gst::Element,
    path: PathBuf,
    split: bool,
    started: Instant,
    has_audio: bool,
}

impl RecordBranch {
    /// Attach a recording branch to a playing pipeline.
    /// `path` is the output file; with `split_minutes > 0` a `-%03d` counter is
    /// inserted before the extension and files roll over on keyframes.
    pub fn start(pipeline: &gst::Pipeline, has_audio: bool, path: &Path, split_minutes: u32) -> Result<Self> {
        let et = pipeline.by_name("et").ok_or_else(|| anyhow!("video tee 'et' missing"))?;
        let eat = if has_audio { pipeline.by_name("eat") } else { None };
        let split = split_minutes > 0;

        let mut elements = Vec::new();
        let (mux, final_sink) = if split {
            let loc = split_location(path);
            let mux = gst::ElementFactory::make("splitmuxsink")
                .property("location", loc)
                .property("muxer-factory", "matroskamux")
                .property("max-size-time", split_minutes as u64 * 60 * gst::ClockTime::SECOND.nseconds())
                .property("send-keyframe-requests", false)
                .build()
                .context("splitmuxsink")?;
            let sink = mux.property::<gst::Element>("sink");
            elements.push(mux.clone());
            (mux, sink)
        } else {
            let mux = gst::ElementFactory::make("matroskamux").property("streamable", false).build().context("matroskamux")?;
            let sink = gst::ElementFactory::make("filesink")
                .property("location", path.display().to_string())
                .property("sync", false)
                .build()
                .context("filesink")?;
            elements.push(mux.clone());
            elements.push(sink.clone());
            (mux, sink)
        };

        let gate = Arc::new(AtomicBool::new(false));
        let mut links = Vec::new();

        // --- video ---
        let vq = queue()?;
        elements.push(vq.clone());
        let vq_sink = vq.static_pad("sink").unwrap();
        {
            let gate = Arc::clone(&gate);
            vq_sink.add_probe(gst::PadProbeType::BUFFER, move |_, info| {
                if let Some(gst::PadProbeData::Buffer(b)) = &info.data
                    && !gate.load(Ordering::Relaxed) {
                        if b.flags().contains(gst::BufferFlags::DELTA_UNIT) {
                            return gst::PadProbeReturn::Drop;
                        }
                        gate.store(true, Ordering::Relaxed);
                    }
                gst::PadProbeReturn::Ok
            });
        }
        // --- audio ---
        let aq = if eat.is_some() {
            let aq = queue()?;
            elements.push(aq.clone());
            let gate = Arc::clone(&gate);
            aq.static_pad("sink").unwrap().add_probe(gst::PadProbeType::BUFFER, move |_, _| {
                if gate.load(Ordering::Relaxed) { gst::PadProbeReturn::Ok } else { gst::PadProbeReturn::Drop }
            });
            Some(aq)
        } else {
            None
        };

        // --- add, link, sync ---
        for e in &elements {
            pipeline.add(e).context("adding record element")?;
        }
        if !split {
            mux.link(&final_sink).context("mux→filesink")?;
        }
        let vpad_name = if split { "video" } else { "video_%u" };
        let apad_name = "audio_%u";
        let mux_v = mux.request_pad_simple(vpad_name).ok_or_else(|| anyhow!("mux video pad"))?;
        vq.static_pad("src").unwrap().link(&mux_v).context("vq→mux")?;
        if let Some(aq) = &aq {
            let mux_a = mux.request_pad_simple(apad_name).ok_or_else(|| anyhow!("mux audio pad"))?;
            aq.static_pad("src").unwrap().link(&mux_a).context("aq→mux")?;
        }
        for e in &elements {
            e.sync_state_with_parent().context("syncing record element state")?;
        }
        // Tee pads last, so data only flows once the branch is ready.
        let et_src = et.request_pad_simple("src_%u").ok_or_else(|| anyhow!("tee pad"))?;
        et_src.link(&vq_sink).context("et→vq")?;
        links.push((et.clone(), et_src, vq_sink));
        if let (Some(eat), Some(aq)) = (&eat, &aq) {
            let eat_src = eat.request_pad_simple("src_%u").ok_or_else(|| anyhow!("tee pad"))?;
            let aq_sink = aq.static_pad("sink").unwrap();
            eat_src.link(&aq_sink).context("eat→aq")?;
            links.push((eat.clone(), eat_src, aq_sink));
        }

        info!(target: "record", "recording to {}{}", path.display(), if split { " (split)" } else { "" });
        Ok(Self { pipeline: pipeline.clone(), elements, links, final_sink, path: path.to_path_buf(), split, started: Instant::now(), has_audio: eat.is_some() })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn has_audio(&self) -> bool {
        self.has_audio
    }

    pub fn elapsed(&self) -> Duration {
        self.started.elapsed()
    }

    /// Bytes written so far (sum over split files).
    pub fn bytes(&self) -> u64 {
        files_for(&self.path, self.split).iter().filter_map(|p| std::fs::metadata(p).ok()).map(|m| m.len()).sum()
    }

    /// Files produced (one, or the split series).
    pub fn files(&self) -> Vec<PathBuf> {
        files_for(&self.path, self.split)
    }

    /// Detach cleanly and finalise the file(s). Blocking: run off the async runtime.
    pub fn stop(self) -> Result<Vec<PathBuf>> {
        // 1. wait for EOS at the final sink
        let (tx, rx) = mpsc::channel::<()>();
        let sink_pad = self.final_sink.static_pad("sink").ok_or_else(|| anyhow!("sink pad"))?;
        sink_pad.add_probe(gst::PadProbeType::EVENT_DOWNSTREAM, move |_, info| {
            if let Some(gst::PadProbeData::Event(e)) = &info.data
                && e.type_() == gst::EventType::Eos {
                    let _ = tx.send(());
                    return gst::PadProbeReturn::Remove;
                }
            gst::PadProbeReturn::Ok
        });

        // 2. block each tee pad, unlink, send EOS into the branch
        for (tee, tee_src, q_sink) in &self.links {
            let tee_src2 = tee_src.clone();
            let q_sink2 = q_sink.clone();
            let tee2 = tee.clone();
            tee_src.add_probe(gst::PadProbeType::BLOCK_DOWNSTREAM, move |_, _| {
                let _ = tee_src2.unlink(&q_sink2);
                q_sink2.send_event(gst::event::Eos::new());
                tee2.release_request_pad(&tee_src2);
                gst::PadProbeReturn::Remove
            });
        }
        match rx.recv_timeout(Duration::from_secs(8)) {
            Ok(()) => {}
            Err(_) => warn!(target: "record", "timed out waiting for the muxer to finalise; file may lack an index"),
        }

        // 3. tear down
        for e in &self.elements {
            let _ = e.set_state(gst::State::Null);
        }
        for e in &self.elements {
            let _ = self.pipeline.remove(e);
        }
        let files = self.files();
        info!(target: "record", "recording stopped: {} file(s), {:.1} MB", files.len(), self.bytes() as f64 / 1e6);
        Ok(files)
    }
}

fn queue() -> Result<gst::Element> {
    gst::ElementFactory::make("queue")
        .property("max-size-buffers", 0u32)
        .property("max-size-bytes", 0u32)
        .property("max-size-time", 2 * gst::ClockTime::SECOND.nseconds())
        .build()
        .context("queue")
}

fn split_location(path: &Path) -> String {
    let stem = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| "recording".into());
    let ext = path.extension().map(|s| s.to_string_lossy().to_string()).unwrap_or_else(|| "mkv".into());
    path.with_file_name(format!("{stem}-%03d.{ext}")).display().to_string()
}

fn files_for(path: &Path, split: bool) -> Vec<PathBuf> {
    if !split {
        return vec![path.to_path_buf()];
    }
    let stem = path.file_stem().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
    let ext = path.extension().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
    let dir = path.parent().unwrap_or(Path::new("."));
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| {
                    let n = p.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
                    n.starts_with(&format!("{stem}-")) && n.ends_with(&format!(".{ext}"))
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// Remux a finished MKV into an MP4 (no re-encode, `moov` at the front).
/// Blocking. The MKV is deleted on success.
/// `has_audio` must match the file: a muxer given an audio pad that never
/// receives data waits for it forever.
pub fn remux_to_mp4(mkv: &Path, has_audio: bool) -> Result<PathBuf> {
    let mp4 = mkv.with_extension("mp4");
    let audio = if has_audio { " d. ! queue ! aacparse ! mux." } else { "" };
    let desc = format!(
        "filesrc location=\"{}\" ! matroskademux name=d \
         d. ! queue ! h264parse ! video/x-h264,stream-format=avc,alignment=au ! mux.{audio} \
         qtmux name=mux faststart=true ! filesink location=\"{}\"",
        esc(mkv),
        esc(&mp4)
    );
    let p = gst::parse::launch(&desc).context("remux pipeline")?;
    let bus = p.bus().unwrap();
    p.set_state(gst::State::Playing).context("remux start")?;
    // A remux is a copy: even hours of footage take well under this.
    let msg = bus.timed_pop_filtered(gst::ClockTime::from_seconds(900), &[gst::MessageType::Eos, gst::MessageType::Error]);
    let _ = p.set_state(gst::State::Null);
    match msg.map(|m| m.type_()) {
        Some(gst::MessageType::Eos) => {
            let _ = std::fs::remove_file(mkv);
            info!(target: "record", "remuxed to {}", mp4.display());
            Ok(mp4)
        }
        Some(_) => {
            let _ = std::fs::remove_file(&mp4);
            bail!("remux of {} failed", mkv.display())
        }
        None => bail!("remux timed out"),
    }
}

fn esc(p: &Path) -> String {
    p.display().to_string().replace('\\', "\\\\").replace('"', "\\\"")
}
