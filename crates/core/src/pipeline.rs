//! GStreamer pipeline: capture → scale/rate → encode once → (record, preview,
//! packet sinks).
//!
//! ```text
//! vsrc ─ videoconvert ─ videoscale ─ videorate ─ caps ─ [timeoverlay] ─ tee(vt)
//!   vt ─ queue ─ videoconvert ─ videoscale ─ RGBA ─ appsink(psink)         (preview)
//!   vt ─ queue ─ VENC ─ h264parse ─ avc caps ─ tee(et)
//!        et ─ queue ─ appsink(vsink)                                      (→ Rust core)
//!        et ─ queue ─ mux.video_0                                         (recording)
//! asrc ─ audioconvert ─ audioresample ─ caps ─ tee(at)
//!   at ─ queue ─ level ─ fakesink                                         (meters)
//!   at ─ queue ─ AENC ─ aacparse ─ raw caps ─ tee(eat)
//!        eat ─ queue ─ appsink(asink)                                     (→ Rust core)
//!        eat ─ queue ─ mux.audio_0                                        (recording)
//! ```
//!
//! Everything downstream of the encoders is encoded data, so the preview is
//! the only extra raw-video cost.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use gstreamer as gst;
use gstreamer::glib;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::config::{AudioSource, Encoder, Profile, VideoSource};
use crate::fanout::Inbound;
use crate::packet::{CodecConfig, FlvParser, Packet, PacketKind, StreamMeta, TAG_AUDIO, TAG_VIDEO};
use crate::preview::PreviewSlot;
use crate::stats::{StatsBus, StatsEvent};

/// Preview resolution pushed to the GUI (keeps the raw copy cheap).
pub const PREVIEW_WIDTH: u32 = 960;
pub const PREVIEW_HEIGHT: u32 = 540;

const VIDEO_ENCODER_PREFERENCE: &[&str] = &[
    // NVIDIA: the Direct3D 11 variant needs no CUDA runtime; the CUDA ones fail
    // with NV_ENC_ERR_INVALID_VERSION / CUDA_ERROR_NO_DEVICE on some driver setups.
    "nvd3d11h264enc",
    "nvh264enc",
    "nvcudah264enc",
    "amfh264enc",
    "qsvh264enc",
    "vah264enc",
    "mfh264enc",
    "x264enc",
    "openh264enc",
];
const AUDIO_ENCODER_PREFERENCE: &[&str] = &["fdkaacenc", "avenc_aac", "mfaacenc", "voaacenc"];

/// A running (or ready-to-run) capture/encode pipeline.
pub struct Pipeline {
    pipeline: gst::Pipeline,
    bus_stop: Arc<AtomicBool>,
    /// Last ERROR text seen by the bus thread (it drains the bus, so
    /// `start()` cannot pop the message itself).
    last_error: Arc<parking_lot::Mutex<Option<String>>>,
    bus_thread: Option<JoinHandle<()>>,
    audio_tee_pad: Option<gst::Pad>,
    pub video_encoder: String,
    pub audio_encoder: Option<String>,
    pub has_audio: bool,
}

/// Outputs created when the pipeline is built.
pub struct Outputs {
    /// Encoded packets + codec config → releaser.
    pub inbound: mpsc::UnboundedReceiver<Inbound>,
}

/// Which encoders are usable on this machine (factory exists *and* an
/// instance can be created, which is what HW plugins gate on), in order of
/// preference. Media Foundation H.264 encoders are slotted by the GPU vendor
/// named in their description: NVIDIA's MFT right after the NVENC elements
/// (it works when the NVENC SDK version is rejected by the driver), Intel's
/// after QSV, AMD's after AMF, unknown ones before the software encoders.
pub fn available_video_encoders() -> Vec<String> {
    let mf = media_foundation_h264_encoders();
    let pick_mf = |vendor: &str| -> Vec<String> {
        mf.iter().filter(|(_, desc)| desc.to_ascii_uppercase().contains(vendor)).map(|(n, _)| n.clone()).collect()
    };
    let mut order: Vec<String> = Vec::new();
    let mut push = |n: String| {
        if !order.contains(&n) {
            order.push(n);
        }
    };
    for n in ["nvd3d11h264enc", "nvh264enc", "nvcudah264enc"] {
        push(n.into());
    }
    pick_mf("NVIDIA").into_iter().for_each(&mut push);
    push("qsvh264enc".into());
    pick_mf("INTEL").into_iter().for_each(&mut push);
    push("amfh264enc".into());
    pick_mf("AMD").into_iter().for_each(&mut push);
    push("vah264enc".into());
    mf.iter().map(|(n, _)| n.clone()).for_each(&mut push);
    for n in ["x264enc", "openh264enc"] {
        push(n.into());
    }
    order.into_iter().filter(|n| element_usable(n)).collect()
}

/// Human-readable description of an encoder element ("Media Foundation
/// NVIDIA H.264 Encoder MFT", "x264 H.264 encoder", ...).
pub fn encoder_description(name: &str) -> String {
    gst::ElementFactory::find(name)
        .and_then(|f| f.metadata("long-name").map(|s| s.to_string()))
        .unwrap_or_else(|| name.to_string())
}

/// (element name, long description) of every Media Foundation H.264 encoder
/// element the registry offers (`mfh264enc`, `mfh264device1enc`, ...).
fn media_foundation_h264_encoders() -> Vec<(String, String)> {
    let registry = gst::Registry::get();
    let mut v: Vec<(String, String)> = registry
        .features_by_plugin("mediafoundation")
        .into_iter()
        .filter_map(|f| f.downcast::<gst::ElementFactory>().ok())
        .filter(|f| {
            let n = f.name();
            n.starts_with("mfh264") && n.ends_with("enc")
        })
        .map(|f| {
            let desc = f.metadata("long-name").map(|s| s.to_string()).unwrap_or_default();
            (f.name().to_string(), desc)
        })
        .collect();
    v.sort();
    v
}

pub fn available_audio_encoders() -> Vec<String> {
    AUDIO_ENCODER_PREFERENCE.iter().filter(|n| element_usable(n)).map(|s| s.to_string()).collect()
}

fn element_usable(name: &str) -> bool {
    gst::ElementFactory::find(name).is_some() && gst::ElementFactory::make(name).build().is_ok()
}

fn pick(requested: &str, preference: &[&str], what: &str) -> Result<String> {
    if requested != "auto" && !requested.is_empty() {
        if element_usable(requested) {
            return Ok(requested.to_string());
        }
        bail!("{what} encoder '{requested}' is not available on this machine");
    }
    preference
        .iter()
        .find(|n| element_usable(n))
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("no usable {what} encoder found (install gst-plugins-ugly/x264 or a HW codec plugin)"))
}

impl Pipeline {
    /// Build the pipeline for `profile`. Recording is attached separately
    /// (see [`crate::record::RecordBranch`]).
    pub fn build(profile: &Profile, preview: Arc<PreviewSlot>, stats: StatsBus) -> Result<(Self, Outputs)> {
        Self::build_with_encoder(profile, preview, stats, None)
    }

    /// Like [`Pipeline::build`] but forcing a particular video encoder
    /// element (used by the engine's fallback when the preferred one cannot
    /// open a session).
    pub fn build_with_encoder(profile: &Profile, preview: Arc<PreviewSlot>, stats: StatsBus, video_encoder: Option<&str>) -> Result<(Self, Outputs)> {
        let enc = &profile.encoder;
        let venc = match video_encoder {
            Some(v) => pick(v, VIDEO_ENCODER_PREFERENCE, "video")?,
            None => pick(&enc.video_encoder, VIDEO_ENCODER_PREFERENCE, "video")?,
        };
        let has_audio = !matches!(profile.input.audio, AudioSource::None);
        let aenc = if has_audio { Some(pick(&enc.audio_encoder, AUDIO_ENCODER_PREFERENCE, "audio")?) } else { None };

        let desc = describe(profile, &venc, aenc.as_deref())?;
        debug!(target: "pipeline", "launch: {desc}");
        let el = gst::parse::launch(&desc).context("building pipeline")?;
        let pipeline = el.downcast::<gst::Pipeline>().map_err(|_| anyhow!("not a pipeline"))?;

        // --- encoder tuning (properties differ per element; set what exists) ---
        let venc_el = pipeline.by_name("venc").ok_or_else(|| anyhow!("venc missing"))?;
        configure_video_encoder(&venc_el, &venc, enc);
        if let (Some(aenc_name), Some(aenc_el)) = (&aenc, pipeline.by_name("aenc")) {
            configure_audio_encoder(&aenc_el, aenc_name, enc);
        }

        // --- packet sinks ---
        let (tx, rx) = mpsc::unbounded_channel::<Inbound>();
        let meta = StreamMeta {
            width: enc.width,
            height: enc.height,
            fps: enc.fps,
            video_kbps: enc.bitrate_kbps,
            audio_kbps: enc.audio_bitrate_kbps,
            sample_rate: enc.audio_sample_rate,
            channels: enc.audio_channels,
        };
        hook_flv_sink(&pipeline, tx.clone(), meta, has_audio)?;
        hook_preview_sink(&pipeline, preview)?;

        // --- audio offset via pad offset (source pads only, per GStreamer) ---
        let audio_tee_pad = pipeline.by_name("aoff").and_then(|t| t.static_pad("src"));
        if let Some(p) = &audio_tee_pad {
            p.set_offset(profile.input.audio_offset_ms * 1_000_000);
        }

        // --- bus watcher thread ---
        let bus_stop = Arc::new(AtomicBool::new(false));
        let bus = pipeline.bus().ok_or_else(|| anyhow!("no bus"))?;
        let last_error: Arc<parking_lot::Mutex<Option<String>>> = Arc::new(parking_lot::Mutex::new(None));
        let bus_thread = {
            let stop = Arc::clone(&bus_stop);
            let tx = tx.clone();
            let stats = stats.clone();
            let slot = Arc::clone(&last_error);
            std::thread::Builder::new()
                .name("gst-bus".into())
                .spawn(move || bus_loop(bus, stop, Some(tx), stats, slot))
                .context("spawning bus thread")?
        };

        Ok((
            Self {
                pipeline,
                bus_stop,
                last_error,
                bus_thread: Some(bus_thread),
                audio_tee_pad,
                video_encoder: venc,
                audio_encoder: aenc,
                has_audio,
            },
            Outputs { inbound: rx },
        ))
    }

    /// Prefer the element's own error message (e.g. "could not open device X")
    /// over the generic state-change failure when one is on the bus.
    fn start_failure_reason(&self, fallback: &str) -> String {
        // The bus thread usually wins the race for the message; give it a moment.
        for _ in 0..30 {
            if let Some(e) = self.last_error.lock().clone() {
                return e;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        bus_error_reason(&self.pipeline, fallback)
    }
}

/// Prefer the element's own error message (e.g. "could not open device X")
/// over a generic state-change failure when one is waiting on the bus.
fn bus_error_reason(pipeline: &gst::Pipeline, fallback: &str) -> String {
    {
        if let Some(bus) = pipeline.bus() {
            while let Some(msg) = bus.pop_filtered(&[gst::MessageType::Error]) {
                if let gst::MessageView::Error(e) = msg.view() {
                    let src = e.src().map(|s| s.name().to_string()).unwrap_or_default();
                    let dbg = e.debug().map(|d| d.to_string()).unwrap_or_default();
                    let detail = dbg.lines().rev().find(|l| !l.trim().is_empty() && !l.contains("gstbasesrc.c")).unwrap_or("").trim().to_string();
                    return if detail.is_empty() { format!("{src}: {}", e.error()) } else { format!("{src}: {} — {detail}", e.error()) };
                }
            }
        }
        fallback.to_string()
    }
}

impl Pipeline {
    /// The underlying GStreamer pipeline (for attaching branches).
    pub fn gst(&self) -> &gst::Pipeline {
        &self.pipeline
    }

    pub fn start(&self) -> Result<()> {
        if let Err(e) = self.pipeline.set_state(gst::State::Playing) {
            bail!("{}", self.start_failure_reason(&format!("starting pipeline: {e}")));
        }
        // Wait for the state change to actually complete before anyone
        // attaches branches; a live pipeline reaches PLAYING without preroll.
        let (res, current, pending) = self.pipeline.state(gst::ClockTime::from_seconds(5));
        match res {
            Ok(_) if current == gst::State::Playing => {}
            _ => bail!("{}", self.start_failure_reason(&format!("pipeline did not reach PLAYING (state {current:?}, pending {pending:?}, result {res:?})"))),
        }
        info!(target: "pipeline", "playing (video: {}, audio: {:?})", self.video_encoder, self.audio_encoder);
        Ok(())
    }

    /// Ask sources to finish; the pipeline posts EOS on the bus once the
    /// encoders and muxers have flushed. Use before [`Pipeline::stop`] to get
    /// a finalised recording.
    pub fn send_eos(&self) {
        self.pipeline.send_event(gst::event::Eos::new());
    }

    /// Live bitrate change on the running video encoder.
    pub fn set_video_bitrate_kbps(&self, kbps: u32) {
        if let Some(enc) = self.pipeline.by_name("venc") {
            let v = if self.video_encoder == "openh264enc" { kbps * 1000 } else { kbps };
            if try_set(&enc, "bitrate", &v.to_string()) {
                try_set(&enc, "max-bitrate", &v.to_string());
                info!(target: "pipeline", "video bitrate → {kbps} kbps");
            }
        }
    }

    /// Live-tunable audio offset.
    pub fn set_audio_offset_ms(&self, ms: i64) {
        if let Some(p) = &self.audio_tee_pad {
            p.set_offset(ms * 1_000_000);
        }
    }

    /// Live program gain.
    pub fn set_audio_gain_db(&self, db: f32) {
        set_gain(&self.pipeline, db);
    }

    pub fn stop(&mut self) {
        if self.bus_stop.swap(true, Ordering::Relaxed) {
            return; // already stopped
        }
        let _ = self.pipeline.set_state(gst::State::Null);
        if let Some(t) = self.bus_thread.take() {
            let _ = t.join();
        }
        info!(target: "pipeline", "stopped");
    }
}

impl Drop for Pipeline {
    fn drop(&mut self) {
        self.stop();
    }
}

// ------------------------------------------------------- input preview ---

/// Source → scaler → RGBA frames (+ audio meter). No encoder, no outputs.
/// Runs while idle so the operator can check the input; must be stopped
/// before the real pipeline opens the same devices.
pub struct PreviewPipeline {
    pipeline: gst::Pipeline,
    bus_stop: Arc<AtomicBool>,
    bus_thread: Option<JoinHandle<()>>,
}

/// Set the `gain` volume element of any pipeline that has one.
pub fn set_gain(pipeline: &gst::Pipeline, db: f32) {
    if let Some(g) = pipeline.by_name("gain") {
        g.set_property("volume", crate::audio_monitor::db_to_linear(db));
    }
}

impl PreviewPipeline {
    pub fn gst(&self) -> &gst::Pipeline {
        &self.pipeline
    }

    pub fn set_audio_gain_db(&self, db: f32) {
        set_gain(&self.pipeline, db);
    }

    pub fn start(profile: &Profile, preview: Arc<PreviewSlot>, stats: StatsBus) -> Result<Self> {
        let (vsrc, asrc) = sources(profile)?;
        let mut d = format!(
            "{vsrc} ! queue max-size-buffers=4 max-size-time=0 max-size-bytes=0 leaky=downstream ! videoconvert ! videoscale \
             ! video/x-raw,format=RGBA,width={pw},height={ph},pixel-aspect-ratio=1/1 ! appsink name=psink max-buffers=1 drop=true sync=false",
            pw = PREVIEW_WIDTH,
            ph = PREVIEW_HEIGHT,
        );
        if let Some(asrc) = asrc {
            d.push_str(&format!(
                " {asrc} ! queue max-size-buffers=0 max-size-bytes=0 max-size-time=500000000 leaky=downstream ! audioconvert ! audioresample \
                 ! audio/x-raw,rate={rate},channels={ch} ! volume name=gain volume={gain} ! tee name=at \
                 at. ! queue ! level name=lvl interval=100000000 ! fakesink sync=false",
                rate = profile.encoder.audio_sample_rate,
                ch = profile.encoder.audio_channels,
                gain = crate::audio_monitor::db_to_linear(profile.input.gain_db),
            ));
        }
        debug!(target: "preview", "launch: {d}");
        let pipeline = gst::parse::launch(&d)
            .context("building preview pipeline")?
            .downcast::<gst::Pipeline>()
            .map_err(|_| anyhow!("not a pipeline"))?;
        hook_preview_sink(&pipeline, preview)?;
        let bus_stop = Arc::new(AtomicBool::new(false));
        let bus = pipeline.bus().ok_or_else(|| anyhow!("no bus"))?;
        let bus_thread = {
            let stop = Arc::clone(&bus_stop);
            let slot = Arc::new(parking_lot::Mutex::new(None));
            std::thread::Builder::new().name("gst-preview-bus".into()).spawn(move || bus_loop(bus, stop, None, stats, slot))?
        };
        pipeline.set_state(gst::State::Playing).context("starting preview")?;
        let (res, current, _) = pipeline.state(gst::ClockTime::from_seconds(5));
        if res.is_err() || current != gst::State::Playing {
            let _ = pipeline.set_state(gst::State::Null);
            bus_stop.store(true, Ordering::Relaxed);
            bail!("{}", bus_error_reason(&pipeline, "input preview could not start (device busy or missing?)"));
        }
        info!(target: "preview", "input preview running");
        Ok(Self { pipeline, bus_stop, bus_thread: Some(bus_thread) })
    }

    pub fn stop(&mut self) {
        if self.bus_stop.swap(true, Ordering::Relaxed) {
            return;
        }
        let _ = self.pipeline.set_state(gst::State::Null);
        if let Some(t) = self.bus_thread.take() {
            let _ = t.join();
        }
        info!(target: "preview", "input preview stopped");
    }
}

impl Drop for PreviewPipeline {
    fn drop(&mut self) {
        self.stop();
    }
}

// ----------------------------------------------------------- description ---

fn caps_fragment(f: &crate::config::CaptureFormat) -> String {
    let mut parts = Vec::new();
    if !f.format.is_empty() {
        if f.format.eq_ignore_ascii_case("MJPG") || f.format.eq_ignore_ascii_case("MJPEG") {
            // image/jpeg needs decoding; handled by adding jpegdec below.
            parts.push("image/jpeg".to_string());
        } else {
            parts.push(format!("video/x-raw,format={}", f.format));
        }
    }
    if let Some(w) = f.width {
        parts.push(format!("width={w}"));
    }
    if let Some(h) = f.height {
        parts.push(format!("height={h}"));
    }
    if let Some(fps) = f.fps {
        parts.push(format!("framerate={fps}/1"));
    }
    if parts.is_empty() {
        return String::new();
    }
    let mut caps = parts.join(",");
    if !caps.starts_with("video/") && !caps.starts_with("image/") {
        caps = format!("video/x-raw,{caps}");
    }
    if caps.starts_with("image/jpeg") {
        format!(" ! {caps} ! jpegdec")
    } else {
        format!(" ! {caps}")
    }
}

/// Source fragments. Returns (video_src, audio_src, both_from_decodebin).
fn sources(profile: &Profile) -> Result<(String, Option<String>)> {
    let v = match &profile.input.video {
        VideoSource::Test { pattern } => format!("videotestsrc is-live=true pattern={pattern} name=vsrc"),
        VideoSource::V4l2 { device, format } => format!("v4l2src device={device} name=vsrc{}", caps_fragment(format)),
        VideoSource::PipeWire { target, format } => {
            let t = if target.is_empty() { String::new() } else { format!(" target-object=\"{}\"", escape(target)) };
            format!("pipewiresrc{t} name=vsrc{}", caps_fragment(format))
        }
        // An empty selector must mean "default device": passing device-path=""
        // makes mfvideosrc look for a device whose path is "" and fail.
        VideoSource::MediaFoundation { device, format } => {
            let device = &unescape_id(device);
            let sel = if device.is_empty() {
                String::new()
            } else if device.starts_with("\\\\?\\") || device.starts_with('@') {
                format!(" device-path=\"{}\"", escape(device))
            } else {
                format!(" device-name=\"{}\"", escape(device))
            };
            format!("mfvideosrc{sel} name=vsrc{}", caps_fragment(format))
        }
        // Media Foundation wraps DirectShow-only devices (OBS Virtual Camera)
        // and, unlike GStreamer's dshowvideosrc, actually opens them. A
        // moniker path therefore goes to mfvideosrc; a plain name still uses
        // dshowvideosrc.
        VideoSource::DirectShow { device, format } => {
            let device = unescape_id(device);
            if device.starts_with("@device:") {
                format!("mfvideosrc device-path=\"{}\" name=vsrc{}", escape(&device), caps_fragment(format))
            } else if device.is_empty() {
                format!("dshowvideosrc name=vsrc{}", caps_fragment(format))
            } else {
                format!("dshowvideosrc device-name=\"{}\" name=vsrc{}", escape(&device), caps_fragment(format))
            }
        }
        // The KS device provider reports paths under the camera interface
        // class while ksvideosrc enumerates the video class, so paths never
        // match; names do.
        VideoSource::KernelStreaming { device, format } => {
            let device = &unescape_id(device);
            let sel = if device.is_empty() {
                String::new()
            } else if device.starts_with("\\\\?\\") {
                format!(" device-path=\"{}\"", escape(device))
            } else {
                format!(" device-name=\"{}\"", escape(device))
            };
            format!("ksvideosrc{sel} name=vsrc{}", caps_fragment(format))
        }
        VideoSource::DeckLink { device_number, mode, connection } => {
            format!("decklinkvideosrc device-number={device_number} mode={mode} connection={connection} name=vsrc")
        }
        VideoSource::Uri { uri } => format!("uridecodebin3 uri=\"{}\" name=dec dec.", escape(uri)),
    };
    let a = match &profile.input.audio {
        AudioSource::None => None,
        AudioSource::Test => Some("audiotestsrc is-live=true wave=sine freq=440 volume=0.1 name=asrc".to_string()),
        AudioSource::FromVideo => match &profile.input.video {
            VideoSource::Uri { .. } => Some("dec.".to_string()),
            VideoSource::DeckLink { device_number, .. } => {
                Some(format!("decklinkaudiosrc device-number={device_number} name=asrc"))
            }
            _ => bail!("audio 'from-video' needs a URI or DeckLink video source"),
        },
        AudioSource::Pulse { device } => {
            if device.is_empty() {
                Some("pulsesrc name=asrc".to_string())
            } else {
                Some(format!("pulsesrc device=\"{}\" name=asrc", escape(device)))
            }
        }
        AudioSource::Wasapi { device, loopback } => {
            let device = unescape_id(device);
            let dev = if device.is_empty() { String::new() } else { format!(" device=\"{}\"", escape(&device)) };
            Some(format!("wasapi2src{dev} loopback={loopback} low-latency=true name=asrc"))
        }
        AudioSource::DeckLink { device_number } => Some(format!("decklinkaudiosrc device-number={device_number} name=asrc")),
    };
    Ok((v, a))
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Undo GStreamer's string serialisation (`\{`, `\}`, `\#`, `\&`, `\@`,
/// `\\` ...) that leaks into device ids copied from older listings or pasted
/// from `gst-device-monitor` output. A string is treated as serialised only
/// when it escapes a character that never needs a backslash in a real id, so
/// genuine Windows paths (`\\?\usb#...\global`) pass through untouched.
fn unescape_id(s: &str) -> String {
    // Raw DirectShow monikers legitimately contain `}\{`, so `\{` alone is not
    // proof. Serialised strings start with an escaped `@`/`{` or a doubled
    // `\\?\` prefix, or escape characters raw ids never escape.
    let serialised = s.starts_with("\\@")
        || s.starts_with("\\{")
        || s.starts_with("\\\\\\\\")
        || s.contains("\\}")
        || s.contains("\\#")
        || s.contains("\\&");
    if !serialised {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some(n) => out.push(n),
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn describe(profile: &Profile, venc: &str, aenc: Option<&str>) -> Result<String> {
    let e = &profile.encoder;
    let (vsrc, asrc) = sources(profile)?;
    let overlay = if profile.input.burn_in_clock {
        " ! timeoverlay time-mode=running-time halignment=center valignment=center font-desc=\"Sans, 36\" shaded-background=true"
    } else {
        ""
    };
    let profile_caps = if e.profile.is_empty() { String::new() } else { format!(",profile={}", e.profile) };

    let mut d = String::new();
    // video
    d.push_str(&format!(
        "{vsrc} ! queue max-size-buffers=4 max-size-time=0 max-size-bytes=0 leaky=downstream ! videoconvert ! videoscale ! videorate ! video/x-raw,width={w},height={h},framerate={fps}/1,pixel-aspect-ratio=1/1{overlay} ! tee name=vt \
         vt. ! queue max-size-buffers=2 leaky=downstream ! videoconvert ! videoscale ! video/x-raw,format=RGBA,width={pw},height={ph} ! appsink name=psink max-buffers=1 drop=true sync=false \
         vt. ! queue max-size-buffers=8 max-size-time=0 max-size-bytes=0 ! {venc} name=venc ! video/x-h264{profile_caps} ! h264parse config-interval=-1 ! video/x-h264,stream-format=avc,alignment=au ! tee name=et \
         et. ! queue ! flv.video ",
        w = e.width,
        h = e.height,
        fps = e.fps,
        pw = PREVIEW_WIDTH,
        ph = PREVIEW_HEIGHT,
    ));
    // audio
    if let (Some(asrc), Some(aenc)) = (asrc, aenc) {
        d.push_str(&format!(
            " {asrc} ! queue max-size-buffers=0 max-size-bytes=0 max-size-time=500000000 leaky=downstream ! audioconvert ! audioresample ! audio/x-raw,rate={rate},channels={ch} ! identity name=aoff ! volume name=gain volume={gain} ! tee name=at \
             at. ! queue ! level name=lvl interval=100000000 ! fakesink sync=false \
             at. ! queue ! {aenc} name=aenc ! aacparse ! audio/mpeg,mpegversion=4,stream-format=raw ! tee name=eat \
             eat. ! queue ! flv.audio ",
            rate = e.audio_sample_rate,
            ch = e.audio_channels,
            gain = crate::audio_monitor::db_to_linear(profile.input.gain_db),
        ));
    }
    // single interleaved FLV stream → Rust core
    d.push_str(" flvmux streamable=true name=flv ! appsink name=fsink sync=false max-buffers=0 ");
    Ok(d)
}

// --------------------------------------------------------- encoder props ---

/// Set a property from its string form if the element has it. Works for
/// ints, enums (by nick) and booleans alike. Returns whether it was applied.
fn try_set(el: &gst::Element, name: &str, value: &str) -> bool {
    let Some(pspec) = el.find_property(name) else { return false };
    match glib::Value::deserialize_with_pspec(value, &pspec) {
        Ok(v) => {
            el.set_property(name, v);
            true
        }
        Err(e) => {
            warn!(target: "pipeline", "{}: cannot set {name}={value}: {e}", el.name());
            false
        }
    }
}

fn try_set_first(el: &gst::Element, candidates: &[(&str, &str)]) -> bool {
    candidates.iter().any(|(n, v)| try_set(el, n, v))
}

fn configure_video_encoder(el: &gst::Element, name: &str, e: &Encoder) {
    let kbps = e.bitrate_kbps.to_string();
    let gop = e.keyframe_interval_frames().to_string();
    let bf = e.bframes.to_string();
    match name {
        "x264enc" => {
            try_set(el, "bitrate", &kbps);
            try_set(el, "pass", "cbr");
            try_set(el, "key-int-max", &gop);
            try_set(el, "bframes", &bf);
            try_set(el, "vbv-buf-capacity", "1000");
            try_set(el, "byte-stream", "false");
            try_set(el, "aud", "false");
            if !e.preset.is_empty() {
                try_set(el, "speed-preset", &e.preset);
            } else {
                try_set(el, "speed-preset", "veryfast");
            }
        }
        "openh264enc" => {
            try_set(el, "bitrate", &(e.bitrate_kbps * 1000).to_string());
            try_set(el, "rate-control", "bitrate");
            try_set(el, "gop-size", &gop);
        }
        _ => {
            // HW encoders: all take kbit/s and some flavour of CBR + GOP.
            try_set(el, "bitrate", &kbps);
            try_set_first(el, &[("rc-mode", "cbr"), ("rate-control", "cbr")]);
            try_set(el, "max-bitrate", &kbps);
            try_set_first(el, &[("gop-size", &gop), ("key-int-max", &gop)]);
            try_set_first(el, &[("bframes", &bf), ("b-frames", &bf)]);
            if !e.preset.is_empty() {
                try_set_first(el, &[("preset", &e.preset), ("target-usage", &e.preset), ("quality", &e.preset)]);
            }
        }
    }
    info!(target: "pipeline", "video encoder {name}: {kbps} kbps, gop {gop}, bframes {bf}");
}

fn configure_audio_encoder(el: &gst::Element, name: &str, e: &Encoder) {
    let bps = (e.audio_bitrate_kbps * 1000).to_string();
    match name {
        // all of these take bit/s
        "fdkaacenc" | "avenc_aac" | "voaacenc" | "mfaacenc" => {
            try_set(el, "bitrate", &bps);
        }
        _ => {
            try_set(el, "bitrate", &bps);
        }
    }
    info!(target: "pipeline", "audio encoder {name}: {} kbps", e.audio_bitrate_kbps);
}

// ------------------------------------------------------------- appsinks ---

fn hook_flv_sink(pipeline: &gst::Pipeline, tx: mpsc::UnboundedSender<Inbound>, meta: StreamMeta, expect_audio: bool) -> Result<()> {
    let sink = pipeline
        .by_name("fsink")
        .ok_or_else(|| anyhow!("fsink missing"))?
        .downcast::<gst_app::AppSink>()
        .map_err(|_| anyhow!("fsink is not an appsink"))?;
    let parser = parking_lot::Mutex::new(FlvParser::new());
    let config = parking_lot::Mutex::new(CodecConfig { meta, ..Default::default() });
    let config_sent = AtomicBool::new(false);
    sink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |s| {
                let sample = s.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                let Some(buffer) = sample.buffer() else { return Ok(gst::FlowSuccess::Ok) };
                let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                let arrival = Instant::now();
                let tags = parser.lock().push(map.as_slice());
                for tag in tags {
                    let kind = match tag.tag_type {
                        TAG_VIDEO => PacketKind::Video,
                        TAG_AUDIO => PacketKind::Audio,
                        _ => continue, // onMetaData script tag: we build our own
                    };
                    if tag.is_video_sequence_header() || tag.is_audio_sequence_header() {
                        let snapshot = {
                            let mut c = config.lock();
                            if kind == PacketKind::Video { c.video_seq = Some(tag.body.clone()) } else { c.audio_seq = Some(tag.body.clone()) }
                            c.clone()
                        };
                        if !config_sent.load(Ordering::Relaxed) && snapshot.is_complete(expect_audio) {
                            config_sent.store(true, Ordering::Relaxed);
                            let _ = tx.send(Inbound::Codec(snapshot));
                        }
                        continue;
                    }
                    let pkt = Packet {
                        kind,
                        dts: Duration::from_millis(tag.timestamp_ms as u64),
                        keyframe: kind == PacketKind::Audio || tag.is_video_keyframe(),
                        body: tag.body,
                    };
                    let _ = tx.send(Inbound::Packet(pkt, arrival));
                }
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );
    Ok(())
}

fn hook_preview_sink(pipeline: &gst::Pipeline, slot: Arc<PreviewSlot>) -> Result<()> {
    let sink = pipeline
        .by_name("psink")
        .ok_or_else(|| anyhow!("psink missing"))?
        .downcast::<gst_app::AppSink>()
        .map_err(|_| anyhow!("psink is not an appsink"))?;
    sink.set_callbacks(
        gst_app::AppSinkCallbacks::builder()
            .new_sample(move |s| {
                let sample = s.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                let (Some(buffer), Some(caps)) = (sample.buffer(), sample.caps()) else { return Ok(gst::FlowSuccess::Ok) };
                let st = caps.structure(0).ok_or(gst::FlowError::Error)?;
                let w = st.get::<i32>("width").unwrap_or(0) as u32;
                let h = st.get::<i32>("height").unwrap_or(0) as u32;
                let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                let expected = (w * h * 4) as usize;
                let data = map.as_slice();
                if data.len() >= expected && expected > 0 {
                    slot.publish(w, h, data[..expected].to_vec());
                }
                Ok(gst::FlowSuccess::Ok)
            })
            .build(),
    );
    Ok(())
}

// ------------------------------------------------------------------ bus ---

/// Bus watcher. With `tx` = None (preview pipeline) errors are reported as
/// warnings and nothing is forwarded to a releaser.
fn bus_loop(bus: gst::Bus, stop: Arc<AtomicBool>, tx: Option<mpsc::UnboundedSender<Inbound>>, stats: StatsBus, last_error: Arc<parking_lot::Mutex<Option<String>>>) {
    use gst::MessageView;
    while !stop.load(Ordering::Relaxed) {
        let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(100)) else { continue };
        match msg.view() {
            MessageView::Eos(_) => {
                info!(target: "pipeline", "EOS");
                if let Some(tx) = &tx {
                    let _ = tx.send(Inbound::Eos);
                }
            }
            MessageView::Error(e) => {
                let src = e.src().map(|s| s.path_string().to_string()).unwrap_or_default();
                let text = format!("{src}: {} ({})", e.error(), e.debug().map(|d| d.to_string()).unwrap_or_default());
                {
                    let short = e.src().map(|s| s.name().to_string()).unwrap_or_default();
                    let dbg = e.debug().map(|d| d.to_string()).unwrap_or_default();
                    let detail = dbg.lines().rev().find(|l| !l.trim().is_empty() && !l.contains("gstbasesrc.c")).unwrap_or("").trim().to_string();
                    let mut slot = last_error.lock();
                    if slot.is_none() {
                        *slot = Some(if detail.is_empty() { format!("{short}: {}", e.error()) } else { format!("{short}: {} — {detail}", e.error()) });
                    }
                }
                match &tx {
                    Some(tx) => {
                        tracing::error!(target: "pipeline", "{text}");
                        stats.emit(StatsEvent::Error(text));
                        // Treat as end of input so the session can wind down cleanly.
                        let _ = tx.send(Inbound::Eos);
                    }
                    None => {
                        warn!(target: "preview", "{text}");
                        stats.emit(StatsEvent::Warning(format!("input preview: {text}")));
                        return;
                    }
                }
            }
            MessageView::Warning(w) => {
                let text = format!("{}", w.error());
                warn!(target: "pipeline", "{text}");
                stats.emit(StatsEvent::Warning(text));
            }
            MessageView::Element(el) => {
                if let Some(s) = el.structure()
                    && s.name() == "level" {
                        let rms = value_array_f64(s, "rms");
                        let peak = value_array_f64(s, "peak");
                        if !rms.is_empty() {
                            stats.emit(StatsEvent::AudioLevel { rms_db: rms, peak_db: peak });
                        }
                    }
            }
            _ => {}
        }
    }
}

fn value_array_f64(s: &gst::StructureRef, field: &str) -> Vec<f64> {
    if let Ok(arr) = s.get::<glib::ValueArray>(field) {
        return arr.iter().filter_map(|v| v.get::<f64>().ok()).collect();
    }
    if let Ok(list) = s.get::<gst::List>(field) {
        return list.iter().filter_map(|v| v.get::<f64>().ok()).collect();
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn description_contains_expected_branches() {
        let mut p = Profile::default();
        p.input.burn_in_clock = true;
        let d = describe(&p, "x264enc", Some("fdkaacenc")).unwrap();
        for needle in ["videotestsrc", "timeoverlay", "tee name=vt", "x264enc name=venc", "flv.video", "appsink name=psink", "fdkaacenc name=aenc", "flv.audio", "flvmux streamable=true name=flv", "appsink name=fsink", "level name=lvl", "tee name=et", "tee name=eat"] {
            assert!(d.contains(needle), "missing {needle} in {d}");
        }
    }

    #[test]
    fn no_audio_has_no_audio_branches() {
        let mut p = Profile::default();
        p.input.audio = AudioSource::None;
        let d = describe(&p, "x264enc", None).unwrap();
        assert!(!d.contains("aenc"));
        assert!(!d.contains("flv.audio"));
    }

    #[test]
    fn windows_device_selectors() {
        use crate::config::VideoSource;
        let mut p = Profile::default();
        p.input.audio = AudioSource::None;
        p.input.video = VideoSource::MediaFoundation { device: String::new(), format: Default::default() };
        let d = describe(&p, "x264enc", None).unwrap();
        assert!(d.contains("mfvideosrc name=vsrc"), "{d}");
        assert!(!d.contains("device-path"), "{d}");
        p.input.video = VideoSource::MediaFoundation { device: "\\\\?\\usb#vid_636e&pid_0bda#{e53}\\global".into(), format: Default::default() };
        let d = describe(&p, "x264enc", None).unwrap();
        assert!(d.contains("mfvideosrc device-path=\"\\\\\\\\?\\\\usb#vid_636e&pid_0bda#{e53}\\\\global\""), "{d}");
        p.input.video = VideoSource::KernelStreaming { device: "ASUS 5M webcam".into(), format: Default::default() };
        let d = describe(&p, "x264enc", None).unwrap();
        assert!(d.contains("ksvideosrc device-name=\"ASUS 5M webcam\""), "{d}");
    }

    #[test]
    fn device_ids_are_unescaped_and_dshow_paths_use_device() {
        use crate::config::{AudioSource, VideoSource};
        assert_eq!(unescape_id("\\{0.0.1.0\\}.\\{bf41\\}"), "{0.0.1.0}.{bf41}");
        assert_eq!(unescape_id("\\\\?\\usb#vid_636e\\global"), "\\\\?\\usb#vid_636e\\global");
        assert_eq!(unescape_id("\\@device:sw:\\{860B\\}\\\\\\{A3FC\\}"), "@device:sw:{860B}\\{A3FC}");
        let mut p = Profile::default();
        p.input.video = VideoSource::DirectShow { device: "@device:sw:{860B}\\{A3FC}".into(), format: Default::default() };
        p.input.audio = AudioSource::Wasapi { device: "\\{0.0.1.0\\}.\\{bf41\\}".into(), loopback: false };
        let d = describe(&p, "x264enc", Some("avenc_aac")).unwrap();
        assert!(d.contains("mfvideosrc device-path=\"@device:sw:{860B}\\\\{A3FC}\""), "{d}");
        assert!(d.contains("wasapi2src device=\"{0.0.1.0}.{bf41}\""), "{d}");
        p.input.video = VideoSource::DirectShow { device: "OBS Virtual Camera".into(), format: Default::default() };
        let d = describe(&p, "x264enc", Some("avenc_aac")).unwrap();
        assert!(d.contains("dshowvideosrc device-name=\"OBS Virtual Camera\""), "{d}");
    }

    #[test]
    fn capture_format_fragments() {
        let f = crate::config::CaptureFormat { width: Some(1920), height: Some(1080), fps: Some(60), format: "NV12".into() };
        assert_eq!(caps_fragment(&f), " ! video/x-raw,format=NV12,width=1920,height=1080,framerate=60/1");
        let f = crate::config::CaptureFormat { width: Some(1280), height: None, fps: None, format: "MJPG".into() };
        assert_eq!(caps_fragment(&f), " ! image/jpeg,width=1280 ! jpegdec");
        assert_eq!(caps_fragment(&Default::default()), "");
    }
}
