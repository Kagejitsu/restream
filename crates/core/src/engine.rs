//! The session state machine that the GUI and CLI drive.
//!
//! ```text
//!  Idle ──go_live──▶ Filling ──(first release)──▶ OnAir
//!    ▲                 │                            │
//!    │            stop_drain / stop_cut        stop_drain ──▶ Draining ──(ring empty)──┐
//!    │                 │                            │                                  │
//!    └─────────────────┴──────── stop_cut ──────────┴──────────────────────────────────┘
//! ```
//!
//! * **stop_drain**: input stops now, destinations keep receiving the
//!   buffered minutes, then disconnect. The normal end of a show.
//! * **stop_cut**: everything buffered is discarded and destinations
//!   disconnect at once. The panic button.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use parking_lot::Mutex;
use restream_rtmp::PublishUrl;
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::Profile;
use crate::fanout::{self, DestSpec, Outbound, ReleaseCommand, ReleaserConfig};
use crate::pipeline::{Pipeline, PreviewPipeline};
use crate::monitor;
use crate::preview::PreviewSlot;
use crate::record::{self, RecordBranch};
use crate::stats::{StatsBus, StatsEvent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineState {
    Idle,
    /// Pipeline running, delay buffer filling, nothing released yet.
    Filling,
    OnAir,
    /// Input stopped; buffered packets still going out.
    Draining,
}

struct Session {
    pipeline: Pipeline,
    releaser: JoinHandle<()>,
    dest_tasks: Vec<JoinHandle<()>>,
    cmd_tx: mpsc::UnboundedSender<ReleaseCommand>,
    stop_tx: watch::Sender<bool>,
    monitor: JoinHandle<()>,
    recorder: Arc<Mutex<Option<RecordBranch>>>,
    rec_ticker: JoinHandle<()>,
}

pub struct Engine {
    profile: Mutex<Profile>,
    /// Encoder to try first when the profile says "auto" (e.g. the one that
    /// worked last time, so a known-bad hardware path is not retried).
    preferred_video_encoder: Mutex<Option<String>>,
    state: watch::Sender<EngineState>,
    /// Kept so the channel is never "closed" (a watch send fails and drops
    /// the value when no receiver exists).
    state_rx: watch::Receiver<EngineState>,
    stats: StatsBus,
    preview: Arc<PreviewSlot>,
    /// Frames of the delayed (on-air) output, when the monitor is enabled.
    monitor_preview: Arc<PreviewSlot>,
    monitor_enabled: AtomicBool,
    session: tokio::sync::Mutex<Option<Session>>,
    /// Input preview while idle (never alive at the same time as a session).
    input_preview: Mutex<Option<PreviewPipeline>>,
    /// Local audio monitor branch, attached to whichever pipeline is live.
    audio_monitor: Mutex<Option<crate::audio_monitor::MonitorBranch>>,
}

impl Engine {
    pub fn new(profile: Profile) -> Arc<Self> {
        let (state, state_rx) = watch::channel(EngineState::Idle);
        Arc::new(Self {
            profile: Mutex::new(profile),
            preferred_video_encoder: Mutex::new(None),
            state,
            state_rx,
            stats: StatsBus::new(),
            preview: PreviewSlot::new(),
            monitor_preview: PreviewSlot::new(),
            monitor_enabled: AtomicBool::new(false),
            session: tokio::sync::Mutex::new(None),
            input_preview: Mutex::new(None),
            audio_monitor: Mutex::new(None),
        })
    }

    /// Encoder tried first when the profile's encoder is "auto".
    pub fn set_preferred_video_encoder(&self, name: Option<String>) {
        *self.preferred_video_encoder.lock() = name.filter(|n| !n.is_empty() && n != "auto");
    }

    pub fn profile(&self) -> Profile {
        self.profile.lock().clone()
    }

    /// Replace the profile. Only allowed while idle.
    pub fn set_profile(&self, p: Profile) -> Result<()> {
        if *self.state.borrow() != EngineState::Idle {
            bail!("cannot change the profile while a session is running");
        }
        *self.profile.lock() = p;
        Ok(())
    }

    pub fn state(&self) -> EngineState {
        *self.state_rx.borrow()
    }

    pub fn watch_state(&self) -> watch::Receiver<EngineState> {
        self.state_rx.clone()
    }

    pub fn stats(&self) -> broadcast::Receiver<StatsEvent> {
        self.stats.subscribe()
    }

    pub fn stats_bus(&self) -> &StatsBus {
        &self.stats
    }

    pub fn preview(&self) -> Arc<PreviewSlot> {
        Arc::clone(&self.preview)
    }

    pub fn monitor_preview(&self) -> Arc<PreviewSlot> {
        Arc::clone(&self.monitor_preview)
    }

    pub fn monitor_enabled(&self) -> bool {
        self.monitor_enabled.load(Ordering::Relaxed)
    }

    /// Enable/disable the On-Air monitor (decoding the delayed output).
    /// Takes effect immediately in a running session and persists for the next.
    pub async fn set_monitor(&self, enabled: bool) {
        self.monitor_enabled.store(enabled, Ordering::Relaxed);
        if let Some(s) = self.session.lock().await.as_ref() {
            if enabled {
                let (tx, rx) = mpsc::channel::<Outbound>(512);
                tokio::spawn(monitor::monitor_task(rx, Arc::clone(&self.monitor_preview)));
                let _ = s.cmd_tx.send(ReleaseCommand::AttachTap(tx));
            } else {
                let _ = s.cmd_tx.send(ReleaseCommand::DetachTap);
                self.monitor_preview.clear();
            }
        }
    }

    /// Show the configured input in the live preview (and audio meters)
    /// without encoding or streaming. Only while idle. Blocking (opens devices).
    pub fn start_input_preview(&self) -> Result<()> {
        if self.state() != EngineState::Idle {
            bail!("a session is running");
        }
        let mut slot = self.input_preview.lock();
        if slot.is_some() {
            return Ok(());
        }
        let profile = self.profile.lock().clone();
        let p = PreviewPipeline::start(&profile, Arc::clone(&self.preview), self.stats.clone())?;
        let gst_p = p.gst().clone();
        *slot = Some(p);
        drop(slot);
        self.attach_audio_monitor_to(&gst_p);
        Ok(())
    }

    /// (Re)attach the audio monitor to `pipeline` according to the profile.
    fn attach_audio_monitor_to(&self, pipeline: &gstreamer::Pipeline) {
        self.detach_audio_monitor();
        let m = self.profile.lock().input.monitor.clone();
        if !m.enabled {
            return;
        }
        match crate::audio_monitor::MonitorBranch::start(pipeline, &m.device, m.volume_db) {
            Ok(b) => *self.audio_monitor.lock() = Some(b),
            Err(e) => {
                warn!(target: "engine", "audio monitor: {e:#}");
                self.stats.emit(StatsEvent::Warning(format!("audio monitor: {e:#}")));
            }
        }
    }

    fn detach_audio_monitor(&self) {
        if let Some(b) = self.audio_monitor.lock().take() {
            b.stop();
        }
    }

    /// The pipeline currently carrying audio (session, else idle preview).
    fn active_audio_pipeline(&self) -> Option<gstreamer::Pipeline> {
        if let Some(s) = self.session.try_lock().ok().and_then(|g| g.as_ref().map(|s| s.pipeline.gst().clone())) {
            return Some(s);
        }
        self.input_preview.lock().as_ref().map(|p| p.gst().clone())
    }

    /// Program gain, live. Blocking-safe (no awaits).
    pub fn set_audio_gain_db(&self, db: f32) {
        self.profile.lock().input.gain_db = db;
        if let Some(p) = self.active_audio_pipeline() {
            crate::pipeline::set_gain(&p, db);
        }
    }

    /// Enable/disable the local audio monitor or change its device, live.
    pub fn set_audio_monitor(&self, enabled: bool, device: String, volume_db: f32) {
        {
            let mut p = self.profile.lock();
            p.input.monitor.enabled = enabled;
            p.input.monitor.device = device;
            p.input.monitor.volume_db = volume_db;
        }
        match self.active_audio_pipeline() {
            Some(p) => self.attach_audio_monitor_to(&p),
            None => self.detach_audio_monitor(),
        }
    }

    pub fn set_audio_monitor_volume_db(&self, db: f32) {
        self.profile.lock().input.monitor.volume_db = db;
        if let Some(b) = self.audio_monitor.lock().as_ref() {
            b.set_volume_db(db);
        }
    }

    /// Stop the idle input preview and release the devices. Blocking.
    pub fn stop_input_preview(&self) {
        self.detach_audio_monitor();
        if let Some(mut p) = self.input_preview.lock().take() {
            p.stop();
        }
        self.preview.clear();
    }

    pub fn input_preview_running(&self) -> bool {
        self.input_preview.lock().is_some()
    }

    /// Start capture, encoding, recording and the delay buffer. Destinations
    /// connect immediately and wait for the first released keyframe.
    pub async fn go_live(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.session.lock().await;
        if guard.is_some() {
            bail!("already running");
        }
        // The devices are exclusive: hand them from the preview to the session.
        {
            let me = Arc::clone(self);
            tokio::task::spawn_blocking(move || me.stop_input_preview()).await?;
        }
        let profile = self.profile.lock().clone();

        // ---- destinations ----
        let mut specs = Vec::new();
        for (i, d) in profile.destinations.iter().enumerate().filter(|(_, d)| d.enabled) {
            let url = if d.key.is_empty() {
                PublishUrl::from_combined(&d.url)
            } else {
                PublishUrl::from_server_and_key(&d.url, &d.key)
            }
            .with_context(|| format!("destination '{}'", d.name))?;
            specs.push(DestSpec {
                index: i,
                name: d.name.clone(),
                url,
                tls_insecure: d.tls_insecure,
                max_lag: Duration::from_millis(profile.delay.max_lag_ms as u64),
            });
        }
        if specs.is_empty() && !profile.recording.enabled {
            bail!("nothing to do: no enabled destinations and recording is off");
        }

        // ---- pipeline, with encoder fallback ----
        // A hardware encoder can be present yet refuse to open a session
        // (driver/session limits, remote desktop, odd multi-GPU setups). Try
        // the configured one first, then the others, so the show goes on.
        let configured = profile.encoder.video_encoder.clone();
        let mut candidates: Vec<String> = Vec::new();
        if configured != "auto" && !configured.is_empty() {
            candidates.push(configured.clone());
        } else if let Some(pref) = self.preferred_video_encoder.lock().clone() {
            candidates.push(pref);
        }
        for c in crate::pipeline::available_video_encoders() {
            if !candidates.contains(&c) {
                candidates.push(c);
            }
        }
        let mut built: Option<(Pipeline, crate::pipeline::Outputs)> = None;
        let mut last_err: Option<anyhow::Error> = None;
        for (i, cand) in candidates.iter().enumerate() {
            match Pipeline::build_with_encoder(&profile, Arc::clone(&self.preview), self.stats.clone(), Some(cand)) {
                Ok((p, o)) => match p.start() {
                    Ok(()) => {
                        if i > 0 {
                            let msg = format!("video encoder '{}' could not start; using '{}' instead", candidates[0], cand);
                            warn!(target: "engine", "{msg}");
                            self.stats.emit(StatsEvent::Warning(msg));
                        }
                        built = Some((p, o));
                        break;
                    }
                    Err(e) => {
                        let text = format!("{e:#}");
                        let lower = text.to_lowercase();
                        let encoder_fault = lower.contains("venc") || lower.contains("encod") || lower.contains("session");
                        warn!(target: "engine", "start with {cand} failed: {text}");
                        last_err = Some(e);
                        if !encoder_fault {
                            break; // a device problem: trying other encoders will not help
                        }
                    }
                },
                Err(e) => {
                    last_err = Some(e);
                }
            }
        }
        let Some((pipeline, outputs)) = built else {
            return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no usable video encoder")));
        };
        let encoder_fallback = pipeline.video_encoder != candidates[0];
        if encoder_fallback {
            // Next time (this run) go straight to what worked.
            *self.preferred_video_encoder.lock() = Some(pipeline.video_encoder.clone());
        }
        let session_specs = StatsEvent::Session {
            video_encoder: pipeline.video_encoder.clone(),
            video_encoder_desc: crate::pipeline::encoder_description(&pipeline.video_encoder),
            audio_encoder: pipeline.audio_encoder.clone(),
            width: profile.encoder.width,
            height: profile.encoder.height,
            fps: profile.encoder.fps,
            bitrate_kbps: profile.encoder.bitrate_kbps,
            audio_kbps: profile.encoder.audio_bitrate_kbps,
            delay_s: profile.delay.seconds,
            destinations: profile.destinations.iter().filter(|d| d.enabled).map(|d| d.name.clone()).collect(),
            recording: profile.recording.enabled,
            fallback: encoder_fallback,
        };

        // ---- fan-out ----
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut senders = Vec::new();
        let mut flags = Vec::new();
        let mut dest_tasks = Vec::new();
        for spec in specs {
            let (tx, rx) = mpsc::channel::<Outbound>(dest_queue_len(&profile));
            let flag = Arc::new(AtomicBool::new(false));
            senders.push(tx);
            flags.push(Arc::clone(&flag));
            dest_tasks.push(tokio::spawn(fanout::destination_task(spec, rx, flag, stop_rx.clone(), self.stats.clone())));
        }
        let expect_audio = !matches!(profile.input.audio, crate::config::AudioSource::None);
        let releaser = tokio::spawn(fanout::releaser_task(
            ReleaserConfig { delay: profile.delay.duration(), max_bytes: profile.delay.max_bytes(), expect_audio },
            outputs.inbound,
            cmd_rx,
            senders,
            flags,
            self.stats.clone(),
        ));

        // ---- state monitor: Filling → OnAir, Draining → Idle ----
        let monitor = {
            let me = Arc::clone(self);
            let mut rx = self.stats.subscribe();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(StatsEvent::Delay { on_air: true, .. }) => {
                            if me.state() == EngineState::Filling {
                                me.state.send_replace(EngineState::OnAir);
                            }
                        }
                        Ok(StatsEvent::Error(_)) => {
                            warn!(target: "engine", "pipeline error; stopping session (drain)");
                            let me2 = Arc::clone(&me);
                            tokio::spawn(async move { let _ = me2.stop_drain().await; });
                        }
                        Ok(_) => {}
                        Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            })
        };


        // ---- recording (attached dynamically; also start/stop-able later) ----
        let recorder: Arc<Mutex<Option<RecordBranch>>> = Arc::new(Mutex::new(None));
        if profile.recording.enabled {
            match recording_path(&profile).and_then(|path| {
                RecordBranch::start(pipeline.gst(), pipeline.has_audio, &path, profile.recording.split_minutes)
            }) {
                Ok(b) => *recorder.lock() = Some(b),
                Err(e) => {
                    warn!(target: "engine", "recording could not start: {e:#}");
                    self.stats.emit(StatsEvent::Warning(format!("recording could not start: {e:#}")));
                }
            }
        }
        let rec_ticker = {
            let recorder = Arc::clone(&recorder);
            let stats = self.stats.clone();
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(1));
                loop {
                    tick.tick().await;
                    let (active, path, bytes, duration) = match recorder.lock().as_ref() {
                        Some(r) => (true, Some(r.path().display().to_string()), r.bytes(), r.elapsed()),
                        None => (false, None, 0, Duration::ZERO),
                    };
                    stats.emit(StatsEvent::Recording { active, path, bytes, duration });
                }
            })
        };

        if self.monitor_enabled() {
            let (tx, rx) = mpsc::channel::<Outbound>(512);
            tokio::spawn(monitor::monitor_task(rx, Arc::clone(&self.monitor_preview)));
            let _ = cmd_tx.send(ReleaseCommand::AttachTap(tx));
        }
        *guard = Some(Session { pipeline, releaser, dest_tasks, cmd_tx, stop_tx, monitor, recorder, rec_ticker });
        let session_gst = guard.as_ref().map(|s| s.pipeline.gst().clone());
        self.state.send_replace(EngineState::Filling);
        info!(target: "engine", "session started: delay {}s", profile.delay.seconds);
        self.stats.emit(session_specs);
        drop(guard);
        if let Some(p) = session_gst {
            self.attach_audio_monitor_to(&p);
        }
        Ok(())
    }

    pub async fn is_recording(&self) -> bool {
        match self.session.lock().await.as_ref() {
            Some(s) => s.recorder.lock().is_some(),
            None => false,
        }
    }

    /// Start recording the live (undelayed) stream into a new file.
    pub async fn start_recording(&self) -> Result<PathBuf> {
        let guard = self.session.lock().await;
        let Some(s) = guard.as_ref() else { bail!("no session running") };
        if s.recorder.lock().is_some() {
            bail!("already recording");
        }
        let profile = self.profile.lock().clone();
        let path = recording_path(&profile)?;
        let b = RecordBranch::start(s.pipeline.gst(), s.pipeline.has_audio, &path, profile.recording.split_minutes)?;
        *s.recorder.lock() = Some(b);
        Ok(path)
    }

    /// Stop recording and finalise the file(s); streaming continues.
    pub async fn stop_recording(&self) -> Result<Vec<PathBuf>> {
        let branch = {
            let guard = self.session.lock().await;
            let Some(s) = guard.as_ref() else { bail!("no session running") };
            s.recorder.lock().take()
        };
        let Some(branch) = branch else { bail!("not recording") };
        let container = self.profile.lock().recording.container.clone();
        let has_audio = branch.has_audio();
        tokio::task::spawn_blocking(move || {
            let files = branch.stop()?;
            finish_files(files, &container, has_audio)
        })
        .await?
    }

    /// Stop the input now and let the buffered stream play out, then disconnect.
    pub async fn stop_drain(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.session.lock().await;
        let Some(session) = guard.as_mut() else { return Ok(()) };
        if self.state() == EngineState::Draining {
            return Ok(());
        }
        self.state.send_replace(EngineState::Draining);
        info!(target: "engine", "stop (drain): sending EOS");
        let cmd_tx = session.cmd_tx.clone();
        send_eos_guarded(session.pipeline.gst().clone(), cmd_tx).await;
        // The pipeline posts EOS → releaser drains → destinations finish.
        let me = Arc::clone(self);
        tokio::spawn(async move { me.finish_when_drained().await });
        Ok(())
    }

    /// Discard the delay buffer and disconnect everything immediately.
    pub async fn stop_cut(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.session.lock().await;
        let Some(session) = guard.as_mut() else { return Ok(()) };
        info!(target: "engine", "stop (CUT): discarding buffer");
        let _ = session.cmd_tx.send(ReleaseCommand::Cut);
        let _ = session.stop_tx.send(true);
        // A cut during a drain only empties the buffer; the drain's own
        // finisher is already waiting for the releaser.
        if self.state() == EngineState::Draining {
            return Ok(());
        }
        self.state.send_replace(EngineState::Draining);
        send_eos_guarded(session.pipeline.gst().clone(), session.cmd_tx.clone()).await;
        let me = Arc::clone(self);
        tokio::spawn(async move { me.finish_when_drained().await });
        Ok(())
    }

    /// Change the delay live (new packets only; already-buffered ones keep their time).
    pub async fn set_delay(&self, d: Duration) {
        if let Some(s) = self.session.lock().await.as_ref() {
            let _ = s.cmd_tx.send(ReleaseCommand::SetDelay(d));
        }
    }

    /// Change the video bitrate live (hardware and x264 encoders accept this
    /// while running). Also stored in the profile for the next session.
    pub async fn set_video_bitrate_kbps(&self, kbps: u32) {
        self.profile.lock().encoder.bitrate_kbps = kbps;
        if let Some(s) = self.session.lock().await.as_ref() {
            s.pipeline.set_video_bitrate_kbps(kbps);
        }
    }

    pub async fn set_audio_offset_ms(&self, ms: i64) {
        self.profile.lock().input.audio_offset_ms = ms;
        if let Some(s) = self.session.lock().await.as_ref() {
            s.pipeline.set_audio_offset_ms(ms);
        }
    }

    async fn finish_when_drained(self: Arc<Self>) {
        // Wait for the releaser (it exits once the ring is drained or cut).
        let releaser = {
            let mut guard = self.session.lock().await;
            guard.as_mut().map(|s| std::mem::replace(&mut s.releaser, tokio::spawn(async {})))
        };
        if let Some(r) = releaser {
            let _ = r.await;
        }
        let mut guard = self.session.lock().await;
        if let Some(mut s) = guard.take() {
            // Destinations end by themselves once they have consumed the
            // releaser's Finish marker (after every queued packet). The hard
            // stop flag is only a backstop for a task stuck in a connect.
            let dest_tasks = std::mem::take(&mut s.dest_tasks);
            let all = async {
                for t in dest_tasks {
                    let _ = t.await;
                }
            };
            if tokio::time::timeout(Duration::from_secs(8), all).await.is_err() {
                warn!(target: "engine", "destination tasks did not finish in time; forcing stop");
                let _ = s.stop_tx.send(true);
            }
            s.rec_ticker.abort();
            // Pipeline teardown blocks: do it off the async runtime. The
            // pipeline EOS has already finalised any recording; just collect it.
            let container = self.profile.lock().recording.container.clone();
            let recorder = Arc::clone(&s.recorder);
            self.detach_audio_monitor();
            let pending = tokio::task::spawn_blocking(move || {
                s.pipeline.stop();
                recorder.lock().take().map(|b| (b.files(), b.has_audio()))
            })
            .await
            .ok()
            .flatten();
            s.monitor.abort();
            // Post-processing (MP4 remux) runs AFTER the session is Idle so the
            // operator is never locked out waiting for it.
            if let Some((files, has_audio)) = pending {
                let stats = self.stats.clone();
                tokio::task::spawn_blocking(move || match finish_files(files, &container, has_audio) {
                    Ok(f) => {
                        let names: Vec<String> = f.iter().map(|p| p.display().to_string()).collect();
                        info!(target: "engine", "recording saved: {names:?}");
                        stats.emit(StatsEvent::Notice(format!("Recording saved: {}", names.join(", "))));
                    }
                    Err(e) => {
                        warn!(target: "engine", "recording post-processing failed: {e:#}");
                        stats.emit(StatsEvent::Warning(format!("recording post-processing failed: {e:#}")));
                    }
                });
            }
        }
        self.preview.clear();
        self.monitor_preview.clear();
        self.stats.emit(StatsEvent::Recording { active: false, path: None, bytes: 0, duration: Duration::ZERO });
        self.state.send_replace(EngineState::Idle);
        info!(target: "engine", "session finished");
    }

    /// Block until the session returns to Idle.
    pub async fn wait_idle(&self) {
        let mut rx = self.state_rx.clone();
        while *rx.borrow() != EngineState::Idle {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Send EOS to the pipeline from a blocking thread with a deadline. A
/// pipeline whose source is wedged can block `send_event` forever; in that
/// case the session must still be able to end, so the releaser is told to
/// finish without the pipeline's EOS and the recording is left unfinalised.
async fn send_eos_guarded(pipeline: gstreamer::Pipeline, cmd_tx: mpsc::UnboundedSender<ReleaseCommand>) {
    use gstreamer::prelude::*;
    let send = tokio::task::spawn_blocking(move || pipeline.send_event(gstreamer::event::Eos::new()));
    match tokio::time::timeout(Duration::from_secs(5), send).await {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => {
            warn!(target: "engine", "pipeline refused EOS; forcing session end");
            let _ = cmd_tx.send(ReleaseCommand::ForceEos);
        }
        _ => {
            tracing::error!(target: "engine", "pipeline did not accept EOS within 5 s (wedged); forcing session end");
            let _ = cmd_tx.send(ReleaseCommand::ForceEos);
        }
    }
}

/// Post-process finished recording files (MP4 remux when requested).
fn finish_files(files: Vec<PathBuf>, container: &str, has_audio: bool) -> Result<Vec<PathBuf>> {
    if container != "mp4" {
        return Ok(files);
    }
    let mut out = Vec::new();
    for f in files {
        match record::remux_to_mp4(&f, has_audio) {
            Ok(m) => out.push(m),
            Err(e) => {
                warn!(target: "engine", "keeping MKV; {e:#}");
                out.push(f);
            }
        }
    }
    Ok(out)
}

fn dest_queue_len(p: &Profile) -> usize {
    // Enough packets for `max_lag` of video + audio at the configured rate.
    let per_sec = p.encoder.fps as usize + (p.encoder.audio_sample_rate as usize / 1024) + 2;
    (per_sec * p.delay.max_lag_ms as usize / 1000).clamp(64, 4096)
}

fn recording_path(p: &Profile) -> Result<PathBuf> {
    let now = chrono_like_now();
    let name = p
        .recording
        .filename_template
        .replace("{profile}", &sanitize(&p.name))
        .replace("{date}", &now.0)
        .replace("{time}", &now.1);
    let ext = if p.recording.container == "mp4" { "mkv" } else { p.recording.container.as_str() };
    std::fs::create_dir_all(&p.recording.directory)
        .with_context(|| format!("creating recording dir {}", p.recording.directory.display()))?;
    Ok(p.recording.directory.join(format!("{name}.{ext}")))
}

fn sanitize(s: &str) -> String {
    s.chars().map(|c| if c.is_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect()
}

/// (YYYY-MM-DD, HHMMSS) in local time without pulling in chrono.
fn chrono_like_now() -> (String, String) {
    let secs = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    // Civil-from-days (Howard Hinnant), UTC. Good enough for file names.
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (format!("{y:04}-{m:02}-{d:02}"), format!("{:02}{:02}{:02}", rem / 3600, (rem % 3600) / 60, rem % 60))
}
