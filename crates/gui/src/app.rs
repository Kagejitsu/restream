//! The egui application: one window, live preview + optional On-Air monitor,
//! a settings side panel, and transport controls (Go Live / Stop / Cut).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use egui::{Color32, ColorImage, RichText, TextureHandle, TextureOptions};
use restream_core::config::{AudioSource, CaptureFormat, Destination, VideoSource};
use restream_core::devices::DeviceInfo;
use restream_core::stats::DestStatus;
use restream_core::{Engine, EngineState, Profile, StatsEvent};
use tokio::sync::broadcast;

use crate::logbuf::LogBuffer;
use crate::settings::Settings;
use crate::{keys, paths};

const PRESETS: &[(&str, &str)] = &[
    ("Twitch", "rtmp://live.twitch.tv/app"),
    ("YouTube", "rtmp://a.rtmp.youtube.com/live2"),
    ("YouTube (RTMPS)", "rtmps://a.rtmps.youtube.com/live2"),
    ("Facebook", "rtmps://live-api-s.facebook.com:443/rtmp"),
    ("Kick", "rtmps://fa723fc1b171.global-contribute.live-video.net/app"),
    ("Custom", ""),
];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Tab {
    Input,
    Encoder,
    Delay,
    Destinations,
    Recording,
    Logs,
}

#[derive(Default, Clone)]
struct DestView {
    name: String,
    status: Option<DestStatus>,
    kbps: f64,
    backlog: usize,
    reconnects: u32,
    dropped: u64,
    error: Option<String>,
}

#[derive(Default)]
struct Live {
    dests: BTreeMap<usize, DestView>,
    fill: Duration,
    buffered_bytes: usize,
    on_air: bool,
    on_air_in: Option<Duration>,
    overflow: u64,
    video_fps: f64,
    video_kbps: f64,
    audio_kbps: f64,
    rms_db: Vec<f64>,
    peak_db: Vec<f64>,
    rec_active: bool,
    rec_path: Option<String>,
    rec_bytes: u64,
    rec_duration: Duration,
    last_error: Option<(Instant, String)>,
    warnings: Vec<(Instant, String)>,
    /// One-line summary of what the running session uses.
    specs: Option<String>,
    specs_fallback: bool,
}

enum UiEvent {
    Error(String),
    PreviewStarted(Result<(), String>),
    GoLiveDone(Result<(), String>),
    Devices(Vec<DeviceInfo>),
    Info(String),
}

pub struct App {
    rt: Arc<tokio::runtime::Runtime>,
    engine: Arc<Engine>,
    logs: LogBuffer,
    profile: Profile,
    profile_dirty: bool,
    profiles: Vec<String>,
    tab: Tab,
    live: Live,
    stats_rx: broadcast::Receiver<StatsEvent>,
    ui_tx: std::sync::mpsc::Sender<UiEvent>,
    ui_rx: std::sync::mpsc::Receiver<UiEvent>,
    live_tex: Option<TextureHandle>,
    live_seq: u64,
    mon_tex: Option<TextureHandle>,
    mon_seq: u64,
    monitor_on: bool,
    devices: Vec<DeviceInfo>,
    devices_loading: bool,
    video_encoders: Vec<String>,
    audio_encoders: Vec<String>,
    confirm_cut: bool,
    toast: Option<(Instant, String, bool)>,
    new_profile_name: String,
    show_keys: bool,
    /// Show the input while idle (auto-restarts when the input settings change).
    preview_wanted: bool,
    preview_input: Option<restream_core::config::Input>,
    preview_dirty_since: Option<Instant>,
    preview_starting: bool,
    /// Set while a Go Live is in flight so the idle preview does not restart
    /// in the gap between its shutdown and the session's start.
    going_live: bool,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>, rt: Arc<tokio::runtime::Runtime>, logs: LogBuffer) -> Self {
        cc.egui_ctx.set_visuals(egui::Visuals::dark());
        let mut profile = load_last_profile();
        keys::hydrate(&mut profile);
        let engine = Engine::new(profile.clone());
        {
            let pref = Settings::load().preferred_video_encoder;
            if !pref.is_empty() {
                engine.set_preferred_video_encoder(Some(pref));
            }
        }
        let stats_rx = engine.stats();
        let (ui_tx, ui_rx) = std::sync::mpsc::channel();
        let video_encoders = restream_core::pipeline::available_video_encoders();
        let audio_encoders = restream_core::pipeline::available_audio_encoders();
        let mut app = Self {
            rt,
            engine,
            logs,
            profile,
            profile_dirty: false,
            profiles: list_profiles(),
            tab: Tab::Destinations,
            live: Live::default(),
            stats_rx,
            ui_tx,
            ui_rx,
            live_tex: None,
            live_seq: 0,
            mon_tex: None,
            mon_seq: 0,
            monitor_on: false,
            devices: Vec::new(),
            devices_loading: false,
            video_encoders,
            audio_encoders,
            confirm_cut: false,
            toast: None,
            new_profile_name: String::new(),
            show_keys: false,
            preview_wanted: true,
            preview_input: None,
            preview_dirty_since: None,
            preview_starting: false,
            going_live: false,
        };
        app.refresh_devices();
        // Developer conveniences: `restream --go-live [--monitor]`
        let args: Vec<String> = std::env::args().collect();
        if args.iter().any(|a| a == "--monitor") {
            app.monitor_on = true;
        }
        if let Some(t) = args.iter().position(|a| a == "--tab").and_then(|i| args.get(i + 1)) {
            app.tab = match t.as_str() {
                "input" => Tab::Input,
                "encoder" => Tab::Encoder,
                "delay" => Tab::Delay,
                "recording" => Tab::Recording,
                "logs" => Tab::Logs,
                _ => Tab::Destinations,
            };
        }
        if args.iter().any(|a| a == "--go-live") {
            app.go_live();
        }
        app
    }

    // ------------------------------------------------------------ actions ---

    fn go_live(&mut self) {
        if let Err(e) = self.engine.set_profile(self.profile.clone()) {
            self.toast(format!("{e}"), true);
            return;
        }
        let engine = Arc::clone(&self.engine);
        let tx = self.ui_tx.clone();
        let monitor = self.monitor_on;
        self.going_live = true;
        self.rt.spawn(async move {
            engine.set_monitor(monitor).await;
            let r = engine.go_live().await.map_err(|e| format!("Go Live failed: {e:#}"));
            let _ = tx.send(UiEvent::GoLiveDone(r));
        });
        self.live = Live::default();
        self.preview_input = None;
    }

    fn stop(&mut self, cut: bool) {
        let engine = Arc::clone(&self.engine);
        let tx = self.ui_tx.clone();
        self.rt.spawn(async move {
            let r = if cut { engine.stop_cut().await } else { engine.stop_drain().await };
            if let Err(e) = r {
                let _ = tx.send(UiEvent::Error(format!("Stop failed: {e:#}")));
            }
        });
    }

    fn toggle_recording(&mut self) {
        let engine = Arc::clone(&self.engine);
        let tx = self.ui_tx.clone();
        let active = self.live.rec_active;
        self.rt.spawn(async move {
            let r = if active {
                engine.stop_recording().await.map(|f| format!("Recording saved: {}", f.iter().map(|p| p.display().to_string()).collect::<Vec<_>>().join(", ")))
            } else {
                engine.start_recording().await.map(|p| format!("Recording to {}", p.display()))
            };
            let _ = tx.send(match r {
                Ok(m) => UiEvent::Info(m),
                Err(e) => UiEvent::Error(format!("{e:#}")),
            });
        });
    }

    fn set_monitor(&mut self, on: bool) {
        self.monitor_on = on;
        let engine = Arc::clone(&self.engine);
        self.rt.spawn(async move { engine.set_monitor(on).await });
        if !on {
            self.mon_tex = None;
            self.mon_seq = 0;
        }
    }

    /// Keep the idle input preview in step with the wish and the settings:
    /// start it when idle, restart it ~0.7 s after the input settings change,
    /// stop it when unwanted.
    fn tick_input_preview(&mut self) {
        if self.going_live {
            return;
        }
        let idle = self.engine.state() == EngineState::Idle;
        let running = self.engine.input_preview_running();
        if !idle || !self.preview_wanted {
            if running && !idle {
                // go_live stops it itself; nothing to do
            } else if running {
                let engine = Arc::clone(&self.engine);
                self.rt.spawn_blocking(move || engine.stop_input_preview());
                self.preview_input = None;
            }
            return;
        }
        if self.preview_starting {
            return;
        }
        let changed = self.preview_input.as_ref().map(input_signature) != Some(input_signature(&self.profile.input));
        if running && changed {
            match self.preview_dirty_since {
                None => self.preview_dirty_since = Some(Instant::now()),
                Some(t) if t.elapsed() > Duration::from_millis(700) => {
                    self.preview_dirty_since = None;
                    let engine = Arc::clone(&self.engine);
                    self.rt.spawn_blocking(move || engine.stop_input_preview());
                    self.preview_input = None;
                }
                _ => {}
            }
            return;
        }
        if !running {
            self.preview_starting = true;
            self.preview_input = Some(self.profile.input.clone());
            if let Err(e) = self.engine.set_profile(self.profile.clone()) {
                self.preview_starting = false;
                self.toast(e.to_string(), true);
                return;
            }
            let engine = Arc::clone(&self.engine);
            let tx = self.ui_tx.clone();
            self.rt.spawn_blocking(move || {
                let r = engine.start_input_preview().map_err(|e| format!("{e:#}"));
                let _ = tx.send(UiEvent::PreviewStarted(r));
            });
        }
    }

    fn refresh_devices(&mut self) {
        if self.devices_loading {
            return;
        }
        self.devices_loading = true;
        let tx = self.ui_tx.clone();
        std::thread::spawn(move || {
            let d = restream_core::devices::enumerate();
            let _ = tx.send(UiEvent::Devices(d));
        });
    }

    fn save_profile(&mut self) {
        match keys::persist_and_strip(&self.profile) {
            Ok(stripped) => {
                let path = paths::profiles_dir().join(format!("{}.toml", sanitize(&self.profile.name)));
                match stripped.save(&path) {
                    Ok(()) => {
                        self.profile_dirty = false;
                        self.profiles = list_profiles();
                        save_last_profile_name(&self.profile.name);
                        self.toast(format!("Saved {}", path.display()), false);
                    }
                    Err(e) => self.toast(format!("Save failed: {e:#}"), true),
                }
            }
            Err(e) => self.toast(format!("Could not store stream keys: {e:#}"), true),
        }
    }

    fn load_profile(&mut self, name: &str) {
        let path = paths::profiles_dir().join(format!("{name}.toml"));
        match Profile::load(&path) {
            Ok(mut p) => {
                keys::hydrate(&mut p);
                self.profile = p;
                self.profile_dirty = false;
                save_last_profile_name(name);
                self.toast(format!("Loaded profile '{name}'"), false);
            }
            Err(e) => self.toast(format!("Load failed: {e:#}"), true),
        }
    }

    fn toast(&mut self, msg: String, error: bool) {
        if error {
            tracing::error!("{msg}");
        } else {
            tracing::info!("{msg}");
        }
        self.toast = Some((Instant::now(), msg, error));
    }

    // ------------------------------------------------------------- events ---

    fn drain_events(&mut self, ctx: &egui::Context) {
        loop {
            match self.stats_rx.try_recv() {
                Ok(ev) => self.apply_stat(ev),
                Err(broadcast::error::TryRecvError::Empty) => break,
                Err(broadcast::error::TryRecvError::Lagged(_)) => continue,
                Err(broadcast::error::TryRecvError::Closed) => break,
            }
        }
        while let Ok(ev) = self.ui_rx.try_recv() {
            match ev {
                UiEvent::Error(e) => self.toast(e, true),
                UiEvent::Info(m) => self.toast(m, false),
                UiEvent::Devices(d) => {
                    self.devices = d;
                    self.devices_loading = false;
                }
                UiEvent::GoLiveDone(r) => {
                    self.going_live = false;
                    if let Err(e) = r {
                        self.toast(e, true);
                    }
                }
                UiEvent::PreviewStarted(r) => {
                    self.preview_starting = false;
                    if let Err(e) = r {
                        self.toast(e, true);
                        self.preview_wanted = false; // do not retry in a loop
                    }
                }
            }
        }
        self.tick_input_preview();
        // preview textures
        if let Some(f) = self.engine.preview().take_newer(self.live_seq) {
            self.live_seq = f.seq;
            let img = ColorImage::from_rgba_unmultiplied([f.width as usize, f.height as usize], &f.rgba);
            match &mut self.live_tex {
                Some(t) => t.set(img, TextureOptions::LINEAR),
                None => self.live_tex = Some(ctx.load_texture("live", img, TextureOptions::LINEAR)),
            }
        }
        if self.monitor_on
            && let Some(f) = self.engine.monitor_preview().take_newer(self.mon_seq) {
                self.mon_seq = f.seq;
                let img = ColorImage::from_rgba_unmultiplied([f.width as usize, f.height as usize], &f.rgba);
                match &mut self.mon_tex {
                    Some(t) => t.set(img, TextureOptions::LINEAR),
                    None => self.mon_tex = Some(ctx.load_texture("monitor", img, TextureOptions::LINEAR)),
                }
            }
    }

    fn apply_stat(&mut self, ev: StatsEvent) {
        let l = &mut self.live;
        match ev {
            StatsEvent::Destination { index, name, status, bitrate_kbps, backlog, reconnects, dropped_packets, last_error, .. } => {
                l.dests.insert(index, DestView { name, status: Some(status), kbps: bitrate_kbps, backlog, reconnects, dropped: dropped_packets, error: last_error });
            }
            StatsEvent::Delay { fill, bytes, on_air, on_air_in, overflow_drops, .. } => {
                l.fill = fill;
                l.buffered_bytes = bytes;
                l.on_air = on_air;
                l.on_air_in = on_air_in;
                l.overflow = overflow_drops;
            }
            StatsEvent::Encoder { video_fps, video_kbps, audio_kbps, .. } => {
                l.video_fps = video_fps;
                l.video_kbps = video_kbps;
                l.audio_kbps = audio_kbps;
            }
            StatsEvent::AudioLevel { rms_db, peak_db } => {
                l.rms_db = rms_db;
                l.peak_db = peak_db;
            }
            StatsEvent::Recording { active, path, bytes, duration } => {
                l.rec_active = active;
                l.rec_path = path;
                l.rec_bytes = bytes;
                l.rec_duration = duration;
            }
            StatsEvent::Notice(n) => self.toast = Some((Instant::now(), n, false)),
            StatsEvent::Session { video_encoder, video_encoder_desc, audio_encoder, width, height, fps, bitrate_kbps, audio_kbps, delay_s, destinations, recording, fallback } => {
                let audio = match audio_encoder {
                    Some(a) => format!("AAC {audio_kbps} kbps ({a})"),
                    None => "no audio".to_string(),
                };
                let delay = fmt_dur(Duration::from_secs(delay_s as u64));
                l.specs = Some(format!(
                    "{width}x{height} @ {fps} fps  ·  {bitrate_kbps} kbps  ·  {video_encoder_desc} [{video_encoder}]  ·  {audio}  ·  delay {delay}  ·  {} destination{}{}",
                    destinations.len(),
                    if destinations.len() == 1 { "" } else { "s" },
                    if recording { "  ·  recording" } else { "" }
                ));
                if self.profile.input.burn_in_clock {
                    l.specs = l.specs.as_ref().map(|s| format!("{s}  ·  CLOCK BURN-IN ON (Input tab)"));
                }
                l.specs_fallback = fallback || self.profile.input.burn_in_clock;
                if fallback {
                    self.toast = Some((Instant::now(), format!("Configured encoder failed; running on {video_encoder_desc}. It will be tried first from now on."), false));
                }
                // Remember what worked so "auto" goes straight to it next launch.
                let mut st = Settings::load();
                if st.preferred_video_encoder != video_encoder {
                    st.preferred_video_encoder = video_encoder;
                    st.save();
                }
            }
            StatsEvent::Error(e) => l.last_error = Some((Instant::now(), e)),
            StatsEvent::Warning(w) => {
                l.warnings.push((Instant::now(), w));
                if l.warnings.len() > 5 {
                    l.warnings.remove(0);
                }
            }
        }
    }

    // ----------------------------------------------------------------- UI ---

    fn top_bar(&mut self, ui: &mut egui::Ui) {
        let state = self.engine.state();
        self.top_controls(ui, state);
        if state != EngineState::Idle
            && let Some(specs) = &self.live.specs
        {
            let color = if self.live.specs_fallback { Color32::from_rgb(230, 160, 30) } else { Color32::from_rgb(140, 200, 140) };
            ui.label(RichText::new(format!("RUNNING WITH:  {specs}")).color(color).small());
        }
    }

    fn top_controls(&mut self, ui: &mut egui::Ui, state: EngineState) {
        ui.horizontal(|ui| {
            ui.heading("Restream");
            ui.separator();
            let (label, color) = match state {
                EngineState::Idle => ("IDLE", Color32::GRAY),
                EngineState::Filling => ("FILLING DELAY", Color32::from_rgb(230, 160, 30)),
                EngineState::OnAir => ("ON AIR", Color32::from_rgb(220, 40, 40)),
                EngineState::Draining => ("DRAINING", Color32::from_rgb(200, 120, 30)),
            };
            ui.label(RichText::new(label).color(color).strong().size(18.0));
            if state == EngineState::Filling
                && let Some(d) = self.live.on_air_in {
                    ui.label(RichText::new(format!("on air in {}", fmt_dur(d))).color(color));
                }
            ui.separator();
            match state {
                EngineState::Idle => {
                    let can = self.profile.destinations.iter().any(|d| d.enabled) || self.profile.recording.enabled;
                    if ui.add_enabled(can, egui::Button::new(RichText::new("  GO LIVE  ").strong().size(16.0)).fill(Color32::from_rgb(30, 120, 50))).clicked() {
                        self.go_live();
                    }
                    if !can {
                        ui.label(RichText::new("enable a destination or recording first").weak());
                    }
                }
                EngineState::Draining => {
                    ui.label(RichText::new(format!("playing out {} of buffered stream…", fmt_dur(self.live.fill))).weak());
                    if ui.button("CUT NOW").clicked() {
                        self.stop(true);
                    }
                }
                _ => {
                    if ui.add(egui::Button::new(RichText::new("  STOP (drain)  ").strong()).fill(Color32::from_rgb(120, 80, 20))).on_hover_text("Stop the input now; the buffered delay keeps playing out to viewers, then disconnects.").clicked() {
                        self.stop(false);
                    }
                    if ui.add(egui::Button::new(RichText::new("  PANIC / CUT  ").strong()).fill(Color32::from_rgb(150, 30, 30))).on_hover_text("Discard everything buffered and disconnect immediately. Viewers never see the last minutes.").clicked() {
                        self.confirm_cut = true;
                    }
                }
            }
            ui.separator();
            if state != EngineState::Idle {
                ui.separator();
                ui.label("Bitrate");
                let mut kbps = self.profile.encoder.bitrate_kbps;
                let r = ui.add(egui::DragValue::new(&mut kbps).range(500..=60000).speed(50).suffix(" kbps"))
                    .on_hover_text("Applies to the running encoder immediately (all destinations get the same stream).");
                if r.changed() && kbps != self.profile.encoder.bitrate_kbps {
                    self.profile.encoder.bitrate_kbps = kbps;
                    let engine = Arc::clone(&self.engine);
                    self.rt.spawn(async move { engine.set_video_bitrate_kbps(kbps).await });
                }
            }
            let rec_label = if self.live.rec_active { "[REC]" } else { "REC" };
            let rec_enabled = state != EngineState::Idle && state != EngineState::Draining;
            if ui.add_enabled(rec_enabled, egui::Button::new(RichText::new(rec_label).color(if self.live.rec_active { Color32::RED } else { Color32::GRAY }))).on_hover_text("Start/stop local recording (undelayed)").clicked() {
                self.toggle_recording();
            }
            if self.live.rec_active {
                ui.label(RichText::new(format!("{}  {:.1} MB", fmt_dur(self.live.rec_duration), self.live.rec_bytes as f64 / 1e6)).weak());
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if state == EngineState::Idle {
                    ui.checkbox(&mut self.preview_wanted, "Preview input").on_hover_text("Show the input while idle (no encoding, nothing goes out). Stops by itself when you go live.");
                }
                let mut on = self.monitor_on;
                if ui.checkbox(&mut on, "On-Air monitor").on_hover_text("Decode the delayed output to see exactly what viewers see (costs one decoder).").changed() {
                    self.set_monitor(on);
                }
                ui.label(RichText::new(format!("profile: {}{}", self.profile.name, if self.profile_dirty { " *" } else { "" })).weak());
            });
        });
    }

    fn previews(&mut self, ui: &mut egui::Ui) {
        let avail = ui.available_size();
        let show_mon = self.monitor_on;
        let live_h = if show_mon { avail.y * 0.62 } else { avail.y - 4.0 };
        ui.vertical(|ui| {
            let title = if self.engine.state() == EngineState::Idle { "INPUT PREVIEW (idle, nothing goes out)" } else { "LIVE INPUT (no delay)" };
            ui.label(RichText::new(title).strong().small());
            draw_frame(ui, self.live_tex.as_ref(), egui::vec2(avail.x, live_h - 18.0), "no signal");
            if show_mon {
                let title = if self.live.on_air { "ON AIR (what viewers see)" } else { "ON AIR monitor (waiting for delay to fill)" };
                ui.label(RichText::new(title).strong().small().color(Color32::from_rgb(220, 80, 80)));
                let h = ui.available_height() - 4.0;
                draw_frame(ui, self.mon_tex.as_ref(), egui::vec2(avail.x, h), "nothing on air yet");
            }
        });
    }

    fn status_bar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal_wrapped(|ui| {
            // delay gauge
            let delay = self.profile.delay.duration().as_secs_f32().max(1.0);
            let frac = (self.live.fill.as_secs_f32() / delay).clamp(0.0, 1.0);
            ui.label("Delay buffer");
            ui.add_sized([180.0, 16.0], egui::ProgressBar::new(frac).text(format!("{} / {}  ({:.0} MB)", fmt_dur(self.live.fill), fmt_dur(self.profile.delay.duration()), self.live.buffered_bytes as f64 / 1e6)));
            if self.live.overflow > 0 {
                ui.label(RichText::new(format!("OVERFLOW: {} packets dropped (raise RAM cap)", self.live.overflow)).color(Color32::RED));
            }
            ui.separator();
            ui.label(format!("Encoder {:.0} fps  {:.0} kbps video  {:.0} kbps audio", self.live.video_fps, self.live.video_kbps, self.live.audio_kbps));
            ui.separator();
            audio_meter(ui, &self.live.rms_db, &self.live.peak_db);
            ui.separator();
            ui.label("Gain");
            let mut gain = self.profile.input.gain_db;
            if ui.add(egui::DragValue::new(&mut gain).range(-30.0..=20.0).speed(0.25).fixed_decimals(1).suffix(" dB"))
                .on_hover_text("Program audio gain: applies to every stream, the recording and the meters. Live.")
                .changed()
            {
                self.profile.input.gain_db = gain;
                self.engine.set_audio_gain_db(gain);
            }
            if gain != 0.0 && ui.small_button("0").on_hover_text("reset to 0 dB").clicked() {
                self.profile.input.gain_db = 0.0;
                self.engine.set_audio_gain_db(0.0);
            }
            ui.separator();
            let mut mon = self.profile.input.monitor.enabled;
            if ui.checkbox(&mut mon, "Monitor").on_hover_text("Play the program audio on the output device chosen in the Input tab").changed() {
                self.profile.input.monitor.enabled = mon;
                let m = self.profile.input.monitor.clone();
                self.engine.set_audio_monitor(m.enabled, m.device, m.volume_db);
            }
            if mon {
                let mut vol = self.profile.input.monitor.volume_db;
                if ui.add(egui::DragValue::new(&mut vol).range(-60.0..=12.0).speed(0.5).fixed_decimals(0).suffix(" dB"))
                    .on_hover_text("Monitor volume (does not affect the stream)")
                    .changed()
                {
                    self.profile.input.monitor.volume_db = vol;
                    self.engine.set_audio_monitor_volume_db(vol);
                }
            }
        });
        ui.horizontal_wrapped(|ui| {
            for (i, d) in self.profile.destinations.iter().enumerate() {
                let v = self.live.dests.get(&i).cloned().unwrap_or_default();
                let (color, filled) = match (d.enabled, v.status) {
                    (false, _) | (_, Some(DestStatus::Disabled)) => (Color32::DARK_GRAY, false),
                    (_, None) | (_, Some(DestStatus::Stopped)) | (_, Some(DestStatus::Standby)) => (Color32::GRAY, false),
                    (_, Some(DestStatus::Connecting)) | (_, Some(DestStatus::WaitingKeyframe)) => (Color32::YELLOW, true),
                    (_, Some(DestStatus::Streaming)) => (Color32::from_rgb(60, 200, 60), true),
                    (_, Some(DestStatus::Lagging)) => (Color32::from_rgb(255, 140, 0), true),
                    (_, Some(DestStatus::Backoff)) => (Color32::RED, true),
                };
                let (dot, _) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
                if filled {
                    ui.painter().circle_filled(dot.center(), 5.0, color);
                } else {
                    ui.painter().circle_stroke(dot.center(), 5.0, (1.5, color));
                }
                let mut text = d.name.clone();
                if let Some(s) = v.status {
                    text.push_str(&format!("  {}", s.label()));
                    if matches!(s, DestStatus::Streaming | DestStatus::Lagging) {
                        text.push_str(&format!("  {:.0} kbps", v.kbps));
                    }
                    if v.reconnects > 0 {
                        text.push_str(&format!("  reconnects:{}", v.reconnects));
                    }
                }
                let resp = ui.label(RichText::new(text).color(color));
                if let Some(e) = &v.error {
                    resp.on_hover_text(e);
                }
                ui.separator();
            }
        });
        if let Some((t, e)) = &self.live.last_error
            && t.elapsed() < Duration::from_secs(30) {
                ui.label(RichText::new(format!("ERROR: {e}")).color(Color32::RED));
            }
        if let Some((t, w)) = self.live.warnings.last()
            && t.elapsed() < Duration::from_secs(30)
        {
            ui.label(RichText::new(format!("warning: {w}")).color(Color32::from_rgb(230, 160, 30)).small());
        }
    }

    fn side_panel(&mut self, ui: &mut egui::Ui) {
        let idle = self.engine.state() == EngineState::Idle;
        ui.horizontal_wrapped(|ui| {
            for (t, label) in [
                (Tab::Destinations, "Destinations"),
                (Tab::Input, "Input"),
                (Tab::Encoder, "Encoder"),
                (Tab::Delay, "Delay"),
                (Tab::Recording, "Recording"),
                (Tab::Logs, "Logs"),
            ] {
                ui.selectable_value(&mut self.tab, t, label);
            }
        });
        ui.separator();
        if !idle && self.tab != Tab::Logs {
            ui.label(RichText::new("Settings are locked while a session is running.").color(Color32::from_rgb(230, 160, 30)).small());
        }
        let before = self.profile.clone();
        egui::ScrollArea::vertical().auto_shrink([false, false]).show(ui, |ui| {
            ui.add_enabled_ui(idle, |ui| match self.tab {
                Tab::Input => self.tab_input(ui),
                Tab::Encoder => self.tab_encoder(ui),
                Tab::Delay => self.tab_delay(ui),
                Tab::Destinations => self.tab_destinations(ui),
                Tab::Recording => self.tab_recording(ui),
                Tab::Logs => {}
            });
            if self.tab == Tab::Logs {
                self.tab_logs(ui);
            }
        });
        if self.profile != before {
            self.profile_dirty = true;
        }
    }

    fn tab_input(&mut self, ui: &mut egui::Ui) {
        ui.heading("Video source");
        let kind = video_kind(&self.profile.input.video);
        let mut new_kind = kind;
        egui::ComboBox::from_id_salt("vkind").selected_text(kind).show_ui(ui, |ui| {
            for k in video_kinds() {
                ui.selectable_value(&mut new_kind, k, k);
            }
        });
        if new_kind != kind {
            self.profile.input.video = default_video(new_kind);
        }
        match &mut self.profile.input.video {
            VideoSource::Test { pattern } => {
                ui.horizontal(|ui| {
                    ui.label("Pattern");
                    egui::ComboBox::from_id_salt("pat").selected_text(pattern.as_str()).show_ui(ui, |ui| {
                        for p in ["smpte", "ball", "snow", "black", "white", "checkers-8", "colors"] {
                            ui.selectable_value(pattern, p.to_string(), p);
                        }
                    });
                });
            }
            VideoSource::V4l2 { device, format }
            | VideoSource::MediaFoundation { device, format }
            | VideoSource::DirectShow { device, format }
            | VideoSource::KernelStreaming { device, format } => {
                device_picker(ui, "Video", &self.devices, device, self.devices_loading);
                format_editor(ui, format);
            }
            VideoSource::PipeWire { target, format } => {
                device_picker(ui, "Video", &self.devices, target, self.devices_loading);
                format_editor(ui, format);
            }
            VideoSource::DeckLink { device_number, mode, connection } => {
                ui.horizontal(|ui| {
                    ui.label("Device #");
                    ui.add(egui::DragValue::new(device_number).range(0..=15));
                });
                ui.horizontal(|ui| {
                    ui.label("Mode");
                    ui.text_edit_singleline(mode).on_hover_text("auto, or e.g. 1080p5994, 1080i5994, 720p60 (see gst-inspect-1.0 decklinkvideosrc)");
                });
                ui.horizontal(|ui| {
                    ui.label("Connection");
                    egui::ComboBox::from_id_salt("dlconn").selected_text(connection.as_str()).show_ui(ui, |ui| {
                        for c in ["auto", "sdi", "hdmi", "optical-sdi", "component", "composite", "svideo"] {
                            ui.selectable_value(connection, c.to_string(), c);
                        }
                    });
                });
            }
            VideoSource::Uri { uri } => {
                ui.label("GStreamer URI (OBS → SRT: srt://:9000?mode=listener ; RTMP: rtmp://127.0.0.1:1935/live/obs)");
                ui.text_edit_singleline(uri);
                ui.label(RichText::new("In OBS: Settings → Stream → Custom, server srt://<this PC's IP>:9000?mode=caller, key empty.").weak().small());
            }
        }
        if ui.button(if self.devices_loading { "Refreshing…" } else { "Refresh devices" }).clicked() {
            self.refresh_devices();
        }

        ui.separator();
        ui.heading("Audio source");
        let akind = audio_kind(&self.profile.input.audio);
        let mut new_akind = akind;
        egui::ComboBox::from_id_salt("akind").selected_text(akind).show_ui(ui, |ui| {
            for k in audio_kinds() {
                ui.selectable_value(&mut new_akind, k, k);
            }
        });
        if new_akind != akind {
            self.profile.input.audio = default_audio(new_akind);
        }
        match &mut self.profile.input.audio {
            AudioSource::Pulse { device } => {
                device_picker(ui, "Audio", &self.devices, device, self.devices_loading);
            }
            AudioSource::Wasapi { device, loopback } => {
                // Render endpoints (speakers, a virtual cable's output side) can
                // only be captured in loopback mode: follow the device's own flag.
                if let Some(d) = device_picker(ui, "Audio", &self.devices, device, self.devices_loading) {
                    *loopback = is_loopback_endpoint(d);
                }
                ui.checkbox(loopback, "Loopback (capture what this device plays)");
            }
            AudioSource::DeckLink { device_number } => {
                ui.horizontal(|ui| {
                    ui.label("Device #");
                    ui.add(egui::DragValue::new(device_number).range(0..=15));
                });
            }
            AudioSource::FromVideo => {
                ui.label(RichText::new("Audio embedded in the video source (DeckLink / SRT / RTMP).").weak());
            }
            AudioSource::None => {
                ui.label(RichText::new("Video only. OBS Virtual Camera carries no audio: pick a WASAPI device (e.g. VB-Cable) instead.").color(Color32::from_rgb(230, 160, 30)));
            }
            AudioSource::Test => {}
        }
        ui.horizontal(|ui| {
            ui.label("Audio offset (ms)");
            ui.add(egui::DragValue::new(&mut self.profile.input.audio_offset_ms).range(-2000..=2000).speed(5));
            ui.label(RichText::new("positive = audio later").weak());
        });
        ui.separator();
        ui.heading("Program gain and monitor");
        ui.horizontal(|ui| {
            ui.label("Gain (dB)");
            let mut gain = self.profile.input.gain_db;
            if ui.add(egui::DragValue::new(&mut gain).range(-30.0..=20.0).speed(0.25).fixed_decimals(1)).changed() {
                self.profile.input.gain_db = gain;
                self.engine.set_audio_gain_db(gain);
            }
            ui.label(RichText::new("applied before the encoder: streams, recording and meters all follow it").weak());
        });
        let mut mon_changed = false;
        ui.horizontal(|ui| {
            mon_changed |= ui.checkbox(&mut self.profile.input.monitor.enabled, "Monitor the program audio on this device:").changed();
        });
        {
            let mut dev = self.profile.input.monitor.device.clone();
            let before = dev.clone();
            sink_picker(ui, &self.devices, &mut dev, self.devices_loading);
            if dev != before {
                self.profile.input.monitor.device = dev;
                mon_changed = true;
            }
        }
        ui.horizontal(|ui| {
            ui.label("Monitor volume (dB)");
            let mut vol = self.profile.input.monitor.volume_db;
            if ui.add(egui::DragValue::new(&mut vol).range(-60.0..=12.0).speed(0.5).fixed_decimals(0)).changed() {
                self.profile.input.monitor.volume_db = vol;
                self.engine.set_audio_monitor_volume_db(vol);
            }
            ui.label(RichText::new("monitor only; the stream is unaffected").weak());
        });
        if mon_changed {
            let m = self.profile.input.monitor.clone();
            self.engine.set_audio_monitor(m.enabled, m.device, m.volume_db);
        }
        ui.separator();
        ui.checkbox(&mut self.profile.input.burn_in_clock, "TEST ONLY: burn a timecode clock into the picture (viewers and recordings will see it)")
            .on_hover_text("For rehearsals: proves every platform shows the same frame and that the delay holds. Turn it off for real shows.");
        if self.profile.input.burn_in_clock {
            ui.label(RichText::new("Clock burn-in is ON: it will be visible on every stream and in recordings.").color(Color32::from_rgb(230, 160, 30)));
        }
    }

    fn tab_encoder(&mut self, ui: &mut egui::Ui) {
        let e = &mut self.profile.encoder;
        ui.heading("Video");
        ui.horizontal(|ui| {
            ui.label("Encoder");
            egui::ComboBox::from_id_salt("venc").selected_text(e.video_encoder.as_str()).show_ui(ui, |ui| {
                ui.selectable_value(&mut e.video_encoder, "auto".to_string(), "auto (best available; remembers what worked)");
                for v in &self.video_encoders {
                    let label = format!("{v}  —  {}", restream_core::pipeline::encoder_description(v));
                    ui.selectable_value(&mut e.video_encoder, v.clone(), label);
                }
            });
        });
        ui.horizontal(|ui| {
            let pref = Settings::load().preferred_video_encoder;
            if !pref.is_empty() {
                ui.label(RichText::new(format!("auto currently prefers: {pref}  —  {}", restream_core::pipeline::encoder_description(&pref))).weak().small());
                if ui.small_button("forget").clicked() {
                    let mut st = Settings::load();
                    st.preferred_video_encoder.clear();
                    st.save();
                    self.engine.set_preferred_video_encoder(None);
                }
            }
        });
        ui.horizontal(|ui| {
            ui.label("Resolution");
            ui.add(egui::DragValue::new(&mut e.width).range(320..=3840).speed(2));
            ui.label("×");
            ui.add(egui::DragValue::new(&mut e.height).range(180..=2160).speed(2));
            for (w, h, l) in [(1920, 1080, "1080p"), (1280, 720, "720p"), (2560, 1440, "1440p")] {
                if ui.small_button(l).clicked() {
                    e.width = w;
                    e.height = h;
                }
            }
        });
        ui.horizontal(|ui| {
            ui.label("Frame rate");
            ui.add(egui::DragValue::new(&mut e.fps).range(10..=120));
            ui.label("Bitrate (kbps)");
            ui.add(egui::DragValue::new(&mut e.bitrate_kbps).range(500..=60000).speed(50));
            ui.label(RichText::new("Twitch max 6000 (8000 for partners); YouTube/Facebook allow more").weak().small());
        });
        ui.horizontal(|ui| {
            ui.label("Keyframe every (s)");
            ui.add(egui::DragValue::new(&mut e.keyframe_interval_s).range(1..=4));
            ui.label("B-frames");
            ui.add(egui::DragValue::new(&mut e.bframes).range(0..=4));
            ui.label("Profile");
            egui::ComboBox::from_id_salt("vprof").selected_text(e.profile.as_str()).show_ui(ui, |ui| {
                for p in ["high", "main", "baseline"] {
                    ui.selectable_value(&mut e.profile, p.to_string(), p);
                }
            });
        });
        ui.horizontal(|ui| {
            ui.label("Preset / quality");
            ui.text_edit_singleline(&mut e.preset).on_hover_text("x264: ultrafast…veryslow. NVENC: p1…p7. AMF: speed/balanced/quality. QSV/VA: 1…7. Empty = encoder default.");
        });
        ui.separator();
        ui.heading("Audio");
        ui.horizontal(|ui| {
            ui.label("Encoder");
            egui::ComboBox::from_id_salt("aenc").selected_text(e.audio_encoder.as_str()).show_ui(ui, |ui| {
                ui.selectable_value(&mut e.audio_encoder, "auto".to_string(), "auto");
                for v in &self.audio_encoders {
                    ui.selectable_value(&mut e.audio_encoder, v.clone(), v.as_str());
                }
            });
            ui.label("Bitrate (kbps)");
            ui.add(egui::DragValue::new(&mut e.audio_bitrate_kbps).range(64..=320).speed(8));
        });
        ui.horizontal(|ui| {
            ui.label("Sample rate");
            egui::ComboBox::from_id_salt("arate").selected_text(e.audio_sample_rate.to_string()).show_ui(ui, |ui| {
                for r in [44100u32, 48000] {
                    ui.selectable_value(&mut e.audio_sample_rate, r, r.to_string());
                }
            });
            ui.label("Channels");
            ui.add(egui::DragValue::new(&mut e.audio_channels).range(1..=2));
        });
    }

    fn tab_delay(&mut self, ui: &mut egui::Ui) {
        let d = &mut self.profile.delay;
        ui.heading("Stream delay");
        ui.horizontal(|ui| {
            ui.label("Delay (seconds)");
            ui.add(egui::DragValue::new(&mut d.seconds).range(0..=3600).speed(1));
            ui.label(RichText::new(format!("= {}", fmt_dur(Duration::from_secs(d.seconds as u64)))).weak());
            for (s, l) in [(0u32, "off"), (30, "30 s"), (180, "3 min"), (300, "5 min")] {
                if ui.small_button(l).clicked() {
                    d.seconds = s;
                }
            }
        });
        if d.seconds < 180 {
            ui.label(RichText::new("League rule: at least 3 minutes.").color(Color32::from_rgb(230, 160, 30)));
        }
        let est_mb = (self.profile.encoder.bitrate_kbps + self.profile.encoder.audio_bitrate_kbps) as f64 * d.seconds as f64 / 8.0 / 1000.0;
        ui.label(format!("Estimated buffer at current bitrate: {est_mb:.0} MB"));
        ui.horizontal(|ui| {
            ui.label("RAM cap (MB)");
            ui.add(egui::DragValue::new(&mut d.max_megabytes).range(64..=16384).speed(16));
            if (d.max_megabytes as f64) < est_mb * 1.2 {
                ui.label(RichText::new("too small for this delay").color(Color32::RED));
            }
        });
        ui.horizontal(|ui| {
            ui.label("Max lag before resync (ms)");
            ui.add(egui::DragValue::new(&mut d.max_lag_ms).range(500..=10000).speed(50));
        });
        ui.label(RichText::new("All destinations and the On-Air monitor read the same buffer, so they are always in lock-step. Recording is never delayed.").weak());
    }

    fn tab_destinations(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("Destinations");
            ui.checkbox(&mut self.show_keys, "show keys");
        });
        let mut remove: Option<usize> = None;
        let n = self.profile.destinations.len();
        for i in 0..n {
            let d = &mut self.profile.destinations[i];
            egui::Frame::group(ui.style()).show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.checkbox(&mut d.enabled, "");
                    ui.add(egui::TextEdit::singleline(&mut d.name).desired_width(140.0).hint_text("name"));
                    egui::ComboBox::from_id_salt(("preset", i)).selected_text("preset…").show_ui(ui, |ui| {
                        for (name, url) in PRESETS {
                            if ui.selectable_label(false, *name).clicked() {
                                if !url.is_empty() {
                                    d.url = url.to_string();
                                }
                                if d.name.starts_with("New destination") || d.name.is_empty() {
                                    d.name = name.split(' ').next().unwrap_or(name).to_string();
                                }
                            }
                        }
                    });
                    if ui.small_button("x").on_hover_text("remove").clicked() {
                        remove = Some(i);
                    }
                });
                ui.horizontal(|ui| {
                    ui.label("Server");
                    ui.add(egui::TextEdit::singleline(&mut d.url).desired_width(ui.available_width() - 8.0).hint_text("rtmp://host/app or rtmps://host:443/app"));
                });
                ui.horizontal(|ui| {
                    ui.label("Key");
                    ui.add(egui::TextEdit::singleline(&mut d.key).desired_width(ui.available_width() - 8.0).password(!self.show_keys).hint_text("stream key (stored in the OS credential store)"));
                });
                ui.horizontal(|ui| {
                    if d.url.starts_with("rtmps://") {
                        ui.checkbox(&mut d.tls_insecure, "accept self-signed certificate").on_hover_text("Only for your own test server");
                    }
                    let check = if d.key.is_empty() {
                        restream_rtmp::PublishUrl::from_combined(&d.url).map(|_| ())
                    } else {
                        restream_rtmp::PublishUrl::from_server_and_key(&d.url, &d.key).map(|_| ())
                    };
                    match check {
                        Ok(()) => ui.label(RichText::new("OK").color(Color32::GREEN).small()),
                        Err(e) => ui.label(RichText::new(format!("invalid: {e}")).color(Color32::RED).small()),
                    };
                });
            });
        }
        if let Some(i) = remove {
            self.profile.destinations.remove(i);
        }
        if ui.button("+ Add destination").clicked() {
            self.profile.destinations.push(Destination::default());
        }
        ui.separator();
        ui.label(RichText::new("One encode feeds every destination; each gets identical bytes at the same moment. A destination that falls behind is resynced on the next keyframe without affecting the others.").weak());
    }

    fn tab_recording(&mut self, ui: &mut egui::Ui) {
        let r = &mut self.profile.recording;
        ui.heading("Recording");
        ui.checkbox(&mut r.enabled, "Record automatically when going live");
        ui.horizontal(|ui| {
            ui.label("Folder");
            let mut s = r.directory.display().to_string();
            if ui.add(egui::TextEdit::singleline(&mut s).desired_width(ui.available_width() - 90.0)).changed() {
                r.directory = s.into();
            }
            if ui.small_button("default").clicked() {
                r.directory = paths::default_recording_dir();
            }
        });
        ui.horizontal(|ui| {
            ui.label("File name");
            ui.text_edit_singleline(&mut r.filename_template).on_hover_text("{profile} {date} {time}");
        });
        ui.horizontal(|ui| {
            ui.label("Container");
            egui::ComboBox::from_id_salt("cont").selected_text(r.container.as_str()).show_ui(ui, |ui| {
                ui.selectable_value(&mut r.container, "mkv".to_string(), "MKV (crash-safe)");
                ui.selectable_value(&mut r.container, "mp4".to_string(), "MP4 (remuxed from MKV when the session ends)");
            });
        });
        ui.horizontal(|ui| {
            ui.label("Split every (minutes, 0 = one file)");
            ui.add(egui::DragValue::new(&mut r.split_minutes).range(0..=240));
        });
        ui.label(RichText::new("Recording is the live, undelayed stream, same encode as the outputs.").weak());
    }

    fn tab_logs(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.heading("Logs");
            if ui.button("Copy diagnostics").clicked() {
                let mut text = format!("Restream {}\nprofile:\n{}\n\nlog:\n", env!("CARGO_PKG_VERSION"), self.profile.without_keys().to_toml().unwrap_or_default());
                text.push_str(&self.logs.snapshot().join("\n"));
                ui.ctx().copy_text(text);
                self.toast("Diagnostics copied to clipboard".into(), false);
            }
            if ui.button("Clear").clicked() {
                self.logs.clear();
            }
            if ui.button("Dump devices").on_hover_text("Write every capture device and its properties to the log").clicked() {
                for d in &self.devices {
                    tracing::info!(target: "devices", "[{}] {} element={:?} props={:?} formats={:?}", d.class, d.display_name, d.element, d.properties, d.formats);
                }
                self.toast(format!("{} devices written to the log", self.devices.len()), false);
            }
        });
        ui.horizontal(|ui| {
            let mut st = Settings::load();
            if ui.checkbox(&mut st.debug_logging, "Debug logging (app + GStreamer; restart to apply)").changed() {
                st.save();
            }
            ui.label(RichText::new(format!("log folder: {}", paths::logs_dir().display())).weak().small());
        });
        egui::ScrollArea::vertical().stick_to_bottom(true).show(ui, |ui| {
            for line in self.logs.snapshot() {
                let color = if line.contains("ERROR") {
                    Color32::from_rgb(255, 90, 90)
                } else if line.contains("WARN") {
                    Color32::from_rgb(240, 180, 60)
                } else {
                    ui.visuals().text_color()
                };
                ui.label(RichText::new(line).monospace().small().color(color));
            }
        });
    }

    fn profile_bar(&mut self, ui: &mut egui::Ui) {
        let idle = self.engine.state() == EngineState::Idle;
        ui.horizontal_wrapped(|ui| {
            ui.label("Profile");
            ui.add_enabled_ui(idle, |ui| {
                let current = self.profile.name.clone();
                let mut chosen: Option<String> = None;
                egui::ComboBox::from_id_salt("profiles").selected_text(current.as_str()).show_ui(ui, |ui| {
                    for p in &self.profiles {
                        if ui.selectable_label(*p == current, p).clicked() {
                            chosen = Some(p.clone());
                        }
                    }
                });
                if let Some(p) = chosen {
                    self.load_profile(&p);
                }
                ui.add(egui::TextEdit::singleline(&mut self.profile.name).desired_width(120.0));
            });
            if ui.button("Save").clicked() {
                self.save_profile();
            }
            ui.add_enabled_ui(idle, |ui| {
                ui.add(egui::TextEdit::singleline(&mut self.new_profile_name).desired_width(120.0).hint_text("new profile name"));
                if ui.button("Save as new").clicked() && !self.new_profile_name.trim().is_empty() {
                    self.profile.name = self.new_profile_name.trim().to_string();
                    self.new_profile_name.clear();
                    self.save_profile();
                }
            });
        });
    }
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.drain_events(&ctx);
        ctx.request_repaint_after(Duration::from_millis(33));

        egui::Panel::top("top").show(ui, |ui| {
            ui.add_space(4.0);
            self.top_bar(ui);
            ui.add_space(4.0);
        });
        egui::Panel::bottom("status").show(ui, |ui| {
            ui.add_space(2.0);
            self.status_bar(ui);
            ui.add_space(2.0);
        });
        egui::Panel::right("side").default_size(520.0).size_range(380.0..=760.0).resizable(true).show(ui, |ui| {
            ui.add_space(4.0);
            self.profile_bar(ui);
            ui.separator();
            self.side_panel(ui);
        });
        egui::CentralPanel::default().show(ui, |ui| self.previews(ui));

        // confirm cut
        if self.confirm_cut {
            egui::Window::new("Cut the stream?").collapsible(false).resizable(false).anchor(egui::Align2::CENTER_CENTER, [0.0, 0.0]).show(&ctx, |ui| {
                ui.label(format!("Everything buffered ({}) will be discarded and all destinations disconnected immediately.", fmt_dur(self.live.fill)));
                ui.horizontal(|ui| {
                    if ui.add(egui::Button::new(RichText::new("CUT").strong()).fill(Color32::from_rgb(150, 30, 30))).clicked() {
                        self.confirm_cut = false;
                        self.stop(true);
                    }
                    if ui.button("Cancel").clicked() {
                        self.confirm_cut = false;
                    }
                });
            });
        }
        // toast
        if let Some((t, msg, err)) = &self.toast {
            if t.elapsed() < Duration::from_secs(6) {
                egui::Area::new(egui::Id::new("toast")).anchor(egui::Align2::CENTER_BOTTOM, [0.0, -60.0]).show(&ctx, |ui| {
                    egui::Frame::popup(ui.style()).fill(if *err { Color32::from_rgb(90, 20, 20) } else { Color32::from_rgb(20, 60, 30) }).show(ui, |ui| {
                        ui.label(RichText::new(msg).color(Color32::WHITE));
                    });
                });
            } else {
                self.toast = None;
            }
        }
    }

    fn on_exit(&mut self) {
        if self.engine.state() != EngineState::Idle {
            tracing::warn!("window closed while live: cutting");
            let engine = Arc::clone(&self.engine);
            let _ = self.rt.block_on(async move {
                let _ = engine.stop_cut().await;
                tokio::time::timeout(Duration::from_secs(5), engine.wait_idle()).await
            });
        }
    }
}

// ----------------------------------------------------------------- helpers ---

fn draw_frame(ui: &mut egui::Ui, tex: Option<&TextureHandle>, size: egui::Vec2, placeholder: &str) {
    let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
    ui.painter().rect_filled(rect, 4.0, Color32::from_rgb(12, 12, 14));
    match tex {
        Some(t) => {
            let ts = t.size_vec2();
            let scale = (rect.width() / ts.x).min(rect.height() / ts.y);
            let draw = ts * scale;
            let r = egui::Rect::from_center_size(rect.center(), draw);
            ui.painter().image(t.id(), r, egui::Rect::from_min_max(egui::pos2(0.0, 0.0), egui::pos2(1.0, 1.0)), Color32::WHITE);
        }
        None => {
            ui.painter().text(rect.center(), egui::Align2::CENTER_CENTER, placeholder, egui::FontId::proportional(16.0), Color32::DARK_GRAY);
        }
    }
}

fn audio_meter(ui: &mut egui::Ui, rms: &[f64], peak: &[f64]) {
    ui.label("Audio");
    for (i, r) in rms.iter().enumerate() {
        let p = peak.get(i).copied().unwrap_or(*r);
        let frac = |db: f64| ((db + 60.0) / 60.0).clamp(0.0, 1.0) as f32;
        let (rect, _) = ui.allocate_exact_size(egui::vec2(120.0, 10.0), egui::Sense::hover());
        ui.painter().rect_filled(rect, 2.0, Color32::from_gray(30));
        let w = rect.width() * frac(*r);
        let color = if p > -3.0 { Color32::RED } else if p > -12.0 { Color32::YELLOW } else { Color32::from_rgb(60, 200, 60) };
        ui.painter().rect_filled(egui::Rect::from_min_size(rect.min, egui::vec2(w, rect.height())), 2.0, color);
        let px = rect.min.x + rect.width() * frac(p);
        ui.painter().line_segment([egui::pos2(px, rect.min.y), egui::pos2(px, rect.max.y)], (1.5, Color32::WHITE));
    }
    if rms.is_empty() {
        ui.label(RichText::new("—").weak());
    }
}

/// Devices whose class names both words, in any order ("Video/Source",
/// "Source/Video" as Media Foundation reports it, "Audio/Source", ...).
fn devices_of<'a>(devices: &'a [DeviceInfo], kind: &str) -> Vec<&'a DeviceInfo> {
    devices.iter().filter(|d| d.class.contains(kind) && d.class.contains("Source")).collect()
}

fn is_loopback_endpoint(d: &DeviceInfo) -> bool {
    d.properties.iter().any(|(k, v)| k == "wasapi2.device.loopback" && v == "true")
}

/// Returns the chosen device (if the selection changed this frame).
fn device_picker<'a>(ui: &mut egui::Ui, class: &str, devices: &'a [DeviceInfo], value: &mut String, loading: bool) -> Option<&'a DeviceInfo> {
    let list = devices_of(devices, class);
    let mut chosen = None;
    ui.horizontal(|ui| {
        ui.label("Device");
        let selected = list.iter().find(|d| device_id(d).as_deref() == Some(value.as_str())).map(|d| d.display_name.clone()).unwrap_or_else(|| if value.is_empty() { "(default)".into() } else { value.clone() });
        egui::ComboBox::from_id_salt(("dev", class)).selected_text(selected).width(300.0).show_ui(ui, |ui| {
            if loading {
                ui.label("scanning…");
            }
            if ui.selectable_label(value.is_empty(), "(default)").clicked() {
                value.clear();
            }
            for d in &list {
                if let Some(id) = device_id(d) {
                    let mut label = match &d.element {
                        Some(e) => format!("{}  [{e}]", d.display_name),
                        None => d.display_name.clone(),
                    };
                    if is_loopback_endpoint(d) {
                        label.push_str("  (loopback: what it plays)");
                    }
                    if ui.selectable_label(*value == id, label).on_hover_text(d.formats.join("\n")).clicked() {
                        *value = id;
                        chosen = Some(*d);
                    }
                }
            }
        });
    });
    ui.horizontal(|ui| {
        ui.label(RichText::new("or id/path").weak());
        ui.text_edit_singleline(value);
    });
    chosen
}

/// Output devices (class "Audio/Sink") for the monitor.
fn sink_picker(ui: &mut egui::Ui, devices: &[DeviceInfo], value: &mut String, loading: bool) {
    let list: Vec<&DeviceInfo> = devices.iter().filter(|d| d.class.contains("Audio") && d.class.contains("Sink")).collect();
    ui.horizontal(|ui| {
        ui.label("Output");
        let selected = list.iter().find(|d| device_id(d).as_deref() == Some(value.as_str())).map(|d| d.display_name.clone()).unwrap_or_else(|| if value.is_empty() { "(system default)".into() } else { value.clone() });
        egui::ComboBox::from_id_salt("monitor-sink").selected_text(selected).width(300.0).show_ui(ui, |ui| {
            if loading {
                ui.label("scanning…");
            }
            if ui.selectable_label(value.is_empty(), "(system default)").clicked() {
                value.clear();
            }
            for d in &list {
                if let Some(id) = device_id(d)
                    && ui.selectable_label(*value == id, &d.display_name).clicked()
                {
                    *value = id;
                }
            }
        });
    });
}

/// The parts of `Input` whose change requires reopening devices (gain and
/// monitor are applied live and must not restart the preview).
fn input_signature(i: &restream_core::config::Input) -> restream_core::config::Input {
    let mut c = i.clone();
    c.gain_db = 0.0;
    c.monitor = Default::default();
    c
}

/// Best-effort device identifier for the source element's selection property.
fn device_id(d: &DeviceInfo) -> Option<String> {
    // Kernel Streaming: select by name (its provider paths never match the source's).
    if d.element.as_deref() == Some("ksvideosrc") {
        return Some(d.display_name.clone());
    }
    for key in ["device.path", "device.id", "device.strid", "api.v4l2.path", "device.name", "device-number", "object.serial", "node.name"] {
        if let Some((_, v)) = d.properties.iter().find(|(k, _)| k == key) {
            return Some(v.trim_matches('"').to_string());
        }
    }
    None
}

fn format_editor(ui: &mut egui::Ui, f: &mut CaptureFormat) {
    ui.horizontal(|ui| {
        ui.label("Capture format");
        let mut w = f.width.unwrap_or(0);
        let mut h = f.height.unwrap_or(0);
        let mut fps = f.fps.unwrap_or(0);
        ui.add(egui::DragValue::new(&mut w).range(0..=7680).speed(2)).on_hover_text("0 = any");
        ui.label("×");
        ui.add(egui::DragValue::new(&mut h).range(0..=4320).speed(2));
        ui.label("@");
        ui.add(egui::DragValue::new(&mut fps).range(0..=240));
        f.width = (w > 0).then_some(w);
        f.height = (h > 0).then_some(h);
        f.fps = (fps > 0).then_some(fps);
        ui.label("fmt");
        ui.add(egui::TextEdit::singleline(&mut f.format).desired_width(60.0).hint_text("any"));
    });
}

fn video_kinds() -> Vec<&'static str> {
    if cfg!(windows) {
        vec!["Media Foundation (webcam / Web Presenter)", "DirectShow (OBS Virtual Camera)", "Kernel Streaming", "DeckLink", "Network (SRT/RTMP from OBS)", "Test pattern"]
    } else {
        vec!["V4L2", "PipeWire", "DeckLink", "Network (SRT/RTMP from OBS)", "Test pattern"]
    }
}

fn video_kind(v: &VideoSource) -> &'static str {
    match v {
        VideoSource::Test { .. } => "Test pattern",
        VideoSource::V4l2 { .. } => "V4L2",
        VideoSource::PipeWire { .. } => "PipeWire",
        VideoSource::MediaFoundation { .. } => "Media Foundation (webcam / Web Presenter)",
        VideoSource::DirectShow { .. } => "DirectShow (OBS Virtual Camera)",
        VideoSource::KernelStreaming { .. } => "Kernel Streaming",
        VideoSource::DeckLink { .. } => "DeckLink",
        VideoSource::Uri { .. } => "Network (SRT/RTMP from OBS)",
    }
}

fn default_video(kind: &str) -> VideoSource {
    match kind {
        "V4L2" => VideoSource::V4l2 { device: "/dev/video0".into(), format: Default::default() },
        "PipeWire" => VideoSource::PipeWire { target: String::new(), format: Default::default() },
        "Media Foundation (webcam / Web Presenter)" => VideoSource::MediaFoundation { device: String::new(), format: Default::default() },
        "DirectShow (OBS Virtual Camera)" => VideoSource::DirectShow { device: "OBS Virtual Camera".into(), format: Default::default() },
        "Kernel Streaming" => VideoSource::KernelStreaming { device: String::new(), format: Default::default() },
        "DeckLink" => VideoSource::DeckLink { device_number: 0, mode: "auto".into(), connection: "auto".into() },
        "Network (SRT/RTMP from OBS)" => VideoSource::Uri { uri: "srt://:9000?mode=listener".into() },
        _ => VideoSource::Test { pattern: "smpte".into() },
    }
}

fn audio_kinds() -> Vec<&'static str> {
    if cfg!(windows) {
        vec!["WASAPI", "DeckLink", "From video source", "Test tone", "None"]
    } else {
        vec!["PulseAudio / PipeWire", "DeckLink", "From video source", "Test tone", "None"]
    }
}

fn audio_kind(a: &AudioSource) -> &'static str {
    match a {
        AudioSource::Test => "Test tone",
        AudioSource::None => "None",
        AudioSource::FromVideo => "From video source",
        AudioSource::Pulse { .. } => "PulseAudio / PipeWire",
        AudioSource::Wasapi { .. } => "WASAPI",
        AudioSource::DeckLink { .. } => "DeckLink",
    }
}

fn default_audio(kind: &str) -> AudioSource {
    match kind {
        "WASAPI" => AudioSource::Wasapi { device: String::new(), loopback: false },
        "PulseAudio / PipeWire" => AudioSource::Pulse { device: String::new() },
        "DeckLink" => AudioSource::DeckLink { device_number: 0 },
        "From video source" => AudioSource::FromVideo,
        "None" => AudioSource::None,
        _ => AudioSource::Test,
    }
}

fn fmt_dur(d: Duration) -> String {
    let s = d.as_secs();
    if s >= 3600 {
        format!("{}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
    } else {
        format!("{}:{:02}", s / 60, s % 60)
    }
}

fn sanitize(s: &str) -> String {
    s.chars().map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == ' ' { c } else { '_' }).collect()
}

fn list_profiles() -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(paths::profiles_dir())
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter_map(|e| e.path().file_stem().map(|s| s.to_string_lossy().to_string()))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

fn load_last_profile() -> Profile {
    let name = Settings::load().last_profile;
    if !name.is_empty()
        && let Ok(p) = Profile::load(&paths::profiles_dir().join(format!("{name}.toml")))
    {
        return p;
    }
    let mut p = Profile::default();
    p.recording.directory = paths::default_recording_dir();
    p
}

fn save_last_profile_name(name: &str) {
    let mut s = Settings::load();
    s.last_profile = name.to_string();
    s.save();
}
