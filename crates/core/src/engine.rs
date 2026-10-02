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
use std::sync::atomic::AtomicBool;
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
use crate::pipeline::Pipeline;
use crate::preview::PreviewSlot;
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
}

pub struct Engine {
    profile: Mutex<Profile>,
    state: watch::Sender<EngineState>,
    stats: StatsBus,
    preview: Arc<PreviewSlot>,
    session: tokio::sync::Mutex<Option<Session>>,
}

impl Engine {
    pub fn new(profile: Profile) -> Arc<Self> {
        let (state, _) = watch::channel(EngineState::Idle);
        Arc::new(Self {
            profile: Mutex::new(profile),
            state,
            stats: StatsBus::new(),
            preview: PreviewSlot::new(),
            session: tokio::sync::Mutex::new(None),
        })
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
        *self.state.borrow()
    }

    pub fn watch_state(&self) -> watch::Receiver<EngineState> {
        self.state.subscribe()
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

    /// Start capture, encoding, recording and the delay buffer. Destinations
    /// connect immediately and wait for the first released keyframe.
    pub async fn go_live(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.session.lock().await;
        if guard.is_some() {
            bail!("already running");
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

        // ---- recording path ----
        let record_path = if profile.recording.enabled { Some(recording_path(&profile)?) } else { None };

        // ---- pipeline ----
        let (pipeline, outputs) = Pipeline::build(&profile, Arc::clone(&self.preview), self.stats.clone(), record_path.as_deref())?;

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
                                let _ = me.state.send(EngineState::OnAir);
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

        pipeline.start()?;
        if let Some(p) = &record_path {
            self.stats.emit(StatsEvent::Recording { active: true, path: Some(p.display().to_string()), bytes: 0, duration: Duration::ZERO });
        }
        *guard = Some(Session { pipeline, releaser, dest_tasks, cmd_tx, stop_tx, monitor });
        let _ = self.state.send(EngineState::Filling);
        info!(target: "engine", "session started: delay {}s", profile.delay.seconds);
        Ok(())
    }

    /// Stop the input now and let the buffered stream play out, then disconnect.
    pub async fn stop_drain(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.session.lock().await;
        let Some(session) = guard.as_mut() else { return Ok(()) };
        if self.state() == EngineState::Draining {
            return Ok(());
        }
        let _ = self.state.send(EngineState::Draining);
        info!(target: "engine", "stop (drain): sending EOS");
        session.pipeline.send_eos();
        // The pipeline posts EOS → releaser drains → destinations finish.
        // Give the muxer a bounded time to finalise before tearing down.
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
        session.pipeline.send_eos();
        let _ = session.stop_tx.send(true);
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
            // Pipeline teardown blocks: do it off the async runtime.
            let _ = tokio::task::spawn_blocking(move || s.pipeline.stop()).await;
            s.monitor.abort();
        }
        self.preview.clear();
        self.stats.emit(StatsEvent::Recording { active: false, path: None, bytes: 0, duration: Duration::ZERO });
        let _ = self.state.send(EngineState::Idle);
        info!(target: "engine", "session finished");
    }

    /// Block until the session returns to Idle.
    pub async fn wait_idle(&self) {
        let mut rx = self.state.subscribe();
        while *rx.borrow() != EngineState::Idle {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
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
