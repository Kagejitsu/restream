//! restream-cli: run a profile headless.
//!
//! ```text
//! restream-cli run profile.toml [--delay SECONDS] [--for SECONDS] [--record DIR]
//! restream-cli devices
//! restream-cli encoders
//! restream-cli example > profile.toml
//! ```
//! Stream keys in the profile may be `${ENV_VAR}` references.
//! Ctrl-C once = stop and drain; twice = cut.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use restream_core::stats::DestStatus;
use restream_core::{Engine, EngineState, Profile, StatsEvent};
use tracing_subscriber::EnvFilter;

fn usage() -> ! {
    eprintln!(
        "usage:\n  restream-cli run <profile.toml> [--delay S] [--for S] [--record DIR] [--no-record] [--quiet]\n  restream-cli devices\n  restream-cli encoders\n  restream-cli example"
    );
    std::process::exit(2)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with_target(true)
        .init();
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent() {
            let plugins = dir.join("gst-plugins");
            if plugins.is_dir() && std::env::var_os("GST_PLUGIN_PATH").is_none() {
                // SAFETY: set before any other thread reads the environment.
                unsafe {
                    std::env::set_var("GST_PLUGIN_PATH", &plugins);
                    std::env::set_var("GST_PLUGIN_SYSTEM_PATH", &plugins);
                    // Out-of-process plugin scanner: a crashing plugin then
                    // only kills the helper, and the registry scan is faster.
                    let scanner = dir.join("gst-plugin-scanner.exe");
                    if scanner.is_file() {
                        std::env::set_var("GST_PLUGIN_SCANNER", &scanner);
                    }
                }
            }
        }
    restream_core::init()?;

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("devices") => {
            for d in restream_core::devices::enumerate() {
                println!("[{}] {}", d.class, d.display_name);
                if let Some(e) = &d.element {
                    println!("    element: {e}");
                }
                for (k, v) in &d.properties {
                    println!("    {k} = {v}");
                }
                for f in d.formats.iter().take(12) {
                    println!("    fmt: {f}");
                }
            }
            Ok(())
        }
        Some("encoders") => {
            println!("video: {:?}", restream_core::pipeline::available_video_encoders());
            println!("audio: {:?}", restream_core::pipeline::available_audio_encoders());
            Ok(())
        }
        Some("example") => {
            let mut p = Profile::default();
            p.name = "example".into();
            p.input.burn_in_clock = true;
            p.delay.seconds = 10;
            p.destinations.push(restream_core::config::Destination {
                name: "Local test".into(),
                url: "rtmp://127.0.0.1:1935/live".into(),
                key: "${RESTREAM_KEY_LOCAL}".into(),
                ..Default::default()
            });
            print!("{}", p.to_toml()?);
            Ok(())
        }
        Some("run") => run(&args[1..]).await,
        _ => usage(),
    }
}

async fn run(args: &[String]) -> Result<()> {
    let Some(path) = args.first() else { usage() };
    let mut profile = Profile::load(&PathBuf::from(path)).with_context(|| format!("loading {path}"))?;
    profile.resolve_env_keys();
    let mut run_for: Option<Duration> = None;
    let mut quiet = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--delay" => {
                profile.delay.seconds = args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or_else(|| usage());
                i += 1;
            }
            "--for" => {
                run_for = Some(Duration::from_secs(args.get(i + 1).and_then(|s| s.parse().ok()).unwrap_or_else(|| usage())));
                i += 1;
            }
            "--record" => {
                profile.recording.enabled = true;
                profile.recording.directory = PathBuf::from(args.get(i + 1).unwrap_or_else(|| usage()));
                i += 1;
            }
            "--no-record" => profile.recording.enabled = false,
            "--quiet" => quiet = true,
            other => bail!("unknown argument {other}"),
        }
        i += 1;
    }

    let engine = Engine::new(profile.clone());
    let mut stats = engine.stats();
    let mut state_rx = engine.watch_state();
    engine.go_live().await?;

    // Live status line (per destination), refreshed every second.
    let printer = {
        let quiet = quiet;
        let n = profile.destinations.len();
        tokio::spawn(async move {
            let mut dests: Vec<String> = vec![String::new(); n];
            let mut delay_line = String::new();
            let mut enc_line = String::new();
            let mut rec_line = String::new();
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    ev = stats.recv() => match ev {
                        Ok(StatsEvent::Destination { index, name, status, bitrate_kbps, backlog, reconnects, dropped_packets, last_error, .. }) => {
                            let err = last_error.map(|e| format!("  [{e}]")).unwrap_or_default();
                            let s = format!("{name:<14} {:<16} {bitrate_kbps:7.0} kbps  backlog {backlog:3}  reconnects {reconnects}  dropped {dropped_packets}{err}", status.label());
                            if index < dests.len() { dests[index] = s; }
                            if matches!(status, DestStatus::Lagging | DestStatus::Backoff) && !quiet { eprintln!("  ! {name}: {}", status.label()); }
                        }
                        Ok(StatsEvent::Delay { fill, bytes, packets, on_air, on_air_in, overflow_drops }) => {
                            let oa = if on_air { "  ON AIR".to_string() } else { on_air_in.map(|d| format!("  on air in {:>3}s", d.as_secs())).unwrap_or_default() };
                            delay_line = format!("delay  buffered {:>5.1}s  {:>7.1} MB  {packets:>6} pkts{oa}{}", fill.as_secs_f64(), bytes as f64 / 1e6, if overflow_drops > 0 { format!("  OVERFLOW drops {overflow_drops}") } else { String::new() });
                        }
                        Ok(StatsEvent::Encoder { video_fps, video_kbps, audio_kbps, .. }) => {
                            enc_line = format!("encode {video_fps:5.1} fps  video {video_kbps:6.0} kbps  audio {audio_kbps:4.0} kbps");
                        }
                        Ok(StatsEvent::Recording { active, path, .. }) => {
                            rec_line = if active { format!("record → {}", path.unwrap_or_default()) } else { String::new() };
                        }
                        Ok(StatsEvent::AudioLevel { .. }) => {}
                        Ok(StatsEvent::Error(e)) => eprintln!("ERROR: {e}"),
                        Ok(StatsEvent::Warning(w)) => if !quiet { eprintln!("warning: {w}") },
                        Ok(StatsEvent::Notice(n)) => eprintln!("{n}"),
                        Ok(StatsEvent::Session { video_encoder, video_encoder_desc, audio_encoder, width, height, fps, bitrate_kbps, audio_kbps, delay_s, fallback, .. }) => {
                            eprintln!("session: {width}x{height}@{fps} {bitrate_kbps} kbps via {video_encoder} ({video_encoder_desc}){}; audio {:?} {audio_kbps} kbps; delay {delay_s}s", if fallback { " [FALLBACK]" } else { "" }, audio_encoder);
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(_) => break,
                    },
                    _ = tick.tick() => {
                        if quiet { continue; }
                        eprintln!("--- {}", delay_line);
                        if !enc_line.is_empty() { eprintln!("    {enc_line}"); }
                        if !rec_line.is_empty() { eprintln!("    {rec_line}"); }
                        for d in dests.iter().filter(|d| !d.is_empty()) { eprintln!("    {d}"); }
                    }
                }
            }
        })
    };

    // Ctrl-C handling: first = drain, second = cut. `--for` = timed drain.
    let engine2 = Arc::clone(&engine);
    tokio::spawn(async move {
        let mut presses = 0;
        loop {
            if tokio::signal::ctrl_c().await.is_err() {
                return;
            }
            presses += 1;
            if presses == 1 {
                eprintln!("\nCtrl-C: stopping input, draining buffer (Ctrl-C again to CUT)");
                let _ = engine2.stop_drain().await;
            } else {
                eprintln!("\nCtrl-C again: CUT");
                let _ = engine2.stop_cut().await;
            }
        }
    });
    if let Some(d) = run_for {
        let engine3 = Arc::clone(&engine);
        tokio::spawn(async move {
            tokio::time::sleep(d).await;
            eprintln!("\n--for elapsed: stopping input, draining");
            let _ = engine3.stop_drain().await;
        });
    }

    // Wait for the session to end.
    loop {
        if *state_rx.borrow() == EngineState::Idle {
            break;
        }
        if state_rx.changed().await.is_err() {
            break;
        }
        if !quiet {
            eprintln!("state: {:?}", *state_rx.borrow());
        }
    }
    printer.abort();
    Ok(())
}
