//! Dynamic recording start/stop on a live pipeline (needs GStreamer + x264enc).
//!
//! Scenario: session with recording enabled, stop the recording mid-session,
//! start a second one, then drain. Expect two playable files whose lengths
//! match the recording windows, and an MP4 remux when requested.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use restream_core::config::{AudioSource, Profile, VideoSource};
use restream_core::{Engine, EngineState};

fn probe_duration(p: &Path) -> f64 {
    let out = Command::new("ffprobe")
        .args(["-v", "error", "-show_entries", "format=duration", "-of", "csv=p=0"])
        .arg(p)
        .output()
        .expect("ffprobe");
    String::from_utf8_lossy(&out.stdout).trim().parse().unwrap_or(0.0)
}

fn test_profile(dir: &Path, container: &str) -> Profile {
    let mut p = Profile::default();
    p.name = "rectest".into();
    p.input.video = VideoSource::Test { pattern: "smpte".into() };
    p.input.audio = AudioSource::Test;
    p.encoder.video_encoder = "x264enc".into();
    p.encoder.width = 640;
    p.encoder.height = 360;
    p.encoder.fps = 30;
    p.encoder.bitrate_kbps = 800;
    p.encoder.keyframe_interval_s = 1;
    p.delay.seconds = 1;
    p.recording.enabled = true;
    p.recording.directory = dir.to_path_buf();
    p.recording.container = container.into();
    p.recording.filename_template = "{profile}-{time}".into();
    p
}

#[tokio::test(flavor = "multi_thread")]
async fn start_stop_restart_recording() {
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-rectest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::new(test_profile(&dir, "mkv"));

    engine.go_live().await.unwrap();
    assert!(engine.is_recording().await);
    tokio::time::sleep(Duration::from_secs(4)).await;

    let first = engine.stop_recording().await.unwrap();
    assert_eq!(first.len(), 1);
    assert!(!engine.is_recording().await);
    tokio::time::sleep(Duration::from_secs(2)).await;

    // second file must have a different name (template uses seconds)
    let second_path = engine.start_recording().await.unwrap();
    assert_ne!(second_path, first[0]);
    tokio::time::sleep(Duration::from_secs(3)).await;

    engine.stop_drain().await.unwrap();
    engine.wait_idle().await;
    assert_eq!(engine.state(), EngineState::Idle);

    let d1 = probe_duration(&first[0]);
    let d2 = probe_duration(&second_path);
    eprintln!("first {d1:.2}s second {d2:.2}s");
    assert!((3.0..5.5).contains(&d1), "first recording {d1}s");
    assert!((2.0..4.5).contains(&d2), "second recording {d2}s");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn mp4_remux_at_session_end() {
    let _ = tracing_subscriber::fmt().with_env_filter("debug").with_test_writer().try_init();
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-rectest-mp4-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::new(test_profile(&dir, "mp4"));
    let mut stats = engine.stats();
    engine.go_live().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    engine.stop_drain().await.unwrap();
    engine.wait_idle().await;
    // the remux runs after Idle; wait for its notice
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        match tokio::time::timeout_at(deadline, stats.recv()).await {
            Ok(Ok(restream_core::StatsEvent::Notice(n))) if n.contains(".mp4") => break,
            Ok(Ok(_)) => {}
            _ => panic!("remux notice not received"),
        }
    }

    let files: Vec<_> = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok().map(|e| e.path())).collect();
    assert_eq!(files.len(), 1, "expected only the mp4, got {files:?}");
    assert_eq!(files[0].extension().unwrap(), "mp4");
    let d = probe_duration(&files[0]);
    assert!((2.5..4.5).contains(&d), "mp4 duration {d}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn live_preview_and_on_air_monitor_produce_frames() {
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-montest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::new(test_profile(&dir, "mkv"));
    engine.set_monitor(true).await;
    engine.go_live().await.unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    let live = engine.preview().take_newer(0).expect("live preview frame");
    assert_eq!((live.width, live.height), (960, 540));
    let mon = engine.monitor_preview().take_newer(0).expect("on-air monitor frame");
    assert_eq!((mon.width, mon.height), (640, 360));
    assert_eq!(mon.rgba.len(), 640 * 360 * 4);
    // Disable mid-session: no new frames after a moment.
    engine.set_monitor(false).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let seq = engine.monitor_preview().latest_seq();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(engine.monitor_preview().latest_seq(), seq, "monitor kept producing after disable");
    engine.stop_drain().await.unwrap();
    engine.wait_idle().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn cut_ends_session_promptly_even_with_long_delay() {
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-cuttest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut p = test_profile(&dir, "mkv");
    p.delay.seconds = 120; // nothing would be released for two minutes
    let engine = Engine::new(p);
    engine.go_live().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let t = std::time::Instant::now();
    engine.stop_cut().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), engine.wait_idle()).await.expect("cut must end the session without waiting for the delay");
    assert!(t.elapsed() < Duration::from_secs(8), "cut took {:?}", t.elapsed());
    assert_eq!(engine.state(), EngineState::Idle);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn live_bitrate_change_reaches_the_encoder() {
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-brtest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut p = test_profile(&dir, "mkv");
    p.input.video = VideoSource::Test { pattern: "snow".into() }; // noisy: bitrate-limited
    p.encoder.bitrate_kbps = 600;
    let engine = Engine::new(p);
    let mut stats = engine.stats();
    engine.go_live().await.unwrap();
    async fn measure(stats: &mut tokio::sync::broadcast::Receiver<restream_core::StatsEvent>) -> f64 {
        // average the next 3 encoder reports
        let mut v = Vec::new();
        while v.len() < 3 {
            if let Ok(restream_core::StatsEvent::Encoder { video_kbps, .. }) = stats.recv().await {
                v.push(video_kbps);
            }
        }
        v.iter().sum::<f64>() / v.len() as f64
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let low = measure(&mut stats).await;
    engine.set_video_bitrate_kbps(4000).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let high = measure(&mut stats).await;
    eprintln!("video kbps before {low:.0} after {high:.0}");
    assert!(high > low * 2.0, "bitrate change not reflected: {low:.0} → {high:.0}");
    engine.stop_cut().await.unwrap();
    engine.wait_idle().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_input_preview_then_go_live() {
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-pvtest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::new(test_profile(&dir, "mkv"));
    engine.start_input_preview().unwrap();
    assert!(engine.input_preview_running());
    tokio::time::sleep(Duration::from_secs(2)).await;
    let f = engine.preview().take_newer(0).expect("preview frame while idle");
    assert_eq!((f.width, f.height), (960, 540));
    // Go live must take the devices over from the preview.
    engine.go_live().await.unwrap();
    assert!(!engine.input_preview_running());
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(engine.preview().latest_seq() > f.seq, "live preview frames after go live");
    engine.stop_cut().await.unwrap();
    engine.wait_idle().await;
    // And the preview can come back afterwards.
    engine.start_input_preview().unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(engine.input_preview_running());
    engine.stop_input_preview();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn encoder_fallback_when_configured_encoder_cannot_start() {
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-fbtest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut p = test_profile(&dir, "mkv");
    p.encoder.video_encoder = "nonexistent-h264-enc".into();
    let engine = Engine::new(p);
    let mut stats = engine.stats();
    engine.go_live().await.expect("falls back to a working encoder");
    let mut warned = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, stats.recv()).await {
            Ok(Ok(restream_core::StatsEvent::Warning(w))) if w.contains("instead") => { warned = true; break; }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(warned, "a warning naming the substitute encoder must be emitted");
    engine.stop_cut().await.unwrap();
    engine.wait_idle().await;
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn mp4_remux_without_audio_does_not_block_idle() {
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-mp4na-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut p = test_profile(&dir, "mp4");
    p.input.audio = AudioSource::None;
    let engine = Engine::new(p);
    let mut stats = engine.stats();
    engine.go_live().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    let t = std::time::Instant::now();
    engine.stop_drain().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), engine.wait_idle()).await.expect("must return to Idle without waiting for the remux");
    assert!(t.elapsed() < Duration::from_secs(8));
    // the remux finishes in the background and announces itself
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut saved = false;
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout_at(deadline, stats.recv()).await {
            Ok(Ok(restream_core::StatsEvent::Notice(n))) if n.contains(".mp4") => { saved = true; break; }
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    assert!(saved, "expected a Notice naming the mp4");
    let files: Vec<_> = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok().map(|e| e.path())).collect();
    assert_eq!(files.len(), 1, "{files:?}");
    assert_eq!(files[0].extension().unwrap(), "mp4");
    assert!((2.5..4.5).contains(&probe_duration(&files[0])));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread")]
async fn gain_changes_meter_and_monitor_attaches_live() {
    restream_core::init().unwrap();
    let dir = std::env::temp_dir().join(format!("restream-gaintest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let engine = Engine::new(test_profile(&dir, "mkv"));
    let mut stats = engine.stats();
    async fn rms(stats: &mut tokio::sync::broadcast::Receiver<restream_core::StatsEvent>) -> f64 {
        // drop queued (stale) events, then average the next 5 fresh meter readings
        while stats.try_recv().is_ok() {}
        let mut v = Vec::new();
        while v.len() < 5 {
            if let Ok(restream_core::StatsEvent::AudioLevel { rms_db, .. }) = stats.recv().await
                && let Some(x) = rms_db.first()
            {
                v.push(*x);
            }
        }
        v.iter().sum::<f64>() / v.len() as f64
    }
    // idle preview first: gain and monitor must work there too
    engine.start_input_preview().unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let base = rms(&mut stats).await;
    engine.set_audio_gain_db(-20.0);
    tokio::time::sleep(Duration::from_millis(800)).await;
    let quiet = rms(&mut stats).await;
    eprintln!("preview rms: 0 dB → {base:.1}, -20 dB → {quiet:.1}");
    assert!((base - quiet - 20.0).abs() < 2.0, "gain not reflected in meter: {base:.1} vs {quiet:.1}");
    engine.set_audio_monitor(true, "fake".into(), -6.0);
    tokio::time::sleep(Duration::from_millis(500)).await;
    engine.set_audio_monitor_volume_db(-12.0);
    // go live: monitor moves to the session pipeline, gain persists
    engine.go_live().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let live = rms(&mut stats).await;
    assert!((live - quiet).abs() < 2.0, "gain lost across go-live: {live:.1} vs {quiet:.1}");
    engine.set_audio_monitor(false, "fake".into(), -12.0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    engine.set_audio_monitor(true, "fake".into(), -12.0);
    tokio::time::sleep(Duration::from_millis(300)).await;
    engine.stop_cut().await.unwrap();
    engine.wait_idle().await;
    let _ = std::fs::remove_dir_all(&dir);
}
