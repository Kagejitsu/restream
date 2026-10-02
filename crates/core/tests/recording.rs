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
    engine.go_live().await.unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    engine.stop_drain().await.unwrap();
    engine.wait_idle().await;

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
