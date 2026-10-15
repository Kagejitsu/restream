//! Profile = everything needed to run a session, serialised as TOML.
//!
//! Stream keys are **not** stored in the profile on disk by the GUI (it keeps
//! them in the OS credential store and fills `key` in memory). The CLI accepts
//! plaintext keys or `${ENV_VAR}` references for scripted use.

use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Profile {
    pub name: String,
    pub input: Input,
    pub encoder: Encoder,
    pub delay: Delay,
    pub destinations: Vec<Destination>,
    pub recording: Recording,
}

impl Default for Profile {
    fn default() -> Self {
        Self {
            name: "Default".into(),
            input: Input::default(),
            encoder: Encoder::default(),
            delay: Delay::default(),
            destinations: Vec::new(),
            recording: Recording::default(),
        }
    }
}

impl Profile {
    pub fn from_toml(s: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(s)?)
    }
    pub fn to_toml(&self) -> anyhow::Result<String> {
        Ok(toml::to_string_pretty(self)?)
    }
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        Self::from_toml(&std::fs::read_to_string(path)?)
    }
    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, self.to_toml()?)?;
        Ok(())
    }
    /// Resolve `${ENV}` key references (CLI convenience).
    pub fn resolve_env_keys(&mut self) {
        for d in &mut self.destinations {
            if let Some(var) = d.key.strip_prefix("${").and_then(|s| s.strip_suffix('}'))
                && let Ok(v) = std::env::var(var) {
                    d.key = v;
                }
        }
    }
    /// A copy with every stream key blanked, safe to write to disk or logs.
    pub fn without_keys(&self) -> Self {
        let mut p = self.clone();
        for d in &mut p.destinations {
            d.key.clear();
        }
        p
    }
}

// ---------------------------------------------------------------- input ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct Input {
    pub video: VideoSource,
    pub audio: AudioSource,
    /// Shift audio relative to video, positive = audio later. Tunable live.
    pub audio_offset_ms: i64,
    /// Burn a running-time clock into the picture (sync testing / rehearsal).
    pub burn_in_clock: bool,
    /// Program audio gain in dB, applied before the encoder (so streams,
    /// recording and meters all reflect it). Tunable live.
    pub gain_db: f32,
    /// Play the program audio to a local output device.
    pub monitor: AudioMonitor,
}

/// Local audio monitoring of the program (post-gain, undelayed).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct AudioMonitor {
    pub enabled: bool,
    /// Output device id ("" = system default; "fake" = discard, for tests).
    pub device: String,
    /// Monitor volume in dB (does not affect the stream).
    pub volume_db: f32,
}

impl Default for AudioMonitor {
    fn default() -> Self {
        Self { enabled: false, device: String::new(), volume_db: 0.0 }
    }
}

impl Default for Input {
    fn default() -> Self {
        Self {
            video: VideoSource::Test { pattern: "smpte".into() },
            audio: AudioSource::Test,
            audio_offset_ms: 0,
            gain_db: 0.0,
            monitor: AudioMonitor::default(),
            burn_in_clock: false,
        }
    }
}

/// Optional capture format request; `None` lets the device choose.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct CaptureFormat {
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub fps: Option<u32>,
    /// e.g. "NV12", "YUY2", "MJPG" — leave empty for any.
    pub format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum VideoSource {
    /// `videotestsrc` — development and rehearsal.
    Test { pattern: String },
    /// Linux V4L2 (`/dev/video0`).
    V4l2 { device: String, #[serde(default)] format: CaptureFormat },
    /// Linux PipeWire camera/screen node (`target` = node name/serial, "" = default).
    PipeWire { target: String, #[serde(default)] format: CaptureFormat },
    /// Windows Media Foundation (UVC webcams, Blackmagic Web Presenter).
    /// `device` is the MF symbolic link / device path.
    MediaFoundation { device: String, #[serde(default)] format: CaptureFormat },
    /// Windows DirectShow (OBS Virtual Camera and other virtual cameras).
    /// `device` is the friendly device name.
    DirectShow { device: String, #[serde(default)] format: CaptureFormat },
    /// Windows Kernel Streaming (`ksvideosrc`).
    KernelStreaming { device: String, #[serde(default)] format: CaptureFormat },
    /// Blackmagic DeckLink (also provides audio when `AudioSource::DeckLink`).
    DeckLink {
        device_number: u32,
        /// "auto" or a mode name like "1080p5994" (see `gst-inspect-1.0 decklinkvideosrc`).
        mode: String,
        /// "auto", "sdi", "hdmi", ...
        connection: String,
    },
    /// Any GStreamer URI that decodes to A+V: `srt://:9000?mode=listener`
    /// (OBS → SRT output), `rtmp://127.0.0.1:1935/live/obs`, a file, ...
    /// Audio is taken from the same stream when `AudioSource::FromVideo`.
    Uri { uri: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum AudioSource {
    Test,
    /// No audio track at all (silence is NOT inserted; outputs are video-only).
    None,
    /// Audio embedded in the `VideoSource::Uri` or `DeckLink` stream.
    FromVideo,
    /// Linux PipeWire/PulseAudio default or named device.
    Pulse { device: String },
    /// Windows WASAPI. `loopback = true` captures what the device *plays*.
    Wasapi { device: String, loopback: bool },
    DeckLink { device_number: u32 },
}

// -------------------------------------------------------------- encoder ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Encoder {
    /// "auto" or a GStreamer element name (x264enc, nvh264enc, amfh264enc,
    /// qsvh264enc, vah264enc, mfh264enc, openh264enc).
    pub video_encoder: String,
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    pub bitrate_kbps: u32,
    /// Keyframe interval in seconds (platforms want 2).
    pub keyframe_interval_s: u32,
    /// x264 speed preset or HW "preset"/"quality" name; "" = encoder default.
    pub preset: String,
    pub bframes: u32,
    /// "high" | "main" | "baseline"
    pub profile: String,
    /// "auto" or fdkaacenc / avenc_aac / mfaacenc / voaacenc
    pub audio_encoder: String,
    pub audio_bitrate_kbps: u32,
    pub audio_sample_rate: u32,
    pub audio_channels: u32,
}

impl Default for Encoder {
    fn default() -> Self {
        Self {
            video_encoder: "auto".into(),
            width: 1920,
            height: 1080,
            fps: 60,
            bitrate_kbps: 6000,
            keyframe_interval_s: 2,
            preset: "".into(),
            bframes: 2,
            profile: "high".into(),
            audio_encoder: "auto".into(),
            audio_bitrate_kbps: 160,
            audio_sample_rate: 48000,
            audio_channels: 2,
        }
    }
}

impl Encoder {
    pub fn keyframe_interval_frames(&self) -> u32 {
        (self.keyframe_interval_s * self.fps).max(1)
    }
}

// ---------------------------------------------------------------- delay ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Delay {
    pub seconds: u32,
    /// Hard cap on RAM used by the delay buffer.
    pub max_megabytes: u32,
    /// A destination more than this far behind the release cursor is
    /// resynchronised at the next keyframe.
    pub max_lag_ms: u32,
}

impl Default for Delay {
    fn default() -> Self {
        Self { seconds: 180, max_megabytes: 1024, max_lag_ms: 2000 }
    }
}

impl Delay {
    pub fn duration(&self) -> Duration {
        Duration::from_secs(self.seconds as u64)
    }
    pub fn max_bytes(&self) -> usize {
        self.max_megabytes as usize * 1024 * 1024
    }
}

// ---------------------------------------------------------- destinations ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Destination {
    pub name: String,
    /// Server URL without the key, e.g. `rtmp://live.twitch.tv/app`.
    pub url: String,
    /// Stream key. Blank on disk when managed by the GUI's credential store.
    pub key: String,
    pub enabled: bool,
    /// Accept self-signed certificates (local test servers only).
    pub tls_insecure: bool,
}

impl Default for Destination {
    fn default() -> Self {
        Self { name: "New destination".into(), url: String::new(), key: String::new(), enabled: true, tls_insecure: false }
    }
}

// ------------------------------------------------------------- recording ---

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct Recording {
    pub enabled: bool,
    pub directory: PathBuf,
    /// `{date}`, `{time}`, `{profile}` placeholders.
    pub filename_template: String,
    /// "mkv" (crash-safe) or "mp4" (remuxed from MKV on stop).
    pub container: String,
    /// 0 = single file.
    pub split_minutes: u32,
}

impl Default for Recording {
    fn default() -> Self {
        Self {
            enabled: false,
            directory: PathBuf::from("recordings"),
            filename_template: "{profile}-{date}-{time}".into(),
            container: "mkv".into(),
            split_minutes: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_toml() {
        let mut p = Profile::default();
        p.destinations.push(Destination { name: "Twitch".into(), url: "rtmp://live.twitch.tv/app".into(), key: "k".into(), ..Default::default() });
        p.input.video = VideoSource::DeckLink { device_number: 0, mode: "auto".into(), connection: "sdi".into() };
        let s = p.to_toml().unwrap();
        let back = Profile::from_toml(&s).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn env_keys_resolve_and_without_keys_blanks() {
        unsafe { std::env::set_var("RS_TEST_KEY", "secret") };
        let mut p = Profile::default();
        p.destinations.push(Destination { key: "${RS_TEST_KEY}".into(), ..Default::default() });
        p.resolve_env_keys();
        assert_eq!(p.destinations[0].key, "secret");
        assert_eq!(p.without_keys().destinations[0].key, "");
    }
}
