//! Small persisted app settings (not part of a profile): last profile name,
//! debug logging switch. Stored as TOML in the config dir.

use serde::{Deserialize, Serialize};

use crate::paths;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub last_profile: String,
    /// Verbose app log + GStreamer's own debug log (gstreamer.log). Takes
    /// effect at the next launch. On by default while the Windows device
    /// layer is being validated.
    pub debug_logging: bool,
    /// Encoder that last worked when the profile said "auto"; tried first.
    pub preferred_video_encoder: String,
}

impl Default for Settings {
    fn default() -> Self {
        Self { last_profile: String::new(), debug_logging: false, preferred_video_encoder: String::new() }
    }
}

impl Settings {
    pub fn load() -> Self {
        std::fs::read_to_string(paths::settings_file()).ok().and_then(|s| toml::from_str(&s).ok()).unwrap_or_default()
    }
    pub fn save(&self) {
        let _ = std::fs::create_dir_all(paths::config_dir());
        if let Ok(s) = toml::to_string_pretty(self) {
            let _ = std::fs::write(paths::settings_file(), s);
        }
    }
}
