//! Where the app keeps its files.
//!
//! Windows: `%APPDATA%\Restream\{profiles,settings.toml}` and
//!          `%LOCALAPPDATA%\Restream\logs`.
//! Linux:   `~/.config/restream` and `~/.local/state/restream/logs`.

use std::path::PathBuf;

pub fn config_dir() -> PathBuf {
    dirs::config_dir().unwrap_or_else(|| PathBuf::from(".")).join(app_dir_name())
}

pub fn profiles_dir() -> PathBuf {
    config_dir().join("profiles")
}

pub fn settings_file() -> PathBuf {
    config_dir().join("settings.toml")
}

pub fn logs_dir() -> PathBuf {
    let base = if cfg!(windows) { dirs::data_local_dir() } else { dirs::state_dir().or_else(dirs::data_local_dir) };
    base.unwrap_or_else(|| PathBuf::from(".")).join(app_dir_name()).join("logs")
}

pub fn default_recording_dir() -> PathBuf {
    dirs::video_dir().or_else(dirs::home_dir).unwrap_or_else(|| PathBuf::from(".")).join("Restream")
}

fn app_dir_name() -> &'static str {
    if cfg!(windows) { "Restream" } else { "restream" }
}
