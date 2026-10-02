//! Shared launcher logic: run `bin\<name>` next to this launcher with `bin\`
//! prepended to PATH so the GStreamer runtime DLLs resolve from there.

use std::path::PathBuf;
use std::process::{Command, ExitCode};

pub fn target(name: &str) -> PathBuf {
    let dir = std::env::current_exe().ok().and_then(|p| p.parent().map(|d| d.to_path_buf())).unwrap_or_else(|| PathBuf::from("."));
    dir.join("bin").join(name)
}

pub fn command(name: &str) -> Command {
    let exe = target(name);
    let bin = exe.parent().map(|d| d.to_path_buf()).unwrap_or_default();
    let mut path = std::ffi::OsString::from(bin.as_os_str());
    if let Some(p) = std::env::var_os("PATH") {
        path.push(if cfg!(windows) { ";" } else { ":" });
        path.push(p);
    }
    let mut c = Command::new(exe);
    c.env("PATH", path).args(std::env::args_os().skip(1));
    c
}

/// Run and wait, propagating the exit code (CLI).
pub fn run_wait(name: &str) -> ExitCode {
    match command(name).status() {
        Ok(s) => ExitCode::from(s.code().unwrap_or(1).clamp(0, 255) as u8),
        Err(e) => {
            eprintln!("cannot start {}: {e}", target(name).display());
            ExitCode::from(2)
        }
    }
}
