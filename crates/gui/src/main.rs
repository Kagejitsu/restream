//! Restream desktop application.
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod app;
mod keys;
mod logbuf;
mod paths;

use std::sync::Arc;

use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // ---- logging: rolling file + in-app ring buffer (+ stderr in debug) ----
    let logs = logbuf::LogBuffer::new();
    std::fs::create_dir_all(paths::logs_dir()).ok();
    let file = tracing_appender::rolling::daily(paths::logs_dir(), "restream.log");
    let (file_nb, _file_guard) = tracing_appender::non_blocking(file);
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,rml_rtmp=warn"));
    let registry = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(file_nb))
        .with(tracing_subscriber::fmt::layer().with_ansi(false).without_time().with_target(true).with_writer(logs.clone()));
    #[cfg(debug_assertions)]
    let registry = registry.with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr));
    registry.init();

    // Panics anywhere must reach the log file.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        tracing::error!("PANIC: {info}");
        default_hook(info);
    }));

    restream_core::init()?;
    let rt = Arc::new(tokio::runtime::Builder::new_multi_thread().enable_all().worker_threads(4).build()?);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1360.0, 860.0])
            .with_min_inner_size([980.0, 640.0])
            .with_title("Restream"),
        ..Default::default()
    };
    let rt2 = Arc::clone(&rt);
    eframe::run_native("Restream", options, Box::new(move |cc| Ok(Box::new(app::App::new(cc, rt2, logs)))))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
