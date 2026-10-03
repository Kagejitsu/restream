//! Restream desktop application.
#![cfg_attr(all(windows, not(debug_assertions)), windows_subsystem = "windows")]

mod app;
mod keys;
mod logbuf;
mod paths;
mod settings;

use std::sync::Arc;

use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

fn main() -> anyhow::Result<()> {
    // ---- logging: rolling file + in-app ring buffer (+ stderr in debug) ----
    let logs = logbuf::LogBuffer::new();
    std::fs::create_dir_all(paths::logs_dir()).ok();
    let settings = settings::Settings::load();
    if settings.debug_logging {
        // GStreamer's own debug log: the only place device-open failures explain themselves.
        let gst_log = paths::logs_dir().join("gstreamer.log");
        // SAFETY: before any thread exists.
        unsafe {
            if std::env::var_os("GST_DEBUG").is_none() {
                std::env::set_var("GST_DEBUG", "2,mf*:6,ks*:6,wasapi*:6,dshow*:6,decklink*:5,amf*:4,nv*:4,qsv*:4,GST_ELEMENT_FACTORY:4,default:3");
            }
            if std::env::var_os("GST_DEBUG_FILE").is_none() {
                std::env::set_var("GST_DEBUG_FILE", &gst_log);
            }
            std::env::set_var("GST_DEBUG_NO_COLOR", "1");
        }
    }
    let file = tracing_appender::rolling::daily(paths::logs_dir(), "restream.log");
    let (file_nb, _file_guard) = tracing_appender::non_blocking(file);
    let default_filter = if settings.debug_logging {
        "debug,rml_rtmp=info,wgpu_core=warn,wgpu_hal=warn,naga=warn,egui_wgpu=info,eframe=info,winit=info,sctk=info,calloop=info"
    } else {
        "info,rml_rtmp=warn"
    };
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
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

    // Self-contained Windows bundle: plugins live next to the exe.
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
    tracing::info!("Restream {} starting; debug logging {}", env!("CARGO_PKG_VERSION"), if settings.debug_logging { "ON (gstreamer.log beside restream.log)" } else { "off" });
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
