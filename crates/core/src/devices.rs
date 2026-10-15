//! Capture-device enumeration through `GstDeviceMonitor`.
//!
//! On Windows this lists Media Foundation cameras (`mfvideosrc`), WASAPI
//! endpoints (`wasapi2src`), DeckLink inputs and, when the DirectShow plugin
//! provides a device provider, DirectShow cameras. OBS Virtual Camera is a
//! DirectShow-only filter; if the provider does not list it, the GUI offers a
//! manual "DirectShow by name" entry.

use gstreamer as gst;
use gstreamer::prelude::*;

#[derive(Debug, Clone)]
pub struct DeviceInfo {
    pub display_name: String,
    /// "Video/Source" or "Audio/Source"
    pub class: String,
    /// GStreamer element factory this device is captured with (mfvideosrc, v4l2src, ...).
    pub element: Option<String>,
    /// Properties to set on that element to select this device.
    pub properties: Vec<(String, String)>,
    /// Human-readable formats this device advertises.
    pub formats: Vec<String>,
}

/// Enumerate video and audio capture devices and audio output devices.
pub fn enumerate() -> Vec<DeviceInfo> {
    let monitor = gst::DeviceMonitor::new();
    monitor.add_filter(Some("Video/Source"), None);
    monitor.add_filter(Some("Audio/Source"), None);
    monitor.add_filter(Some("Audio/Sink"), None);
    if let Err(e) = monitor.start() {
        tracing::warn!("device monitor failed to start: {e}");
        return Vec::new();
    }
    let devices = monitor.devices();
    monitor.stop();

    devices
        .iter()
        .map(|d| {
            let props = d
                .properties()
                .map(|s| {
                    s.iter()
                        .filter_map(|(k, v)| {
                            // Strings must stay raw: serialising would add GStreamer's
                            // own escapes (\#, \&, \{ ...) and the id would never match.
                            let text = match v.get::<String>() {
                                Ok(t) => t,
                                Err(_) => v.serialize().ok()?.to_string(),
                            };
                            Some((k.to_string(), text))
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let element = d.create_element(None).ok().and_then(|e| e.factory().map(|f| f.name().to_string()));
            let formats = d
                .caps()
                .map(|caps| caps.iter().map(summarize_structure).collect::<Vec<_>>())
                .unwrap_or_default();
            DeviceInfo {
                display_name: d.display_name().to_string(),
                class: d.device_class().to_string(),
                element,
                properties: props,
                formats: dedup(formats),
            }
        })
        .collect()
}

fn summarize_structure(s: &gst::StructureRef) -> String {
    let name = s.name();
    let fmt = s.get::<String>("format").ok();
    let w = s.get::<i32>("width").ok();
    let h = s.get::<i32>("height").ok();
    let fr = s.get::<gst::Fraction>("framerate").ok().map(|f| format!("{:.0}", f.numer() as f64 / f.denom().max(1) as f64));
    let rate = s.get::<i32>("rate").ok();
    let ch = s.get::<i32>("channels").ok();
    let mut out = String::new();
    out.push_str(name.as_str());
    if let Some(f) = fmt {
        out.push(' ');
        out.push_str(&f);
    }
    if let (Some(w), Some(h)) = (w, h) {
        out.push_str(&format!(" {w}x{h}"));
    }
    if let Some(fr) = fr {
        out.push_str(&format!("@{fr}"));
    }
    if let Some(r) = rate {
        out.push_str(&format!(" {r} Hz"));
    }
    if let Some(c) = ch {
        out.push_str(&format!(" {c}ch"));
    }
    out
}

fn dedup(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v.dedup();
    v
}
