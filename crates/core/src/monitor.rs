//! On-Air monitor: decodes the *delayed* output so the operator can see
//! exactly what viewers see, next to the live preview.
//!
//! It is a tap on the releaser: it receives the same packets destinations
//! receive, re-serialises them as an FLV byte stream and pushes that into a
//! small decode pipeline (`appsrc ! flvdemux ! decodebin ! RGBA appsink`).
//! Costs one video decode while enabled; nothing when off.

use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, BytesMut};
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::fanout::Outbound;
use crate::packet::{CodecConfig, PacketKind, TAG_AUDIO, TAG_VIDEO};
use crate::preview::PreviewSlot;

pub const MONITOR_WIDTH: u32 = 640;
pub const MONITOR_HEIGHT: u32 = 360;

/// Serialise one FLV tag (header + body + PreviousTagSize).
pub fn flv_tag(tag_type: u8, ts_ms: u32, body: &[u8]) -> BytesMut {
    let mut b = BytesMut::with_capacity(15 + body.len());
    b.put_u8(tag_type);
    let size = body.len() as u32;
    b.put_u8((size >> 16) as u8);
    b.put_u8((size >> 8) as u8);
    b.put_u8(size as u8);
    b.put_u8((ts_ms >> 16) as u8);
    b.put_u8((ts_ms >> 8) as u8);
    b.put_u8(ts_ms as u8);
    b.put_u8((ts_ms >> 24) as u8);
    b.put_slice(&[0, 0, 0]);
    b.put_slice(body);
    b.put_u32(11 + size);
    b
}

/// FLV file header (audio+video flags) + PreviousTagSize0.
pub fn flv_header(has_audio: bool) -> [u8; 13] {
    let flags = if has_audio { 0x05 } else { 0x01 };
    [b'F', b'L', b'V', 1, flags, 0, 0, 0, 9, 0, 0, 0, 0]
}

struct Decoder {
    pipeline: gst::Pipeline,
    src: gst_app::AppSrc,
}

impl Decoder {
    fn new(slot: Arc<PreviewSlot>, has_audio: bool) -> Result<Self> {
        let audio = if has_audio { " d.audio ! queue ! fakesink sync=false" } else { "" };
        let desc = format!(
            "appsrc name=src is-live=true format=bytes stream-type=stream ! queue ! flvdemux name=d \
             d.video ! queue ! decodebin ! videoconvert ! videoscale ! video/x-raw,format=RGBA,width={w},height={h} \
             ! appsink name=sink max-buffers=1 drop=true sync=false{audio}",
            w = MONITOR_WIDTH,
            h = MONITOR_HEIGHT,
        );
        let pipeline = gst::parse::launch(&desc)
            .context("monitor pipeline")?
            .downcast::<gst::Pipeline>()
            .map_err(|_| anyhow!("not a pipeline"))?;
        let src = pipeline.by_name("src").unwrap().downcast::<gst_app::AppSrc>().map_err(|_| anyhow!("appsrc"))?;
        let sink = pipeline.by_name("sink").unwrap().downcast::<gst_app::AppSink>().map_err(|_| anyhow!("appsink"))?;
        sink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |s| {
                    let sample = s.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let (Some(buffer), Some(caps)) = (sample.buffer(), sample.caps()) else { return Ok(gst::FlowSuccess::Ok) };
                    let st = caps.structure(0).ok_or(gst::FlowError::Error)?;
                    let w = st.get::<i32>("width").unwrap_or(0) as u32;
                    let h = st.get::<i32>("height").unwrap_or(0) as u32;
                    let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                    let n = (w * h * 4) as usize;
                    if n > 0 && map.len() >= n {
                        slot.publish(w, h, map.as_slice()[..n].to_vec());
                    }
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
        // Errors in the monitor must never affect the session: just log.
        let bus = pipeline.bus().unwrap();
        std::thread::Builder::new()
            .name("monitor-bus".into())
            .spawn(move || {
                while let Some(msg) = bus.timed_pop(gst::ClockTime::NONE) {
                    match msg.view() {
                        gst::MessageView::Error(e) => {
                            warn!(target: "monitor", "decode error: {}", e.error());
                            break;
                        }
                        gst::MessageView::Eos(_) => break,
                        _ => {}
                    }
                }
            })
            .ok();
        pipeline.set_state(gst::State::Playing).context("monitor start")?;
        Ok(Self { pipeline, src })
    }

    fn push(&self, bytes: BytesMut) {
        let buf = gst::Buffer::from_mut_slice(bytes.to_vec());
        if let Err(e) = self.src.push_buffer(buf) {
            debug!(target: "monitor", "push failed: {e:?}");
        }
    }
}

impl Drop for Decoder {
    fn drop(&mut self) {
        let _ = self.src.end_of_stream();
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

/// Runs until the tap is closed. Starts decoding at the first keyframe after
/// the codec config is known; timestamps are rebased like a destination.
pub async fn monitor_task(mut rx: mpsc::Receiver<Outbound>, slot: Arc<PreviewSlot>) {
    let mut codec: Option<CodecConfig> = None;
    let mut dec: Option<Decoder> = None;
    let mut base_ms: Option<u64> = None;
    while let Some(msg) = rx.recv().await {
        match msg {
            Outbound::Codec(c) => codec = Some(c),
            Outbound::Prepare => {}
            Outbound::Packet(p) => {
                let Some(c) = &codec else { continue };
                if dec.is_none() {
                    if p.kind != PacketKind::Video || !p.keyframe {
                        continue;
                    }
                    match Decoder::new(Arc::clone(&slot), c.audio_seq.is_some()) {
                        Ok(d) => {
                            let mut head = BytesMut::from(&flv_header(c.audio_seq.is_some())[..]);
                            if let Some(v) = &c.video_seq {
                                head.extend_from_slice(&flv_tag(TAG_VIDEO, 0, v));
                            }
                            if let Some(a) = &c.audio_seq {
                                head.extend_from_slice(&flv_tag(TAG_AUDIO, 0, a));
                            }
                            d.push(head);
                            base_ms = Some(p.dts_ms());
                            dec = Some(d);
                        }
                        Err(e) => {
                            warn!(target: "monitor", "cannot start monitor decoder: {e:#}");
                            break;
                        }
                    }
                }
                let d = dec.as_ref().unwrap();
                let ts = p.dts_ms().saturating_sub(base_ms.unwrap_or(0)) as u32;
                let t = match p.kind {
                    PacketKind::Video => TAG_VIDEO,
                    PacketKind::Audio => TAG_AUDIO,
                };
                d.push(flv_tag(t, ts, &p.body));
            }
            Outbound::Finish => break,
        }
    }
    drop(dec);
    slot.clear();
    debug!(target: "monitor", "monitor stopped");
}
