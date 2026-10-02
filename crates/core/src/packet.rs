//! The encoded-media packet that crosses from GStreamer into the Rust core.
//!
//! Packets are FLV tag *bodies* exactly as `flvmux` produced them, already
//! interleaved by decode time across audio and video. The core never looks
//! inside a body except to classify it (sequence header vs. frame, keyframe
//! vs. inter frame), and destinations send the bodies verbatim with rebased
//! timestamps. This keeps the muxing — the part platforms are picky about —
//! in a muxer that OBS-class software has exercised for years.

use std::time::Duration;

use bytes::{Buf, Bytes, BytesMut};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PacketKind {
    Video,
    Audio,
}

/// One FLV media tag.
#[derive(Debug, Clone)]
pub struct Packet {
    pub kind: PacketKind,
    /// FLV tag timestamp (decode time) as produced by the muxer.
    pub dts: Duration,
    /// Video: IDR access unit. Audio: always true.
    pub keyframe: bool,
    /// FLV tag body (VIDEODATA / AUDIODATA), header bytes included.
    pub body: Bytes,
}

impl Packet {
    pub fn len(&self) -> usize {
        self.body.len()
    }
    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }
    pub fn dts_ms(&self) -> u64 {
        self.dts.as_millis() as u64
    }
}

/// Stream description sent to every destination before its first frame.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodecConfig {
    /// AVC sequence header tag body (`17 00 00 00 00 <avcC>`).
    pub video_seq: Option<Bytes>,
    /// AAC sequence header tag body (`AF 00 <AudioSpecificConfig>`).
    pub audio_seq: Option<Bytes>,
    pub meta: StreamMeta,
}

impl CodecConfig {
    pub fn is_complete(&self, expect_audio: bool) -> bool {
        self.video_seq.is_some() && (!expect_audio || self.audio_seq.is_some())
    }
}

/// Values for the RTMP `onMetaData` message (informational for platforms).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamMeta {
    pub width: u32,
    pub height: u32,
    pub fps: u32,
    pub video_kbps: u32,
    pub audio_kbps: u32,
    pub sample_rate: u32,
    pub channels: u32,
}

// ------------------------------------------------------------ FLV parser ---

pub const TAG_AUDIO: u8 = 8;
pub const TAG_VIDEO: u8 = 9;
pub const TAG_SCRIPT: u8 = 18;

/// A parsed FLV tag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlvTag {
    pub tag_type: u8,
    pub timestamp_ms: u32,
    pub body: Bytes,
}

impl FlvTag {
    pub fn is_video_sequence_header(&self) -> bool {
        self.tag_type == TAG_VIDEO && self.body.len() >= 2 && (self.body[0] & 0x0F) == 7 && self.body[1] == 0
    }
    pub fn is_audio_sequence_header(&self) -> bool {
        self.tag_type == TAG_AUDIO && self.body.len() >= 2 && (self.body[0] >> 4) == 10 && self.body[1] == 0
    }
    pub fn is_video_keyframe(&self) -> bool {
        self.tag_type == TAG_VIDEO && !self.body.is_empty() && (self.body[0] >> 4) == 1
    }
}

/// Incremental parser for an FLV byte stream (file header + tags), tolerant
/// of arbitrary chunking.
#[derive(Debug, Default)]
pub struct FlvParser {
    buf: BytesMut,
    header_done: bool,
}

impl FlvParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed bytes; returns every complete tag now available.
    pub fn push(&mut self, data: &[u8]) -> Vec<FlvTag> {
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        if !self.header_done {
            // "FLV" version flags header_size(4) + PreviousTagSize0(4)
            if self.buf.len() < 13 {
                return out;
            }
            if &self.buf[..3] != b"FLV" {
                // Not at a file header: assume raw tags follow.
                self.header_done = true;
            } else {
                let hdr = u32::from_be_bytes([self.buf[5], self.buf[6], self.buf[7], self.buf[8]]) as usize;
                let skip = hdr + 4;
                if self.buf.len() < skip {
                    return out;
                }
                self.buf.advance(skip);
                self.header_done = true;
            }
        }
        loop {
            if self.buf.len() < 11 {
                break;
            }
            let b = &self.buf[..];
            let tag_type = b[0] & 0x1F;
            let size = u32::from_be_bytes([0, b[1], b[2], b[3]]) as usize;
            let ts = u32::from_be_bytes([b[7], b[4], b[5], b[6]]);
            let total = 11 + size + 4; // + PreviousTagSize
            if self.buf.len() < total {
                break;
            }
            let body = self.buf.split_to(total).freeze().slice(11..11 + size);
            out.push(FlvTag { tag_type, timestamp_ms: ts, body });
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag_bytes(t: u8, ts: u32, body: &[u8]) -> Vec<u8> {
        let mut v = vec![t];
        v.extend_from_slice(&(body.len() as u32).to_be_bytes()[1..]);
        v.extend_from_slice(&[(ts >> 16) as u8, (ts >> 8) as u8, ts as u8, (ts >> 24) as u8]);
        v.extend_from_slice(&[0, 0, 0]);
        v.extend_from_slice(body);
        v.extend_from_slice(&((11 + body.len()) as u32).to_be_bytes());
        v
    }

    #[test]
    fn parses_header_and_tags_across_chunks() {
        let mut stream = vec![b'F', b'L', b'V', 1, 5, 0, 0, 0, 9, 0, 0, 0, 0];
        stream.extend(tag_bytes(TAG_VIDEO, 0, &[0x17, 0, 0, 0, 0, 1, 2, 3]));
        stream.extend(tag_bytes(TAG_AUDIO, 21, &[0xAF, 0, 0x11, 0x90]));
        stream.extend(tag_bytes(TAG_VIDEO, 0x0100_0010, &[0x27, 1, 0, 0, 0, 9]));
        let mut p = FlvParser::new();
        let mut tags = Vec::new();
        for chunk in stream.chunks(7) {
            tags.extend(p.push(chunk));
        }
        assert_eq!(tags.len(), 3);
        assert!(tags[0].is_video_sequence_header());
        assert!(tags[0].is_video_keyframe());
        assert!(tags[1].is_audio_sequence_header());
        assert_eq!(tags[1].timestamp_ms, 21);
        assert_eq!(tags[2].timestamp_ms, 0x0100_0010, "extended timestamp byte is the high byte");
        assert!(!tags[2].is_video_keyframe());
        assert_eq!(&tags[2].body[..], &[0x27, 1, 0, 0, 0, 9]);
    }
}
