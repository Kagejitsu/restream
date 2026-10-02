//! FLV tag *bodies* for H.264 (AVC) video and AAC audio.
//!
//! RTMP carries the FLV `VideoData` / `AudioData` tag bodies as message
//! payloads; the tag header (type, size, timestamp) becomes the RTMP message
//! header. These helpers therefore produce only the body bytes.
//!
//! Layout references: Adobe "Video File Format Specification v10.1",
//! sections E.4.3.1 (VIDEODATA / AVCVIDEOPACKET) and E.4.2.1 (AUDIODATA /
//! AACAUDIODATA).

use bytes::{BufMut, Bytes, BytesMut};

/// FrameType (high nibble) | CodecID (low nibble, 7 = AVC).
const AVC_KEYFRAME: u8 = 0x17;
const AVC_INTERFRAME: u8 = 0x27;

/// SoundFormat 10 (AAC) << 4 | SoundRate 3 (44 kHz marker, always 3 for AAC)
/// << 2 | SoundSize 1 (16-bit) << 1 | SoundType 1 (stereo). For AAC the
/// rate/channel bits are informational only; the real values live in the
/// AudioSpecificConfig.
const AAC_HEADER: u8 = 0xAF;

/// Build the AVC sequence header body (`AVCDecoderConfigurationRecord`).
pub fn avc_sequence_header(avcc: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(5 + avcc.len());
    b.put_u8(AVC_KEYFRAME);
    b.put_u8(0); // AVCPacketType 0 = sequence header
    b.put_slice(&[0, 0, 0]); // composition time
    b.put_slice(avcc);
    b.freeze()
}

/// Build an AVC NALU body for one access unit already in AVCC
/// (4-byte-length-prefixed) form.
///
/// `cts_ms` is the composition time offset (`pts - dts`) in milliseconds;
/// it is encoded as a signed 24-bit integer.
pub fn avc_frame(keyframe: bool, cts_ms: i32, au: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(5 + au.len());
    b.put_u8(if keyframe { AVC_KEYFRAME } else { AVC_INTERFRAME });
    b.put_u8(1); // AVCPacketType 1 = NALU
    let cts = cts_ms.clamp(-(1 << 23), (1 << 23) - 1) as u32 & 0x00FF_FFFF;
    b.put_u8((cts >> 16) as u8);
    b.put_u8((cts >> 8) as u8);
    b.put_u8(cts as u8);
    b.put_slice(au);
    b.freeze()
}

/// Build the AAC sequence header body (`AudioSpecificConfig`).
pub fn aac_sequence_header(asc: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(2 + asc.len());
    b.put_u8(AAC_HEADER);
    b.put_u8(0); // AACPacketType 0 = sequence header
    b.put_slice(asc);
    b.freeze()
}

/// Build an AAC raw frame body (no ADTS header).
pub fn aac_frame(frame: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(2 + frame.len());
    b.put_u8(AAC_HEADER);
    b.put_u8(1); // AACPacketType 1 = raw
    b.put_slice(frame);
    b.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avc_sequence_header_layout() {
        let avcc = [0x01, 0x64, 0x00, 0x1f, 0xff];
        let b = avc_sequence_header(&avcc);
        assert_eq!(&b[..5], &[0x17, 0x00, 0, 0, 0]);
        assert_eq!(&b[5..], &avcc);
    }

    #[test]
    fn avc_frame_keyframe_and_cts() {
        let au = [0, 0, 0, 2, 0x65, 0x88];
        let b = avc_frame(true, 0x0102_03, &au);
        assert_eq!(&b[..5], &[0x17, 0x01, 0x01, 0x02, 0x03]);
        assert_eq!(&b[5..], &au);
        let b = avc_frame(false, -1, &au);
        assert_eq!(&b[..5], &[0x27, 0x01, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn aac_bodies() {
        assert_eq!(&aac_sequence_header(&[0x11, 0x90])[..], &[0xAF, 0x00, 0x11, 0x90]);
        assert_eq!(&aac_frame(&[1, 2, 3])[..], &[0xAF, 0x01, 1, 2, 3]);
    }
}
