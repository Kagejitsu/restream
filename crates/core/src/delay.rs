//! The stream-delay ring buffer.
//!
//! Packets are held in arrival order and released `delay` after their media
//! time would have gone out live. Release time is anchored to the pipeline's
//! own timeline, so inter-packet spacing is preserved exactly and jitter in
//! packet *arrival* (encoder look-ahead, thread scheduling) does not leak into
//! the output.
//!
//! ```text
//!   release_at(p) = max(arrival(p), epoch + p.dts + delay)
//!   epoch         = arrival(first packet) - dts(first packet)
//! ```
//!
//! With `delay == 0` this degrades to "forward as soon as it arrives".
//! The ring is single-owner (used only by the releaser task) and therefore
//! lock-free; it never blocks a GStreamer thread.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::packet::Packet;

#[derive(Debug)]
struct Entry {
    pkt: Packet,
    release_at: Instant,
}

#[derive(Debug)]
pub struct DelayRing {
    delay: Duration,
    max_bytes: usize,
    epoch: Option<Instant>,
    items: VecDeque<Entry>,
    bytes: usize,
    /// Set when the input has ended (drain mode): no more pushes will arrive.
    eos: bool,
    /// Packets discarded because `max_bytes` was exceeded.
    pub overflow_drops: u64,
}

impl DelayRing {
    pub fn new(delay: Duration, max_bytes: usize) -> Self {
        Self {
            delay,
            max_bytes,
            epoch: None,
            items: VecDeque::new(),
            bytes: 0,
            eos: false,
            overflow_drops: 0,
        }
    }

    pub fn delay(&self) -> Duration {
        self.delay
    }

    /// Change the delay. Packets already queued keep their release time, so
    /// shortening the delay takes effect gradually and never reorders output.
    pub fn set_delay(&mut self, delay: Duration) {
        self.delay = delay;
    }

    pub fn push(&mut self, pkt: Packet, arrival: Instant) {
        let epoch = *self.epoch.get_or_insert_with(|| arrival.checked_sub(pkt.dts).unwrap_or(arrival));
        let ideal = epoch + pkt.dts + self.delay;
        let release_at = ideal.max(arrival);
        // Never release out of order even if timestamps went backwards.
        let release_at = match self.items.back() {
            Some(last) => release_at.max(last.release_at),
            None => release_at,
        };
        self.bytes += pkt.len();
        self.items.push_back(Entry { pkt, release_at });
        self.enforce_cap();
    }

    /// Mark the input as finished. [`DelayRing::is_drained`] becomes true once
    /// every queued packet has been popped.
    pub fn mark_eos(&mut self) {
        self.eos = true;
    }

    pub fn is_eos(&self) -> bool {
        self.eos
    }

    pub fn is_drained(&self) -> bool {
        self.eos && self.items.is_empty()
    }

    /// Pop the oldest packet if its release time has come.
    pub fn pop_due(&mut self, now: Instant) -> Option<Packet> {
        match self.items.front() {
            Some(e) if e.release_at <= now => {
                let e = self.items.pop_front().expect("front exists");
                self.bytes -= e.pkt.len();
                Some(e.pkt)
            }
            _ => None,
        }
    }

    /// When the next packet becomes due (`None` if empty).
    pub fn next_due_at(&self) -> Option<Instant> {
        self.items.front().map(|e| e.release_at)
    }

    /// Discard everything buffered (the "panic" button).
    pub fn cut(&mut self) -> usize {
        let n = self.items.len();
        self.items.clear();
        self.bytes = 0;
        n
    }

    /// Reset for a fresh session.
    pub fn reset(&mut self) {
        self.cut();
        self.epoch = None;
        self.eos = false;
        self.overflow_drops = 0;
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// Wall-clock span of buffered output: how long destinations would keep
    /// receiving data if the input stopped now. Release times are monotonic,
    /// so this never dips when audio and video interleave with different
    /// encoder latencies.
    pub fn fill(&self) -> Duration {
        match (self.items.front(), self.items.back()) {
            (Some(a), Some(b)) => b.release_at.saturating_duration_since(a.release_at),
            _ => Duration::ZERO,
        }
    }

    /// Time until the oldest packet is released (`None` if empty or already due).
    pub fn time_to_next_release(&self, now: Instant) -> Option<Duration> {
        self.items.front().map(|e| e.release_at.saturating_duration_since(now))
    }

    /// Drop from the *front* when over the byte cap. The output will skip
    /// ahead; the fan-out layer resynchronises on the next keyframe.
    fn enforce_cap(&mut self) {
        while self.bytes > self.max_bytes && self.items.len() > 1 {
            if let Some(e) = self.items.pop_front() {
                self.bytes -= e.pkt.len();
                self.overflow_drops += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::PacketKind;
    use bytes::Bytes;

    fn pkt(dts_ms: u64, len: usize) -> Packet {
        Packet {
            kind: PacketKind::Video,
            dts: Duration::from_millis(dts_ms),
            keyframe: dts_ms % 2000 == 0,
            body: Bytes::from(vec![0u8; len]),
        }
    }

    #[test]
    fn releases_exactly_delay_after_media_time() {
        let t0 = Instant::now();
        let mut ring = DelayRing::new(Duration::from_secs(3), usize::MAX);
        ring.push(pkt(0, 10), t0);
        ring.push(pkt(500, 10), t0 + Duration::from_millis(500));
        assert!(ring.pop_due(t0 + Duration::from_millis(2999)).is_none());
        assert_eq!(ring.pop_due(t0 + Duration::from_millis(3000)).unwrap().dts, Duration::ZERO);
        assert!(ring.pop_due(t0 + Duration::from_millis(3499)).is_none());
        assert_eq!(ring.pop_due(t0 + Duration::from_millis(3500)).unwrap().dts, Duration::from_millis(500));
        assert!(ring.is_empty());
    }

    #[test]
    fn arrival_jitter_does_not_change_release_time() {
        let t0 = Instant::now();
        let mut ring = DelayRing::new(Duration::from_secs(1), usize::MAX);
        ring.push(pkt(0, 1), t0);
        // Packet for t=100ms arrives 400ms late (encoder look-ahead).
        ring.push(pkt(100, 1), t0 + Duration::from_millis(500));
        ring.pop_due(t0 + Duration::from_secs(1)).unwrap();
        assert!(ring.pop_due(t0 + Duration::from_millis(1099)).is_none());
        assert!(ring.pop_due(t0 + Duration::from_millis(1100)).is_some());
    }

    #[test]
    fn zero_delay_forwards_on_arrival() {
        let t0 = Instant::now();
        let mut ring = DelayRing::new(Duration::ZERO, usize::MAX);
        ring.push(pkt(0, 1), t0);
        ring.push(pkt(100, 1), t0 + Duration::from_millis(130)); // arrived late
        assert!(ring.pop_due(t0).is_some());
        assert!(ring.pop_due(t0 + Duration::from_millis(129)).is_none());
        assert!(ring.pop_due(t0 + Duration::from_millis(130)).is_some());
    }

    #[test]
    fn fill_bytes_cut_and_drain() {
        let t0 = Instant::now();
        let mut ring = DelayRing::new(Duration::from_secs(10), usize::MAX);
        for i in 0..5 {
            ring.push(pkt(i * 1000, 100), t0 + Duration::from_secs(i));
        }
        assert_eq!(ring.fill(), Duration::from_secs(4));
        assert_eq!(ring.bytes(), 500);
        assert_eq!(ring.len(), 5);
        ring.mark_eos();
        assert!(!ring.is_drained());
        assert_eq!(ring.cut(), 5);
        assert!(ring.is_drained());
        assert_eq!(ring.bytes(), 0);
    }

    #[test]
    fn byte_cap_drops_oldest() {
        let t0 = Instant::now();
        let mut ring = DelayRing::new(Duration::from_secs(10), 250);
        for i in 0..5 {
            ring.push(pkt(i * 1000, 100), t0);
        }
        assert_eq!(ring.len(), 2);
        assert_eq!(ring.overflow_drops, 3);
        assert_eq!(ring.pop_due(t0 + Duration::from_secs(20)).unwrap().dts, Duration::from_secs(3));
    }

    #[test]
    fn never_reorders_when_timestamps_go_backwards() {
        let t0 = Instant::now();
        let mut ring = DelayRing::new(Duration::from_secs(1), usize::MAX);
        ring.push(pkt(1000, 1), t0);
        ring.push(pkt(900, 1), t0 + Duration::from_millis(10));
        let a = ring.next_due_at().unwrap();
        ring.pop_due(a).unwrap();
        assert!(ring.next_due_at().unwrap() >= a);
    }
}
