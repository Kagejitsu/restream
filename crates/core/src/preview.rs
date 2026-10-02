//! Latest-frame slot for the GUI preview.
//!
//! The pipeline writes downscaled RGBA frames here; the GUI reads the newest
//! one each repaint. Only the latest frame is kept, so a slow GUI never
//! backs up the pipeline.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

#[derive(Debug, Clone)]
pub struct Frame {
    pub width: u32,
    pub height: u32,
    /// Tightly packed RGBA8.
    pub rgba: Arc<Vec<u8>>,
    pub seq: u64,
}

#[derive(Debug, Default)]
pub struct PreviewSlot {
    latest: Mutex<Option<Frame>>,
    seq: AtomicU64,
}

impl PreviewSlot {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn publish(&self, width: u32, height: u32, rgba: Vec<u8>) {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        *self.latest.lock() = Some(Frame { width, height, rgba: Arc::new(rgba), seq });
    }

    /// Returns the newest frame if its sequence number is newer than `after`.
    pub fn take_newer(&self, after: u64) -> Option<Frame> {
        let g = self.latest.lock();
        match &*g {
            Some(f) if f.seq > after => Some(f.clone()),
            _ => None,
        }
    }

    pub fn latest_seq(&self) -> u64 {
        self.seq.load(Ordering::Relaxed)
    }

    pub fn clear(&self) {
        *self.latest.lock() = None;
    }
}
