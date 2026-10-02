//! In-memory ring of recent log lines for the Logs tab, usable as a
//! `tracing_subscriber` writer alongside the rolling log file.

use std::collections::VecDeque;
use std::io;
use std::sync::Arc;

use parking_lot::Mutex;
use tracing_subscriber::fmt::MakeWriter;

const CAPACITY: usize = 2000;

#[derive(Clone, Default)]
pub struct LogBuffer {
    lines: Arc<Mutex<VecDeque<String>>>,
}

impl LogBuffer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> Vec<String> {
        self.lines.lock().iter().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.lines.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        self.lines.lock().clear();
    }
}

pub struct LogWriter {
    lines: Arc<Mutex<VecDeque<String>>>,
    pending: Vec<u8>,
}

impl io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.pending.extend_from_slice(buf);
        while let Some(pos) = self.pending.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.pending.drain(..=pos).collect();
            let text = String::from_utf8_lossy(&line).trim_end().to_string();
            let mut g = self.lines.lock();
            if g.len() >= CAPACITY {
                g.pop_front();
            }
            g.push_back(text);
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogBuffer {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter { lines: Arc::clone(&self.lines), pending: Vec::new() }
    }
}
