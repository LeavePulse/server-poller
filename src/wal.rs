//! Disk-backed WAL (write-ahead log) buffer for ingest batches.
//!
//! When monitoring-service is unreachable, batches are persisted to segmented
//! append-log files on disk. On recovery they are drained oldest-first.
//!
//! Record format: `[4B u32 BE length][payload bytes][0x0A sentinel]`
//! Segment files: `buffer-{epoch_millis}.wal`

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tracing::{info, warn};

use crate::metrics::{
    BUFFER_BYTES, BUFFER_DRAINED, BUFFER_EVICTED, BUFFER_SEGMENTS, BUFFER_WRITES,
};

const SENTINEL: u8 = 0x0A;

pub struct WalBuffer {
    dir: PathBuf,
    max_bytes: u64,
    segment_max_bytes: u64,
    current_segment: Option<SegmentWriter>,
    total_bytes: u64,
}

struct SegmentWriter {
    path: PathBuf,
    file: File,
    size: u64,
}

impl WalBuffer {
    /// Open (or create) the buffer directory and scan existing segments.
    pub fn open(dir: PathBuf, max_bytes: u64, segment_max_bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(&dir)?;

        let mut total_bytes = 0u64;
        for entry in Self::sorted_segments(&dir)? {
            total_bytes += entry.metadata()?.len();
        }

        info!(
            dir = %dir.display(),
            existing_segments = Self::sorted_segments(&dir)?.len(),
            existing_bytes = total_bytes,
            max_bytes,
            "WAL buffer opened"
        );

        let wal = WalBuffer {
            dir,
            max_bytes,
            segment_max_bytes,
            current_segment: None,
            total_bytes,
        };
        wal.sync_gauges();
        Ok(wal)
    }

    /// Persist a batch of JSON values to the current segment.
    pub fn write_batch(&mut self, items: &[Value]) -> io::Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        for item in items {
            let json_bytes = serde_json::to_vec(item)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let record_size = 4 + json_bytes.len() as u64 + 1;

            // Evict oldest segments if over budget.
            while self.total_bytes + record_size > self.max_bytes {
                if !self.evict_oldest()? {
                    break;
                }
            }

            // Rotate segment if current is full or absent.
            let needs_rotate = match &self.current_segment {
                Some(s) => s.size + record_size > self.segment_max_bytes,
                None => true,
            };
            if needs_rotate {
                self.rotate_segment()?;
            }

            let seg = self.current_segment.as_mut().unwrap();
            seg.file
                .write_all(&(json_bytes.len() as u32).to_be_bytes())?;
            seg.file.write_all(&json_bytes)?;
            seg.file.write_all(&[SENTINEL])?;
            seg.size += record_size;
            self.total_bytes += record_size;
        }

        // Fsync for durability.
        if let Some(seg) = &self.current_segment {
            seg.file.sync_data()?;
        }

        BUFFER_WRITES.inc();
        self.sync_gauges();
        Ok(())
    }

    /// Whether there are completed (non-current) segments waiting to be drained.
    pub fn has_pending(&self) -> bool {
        let Ok(segments) = Self::sorted_segments(&self.dir) else {
            return false;
        };
        let current_path = self.current_segment.as_ref().map(|s| &s.path);
        segments.iter().any(|e| Some(&e.path()) != current_path)
    }

    /// Read all records from the oldest completed segment (up to `max_items`).
    /// Returns the segment path and the decoded items.
    pub fn read_oldest_batch(&self, max_items: usize) -> io::Result<Option<(PathBuf, Vec<Value>)>> {
        let segments = Self::sorted_segments(&self.dir)?;
        let current_path = self.current_segment.as_ref().map(|s| &s.path);

        let oldest = segments.iter().find(|e| Some(&e.path()) != current_path);

        let Some(entry) = oldest else {
            return Ok(None);
        };

        let path = entry.path();
        let items = Self::read_segment(&path, max_items)?;
        Ok(Some((path, items)))
    }

    /// Delete a fully-drained segment file.
    pub fn delete_segment(&mut self, path: &Path) -> io::Result<()> {
        let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        fs::remove_file(path)?;
        self.total_bytes = self.total_bytes.saturating_sub(size);
        BUFFER_DRAINED.inc();
        self.sync_gauges();
        Ok(())
    }

    // ------------------------------------------------------------------
    // Internal helpers
    // ------------------------------------------------------------------

    fn sorted_segments(dir: &Path) -> io::Result<Vec<fs::DirEntry>> {
        let mut entries: Vec<fs::DirEntry> = fs::read_dir(dir)?
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
            .collect();
        entries.sort_by_key(|e| e.file_name());
        Ok(entries)
    }

    fn rotate_segment(&mut self) -> io::Result<()> {
        // Close current segment (drop flushes).
        self.current_segment = None;

        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let path = self.dir.join(format!("buffer-{ts}.wal"));

        let file = OpenOptions::new().create(true).append(true).open(&path)?;

        self.current_segment = Some(SegmentWriter {
            path,
            file,
            size: 0,
        });
        Ok(())
    }

    fn evict_oldest(&mut self) -> io::Result<bool> {
        let segments = Self::sorted_segments(&self.dir)?;
        let current_path = self.current_segment.as_ref().map(|s| &s.path);

        // Evict the oldest segment that is NOT the current one.
        let target = segments.iter().find(|e| Some(&e.path()) != current_path);

        let Some(entry) = target else {
            return Ok(false);
        };

        let path = entry.path();
        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        fs::remove_file(&path)?;
        self.total_bytes = self.total_bytes.saturating_sub(size);

        warn!(
            segment = %path.display(),
            freed_bytes = size,
            "WAL buffer evicted oldest segment (over budget)"
        );
        BUFFER_EVICTED.inc();
        Ok(true)
    }

    fn read_segment(path: &Path, max_items: usize) -> io::Result<Vec<Value>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut items = Vec::new();

        loop {
            if items.len() >= max_items {
                break;
            }

            // Read 4-byte length prefix.
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let payload_len = u32::from_be_bytes(len_buf) as usize;

            // Sanity check: reject absurdly large records (> 16 MB).
            if payload_len > 16 * 1024 * 1024 {
                warn!(
                    segment = %path.display(),
                    claimed_len = payload_len,
                    "WAL corrupt record (absurd length), skipping rest of segment"
                );
                break;
            }

            // Read payload.
            let mut payload_buf = vec![0u8; payload_len];
            match reader.read_exact(&mut payload_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    warn!(
                        segment = %path.display(),
                        "WAL truncated record, discarding"
                    );
                    break;
                }
                Err(e) => return Err(e),
            }

            // Read sentinel.
            let mut sentinel = [0u8; 1];
            match reader.read_exact(&mut sentinel) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    warn!(
                        segment = %path.display(),
                        "WAL missing sentinel, discarding record"
                    );
                    break;
                }
                Err(e) => return Err(e),
            }

            if sentinel[0] != SENTINEL {
                warn!(
                    segment = %path.display(),
                    "WAL invalid sentinel byte, skipping record"
                );
                continue;
            }

            // Parse JSON.
            match serde_json::from_slice::<Value>(&payload_buf) {
                Ok(val) => items.push(val),
                Err(e) => {
                    warn!(
                        segment = %path.display(),
                        error = %e,
                        "WAL corrupt JSON, skipping record"
                    );
                    continue;
                }
            }
        }

        Ok(items)
    }

    fn sync_gauges(&self) {
        BUFFER_BYTES.set(self.total_bytes as i64);
        let count = Self::sorted_segments(&self.dir)
            .map(|s| s.len())
            .unwrap_or(0);
        BUFFER_SEGMENTS.set(count as i64);
    }
}
