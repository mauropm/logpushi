//! Deterministic PRNG (SplitMix64) plus the sampling strategies for both modes.

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::discovery::{discover, LogFile};
use crate::event::RawEvent;

#[derive(thiserror::Error, Debug)]
pub enum SampleError {
    #[error("no eligible log files found under {0}")]
    NoFiles(String),
    #[error("file {0} is not readable: {1}")]
    FileRead(String, #[source] std::io::Error),
}

/// SplitMix64 — small, fast, deterministic.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed)
    }

    /// Seed derived from the current time unless one is given.
    pub fn from_seed_opt(seed: Option<u64>) -> Self {
        let seed = seed.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9E37_79B9_7F4A_7C15)
        });
        Rng(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in [0, n).
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }

    /// Uniform f64 in [0, 1).
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u128 << 53) as f64
    }
}

/// Raw event source feeding the pipeline; `None` means exhausted.
pub trait EventSource: Send {
    fn next(&mut self) -> Result<Option<RawEvent>, SampleError>;
}

impl<T: EventSource + ?Sized> EventSource for Box<T> {
    fn next(&mut self) -> Result<Option<RawEvent>, SampleError> {
        (**self).next()
    }
}

pub fn decode_line(mut bytes: Vec<u8>) -> String {
    // Trim the trailing newline (and CR for CRLF sources).
    while bytes.last() == Some(&b'\n') || bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    match String::from_utf8(bytes) {
        Ok(s) => s,
        Err(e) => String::from_utf8_lossy(e.as_bytes()).into_owned(),
    }
}

fn open_reader(path: &Path) -> Result<BufReader<File>, SampleError> {
    let f = File::open(path).map_err(|e| SampleError::FileRead(path.display().to_string(), e))?;
    Ok(BufReader::with_capacity(64 * 1024, f))
}

// ---------------------------------------------------------------------------
// Replay: one file, sequential stream, optional random start offset, wrap.
// ---------------------------------------------------------------------------

pub struct ReplaySampler {
    reader: BufReader<File>,
    path: std::path::PathBuf,
    source_file: String,
    source_type: &'static str,
    line_no: u64,
    wrap: bool,
    exhausted: bool,
}

impl ReplaySampler {
    /// Pick a random (seeded) file — unless `file` pins one — optionally start
    /// at a random byte offset aligned to the next full line, and stream
    /// sequentially from there.
    pub fn new(
        dir: &Path,
        file: Option<&Path>,
        rng: &mut Rng,
        random_offset: bool,
        wrap: bool,
    ) -> Result<Self, SampleError> {
        let (path, rel) = match file {
            Some(p) => {
                let md = std::fs::metadata(p)
                    .map_err(|e| SampleError::FileRead(p.display().to_string(), e))?;
                if md.is_file() {
                    (
                        p.to_path_buf(),
                        p.file_name()
                            .map(|s| s.to_string_lossy().into_owned())
                            .unwrap_or_default(),
                    )
                } else {
                    return Err(SampleError::FileRead(
                        p.display().to_string(),
                        std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a regular file"),
                    ));
                }
            }
            None => {
                let files = discover(dir).map_err(|e| {
                    SampleError::FileRead(
                        dir.display().to_string(),
                        std::io::Error::other(e.to_string()),
                    )
                })?;
                if files.is_empty() {
                    return Err(SampleError::NoFiles(dir.display().to_string()));
                }
                let pick: &LogFile = &files[rng.below(files.len() as u64) as usize];
                (pick.path.clone(), pick.rel_str())
            }
        };
        let source_type = crate::event::dataset_family(Path::new(&rel));
        let mut reader = open_reader(&path)?;
        if random_offset {
            let size = reader.get_ref().metadata().map(|m| m.len()).unwrap_or(0);
            if size > 4096 {
                let offset = rng.below(size - 1);
                if reader.seek(SeekFrom::Start(offset)).is_ok() {
                    // Skip the partial line at the offset.
                    let mut scrap = Vec::new();
                    let _ = reader.read_until(b'\n', &mut scrap);
                }
            }
        }
        Ok(Self {
            reader,
            path,
            source_file: rel,
            source_type,
            line_no: 0,
            wrap,
            exhausted: false,
        })
    }

    fn raw(&self, line: String) -> RawEvent {
        RawEvent {
            line,
            source_file: self.source_file.clone(),
            source_type: self.source_type,
            line_no: self.line_no,
        }
    }
}

impl EventSource for ReplaySampler {
    fn next(&mut self) -> Result<Option<RawEvent>, SampleError> {
        if self.exhausted {
            return Ok(None);
        }
        let (mut bytes, n) = self.read_once()?;
        if n == 0 {
            if self.wrap {
                self.reader = open_reader(&self.path)?;
                self.line_no = 0;
                let (retry, n) = self.read_once()?;
                bytes = retry;
                if n == 0 {
                    // File has zero usable lines.
                    self.exhausted = true;
                    return Ok(None);
                }
            } else {
                self.exhausted = true;
                return Ok(None);
            }
        }
        self.line_no += 1;
        Ok(Some(self.raw(decode_line(bytes))))
    }
}

impl ReplaySampler {
    fn read_once(&mut self) -> Result<(Vec<u8>, usize), SampleError> {
        let mut bytes = Vec::new();
        let n = self
            .reader
            .read_until(b'\n', &mut bytes)
            .map_err(|e| SampleError::FileRead(self.path.display().to_string(), e))?;
        Ok((bytes, n))
    }
}

// ---------------------------------------------------------------------------
// Synthetic-today: for each event, pick a random file and a random byte offset
// within it, then read the first full line at or after that offset. This gives
// cross-corpus spread with O(1) memory and no persistent handles; deterministic
// under a fixed seed.
// ---------------------------------------------------------------------------

pub struct SyntheticSampler {
    files: Vec<LogFile>,
    rng: Rng,
    #[allow(dead_code)]
    module_hint: fn() -> &'static str,
}

impl SyntheticSampler {
    pub fn new(dir: &Path, rng: Rng, file: Option<&Path>) -> Result<Self, SampleError> {
        let files = if let Some(f) = file {
            let md = std::fs::metadata(f)
                .map_err(|e| SampleError::FileRead(f.display().to_string(), e))?;
            if !md.is_file() {
                return Err(SampleError::FileRead(
                    f.display().to_string(),
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a regular file"),
                ));
            }
            vec![LogFile {
                path: f.to_path_buf(),
                rel_path: f
                    .file_name()
                    .map(|s| PathBuf::from(s.to_string_lossy().into_owned()))
                    .unwrap_or_default(),
                size: md.len(),
            }]
        } else {
            discover(dir).map_err(|e| {
                SampleError::FileRead(
                    dir.display().to_string(),
                    std::io::Error::other(e.to_string()),
                )
            })?
        };
        if files.is_empty() {
            return Err(SampleError::NoFiles(dir.display().to_string()));
        }
        Ok(Self {
            files,
            rng,
            module_hint: module_marker,
        })
    }
}

fn module_marker() -> &'static str {
    "synthetic-sampler"
}

impl EventSource for SyntheticSampler {
    fn next(&mut self) -> Result<Option<RawEvent>, SampleError> {
        // A few retries guard against repeatedly landing near EOF/empty spots.
        for _ in 0..8 {
            let idx = self.rng.below(self.files.len() as u64) as usize;
            if let Some(ev) = self.read_at(idx)? {
                return Ok(Some(ev));
            }
        }
        // Fallback: read deterministically from the start of a random file.
        let idx = self.rng.below(self.files.len() as u64) as usize;
        self.read_from(idx, 0)
    }
}

impl SyntheticSampler {
    fn read_at(&mut self, idx: usize) -> Result<Option<RawEvent>, SampleError> {
        let log = &self.files[idx];
        if log.size == 0 {
            return Ok(None);
        }
        let offset = self.rng.below(log.size - 1);
        self.read_from(idx, offset)
    }

    fn read_from(&mut self, idx: usize, offset: u64) -> Result<Option<RawEvent>, SampleError> {
        let log = &self.files[idx];
        if log.size == 0 {
            return Ok(None);
        }
        let mut reader = open_reader(&log.path)?;
        if offset > 0 {
            reader
                .seek(SeekFrom::Start(offset))
                .map_err(|e| SampleError::FileRead(log.path.display().to_string(), e))?;
            // Skip the partial line at the offset.
            let mut scrap = Vec::new();
            let _ = reader.read_until(b'\n', &mut scrap);
        }
        let mut line = Vec::new();
        let n = reader
            .read_until(b'\n', &mut line)
            .map_err(|e| SampleError::FileRead(log.path.display().to_string(), e))?;
        if n == 0 {
            return Ok(None);
        }
        Ok(Some(RawEvent {
            line: decode_line(line),
            source_file: log.rel_str(),
            source_type: crate::event::dataset_family(&log.rel_path),
            line_no: 0,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_logs() -> (tempfile::TempDir, usize) {
        let dir = tempfile::TempDir::new().unwrap();
        let mut total = 0usize;
        for (name, lines) in [
            ("a.log", "alpha one\nalpha two\nalpha three\n"),
            ("b.log", "beta\n"),
        ] {
            let mut f = std::fs::File::create(dir.path().join(name)).unwrap();
            f.write_all(lines.as_bytes()).unwrap();
            total += lines.lines().count();
        }
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/c.log"), "gamma\n").unwrap();
        (dir, total)
    }

    #[test]
    fn seeded_replay_is_deterministic() {
        let (dir, _) = temp_logs();
        let mut s1 = ReplaySampler::new(dir.path(), None, &mut Rng::new(7), true, true).unwrap();
        let mut s2 = ReplaySampler::new(dir.path(), None, &mut Rng::new(7), true, true).unwrap();
        for _ in 0..5 {
            let e1 = s1.next().unwrap().unwrap();
            let e2 = s2.next().unwrap().unwrap();
            assert_eq!(e1.line, e2.line);
            assert_eq!(e1.source_file, e2.source_file);
        }
    }

    #[test]
    fn different_seed_picks_different_start() {
        let (dir, _) = temp_logs();
        // Highly likely: two unrelated offsets stream different starting lines.
        let seen: std::collections::HashSet<String> = (0..12)
            .map(|seed| {
                let mut s =
                    ReplaySampler::new(dir.path(), None, &mut Rng::new(seed), true, false).unwrap();
                match s.next() {
                    Ok(Some(e)) => e.line.clone(),
                    _ => String::new(),
                }
            })
            .collect();
        assert!(seen.len() > 1, "expected variation, got {seen:?}")
    }

    #[test]
    fn wrap_never_exhausts() {
        let (dir, _) = temp_logs();
        let mut s = ReplaySampler::new(dir.path(), None, &mut Rng::new(3), false, true).unwrap();
        for _ in 0..15 {
            assert!(s.next().unwrap().is_some());
        }
    }

    #[test]
    fn synthetic_sampler_streams_random_events() {
        let (dir, _) = temp_logs();
        let mut s = SyntheticSampler::new(dir.path(), Rng::new(9), None).unwrap();
        for _ in 0..20 {
            assert!(s.next().unwrap().is_some());
        }
    }

    #[test]
    fn respects_pinned_file() {
        let (dir, _) = temp_logs();
        let f = dir.path().join("b.log");
        let mut s = ReplaySampler::new(
            dir.path(),
            Some(f.as_path()),
            &mut Rng::new(11),
            false,
            true,
        )
        .unwrap();
        for _ in 0..10 {
            let e = s.next().unwrap().unwrap();
            assert_eq!(e.source_file, "b.log");
        }
    }

    #[test]
    fn decode_line_trims_cr_lf() {
        assert_eq!(decode_line(b"hello\r\n".to_vec()), "hello");
        assert_eq!(decode_line(b"hello\n".to_vec()), "hello");
    }
}
