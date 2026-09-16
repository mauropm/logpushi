//! Event model: transport-independent representation of a log event.

use chrono::{DateTime, Utc};

/// Raw event straight from disk.
#[derive(Debug, Clone)]
pub struct RawEvent {
    /// Original line content, verbatim (or timestamp-transformed in
    /// synthetic-today mode; only the detected timestamp substring differs).
    pub line: String,
    /// Path as first seen by discovery entry, relative to the log dir.
    pub source_file: String,
    /// Dataset family (e.g. "linux-syslog", "apache").
    pub source_type: &'static str,
    /// 1-based line number within the source file.
    pub line_no: u64,
}

/// Normalized event consumed by the batcher and transports.
#[derive(Debug, Clone)]
pub struct Event {
    pub body: String,
    /// Parsed event timestamp (transformed in synthetic-today; detected or None).
    pub timestamp: Option<DateTime<Utc>>,
    /// Wall time when Logpushi generated the event.
    pub observed: DateTime<Utc>,
    /// Best-effort severity (log4j level, syslog token); None when undetected.
    pub severity: Option<&'static str>,
    pub source_file: String,
    pub source_type: &'static str,
    /// Timestamp confidently detected in the body.
    pub ts_detected: bool,
    pub ts_year_inferred: bool,
    pub line_no: u64,
}

impl Event {
    /// Extract a plausible host token from a syslog-style line
    /// (`Mon DD HH:MM:SS host ...\`); empty string otherwise.
    pub fn extracted_host(&self) -> String {
        crate::timestamp::syslog_host(&self.body).unwrap_or_default()
    }
}

/// Map a source path to a dataset-family name (HEC `sourcetype` /
/// OTel `source.type` attribute).
pub fn dataset_family(rel_path: &std::path::Path) -> &'static str {
    // Decide from the most specific directory then the file name.
    let name = rel_path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    let parent = rel_path.parent().and_then(|s| s.to_str()).unwrap_or("");
    let hay = format!("{parent}/{name}").to_ascii_lowercase();
    if hay.contains("apache") {
        "apache"
    } else if hay.contains("android") {
        "android"
    } else if hay.contains("bgl") {
        "bgl"
    } else if hay.contains("hadoop") || hay.contains("yarn") {
        "hadoop"
    } else if hay.contains("hpc") {
        "hpc"
    } else if hay.contains("openstack") {
        "openstack"
    } else if hay.contains("proxifier") {
        "proxifier"
    } else if hay.contains("ssh") {
        "ssh-syslog"
    } else if hay.contains("zookeeper") {
        "zookeeper"
    } else if hay.contains("healthapp") {
        "health-app"
    } else if hay.contains("linux") {
        "linux-syslog"
    } else if hay.contains("mac") {
        "mac-syslog"
    } else {
        "generic"
    }
}

/// Best-effort severity detection. Returns a normalized level keyword or None.
pub fn detect_severity(line: &str) -> Option<&'static str> {
    const PATTERNS: [(&str, &str); 12] = [
        ("FATAL", "FATAL"),
        ("ERROR", "ERROR"),
        ("ERR:", "ERROR"),
        ("error", "ERROR"),
        ("WARN", "WARN"),
        ("WRN", "WARN"),
        ("NOTICE", "NOTICE"),
        ("CRITICAL", "CRITICAL"),
        ("INFO", "INFO"),
        ("DEBUG", "DEBUG"),
        ("TRACE", "TRACE"),
        ("VERBOSE", "VERBOSE"),
    ];
    for (pat, level) in PATTERNS {
        if line.contains(pat) {
            return Some(level);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_basic() {
        assert_eq!(
            detect_severity("2015-07-29 17:41:41,536 WARN something"),
            Some("WARN")
        );
        assert_eq!(detect_severity("plain message"), None);
        assert_eq!(detect_severity("level=error worker stopped"), Some("ERROR"));
    }

    #[test]
    fn dataset_families() {
        use std::path::Path;
        assert_eq!(dataset_family(Path::new("BGL/BGL.log")), "bgl");
        assert_eq!(dataset_family(Path::new("Hadoop/app1/src1")), "hadoop");
        assert_eq!(dataset_family(Path::new("unknown.log")), "generic");
    }
}
