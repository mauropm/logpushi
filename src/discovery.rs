//! Recursive log-file discovery under the configured log directory.

use std::fs;
use std::path::{Path, PathBuf};

/// Metadata for one discovered log file.
#[derive(Debug, Clone)]
pub struct LogFile {
    pub path: PathBuf,
    /// Path relative to the discovery root (used for source attribution).
    pub rel_path: PathBuf,
    pub size: u64,
}

impl LogFile {
    pub fn rel_str(&self) -> String {
        self.rel_path.to_string_lossy().into_owned()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("log directory {0} does not exist")]
    MissingDir(String),
    #[error("log directory {0} is not a directory")]
    NotADirectory(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Names that are documentation/metadata, not log events.
fn is_excluded_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    name.starts_with('.') // dot-files (.DS_Store, ...)
        || lower == "readme.md"
        || lower.ends_with("_label.txt")
        || lower.ends_with("_labels.txt")
}

/// Recursively discover eligible log files, sorted by relative path for
/// determinism. Unreadable entries are skipped without failing the walk.
/// Symlinks are not followed (loop safety).
pub fn discover(dir: &Path) -> Result<Vec<LogFile>, DiscoveryError> {
    let meta = fs::metadata(dir).map_err(DiscoveryError::Io)?;
    if !meta.is_dir() {
        return Err(DiscoveryError::NotADirectory(dir.display().to_string()));
    }
    let mut out = Vec::new();
    // Iterative traversal with (abs, rel) pairs; recursion depth is therefore
    // not a concern and symlink loops cannot occur since symlinks are skipped.
    let mut stack = vec![(dir.to_path_buf(), PathBuf::new())];
    while let Some((dir, rel)) = stack.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(e) => e,
            Err(e) => {
                eprintln!("warning: cannot read directory {}: {e}", dir.display());
                continue;
            }
        };
        for entry in entries {
            let entry = match entry {
                Ok(e) => e,
                Err(_) => continue,
            };
            let path = entry.path();
            let file_type = match entry.file_type() {
                Ok(t) => t,
                Err(_) => continue,
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            if file_type.is_dir() {
                stack.push((path, rel.join(&name)));
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            if is_excluded_name(&name) {
                continue;
            }
            let md = match entry.metadata() {
                Ok(m) => m,
                Err(_) => continue,
            };
            if md.len() == 0 {
                continue;
            }
            if !readable(&path) {
                eprintln!("warning: unreadable file skipped: {}", path.display());
                continue;
            }
            out.push(LogFile {
                path,
                rel_path: rel.join(&name),
                size: md.len(),
            });
        }
    }
    out.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(out)
}

#[cfg(unix)]
fn readable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    fs::metadata(path)
        .map(|md| md.permissions().mode() & 0o044 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn readable(path: &Path) -> bool {
    fs::File::open(path).is_ok()
}

/// Count lines by streaming the file in bounded chunks.
pub fn count_lines(path: &Path) -> std::io::Result<u64> {
    use std::io::{BufRead, BufReader};
    let f = fs::File::open(path)?;
    let reader = BufReader::with_capacity(256 * 1024, f);
    let count = reader
        .lines()
        .try_fold(0u64, |acc, _| Ok::<u64, std::io::Error>(acc + 1))?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn discovers_nested_files_sorted() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::create_dir_all(root.join("sub/deeper")).unwrap();
        fs::write(root.join("b.log"), "b\n").unwrap();
        fs::write(root.join("sub/a.log"), "a\n").unwrap();
        fs::write(root.join("sub/deeper/c.log"), "c\n").unwrap();
        let files = discover(root).unwrap();
        let names: Vec<String> = files.iter().map(|f| f.rel_str()).collect();
        assert_eq!(names, vec!["b.log", "sub/a.log", "sub/deeper/c.log"]);
    }

    #[test]
    fn excludes_dotfiles_readmes_labels_empty() {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path();
        fs::write(root.join(".DS_Store"), "junk").unwrap();
        fs::write(root.join("README.md"), "docs").unwrap();
        fs::write(root.join("anomaly_labels.txt"), "labels").unwrap();
        fs::write(root.join("empty.log"), "").unwrap();
        fs::write(root.join("real.log"), "data\n").unwrap();
        let files = discover(root).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].rel_str(), "real.log");
    }

    #[test]
    fn empty_dir_is_ok() {
        let tmp = TempDir::new().unwrap();
        assert_eq!(discover(tmp.path()).unwrap().len(), 0);
    }

    #[test]
    fn missing_dir_is_error() {
        match discover(Path::new("/nonexistent-dir-for-sure")) {
            Err(DiscoveryError::Io(_)) => {}
            other => panic!("expected IO error, got {other:?}"),
        }
    }

    #[test]
    fn count_lines_counts_trailing_newline_correctly() {
        let tmp = TempDir::new().unwrap();
        let p = tmp.path().join("x.log");
        std::fs::write(&p, "l1\nl2\nl3\n").unwrap();
        assert_eq!(count_lines(&p).unwrap(), 3);
        std::fs::write(&p, "l1\nl2").unwrap();
        assert_eq!(count_lines(&p).unwrap(), 2);
    }
}
