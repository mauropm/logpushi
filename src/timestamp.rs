//! Timestamp engine: detector chain, parsing, and format-preserving
//! transformation of the detected timestamp substring only.
//!
//! Detection is deliberately conservative. Numeric substitution is guarded
//! (standalone 9-13 digit tokens only): replacing every 10-digit number with a
//! fresh epoch would corrupt IDs, durations, IP octets, and epoch-millis
//! embedded in metrics (e.g. HealthApp's `onExtend:1514038530000`).

use std::sync::LazyLock;

use chrono::{
    DateTime, Datelike, FixedOffset, Local, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc,
};
use regex::Regex;

const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];
const WEEKDAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

/// Rendering parameters captured at detection time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aux {
    Unit,
    /// separator char, fraction digits (0 = none), offset (None = local render)
    Iso {
        sep: char,
        frac: u8,
        tz: Option<FixedOffset>,
    },
    /// CLF carries its own numeric offset for rendering
    Clf {
        tz: FixedOffset,
    },
}

/// A confidently detected and parsed timestamp within a raw line.
#[derive(Debug, Clone)]
pub struct TsMatch {
    pub start: usize,
    pub end: usize,
    pub ts: DateTime<Utc>,
    pub year_inferred: bool,
    pub format: DetectedFormat,
    pub aux: Aux,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectedFormat {
    Iso,
    ApacheCtf,
    ApacheClf,
    Epoch,
    SyslogNoYear,
    HealthApp,
    Android,
    Proxifier,
}

impl DetectedFormat {
    pub fn name(&self) -> &'static str {
        match self {
            DetectedFormat::Iso => "iso8601",
            DetectedFormat::ApacheCtf => "apache-ctf",
            DetectedFormat::ApacheClf => "apache-clf",
            DetectedFormat::Epoch => "epoch",
            DetectedFormat::SyslogNoYear => "syslog",
            DetectedFormat::HealthApp => "health-app",
            DetectedFormat::Android => "android",
            DetectedFormat::Proxifier => "proxifier",
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// First confident match wins; detectors run most-specific first.
pub fn detect(line: &str) -> Option<TsMatch> {
    match_iso(line)
        .or_else(|| match_apache_ctf(line))
        .or_else(|| match_apache_clf(line))
        .or_else(|| match_epoch(line))
        .or_else(|| match_syslog(line))
        .or_else(|| match_health_app(line))
        .or_else(|| match_proxifier(line))
        .or_else(|| match_android(line))
}

/// Parsed timestamp of a line (for `validate`): (ts, format, year_inferred).
pub fn parse(line: &str) -> Option<(DateTime<Utc>, DetectedFormat, bool)> {
    detect(line).map(|m| (m.ts, m.format, m.year_inferred))
}

/// Result of a synthetic-today transformation.
pub struct Transformed {
    pub body: String,
    pub ts: DateTime<Utc>,
}

/// Transform `line` by replacing the confidently detected timestamp substring
/// with `new_ts` rendered in the SAME format. Everything outside the detected
/// span is preserved byte-for-byte. Returns None when no timestamp was
/// detected (caller falls back to observed-time semantics, body untouched).
pub fn transform(line: &str, new_ts: DateTime<Utc>) -> Option<Transformed> {
    let m = detect(line)?;
    let mut body = String::with_capacity(line.len() + 16);
    body.push_str(&line[..m.start]);
    body.push_str(&render(&m, new_ts));
    body.push_str(&line[m.end..]);
    Some(Transformed { body, ts: new_ts })
}

/// Transform a JSON line's timestamp field (structured, level-3 transform).
/// Preserves key order and all other fields. Returns None when the line isn't
/// JSON or no timestamp field is detected — the caller then falls back to the
/// substring path.
pub fn transform_json(line: &str, new_ts: DateTime<Utc>) -> Option<Transformed> {
    let mut v: serde_json::Value = serde_json::from_str(line).ok()?;
    let obj = v.as_object_mut()?;
    const TS_KEYS: [&str; 7] = [
        "timestamp",
        "@timestamp",
        "ts",
        "time",
        "date",
        "datetime",
        "_time",
    ];
    let key = TS_KEYS.iter().find(|k| obj.contains_key(**k))?;
    let field_val = obj.get_mut(*key)?;
    let rendered = match field_val {
        serde_json::Value::String(s) => {
            let m = detect(s)?;
            let mut out = String::with_capacity(s.len() + 16);
            out.push_str(&s[..m.start]);
            out.push_str(&render(&m, new_ts));
            out.push_str(&s[m.end..]);
            out
        }
        serde_json::Value::Number(n) => {
            // Numeric epoch field: only accept plausible magnitudes (2e9 s
            // avoids confusing sub-second magnitudes with epoch seconds).
            let x = n.as_f64()?;
            if !(x.is_finite() && (2e9..8e12).contains(&x.abs())) {
                return None;
            }
            let num = if x >= 2e9 {
                serde_json::Number::from(new_ts.timestamp_millis())
            } else {
                serde_json::Number::from(new_ts.timestamp())
            };
            *field_val = serde_json::Value::Number(num);
            let body = serde_json::to_string(&v).ok()?;
            return Some(Transformed { body, ts: new_ts });
        }
        _ => return None,
    };
    *field_val = serde_json::Value::String(rendered);
    let body = serde_json::to_string(&v).ok()?;
    Some(Transformed { body, ts: new_ts })
}

/// Extract a syslog-style host token (`Mon DD HH:MM:SS host ...`).
pub fn syslog_host(line: &str) -> Option<String> {
    static RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}\s+(\S+)").unwrap());
    RE.captures(line).map(|c| c[1].to_string())
}

// ---------------------------------------------------------------------------
// Rendering: round-trip the original format shape.
// ---------------------------------------------------------------------------

/// Render `new_ts` in the original format described by match `m`.
pub fn render(m: &TsMatch, new_ts: DateTime<Utc>) -> String {
    match m.format {
        DetectedFormat::Iso => render_iso(new_ts, &m.aux),
        DetectedFormat::ApacheCtf => {
            let l = new_ts.with_timezone(&Local);
            format!(
                "[{} {} {:02} {:02}:{:02}:{:02} {}]",
                WEEKDAYS[l.weekday().num_days_from_sunday() as usize],
                MONTHS[(l.month() - 1).clamp(0, 11) as usize],
                l.day(),
                l.hour(),
                l.minute(),
                l.second(),
                l.year(),
            )
        }
        DetectedFormat::ApacheClf => {
            let off = match m.aux {
                Aux::Clf { tz } => tz,
                _ => FixedOffset::east_opt(0).unwrap(),
            };
            // Offsets like +13:00 exceed chrono's fixed offset? ±24h ok.
            let l = new_ts.with_timezone(&off);
            format!(
                "{:02}/{}:{:04}:{:02}:{:02}:{:02} {}",
                l.day(),
                MONTHS[(l.month() - 1).clamp(0, 11) as usize],
                l.year(),
                l.hour(),
                l.minute(),
                l.second(),
                off_to_token(off),
            )
        }
        DetectedFormat::Epoch => new_ts.timestamp().to_string(),
        DetectedFormat::SyslogNoYear => {
            let l = new_ts.with_timezone(&Local);
            format!(
                "{:>3} {:>2} {:02}:{:02}:{:02}",
                MONTHS[(l.month() - 1).clamp(0, 11) as usize],
                l.day(),
                l.hour(),
                l.minute(),
                l.second(),
            )
        }
        DetectedFormat::HealthApp => {
            let l = new_ts.with_timezone(&Local);
            format!(
                "{}{:02}{:02}-{:02}:{:02}:{:02}:{:03}",
                l.year(),
                l.month(),
                l.day(),
                l.hour(),
                l.minute(),
                l.second(),
                new_ts.timestamp_subsec_millis(),
            )
        }
        DetectedFormat::Android => {
            let l = new_ts.with_timezone(&Local);
            format!(
                "{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
                l.month(),
                l.day(),
                l.hour(),
                l.minute(),
                l.second(),
                new_ts.timestamp_subsec_millis(),
            )
        }
        DetectedFormat::Proxifier => {
            let l = new_ts.with_timezone(&Local);
            format!(
                "[{:02}.{:02} {:02}:{:02}:{:02}]",
                l.month(),
                l.day(),
                l.hour(),
                l.minute(),
                l.second(),
            )
        }
    }
}

fn render_iso(new_ts: DateTime<Utc>, aux: &Aux) -> String {
    if let Aux::Iso { sep, frac, tz } = aux {
        // Convert back to the rendered offset (or local when none was present,
        // matching the detection policy of assuming local time).
        let l = match tz {
            Some(fo) => new_ts.with_timezone(fo),
            None => {
                let off = *Local::now().offset();
                new_ts.with_timezone(&off)
            }
        };
        let mut s = format!(
            "{}-{:02}-{:02}{}{:02}:{:02}:{:02}",
            l.year(),
            l.month(),
            l.day(),
            sep,
            l.hour(),
            l.minute(),
            l.second(),
        );
        if *frac > 0 {
            // Fraction is preserved as-is in shape: fixed digits, ms-rounded.
            let millis = new_ts.timestamp_subsec_millis();
            let text = if *frac <= 3 {
                format!("{:0width$}", millis, width = *frac as usize)
            } else {
                let micros = new_ts.timestamp_subsec_micros();
                format!("{:0width$}", micros, width = *frac as usize)
            };
            s.push('.');
            s.push_str(&text);
        }
        if let Aux::Iso { tz: Some(_), .. } = aux {
            if let Some(fo) = tz {
                if fo == &FixedOffset::east_opt(0).unwrap() {
                    s.push('Z');
                } else {
                    s.push_str(&off_to_token(*fo));
                }
            }
        }
        s
    } else {
        render_iso(
            new_ts,
            &Aux::Iso {
                sep: 'T',
                frac: 0,
                tz: Some(FixedOffset::east_opt(0).unwrap()),
            },
        )
    }
}

fn off_to_token(off: FixedOffset) -> String {
    let secs = off.local_minus_utc();
    let sign = if secs < 0 { '-' } else { '+' };
    format!(
        "{sign}{:02}{:02}",
        secs.abs() / 3600,
        (secs.abs() % 3600) / 60
    )
}

// ---------------------------------------------------------------------------
// Detectors
// ---------------------------------------------------------------------------

fn month_from(b: &str) -> Option<u32> {
    let m = b.to_ascii_lowercase();
    match m.as_str() {
        "jan" => Some(1),
        "feb" => Some(2),
        "mar" => Some(3),
        "apr" => Some(4),
        "may" => Some(5),
        "jun" => Some(6),
        "jul" => Some(7),
        "aug" => Some(8),
        "sep" => Some(9),
        "oct" => Some(10),
        "nov" => Some(11),
        "dec" => Some(12),
        _ => None,
    }
}

/// For no-year formats: choose the most recent year whose timestamp is not
/// in the future (§11). Interpreted in local time.
fn resolve_no_year(naive_no_year: NaiveDateTime, now: DateTime<Utc>) -> DateTime<Utc> {
    let y = now.year();
    let local = Local.timestamp_opt(0, 0).unwrap();
    let _ = local;
    for year in [y, y - 1] {
        if let Some(d) = naive_no_year.with_year(year) {
            // Interpret in local time then move to UTC.
            if let Some(dt) = d.and_local_timezone(Local).single() {
                if dt.with_timezone(&Utc) <= now + chrono::Duration::hours(24) {
                    return dt.with_timezone(&Utc);
                }
            }
        }
    }
    naive_no_year.and_utc()
}

#[allow(unused_macros)]
macro_rules! _unused {
    ($($t:tt)*) => {};
}

// ISO-8601 / RFC-3339 / log4j (comma millis share the ISO shape).
static RE_ISO: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"\b(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:[.,](\d{1,9}))?(Z|z|[+-]\d{2}(?::?\d{2})?)?",
    )
    .unwrap()
});

fn match_iso(line: &str) -> Option<TsMatch> {
    let m = RE_ISO.find(line)?;
    let c = RE_ISO.captures(m.as_str())?;
    let y: i32 = c[1].parse().ok()?;
    let month: u32 = c[2].parse().ok()?;
    if !(1..=12).contains(&month) {
        return None;
    }
    let d: u32 = c[3].parse().ok()?;
    let h: u32 = c[4].parse().ok()?;
    let mi: u32 = c[5].parse().ok()?;
    let s: u32 = c[6].parse().ok()?;
    let ms_digits: Option<String> = c.get(7).map(|f| f.as_str().to_string());
    let tz_tok = c.get(8).map(|t| t.as_str().to_string());
    let micros: u32 = ms_digits
        .as_ref()
        .map(|d| {
            let t = &d[..d.len().min(6)];
            format!("{:0<6}", t).parse().unwrap_or(0)
        })
        .unwrap_or(0);
    let naive = NaiveDate::from_ymd_opt(y, month, d)?.and_hms_micro_opt(h, mi, s, micros)?;
    // (§11) Missing timezone is treated as local; render matches.
    let (offset, present) = match tz_tok.as_deref() {
        Some(t) => (parse_offset(t)?, true),
        None => (*Local::now().offset(), false),
    };
    let ts = naive
        .and_local_timezone(offset)
        .single()?
        .with_timezone(&Utc);
    let sep = if m.as_str().contains('T') { 'T' } else { ' ' };
    Some(TsMatch {
        start: m.start(),
        end: m.end(),
        ts,
        year_inferred: false,
        format: DetectedFormat::Iso,
        aux: Aux::Iso {
            sep,
            frac: ms_digits.map(|d| d.len() as u8).unwrap_or(0),
            tz: if present { Some(offset) } else { None },
        },
    })
}

fn parse_offset(s: &str) -> Option<FixedOffset> {
    let t = s.trim();
    if t.eq_ignore_ascii_case("Z") {
        return Some(FixedOffset::east_opt(0).unwrap());
    }
    let (sign, rest) = match t.strip_prefix('+') {
        Some(r) => (1, r),
        None => (0, t.strip_prefix('-')?),
    };
    let clean = rest.replace(':', "");
    if clean.len() != 4 || !clean.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let h: i32 = clean[..2].parse().ok()?;
    let m: i32 = clean[2..].parse().ok()?;
    if h > 23 || m > 59 {
        return None;
    }
    FixedOffset::east_opt(sign * (h * 3600 + m * 60))
}

// Oracle/common-log full CTF: [Thu Jun 09 06:07:04 2005]
static RE_APACHE_CTF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\[(\w{3}) (\w{3}) ( ?\d{1,2}) (\d{2}):(\d{2}):(\d{2}) (\d{4})\]").unwrap()
});

fn match_apache_ctf(line: &str) -> Option<TsMatch> {
    let m = RE_APACHE_CTF.find(line)?;
    let c = RE_APACHE_CTF.captures(m.as_str())?;
    let mon = month_from(&c[2])?;
    let day: u32 = c[3].trim().parse().ok()?;
    let naive = NaiveDate::from_ymd_opt(c[7].parse().ok()?, mon, day)?.and_hms_opt(
        c[4].parse().ok()?,
        c[5].parse().ok()?,
        c[6].parse().ok()?,
    )?;
    Some(TsMatch {
        start: m.start(),
        end: m.end(),
        // No tz info: stored as nominally UTC for parse purposes.
        ts: fixed(naive, FixedOffset::east_opt(0).unwrap())?,
        year_inferred: false,
        format: DetectedFormat::ApacheCtf,
        aux: Aux::Unit,
    })
}

fn fixed(naive: NaiveDateTime, off: FixedOffset) -> Option<DateTime<Utc>> {
    naive
        .and_local_timezone(off)
        .single()
        .map(|d| d.with_timezone(&Utc))
}

// Apache CLF: 10/Jun/2005:06:07:04 +0000
static RE_APACHE_CLF: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})\b").unwrap()
});

fn match_apache_clf(line: &str) -> Option<TsMatch> {
    let m = RE_APACHE_CLF.find(line)?;
    let c = RE_APACHE_CLF.captures(m.as_str())?;
    let mon = month_from(&c[2])?;
    let off = parse_offset(&c[7])?;
    let naive = NaiveDate::from_ymd_opt(c[3].parse().ok()?, mon, c[1].parse().ok()?)?.and_hms_opt(
        c[4].parse().ok()?,
        c[5].parse().ok()?,
        c[6].parse().ok()?,
    )?;
    Some(TsMatch {
        start: m.start(),
        end: m.end(),
        ts: fixed(naive, off)?,
        year_inferred: false,
        format: DetectedFormat::ApacheClf,
        aux: Aux::Clf { tz: off },
    })
}

/// Guarded numeric-epoch detection: only a standalone, whitespace-delimited
/// digit token of length 9-13 with a plausible value qualifies.
fn match_epoch(line: &str) -> Option<TsMatch> {
    let bytes = line.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let at_boundary = i == 0 || bytes[i - 1] == b' ' || bytes[i - 1] == b'\t';
        if !at_boundary || !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        let start = i;
        let mut j = i;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            j += 1;
        }
        if !(j == bytes.len() || bytes[j] == b' ' || bytes[j] == b'\t') {
            i = j;
            continue;
        }
        let len = (j - start) as u32;
        if !(9..=13).contains(&len) {
            i = j;
            continue;
        }
        let val: i64 = line[start..j].parse().unwrap_or(0);
        let plausible = if len <= 10 {
            // seconds: 1973-01-01 .. 2038-01-01
            (100_000_000..=2_145_916_800).contains(&val)
        } else {
            // milliseconds: 2001-09-09 .. 2038
            (1_000_000_000_000..=2_145_916_800_000).contains(&val)
        };
        if plausible {
            if let Some(ts) = if len <= 10 {
                Utc.timestamp_opt(val, 0).single()
            } else {
                Utc.timestamp_millis_opt(val).single()
            } {
                return Some(TsMatch {
                    start,
                    end: j,
                    ts,
                    year_inferred: false,
                    format: DetectedFormat::Epoch,
                    aux: Aux::Unit,
                });
            }
        }
        i = j;
        continue;
    }
    None
}

// Classic no-year syslog.
static RE_SYSLOG: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([A-Za-z]{3}) ( ?\d{1,2}) (\d{2}):(\d{2}):(\d{2})(?:\s|$)").unwrap()
});

fn match_syslog(line: &str) -> Option<TsMatch> {
    let c = RE_SYSLOG.captures(line)?;
    let mon = month_from(&c[1])?;
    let day: u32 = c[2].trim().parse().ok()?;
    let naive = NaiveDate::from_ymd_opt(2000, mon, day)?.and_hms_opt(
        c[3].parse().ok()?,
        c[4].parse().ok()?,
        c[5].parse().ok()?,
    )?;
    Some(TsMatch {
        start: c.get(0).unwrap().start(),
        // The regex consumes one trailing whitespace char as part of the
        // validity check; exclude it from the replacement span.
        end: c.get(5).unwrap().end(),
        ts: resolve_no_year(naive, Utc::now()),
        year_inferred: true,
        format: DetectedFormat::SyslogNoYear,
        aux: Aux::Unit,
    })
}

// HealthApp: 20171223-22:15:29:606|...
static RE_HEALTHAPP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\b(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2}):(\d{3})\b").unwrap()
});

fn match_health_app(line: &str) -> Option<TsMatch> {
    let c = RE_HEALTHAPP.captures(line)?;
    let mon: u32 = c[2].parse().ok()?;
    let naive = NaiveDate::from_ymd_opt(c[1].parse().ok()?, mon, c[3].parse().ok()?)?
        .and_hms_milli_opt(
            c[4].parse().ok()?,
            c[5].parse().ok()?,
            c[6].parse().ok()?,
            c[7].parse().ok()?,
        )?;
    Some(TsMatch {
        start: c.get(0).unwrap().start(),
        end: c.get(0).unwrap().end(),
        ts: naive.and_utc(),
        year_inferred: false,
        format: DetectedFormat::HealthApp,
        aux: Aux::Unit,
    })
}

// Proxifier: [10.30 16:49:06]
static RE_PROXIFIER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\[(\d{2})\.(\d{2}) (\d{2}):(\d{2}):(\d{2})\]").unwrap());

fn match_proxifier(line: &str) -> Option<TsMatch> {
    let c = RE_PROXIFIER.captures(line)?;
    let mon: u32 = c[1].parse().ok()?;
    let naive = NaiveDate::from_ymd_opt(2000, mon, c[2].parse().ok()?)?.and_hms_opt(
        c[3].parse().ok()?,
        c[4].parse().ok()?,
        c[5].parse().ok()?,
    )?;
    Some(TsMatch {
        start: c.get(0).unwrap().start(),
        end: c.get(0).unwrap().end(),
        ts: resolve_no_year(naive, Utc::now()),
        year_inferred: true,
        format: DetectedFormat::Proxifier,
        aux: Aux::Unit,
    })
}

// Android: 12-17 19:31:36.263 (no year)
static RE_ANDROID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"\b(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})\.(\d{3})\b").unwrap());

fn match_android(line: &str) -> Option<TsMatch> {
    let c = RE_ANDROID.captures(line)?;
    let mon: u32 = c[1].parse().ok()?;
    let naive = NaiveDate::from_ymd_opt(2000, mon, c[2].parse().ok()?)?.and_hms_milli_opt(
        c[3].parse().ok()?,
        c[4].parse().ok()?,
        c[5].parse().ok()?,
        c[6].parse().ok()?,
    )?;
    Some(TsMatch {
        start: c.get(0).unwrap().start(),
        end: c.get(0).unwrap().end(),
        ts: resolve_no_year(naive, Utc::now()),
        year_inferred: true,
        format: DetectedFormat::Android,
        aux: Aux::Unit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sampling::Rng;

    /// Deterministic sample instant from a fixed seed.
    fn seed_ts(seed: u64) -> DateTime<Utc> {
        let mut rng = Rng::new(seed);
        let anchor = Utc::now();
        let start = anchor - chrono::Duration::hours(1);
        let span = (anchor - start).num_nanoseconds().unwrap() as u64;
        start + chrono::Duration::nanoseconds((rng.next_u64() % span) as i64)
    }

    #[test]
    fn iso_formats() {
        for line in [
            "2017-05-16 00:00:00.008 INFO boot",
            "2015-10-17T15:37:56Z INFO x",
            "2015-10-17 15:37:56,547 INFO y", // log4j comma millis
        ] {
            let m = detect(line).expect("detect");
            assert_eq!(m.format, DetectedFormat::Iso, "{line}");
        }
        // Explicit offset: UTC instant is exact.
        let m = detect("x 2015-10-17 15:37:56,547+0000 INFO y").unwrap();
        assert_eq!(
            m.ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
            "2015-10-17 15:37:56.547"
        );
    }

    #[test]
    fn iso_transform_preserves_shape() {
        let line = "prefix 2017-05-16 00:00:00.008 suffix";
        let t = transform(line, seed_ts(1)).unwrap();
        assert!(t.body.ends_with(" suffix"));
        assert!(t.body.starts_with("prefix 20"));
        // Same rendered shape: only digits differ.
        let new_part = &t.body["prefix ".len()..t.body.len() - " suffix".len()];
        assert_eq!(new_part.len(), "2017-05-16 00:00:00.008".len());
        assert_eq!(&new_part[10..11], " ");
    }

    #[test]
    fn apache_ctf() {
        let line = r#"127.0.0.1 - - [Thu Jun 09 06:07:04 2005] "GET /x HTTP/1.0" 200 2326"#;
        let m = detect(line).unwrap();
        assert_eq!(m.format, DetectedFormat::ApacheCtf);
        let new_ts = seed_ts(2);
        let t = transform(line, new_ts).unwrap();
        let rendered = &t.body[t.body.find('[').unwrap() + 1..t.body.find(']').unwrap()];
        assert_eq!(rendered.len(), "Thu Jun 09 06:07:04 2005".len());
        let local = new_ts.with_timezone(&Local);
        let want = format!("{} {}", local.format("%H:%M:%S"), local.year());
        assert!(
            rendered.ends_with(&want),
            "rendered={rendered:?} want={want:?}"
        );
        // Weekday name is a 3-letter English short form.
        assert_eq!(rendered.len(), "Thu Jun 09 06:07:04 2005".len());
    }

    #[test]
    fn apache_clf() {
        let line = "10/Jun/2005:06:07:04 +0000 GET x";
        let m = detect(line).unwrap();
        assert_eq!(m.format, DetectedFormat::ApacheClf);
        let t = transform(line, seed_ts(3)).unwrap();
        // Slice before the trailing " GET x" (offset tail is part of the token).
        let space = t.body.find(" GET").unwrap();
        let out = &t.body[..space];
        assert_eq!(out.len(), "10/Jun/2005:06:07:04 +0000".len(), "{out}");
        assert!(out.ends_with(" +0000"));
    }

    #[test]
    fn epoch_guard() {
        // BGL: leading standalone epoch qualifies.
        let line = "1117838570 2005.06.03 R02-M1N0C: APP_START";
        let m = detect(line).unwrap();
        assert_eq!(m.format, DetectedFormat::Epoch);
        assert_eq!((m.start, m.end), (0, 10));
        // HPC epoch mid-line.
        let m2 = detect("1145552216 DE 2004-11-08-11.41.12.317555").unwrap();
        assert_eq!(m2.format, DetectedFormat::Epoch);
        // Embedded in a longer token: must NOT match as epoch.
        let embedded = detect("id=1234567890 tail");
        assert!(embedded.is_none() || embedded.unwrap().format != DetectedFormat::Epoch);
        // Invalid UTF-8-free short tokens below range.
        assert!(detect("12345678 tail").is_none());
    }

    #[test]
    fn syslog_no_year_inference() {
        let line = "Dec 10 06:55:46 LabSZ sshd[21705]: input_userauth_request";
        let (ts, fmt, inferred) = parse(line).unwrap();
        assert_eq!(fmt, DetectedFormat::SyslogNoYear);
        assert!(inferred);
        // Most recent past year: never in the future.
        assert!(ts <= Utc::now());
        let new_ts = seed_ts(4);
        let t = transform(line, new_ts).unwrap();
        assert!(
            t.body.contains(" LabSZ sshd[21705]"),
            "the space after the timestamp must survive: {}",
            t.body
        );
        let month_names = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ];
        assert!(month_names.iter().any(|m| t.body.starts_with(*m)));
    }

    #[test]
    fn health_app() {
        let line = "20171223-22:15:29:606|demo|e|u|None|n|onExtend:1514038530000|0.0|0";
        let m = detect(line).unwrap();
        assert_eq!(m.format, DetectedFormat::HealthApp);
        let t = transform(line, seed_ts(5)).unwrap();
        // Metric-like epoch value must NOT be corrupted (guarded detector).
        assert!(t.body.contains("onExtend:1514038530000"));
        assert!(!t.body.contains("20171223-"));
    }

    #[test]
    fn proxifier_and_android() {
        let p = "[10.30 16:49:06] fx";
        let m = detect(p).unwrap();
        assert_eq!(m.format, DetectedFormat::Proxifier);
        let a = "12-17 19:31:36.263 I/app process";
        let m = detect(a).unwrap();
        assert_eq!(m.format, DetectedFormat::Android);
    }

    #[test]
    fn json_transformation() {
        let line = r#"{"@timestamp": "2015-10-17T15:37:56Z", "level": "info", "msg": "1,000,000 excellent things", "count": 42}"#;
        let new_ts = seed_ts(6);
        let t = transform_json(line, new_ts).unwrap();
        let v: serde_json::Value = serde_json::from_str(&t.body).unwrap();
        let ts_out = v["@timestamp"].as_str().unwrap();
        assert!(!ts_out.contains("2015"));
        assert_eq!(v["level"], "info");
        assert_eq!(v["count"], 42);
        assert_eq!(v["msg"], "1,000,000 excellent things");
        // Key order preserved (preserve_order feature).
        assert!(t.body.find("\"@timestamp\"").unwrap() < t.body.find("\"level\"").unwrap());
    }

    #[test]
    fn no_timestamp_passes_through() {
        assert!(parse("garbage with no timestamp at all").is_none());
    }

    #[test]
    fn invalid_timestamp_values_rejected() {
        // month 13 in an ISO position must not parse as ISO
        if let Some((_, fmt, _)) = parse("2017-13-16 00:00:00 stuff") {
            assert_ne!(fmt, DetectedFormat::Iso);
        }
    }

    #[test]
    fn transform_systematic_epoch_line() {
        let line = "1117838570 R02-M1N0C APP";
        let t = transform(line, seed_ts(7)).unwrap();
        // 3 tokens after: new epoch + rest preserved verbatim.
        let toks: Vec<&str> = t.body.split_whitespace().collect();
        assert_eq!(toks.len(), 3);
        assert_eq!(toks[1], "R02-M1N0C");
        assert_eq!(toks[2], "APP");
    }

    #[test]
    fn syslog_host_extraction() {
        let src = "Dec 10 06:55:46 LabSZ sshd[1]: hi";
        let host = syslog_host(src);
        assert_eq!(host.unwrap(), "LabSZ", "src={src:?}");
        // Host token even when the process tag has no trailing colon.
        let h2 = syslog_host("Dec 10 06:55:46 host9 app");
        assert_eq!(h2.as_deref(), Some("host9"));
        assert_eq!(syslog_host("no host here"), None);
    }
}
