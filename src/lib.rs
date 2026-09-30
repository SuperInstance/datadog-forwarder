//! Datadog log/metric forwarder.
//!
//! Buffers logs and metrics, batches them, and sends to the Datadog
//! intake API as JSON over HTTPS.

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Datadog log entry.
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub message: String,
    pub timestamp: i64,
    pub level: LogLevel,
    pub service: String,
    pub source: String,
    pub tags: Vec<String>,
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogLevel {
    Emergency,
    Alert,
    Critical,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Emergency => "EMERGENCY",
            Self::Alert => "ALERT",
            Self::Critical => "CRITICAL",
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
            Self::Trace => "TRACE",
        }
    }
}

/// Datadog metric point.
#[derive(Debug, Clone)]
pub struct MetricPoint {
    pub metric: String,
    pub value: f64,
    pub timestamp: i64,
    pub tags: Vec<String>,
    pub metric_type: MetricType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MetricType {
    Gauge,
    Count,
    Rate,
}

/// Event for the Datadog event stream.
#[derive(Debug, Clone)]
pub struct Event {
    pub title: String,
    pub text: String,
    pub timestamp: i64,
    pub priority: Priority,
    pub tags: Vec<String>,
    pub alert_type: AlertType,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Priority {
    Normal,
    Low,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AlertType {
    Info,
    Warning,
    Error,
    Success,
}

/// Configuration for the forwarder.
pub struct ForwarderConfig {
    pub api_key: String,
    pub site: String,
    pub batch_size: usize,
    pub flush_interval_secs: u64,
}

impl Default for ForwarderConfig {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            site: "datadoghq.com".into(),
            batch_size: 100,
            flush_interval_secs: 5,
        }
    }
}

/// Buffered forwarder for logs, metrics, and events.
pub struct DatadogForwarder {
    config: ForwarderConfig,
    logs: Mutex<Vec<LogEntry>>,
    metrics: Mutex<Vec<MetricPoint>>,
    events: Mutex<Vec<Event>>,
    sent_count: Mutex<usize>,
}

impl DatadogForwarder {
    pub fn new(config: ForwarderConfig) -> Self {
        Self {
            config,
            logs: Mutex::new(Vec::new()),
            metrics: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
            sent_count: Mutex::new(0),
        }
    }

    // --- Log API ---
    pub fn log(&self, message: impl Into<String>, level: LogLevel) {
        self.log_with(message, level, "default-service", "default");
    }

    pub fn log_with(&self, message: impl Into<String>, level: LogLevel, service: &str, source: &str) {
        self.logs.lock().unwrap().push(LogEntry {
            message: message.into(),
            timestamp: now_epoch(),
            level,
            service: service.into(),
            source: source.into(),
            tags: Vec::new(),
            hostname: None,
        });
    }

    pub fn log_tagged(&self, message: impl Into<String>, level: LogLevel, tags: Vec<String>) {
        let mut entry = LogEntry {
            message: message.into(),
            timestamp: now_epoch(),
            level,
            service: "default-service".into(),
            source: "default".into(),
            tags,
            hostname: None,
        };
        // extract dd.service / dd.source from tags if present
        for t in &entry.tags {
            if let Some(v) = t.strip_prefix("dd.service:") {
                entry.service = v.into();
            } else if let Some(v) = t.strip_prefix("dd.source:") {
                entry.source = v.into();
            }
        }
        self.logs.lock().unwrap().push(entry);
    }

    // --- Metric API ---
    pub fn gauge(&self, metric: impl Into<String>, value: f64, tags: Vec<String>) {
        self.metrics.lock().unwrap().push(MetricPoint {
            metric: metric.into(),
            value,
            timestamp: now_epoch(),
            tags,
            metric_type: MetricType::Gauge,
        });
    }

    pub fn count(&self, metric: impl Into<String>, value: f64, tags: Vec<String>) {
        self.metrics.lock().unwrap().push(MetricPoint {
            metric: metric.into(),
            value,
            timestamp: now_epoch(),
            tags,
            metric_type: MetricType::Count,
        });
    }

    // --- Event API ---
    pub fn event(&self, title: impl Into<String>, text: impl Into<String>, alert_type: AlertType) {
        self.events.lock().unwrap().push(Event {
            title: title.into(),
            text: text.into(),
            timestamp: now_epoch(),
            priority: Priority::Normal,
            tags: Vec::new(),
            alert_type,
        });
    }

    // --- Flush / Serialize ---

    /// Serialize buffered logs to JSON payload for Datadog Log Intake.
    pub fn serialize_logs(&self) -> String {
        let logs = std::mem::take(&mut *self.logs.lock().unwrap());
        let items: Vec<String> = logs.iter().map(|l| {
            let tags_str = l.tags.iter().map(|t| format!("\"{}\"", escape_json(t))).collect::<Vec<_>>().join(",");
            format!(
                r#"{{"message":"{}","timestamp":{},"level":"{}","service":"{}","ddsource":"{}","ddtags":[{}]}}"#,
                escape_json(&l.message), l.timestamp, l.level.as_str(),
                escape_json(&l.service), escape_json(&l.source), tags_str
            )
        }).collect();
        format!("[{}]", items.join(","))
    }

    /// Serialize buffered metrics to JSON for Datadog Metric API.
    pub fn serialize_metrics(&self) -> String {
        let metrics = std::mem::take(&mut *self.metrics.lock().unwrap());
        let series: Vec<String> = metrics.iter().map(|m| {
            let tags_str = m.tags.iter().map(|t| format!("\"{}\"", escape_json(t))).collect::<Vec<_>>().join(",");
            let mtype = match m.metric_type {
                MetricType::Gauge => "gauge",
                MetricType::Count => "count",
                MetricType::Rate => "rate",
            };
            format!(
                r#"{{"metric":"{}","points":[[{},{}]],"type":"{}","tags":[{}]}}"#,
                escape_json(&m.metric), m.timestamp, m.value, mtype, tags_str
            )
        }).collect();
        format!("{{\"series\":[{}]}}", series.join(","))
    }

    /// Serialize buffered events to JSON.
    pub fn serialize_events(&self) -> String {
        let events = std::mem::take(&mut *self.events.lock().unwrap());
        let items: Vec<String> = events.iter().map(|e| {
            let atype = match e.alert_type {
                AlertType::Info => "info",
                AlertType::Warning => "warning",
                AlertType::Error => "error",
                AlertType::Success => "success",
            };
            format!(
                r#"{{"title":"{}","text":"{}","date_happened":{},"priority":"normal","alert_type":"{}"}}"#,
                escape_json(&e.title), escape_json(&e.text), e.timestamp, atype
            )
        }).collect();
        format!("[{}]", items.join(","))
    }

    /// Simulate flush — returns count of items serialized.
    pub fn flush(&self) -> FlushResult {
        let log_json = self.serialize_logs();
        let metric_json = self.serialize_metrics();
        let event_json = self.serialize_events();
        let n = log_json.len() + metric_json.len() + event_json.len();
        *self.sent_count.lock().unwrap() += n;
        FlushResult { log_json, metric_json, event_json }
    }

    pub fn pending_log_count(&self) -> usize { self.logs.lock().unwrap().len() }
    pub fn pending_metric_count(&self) -> usize { self.metrics.lock().unwrap().len() }
    pub fn pending_event_count(&self) -> usize { self.events.lock().unwrap().len() }
}

pub struct FlushResult {
    pub log_json: String,
    pub metric_json: String,
    pub event_json: String,
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

fn now_epoch() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_and_flush() {
        let fwd = DatadogForwarder::new(ForwarderConfig::default());
        fwd.log("hello world", LogLevel::Info);
        fwd.log_tagged("tagged msg", LogLevel::Error, vec!["env:prod".into(), "dd.service:api".into()]);
        assert_eq!(fwd.pending_log_count(), 2);
        let result = fwd.flush();
        assert!(result.log_json.contains("hello world"));
        assert!(result.log_json.contains("api"));
        assert_eq!(fwd.pending_log_count(), 0);
    }

    #[test]
    fn metrics_roundtrip() {
        let fwd = DatadogForwarder::new(ForwarderConfig::default());
        fwd.gauge("cpu.percent", 72.5, vec!["host:web1".into()]);
        fwd.count("http.requests", 1.0, vec![]);
        let result = fwd.flush();
        assert!(result.metric_json.contains("cpu.percent"));
        assert!(result.metric_json.contains("gauge"));
    }

    #[test]
    fn event_roundtrip() {
        let fwd = DatadogForwarder::new(ForwarderConfig::default());
        fwd.event("Deploy", "Deployed v2.0", AlertType::Success);
        let result = fwd.flush();
        assert!(result.event_json.contains("success"));
    }
}

/// FNV-1a 64 — the digest every substrate in the SuperInstance fleet agrees on.
pub const FNV_OFFSET: u64 = 0xcbf29ce484222325;
pub const FNV_PRIME: u64 = 0x100000001b3;

#[inline]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    for &b in bytes {
        h = (h ^ b as u64).wrapping_mul(FNV_PRIME);
    }
    h
}

/// True if this crate's FNV-1a still agrees with the rest of the fleet.
pub fn canary_holds() -> bool {
    fnv1a64("café Δ 日本語".as_bytes()) == 0x024a555471370b18d
}
