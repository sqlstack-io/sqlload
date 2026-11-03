use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;

use crate::batching::models::BufferGauge;
use crate::http::models::{ConnectionCounter, InFlight};

/// The stream label every per-stream metric carries.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StreamLabel {
    pub stream: String,
}

/// Every exported metric.
#[derive(Debug)]
pub struct Metrics {
    pub registry: Registry,
    pub rows: Family<StreamLabel, Counter>,
    pub uncompressed_bytes: Family<StreamLabel, Counter>,
    pub sent_bytes: Family<StreamLabel, Counter>,
    pub requests: Family<StreamLabel, Counter>,
    pub batches: Family<StreamLabel, Counter>,
    pub delivery_errors: Family<StreamLabel, Counter>,
    pub skipped_rows: Family<StreamLabel, Counter>,
    pub error_rows: Family<StreamLabel, Counter>,
    pub last_delivery: Family<StreamLabel, Gauge>,
    pub connections: Gauge,
    pub buffered_bytes: Gauge,
    pub in_flight_wait: Gauge,
    counter: ConnectionCounter,
    gauge: BufferGauge,
    in_flight: InFlight,
}

impl Metrics {
    pub fn new(counter: ConnectionCounter, gauge: BufferGauge, in_flight: InFlight) -> Self {
        let mut registry = Registry::with_prefix("sqlload");
        let family = |registry: &mut Registry, name: &str, help: &str| {
            let metric = Family::<StreamLabel, Counter>::default();
            registry.register(name, help, metric.clone());
            metric
        };
        let rows = family(&mut registry, "rows", "Rows delivered");
        let uncompressed_bytes = family(
            &mut registry,
            "uncompressed_bytes",
            "NDJSON bytes delivered before compression",
        );
        let sent_bytes = family(&mut registry, "sent_bytes", "Compressed bytes sent");
        let requests = family(&mut registry, "requests", "Append requests or files sent");
        let batches = family(&mut registry, "batches", "Batches delivered");
        let delivery_errors = family(&mut registry, "delivery_errors", "Failed delivery attempts");
        let skipped_rows = family(
            &mut registry,
            "skipped_rows",
            "Rows skipped by the oversized policy",
        );
        let error_rows = family(
            &mut registry,
            "error_rows",
            "Rows Snowflake rejected into the error table",
        );
        let last_delivery = Family::<StreamLabel, Gauge>::default();
        registry.register(
            "last_delivery_timestamp_seconds",
            "Unix time of the last successful delivery",
            last_delivery.clone(),
        );
        let connections = Gauge::default();
        registry.register(
            "connections_opened",
            "New HTTP connections opened since start",
            connections.clone(),
        );
        let buffered_bytes = Gauge::default();
        registry.register(
            "buffered_bytes",
            "Bytes buffered across all streams",
            buffered_bytes.clone(),
        );
        let in_flight_wait = Gauge::default();
        registry.register(
            "in_flight_wait_milliseconds",
            "Time uploads and loads waited for a staging.max_in_flight slot since start",
            in_flight_wait.clone(),
        );
        Self {
            registry,
            rows,
            uncompressed_bytes,
            sent_bytes,
            requests,
            batches,
            delivery_errors,
            skipped_rows,
            error_rows,
            last_delivery,
            connections,
            buffered_bytes,
            in_flight_wait,
            counter,
            gauge,
            in_flight,
        }
    }

    /// Refresh gauges read from shared counters.
    pub fn refresh(&self) {
        self.connections.set(self.counter.get() as i64);
        self.buffered_bytes.set(self.gauge.get() as i64);
        self.in_flight_wait
            .set(i64::try_from(self.in_flight.waited_ms()).unwrap_or(i64::MAX));
    }

    /// The label for a stream.
    pub fn label(stream: &str) -> StreamLabel {
        StreamLabel {
            stream: stream.to_owned(),
        }
    }

    /// Text exposition of every metric.
    pub fn encode(&self) -> String {
        self.refresh();
        let mut output = String::new();
        let _ = prometheus_client::encoding::text::encode(&mut output, &self.registry);
        output
    }

    /// Total rows delivered across streams, for run summaries.
    pub fn total_rows(&self, streams: &[String]) -> u64 {
        streams
            .iter()
            .map(|stream| self.rows.get_or_create(&Self::label(stream)).get())
            .sum()
    }
}
