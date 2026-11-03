use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

use crate::fake_snowflake::_helpers::loading::{ndjson_rows, parquet_rows};
use crate::fake_snowflake::constants::PARQUET_MAGIC;

/// One fake table: its declared columns and the rows committed to it.
#[derive(Debug, Clone, Default)]
pub struct FakeTable {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Value>,
}

/// Rows appended under one offset token, not yet committed.
#[derive(Debug, Clone, Default)]
pub struct PendingAppend {
    pub offset_token: String,
    pub rows: Vec<Value>,
}

/// One fake channel.
#[derive(Debug, Clone, Default)]
pub struct FakeChannel {
    pub continuation: u64,
    pub committed: Option<String>,
    pub uncommitted: Vec<PendingAppend>,
    pub error_rows: i64,
}

/// Everything the fake service remembers.
#[derive(Debug, Default)]
pub struct FakeState {
    pub host: String,
    pub tables: BTreeMap<String, FakeTable>,
    pub channels: BTreeMap<String, FakeChannel>,
    pub append_failures: VecDeque<u16>,
    pub appends: u64,
    pub statements: Vec<String>,
    /// Uploaded objects by `bucket/key`; every external stage is the root of one bucket.
    pub objects: BTreeMap<String, Vec<u8>>,
    pub put_failures: VecDeque<u16>,
    /// Successful and failed upload attempts, in order.
    pub puts: Vec<String>,
    pub pipes: BTreeMap<String, FakePipe>,
    /// Objects already loaded, which the real load history skips.
    pub loaded: BTreeSet<String>,
    /// Paths each pipe received through `insertFiles`, which it skips when sent again.
    pub received: BTreeSet<(String, String)>,
    /// Snowpipe load events by pipe, as `insertReport` lists them.
    pub load_events: BTreeMap<String, Vec<Value>>,
    /// While set, `insertReport` and `loadHistoryScan` report nothing.
    pub withhold_reports: bool,
    /// First errors for the next files Snowpipe fails to load.
    pub load_failures: VecDeque<String>,
}

impl FakeState {
    /// Load one gzip NDJSON or Parquet object into `table` unless it was loaded before or does not exist.
    pub fn load_object(&mut self, table: &str, key: &str) {
        if self.loaded.contains(key) {
            return;
        }
        let Some(body) = self.objects.get(key) else {
            return;
        };
        let rows = match body.starts_with(PARQUET_MAGIC.as_bytes()) {
            true => {
                let columns = self.tables.get(table).map(|found| found.columns.clone());
                parquet_rows(body, &columns.unwrap_or_default())
            }
            false => ndjson_rows(body),
        };
        let Some(rows) = rows else {
            return;
        };
        self.loaded.insert(key.to_owned());
        self.tables
            .entry(table.to_owned())
            .or_default()
            .rows
            .extend(rows);
    }
}

/// A stage pipe: its table and the object key prefix it loads from.
#[derive(Debug, Clone, Default)]
pub struct FakePipe {
    pub table: String,
    pub location: String,
}

/// Shared fake state.
#[derive(Debug, Clone, Default)]
pub struct SharedState(pub Arc<Mutex<FakeState>>);

impl SharedState {
    /// The state, recovering it if a handler panicked while holding the lock.
    pub fn lock(&self) -> MutexGuard<'_, FakeState> {
        match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// A running fake Snowflake.
#[derive(Debug, Clone)]
pub struct FakeSnowflake {
    pub base_url: String,
    pub state: SharedState,
}

impl FakeSnowflake {
    /// Committed rows of `table`.
    pub fn rows(&self, table: &str) -> Vec<Value> {
        let state = self.state.lock();
        state
            .tables
            .get(table)
            .map(|found| found.rows.clone())
            .unwrap_or_default()
    }

    /// Make the next `count` append requests fail with HTTP `status`.
    pub fn fail_next_appends(&self, count: usize, status: u16) {
        self.state
            .lock()
            .append_failures
            .extend(std::iter::repeat_n(status, count));
    }

    /// How many append requests succeeded.
    pub fn appends(&self) -> u64 {
        self.state.lock().appends
    }

    /// Make the next `count` object uploads fail with HTTP `status`; 400 answers `ExpiredToken`.
    pub fn fail_next_puts(&self, count: usize, status: u16) {
        self.state
            .lock()
            .put_failures
            .extend(std::iter::repeat_n(status, count));
    }

    /// Hold back Snowpipe load reports while `withhold` is set; files still load.
    pub fn withhold_load_reports(&self, withhold: bool) {
        self.state.lock().withhold_reports = withhold;
    }

    /// Make the next `count` files sent to `insertFiles` fail to load with `error`.
    pub fn fail_next_loads(&self, count: usize, error: &str) {
        self.state
            .lock()
            .load_failures
            .extend(std::iter::repeat_n(error.to_owned(), count));
    }

    /// Every stored object key, `bucket/key`.
    pub fn object_keys(&self) -> Vec<String> {
        self.state.lock().objects.keys().cloned().collect()
    }

    /// Every upload attempt's `bucket/key`, in order.
    pub fn puts(&self) -> Vec<String> {
        self.state.lock().puts.clone()
    }

    /// Every SQL statement received, in order.
    pub fn statements(&self) -> Vec<String> {
        self.state.lock().statements.clone()
    }
}
