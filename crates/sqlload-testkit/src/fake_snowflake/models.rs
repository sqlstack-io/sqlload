use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

use crate::fake_snowflake::_helpers::loading::{ndjson_rows, parquet_rows};
use crate::fake_snowflake::_helpers::objects::load_event;
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
    /// While set, `loadHistoryScan` reports nothing.
    pub withhold_history: bool,
    /// What happens to the next files sent to `insertFiles`; files load once it is empty.
    pub load_script: VecDeque<LoadOutcome>,
    /// Files accepted but not loaded until [`FakeSnowflake::release_stalled`].
    pub stalled: Vec<StalledFile>,
    /// Every path sent to `insertFiles`, in order, including repeats.
    pub inserted: Vec<String>,
    /// HTTP statuses for the next `loadHistoryScan` calls.
    pub history_failures: VecDeque<u16>,
    pub history_scans: u64,
}

impl FakeState {
    /// Load a file sent to `pipe` and record its load event; a missing object fails to load.
    pub fn load_file(&mut self, pipe: &str, table: &str, path: &str, key: &str) {
        let event = match self.objects.contains_key(key) {
            false => load_event(path, "LOAD_FAILED", 0, Some("file not found")),
            true => {
                let before = self.tables.get(table).map_or(0, |found| found.rows.len());
                self.load_object(table, key);
                let after = self.tables.get(table).map_or(0, |found| found.rows.len());
                load_event(path, "LOADED", after - before, None)
            }
        };
        self.load_events
            .entry(pipe.to_owned())
            .or_default()
            .push(event);
    }

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

/// What the fake Snowpipe does with one file sent to `insertFiles`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadOutcome {
    /// Load it and report it.
    Load,
    /// Report it as failed with this first error.
    Fail(&'static str),
    /// Accept it but neither load nor report it until released.
    Stall,
    /// Accept it, delete the object and never report it.
    Lose,
}

/// A file accepted by `insertFiles` and held back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StalledFile {
    pub pipe: String,
    pub table: String,
    pub path: String,
    pub key: String,
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

    /// Hold back `loadHistoryScan` results while `withhold` is set.
    pub fn withhold_load_history(&self, withhold: bool) {
        self.state.lock().withhold_history = withhold;
    }

    /// Decide what happens to the next files sent to `insertFiles`, in order.
    pub fn script_loads(&self, outcomes: &[LoadOutcome]) {
        self.state
            .lock()
            .load_script
            .extend(outcomes.iter().copied());
    }

    /// Load and report every stalled file.
    pub fn release_stalled(&self) {
        let mut state = self.state.lock();
        let stalled = std::mem::take(&mut state.stalled);
        for file in stalled {
            state.load_file(&file.pipe, &file.table, &file.path, &file.key);
        }
    }

    /// Make the next `count` `loadHistoryScan` calls fail with HTTP `status`.
    pub fn fail_next_history_scans(&self, count: usize, status: u16) {
        self.state
            .lock()
            .history_failures
            .extend(std::iter::repeat_n(status, count));
    }

    /// Every path sent to `insertFiles`, in order.
    pub fn inserted(&self) -> Vec<String> {
        self.state.lock().inserted.clone()
    }

    /// How many `loadHistoryScan` calls arrived.
    pub fn history_scans(&self) -> u64 {
        self.state.lock().history_scans
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
