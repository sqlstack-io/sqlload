use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

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

    /// Every SQL statement received, in order.
    pub fn statements(&self) -> Vec<String> {
        self.state.lock().statements.clone()
    }
}
