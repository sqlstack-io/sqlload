use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use serde_json::Value;

/// One fake table: its declared columns and the rows committed to it.
#[derive(Debug, Clone, Default)]
pub struct FakeTable {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Value>,
}

/// One fake channel.
#[derive(Debug, Clone, Default)]
pub struct FakeChannel {
    pub continuation: u64,
    pub committed: Option<String>,
    pub uncommitted: Vec<(String, Vec<Value>)>,
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

/// A running fake Snowflake; dropping it does not stop the server task.
#[derive(Debug, Clone)]
pub struct FakeSnowflake {
    pub base_url: String,
    pub state: Arc<Mutex<FakeState>>,
}

impl FakeSnowflake {
    /// Committed rows of `table`.
    pub fn rows(&self, table: &str) -> Vec<Value> {
        self.with_state(|state| {
            state
                .tables
                .get(table)
                .map(|found| found.rows.clone())
                .unwrap_or_default()
        })
    }

    /// Make the next `count` append requests fail with HTTP `status`.
    pub fn fail_next_appends(&self, count: usize, status: u16) {
        self.with_state(|state| {
            state
                .append_failures
                .extend(std::iter::repeat_n(status, count))
        });
    }

    /// How many append requests succeeded.
    pub fn appends(&self) -> u64 {
        self.with_state(|state| state.appends)
    }

    /// Every SQL statement received, in order.
    pub fn statements(&self) -> Vec<String> {
        self.with_state(|state| state.statements.clone())
    }

    fn with_state<T>(&self, read: impl FnOnce(&mut FakeState) -> T) -> T {
        let mut guard = match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        read(&mut guard)
    }
}
