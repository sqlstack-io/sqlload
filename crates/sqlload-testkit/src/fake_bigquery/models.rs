use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

/// One fake table: its declared columns (name, DDL type) and its rows, each value as text or null.
#[derive(Debug, Clone, Default)]
pub struct FakeTable {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Value>,
}

/// One pending write stream: the table it writes (`dataset.table`), its writer schema, the rows appended, and whether it is finalized.
#[derive(Debug, Clone, Default)]
pub struct FakeStream {
    pub table: String,
    pub schema: Vec<u8>,
    pub rows: Vec<Value>,
    pub finalized: bool,
}

/// Everything the fake project remembers.
#[derive(Debug, Default)]
pub struct FakeState {
    /// Tables by lower-cased `dataset.table`.
    pub tables: BTreeMap<String, FakeTable>,
    /// Every query run, in order.
    pub statements: Vec<String>,
    /// Open and finalized streams by name; a committed stream is removed.
    pub streams: BTreeMap<String, FakeStream>,
    /// Streams created so far.
    pub stream_ids: u64,
    /// Each commit: the table and how many streams it committed.
    pub commits: Vec<(String, usize)>,
    /// Messages the next appends are refused with, one refused row each.
    pub refused_appends: VecDeque<String>,
    /// Messages the next commits are refused with, as a stream error on every stream.
    pub refused_commits: VecDeque<String>,
    /// Tokens handed out.
    pub tokens: u64,
    /// How many of the next authorized REST requests are refused with 401, as an expired token is; the token each was sent with stays refused.
    pub refuse_requests: usize,
    /// Tokens refused so far; a request sending one again is refused too.
    pub refused_tokens: Vec<String>,
    /// Storage Write API calls that came without a routing header.
    pub unrouted_calls: usize,
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

/// A running fake BigQuery project.
#[derive(Debug, Clone)]
pub struct FakeBigQuery {
    /// The REST API's root, as a connection's `base_url`.
    pub base_url: String,
    /// The Storage Write API's root, as a connection's `storage_url`.
    pub storage_url: String,
    /// A service-account key file for the fake, its token endpoint the fake's own.
    pub key_json: String,
    pub state: SharedState,
}

impl FakeBigQuery {
    /// Rows of `dataset.table`.
    pub fn rows(&self, table: &str) -> Vec<Value> {
        self.state
            .lock()
            .tables
            .get(&table.to_ascii_lowercase())
            .map(|found| found.rows.clone())
            .unwrap_or_default()
    }

    /// Names of the tables in `dataset`.
    pub fn tables_in(&self, dataset: &str) -> Vec<String> {
        let prefix = format!("{}.", dataset.to_ascii_lowercase());
        self.state
            .lock()
            .tables
            .keys()
            .filter_map(|key| key.strip_prefix(&prefix).map(str::to_owned))
            .collect()
    }

    /// Put an empty table `dataset.table` of `columns` (name, DDL type) in the project.
    pub fn place_table(&self, table: &str, columns: &[(&str, &str)]) {
        self.state.lock().tables.insert(
            table.to_ascii_lowercase(),
            FakeTable {
                columns: columns
                    .iter()
                    .map(|(name, sql_type)| ((*name).to_owned(), (*sql_type).to_owned()))
                    .collect(),
                rows: Vec::new(),
            },
        );
    }
}
