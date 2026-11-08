use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

/// One tab: its title, and its rows of cells as the Sheets API writes them (`{"effectiveValue": ..., "formattedValue": ...}`).
#[derive(Debug, Clone, Default)]
pub struct FakeTab {
    pub title: String,
    pub rows: Vec<Vec<Value>>,
}

/// Everything the fake remembers.
#[derive(Debug, Default)]
pub struct FakeSheetsState {
    /// Each spreadsheet's tabs, in order, by ID.
    pub spreadsheets: BTreeMap<String, Vec<FakeTab>>,
    /// Tokens handed out.
    pub tokens: u64,
    /// How many of the next reads are refused with 401, as an expired token is; the token each was sent with stays refused.
    pub refuse_requests: usize,
    /// Tokens refused so far.
    pub refused_tokens: Vec<String>,
    /// Each read: the range asked for, or `*` for every tab.
    pub reads: Vec<String>,
}

/// Shared fake state.
#[derive(Debug, Clone, Default)]
pub struct SharedSheets(pub Arc<Mutex<FakeSheetsState>>);

impl SharedSheets {
    /// The state, recovering it if a handler panicked while holding the lock.
    pub fn lock(&self) -> MutexGuard<'_, FakeSheetsState> {
        match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

/// A running fake: the spreadsheets root a connection's `base_url` takes, a service-account key exchanging tokens with it, and its state.
#[derive(Debug, Clone)]
pub struct FakeSheets {
    pub base_url: String,
    pub key_json: String,
    pub state: SharedSheets,
}

impl FakeSheets {
    /// Hold spreadsheet `id` with `tabs`, replacing what it held.
    pub fn place(&self, id: &str, tabs: Vec<FakeTab>) {
        self.state.lock().spreadsheets.insert(id.to_owned(), tabs);
    }
}
