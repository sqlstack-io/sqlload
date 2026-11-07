use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

use serde_json::Value;

use crate::fake_databricks::constants::FAKE_VOLUME_PATH;

/// One fake Delta table: its declared columns (name, `full_data_type`) and its rows.
#[derive(Debug, Clone, Default)]
pub struct FakeTable {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Value>,
}

/// One file in the volume: its bytes and when it was written, in milliseconds since the epoch.
#[derive(Debug, Clone, Default)]
pub struct FakeFile {
    pub body: Vec<u8>,
    pub modified_ms: i64,
}

/// Everything the fake workspace remembers.
#[derive(Debug, Default)]
pub struct FakeState {
    /// Tables by lower-cased `schema.table`.
    pub tables: BTreeMap<String, FakeTable>,
    /// Every statement run, in order.
    pub statements: Vec<String>,
    /// Volume files by full path, `/Volumes/...`.
    pub files: BTreeMap<String, FakeFile>,
    /// Volume directories by full path, the volume itself included.
    pub directories: BTreeSet<String>,
    /// The error messages the next `COPY INTO` statements fail with.
    pub copy_failures: VecDeque<String>,
    /// The files each `COPY INTO` read, in order.
    pub copied: Vec<Vec<String>>,
    /// Tokens handed out.
    pub tokens: u64,
    /// How many of the next authorized requests are refused with 401, as an expired token is; the token each was sent with stays refused.
    pub refuse_requests: usize,
    /// Tokens refused so far; a request sending one again is refused too.
    pub refused_tokens: BTreeSet<String>,
    /// Polls each new statement answers `PENDING` to before it is done.
    pub pending_polls: usize,
    /// HTTP statuses the next statement polls fail with.
    pub poll_failures: VecDeque<u16>,
    /// Statements not yet done: the polls left and the finished answer, by statement id.
    pub running: BTreeMap<String, (usize, Value)>,
    /// When set, results come in chunks of this many rows, each after the first fetched by its link.
    pub chunk_rows: Option<usize>,
    /// Later result chunks, by statement id and chunk index.
    pub chunks: BTreeMap<(String, usize), Value>,
    /// When set, directory listings come in pages of this many entries.
    pub page_entries: Option<usize>,
    /// Statements cancelled, by id.
    pub cancelled: Vec<String>,
    /// Statement ids handed out.
    pub statement_ids: u64,
    /// Every file upload, by path, including overwrites.
    pub puts: Vec<String>,
    /// A folder and a file written into it just after the folder is next listed, as a load starting meanwhile would.
    pub write_after_listing: Option<(String, String)>,
}

impl FakeState {
    /// Paths of the files under `folder`.
    pub fn files_under(&self, folder: &str) -> Vec<String> {
        let prefix = format!("{}/", folder.trim_end_matches('/'));
        self.files
            .keys()
            .filter(|path| path.starts_with(&prefix))
            .cloned()
            .collect()
    }
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

/// A running fake Databricks workspace.
#[derive(Debug, Clone)]
pub struct FakeDatabricks {
    pub base_url: String,
    pub state: SharedState,
}

impl FakeDatabricks {
    /// Rows of `schema.table`.
    pub fn rows(&self, table: &str) -> Vec<Value> {
        self.state
            .lock()
            .tables
            .get(&table.to_ascii_lowercase())
            .map(|found| found.rows.clone())
            .unwrap_or_default()
    }

    /// Every file and folder left under the volume's `sqlload/` folder, which loads share and so keep.
    pub fn volume_leftovers(&self) -> Vec<String> {
        let state = self.state.lock();
        let prefix = format!("{FAKE_VOLUME_PATH}/sqlload/");
        state
            .files
            .keys()
            .chain(state.directories.iter())
            .filter(|path| path.starts_with(&prefix))
            .cloned()
            .collect()
    }

    /// Make an empty folder, and the folders above it.
    pub fn place_folder(&self, path: &str) {
        let mut state = self.state.lock();
        let mut folder = path.trim_end_matches('/');
        while folder.len() > FAKE_VOLUME_PATH.len() {
            state.directories.insert(folder.to_owned());
            folder = folder.rsplit_once('/').map_or("", |(above, _)| above);
        }
    }

    /// Put a file in the volume as if written at `modified_ms`, creating its folders.
    pub fn place_file(&self, path: &str, body: &[u8], modified_ms: i64) {
        self.state
            .lock()
            .write_file(path, body.to_vec(), modified_ms);
    }
}

impl FakeState {
    /// Store a file and every folder above it up to the volume.
    pub fn write_file(&mut self, path: &str, body: Vec<u8>, modified_ms: i64) {
        let mut parent = path;
        while let Some((above, _)) = parent.rsplit_once('/') {
            if above.len() < FAKE_VOLUME_PATH.len() {
                break;
            }
            self.directories.insert(above.to_owned());
            parent = above;
        }
        self.files
            .insert(path.to_owned(), FakeFile { body, modified_ms });
    }
}
