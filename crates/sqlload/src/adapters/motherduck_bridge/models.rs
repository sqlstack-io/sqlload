use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Mutex};

use crate::adapters::motherduck_bridge::errors::BridgeError;

/// A connection to the database the bridge opened: one DuckDB cursor of the bridge process, shared by its clones and closed when the last one drops.
#[derive(Debug, Clone)]
pub struct BridgeConnection {
    pub cursor: Arc<BridgeCursor>,
}

/// One cursor of the bridge process, by its id; the bridge creates it on its first statement and runs its statements in order.
#[derive(Debug)]
pub struct BridgeCursor {
    pub id: u64,
    pub process: Arc<BridgeProcess>,
}

impl Drop for BridgeCursor {
    fn drop(&mut self) {
        let close = serde_json::json!({"op": "close", "cursor": self.id});
        let _ = self.process.lines.send(close.to_string());
    }
}

/// A running bridge process: the lines going to its stdin, the requests waiting for its replies, and the tail of its stderr. The process ends when the last handle drops, which closes its stdin.
#[derive(Debug)]
pub struct BridgeProcess {
    /// The Python it runs on, as messages name it.
    pub python: String,
    pub lines: Sender<String>,
    pub replies: Arc<Mutex<Replies>>,
    pub next_request: AtomicU64,
    pub next_cursor: AtomicU64,
    pub stderr: Arc<Mutex<String>>,
}

/// The requests waiting for a reply, by id, each with where its reply goes; once the process has ended, why, which every later request fails with.
#[derive(Debug, Default)]
pub struct Replies {
    pub waiting: HashMap<u64, Sender<Result<serde_json::Value, BridgeError>>>,
    pub stopped: Option<String>,
}
