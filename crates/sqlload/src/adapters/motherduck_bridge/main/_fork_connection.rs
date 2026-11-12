//! Give a load another connection to the bridge's database.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::adapters::motherduck_bridge::models::{BridgeConnection, BridgeCursor};

/// A new connection to the database `connection` opened, on the same bridge: its own DuckDB cursor, so its transactions never mix with another connection's.
pub(in crate::adapters) fn fork_connection(connection: &BridgeConnection) -> BridgeConnection {
    let process = connection.cursor.process.clone();
    let id = process.next_cursor.fetch_add(1, Ordering::Relaxed);
    BridgeConnection {
        cursor: Arc::new(BridgeCursor { id, process }),
    }
}
