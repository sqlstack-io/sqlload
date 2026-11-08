//! Stop a table's statements, for tests of a run killed midway.

use crate::adapters::clickhouse::_helpers::session::stops::STOP;

/// Tests only (absent from released builds): every ClickHouse statement naming `table` fails once `statements` of them have run, until called again with `None`, so a test can stop a load at each point a kill could and check the next run converges.
pub fn stop_after(table: &str, statements: Option<usize>) {
    if let Ok(mut stop) = STOP.lock() {
        stop.retain(|(stopped, _)| stopped != table);
        stop.extend(statements.map(|left| (table.to_owned(), left)));
    }
}
