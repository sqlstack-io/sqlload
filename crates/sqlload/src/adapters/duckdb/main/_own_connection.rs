//! Give one table's load its own connection.

use std::sync::{Arc, Mutex};

use crate::adapters::duckdb::errors::DuckDbError;
use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::adapters::motherduck_bridge::main::fork_connection::fork_connection;
use crate::memory_budget::main::table_memory::table_memory;

/// A new connection to the same database, so one table's transaction never mixes with another table's loading at the same time. Under the memory watchdog, the table it loads can have the connection's running statement interrupted.
pub(in crate::adapters) fn own_connection(
    destination: &DuckDbDestination,
) -> Result<DuckDbDestination, DuckDbError> {
    let watched = table_memory();
    let (sender, receiver) = std::sync::mpsc::channel::<()>();
    let connection = match &destination.connection {
        DuckDbConnection::Local(connection) => {
            let connection = connection
                .lock()
                .map_err(|_| DuckDbError::Task("the connection lock is poisoned".to_owned()))?
                .try_clone()?;
            if watched.is_some() {
                let handle = connection.interrupt_handle();
                std::thread::spawn(move || {
                    for () in receiver {
                        handle.interrupt();
                    }
                });
            }
            DuckDbConnection::Local(Arc::new(Mutex::new(connection)))
        }
        DuckDbConnection::Bridge(bridge) => {
            let forked = fork_connection(bridge);
            if watched.is_some() {
                let (process, cursor) = (forked.cursor.process.clone(), forked.cursor.id);
                std::thread::spawn(move || {
                    for () in receiver {
                        let interrupt = serde_json::json!({"op": "interrupt", "cursor": cursor});
                        let _ = process.lines.send(interrupt.to_string());
                    }
                });
            }
            DuckDbConnection::Bridge(forked)
        }
    };
    if let Some(watched) = watched {
        watched
            .table
            .interrupts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(sender);
    }
    Ok(DuckDbDestination {
        connection,
        ddl: destination.ddl.clone(),
        motherduck: destination.motherduck,
        bridge_duckdb: destination.bridge_duckdb.clone(),
        memory: destination.memory,
        own_limit: destination.own_limit,
        unflushed: destination.unflushed.clone(),
        secrets: destination.secrets.clone(),
        stage: destination.stage.clone(),
    })
}
