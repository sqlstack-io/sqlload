use std::sync::{Arc, Mutex};

/// An open DuckDB database shared by the load's tasks.
#[derive(Debug, Clone)]
pub struct DuckDbDestination {
    pub connection: Arc<Mutex<duckdb::Connection>>,
}
