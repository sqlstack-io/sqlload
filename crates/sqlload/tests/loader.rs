//! Docker-backed loader tests against real systems.

#[path = "loader/helpers.rs"]
mod helpers;
#[path = "loader/test_loader_edge_cases.rs"]
mod test_loader_edge_cases;
#[path = "loader/test_postgres_reader.rs"]
mod test_postgres_reader;
#[path = "loader/test_postgres_to_duckdb.rs"]
mod test_postgres_to_duckdb;
#[path = "loader/test_types.rs"]
mod test_types;
