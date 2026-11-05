//! Docker-backed loader tests against real systems.

#[path = "loader/helpers.rs"]
mod helpers;
#[path = "loader/test_batched_loads.rs"]
mod test_batched_loads;
#[path = "loader/test_debug_project.rs"]
mod test_debug_project;
#[path = "loader/test_file_store.rs"]
mod test_file_store;
#[path = "loader/test_files_round_trip.rs"]
mod test_files_round_trip;
#[path = "loader/test_files_to_duckdb.rs"]
mod test_files_to_duckdb;
#[path = "loader/test_loader_edge_cases.rs"]
mod test_loader_edge_cases;
#[path = "loader/test_mixed_cursors.rs"]
mod test_mixed_cursors;
#[path = "loader/test_postgres_reader.rs"]
mod test_postgres_reader;
#[path = "loader/test_postgres_to_duckdb.rs"]
mod test_postgres_to_duckdb;
#[path = "loader/test_postgres_to_files.rs"]
mod test_postgres_to_files;
#[path = "loader/test_snowflake_routes.rs"]
mod test_snowflake_routes;
#[path = "loader/test_text_files_to_duckdb.rs"]
mod test_text_files_to_duckdb;
#[path = "loader/test_types.rs"]
mod test_types;
