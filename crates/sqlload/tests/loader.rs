//! Docker-backed loader tests against real systems.

#![recursion_limit = "256"]

#[path = "loader/after_load/helpers.rs"]
mod after_load_helpers;
#[path = "loader/batched/helpers.rs"]
mod batched_helpers;
#[path = "loader/bigquery/helpers.rs"]
mod bigquery_helpers;
#[path = "loader/clickhouse/helpers.rs"]
mod clickhouse_helpers;
#[path = "loader/cursors/helpers.rs"]
mod cursor_helpers;
#[path = "loader/databricks/helpers.rs"]
mod databricks_helpers;
#[cfg(feature = "duckdb")]
#[path = "loader/duckdb/helpers.rs"]
mod duckdb_helpers;
#[path = "loader/files/helpers.rs"]
mod files_helpers;
#[path = "loader/files_target/helpers.rs"]
mod files_target_helpers;
#[path = "loader/google/helpers.rs"]
mod google_helpers;
#[path = "loader/helpers.rs"]
mod helpers;
#[path = "loader/memory/helpers.rs"]
mod memory_helpers;
#[path = "loader/merge/helpers.rs"]
mod merge_helpers;
#[cfg(feature = "odbc")]
#[path = "loader/odbc/helpers.rs"]
mod odbc_helpers;
#[path = "loader/postgres/helpers.rs"]
mod postgres_helpers;
#[path = "loader/session/helpers.rs"]
mod session_helpers;
#[path = "loader/sftp/helpers.rs"]
mod sftp_helpers;
#[path = "loader/sheets/helpers.rs"]
mod sheets_helpers;
#[path = "loader/test_after_load.rs"]
mod test_after_load;
#[path = "loader/test_batched_loads.rs"]
mod test_batched_loads;
#[path = "loader/test_bigquery_loads.rs"]
mod test_bigquery_loads;
#[path = "loader/test_bigquery_source.rs"]
mod test_bigquery_source;
#[path = "loader/test_clickhouse_session.rs"]
mod test_clickhouse_session;
#[path = "loader/test_column_names.rs"]
mod test_column_names;
#[path = "loader/test_databricks_loads.rs"]
mod test_databricks_loads;
#[path = "loader/test_databricks_source.rs"]
mod test_databricks_source;
#[path = "loader/test_debug_project.rs"]
mod test_debug_project;
#[cfg(feature = "duckdb")]
#[path = "loader/test_duckdb_source.rs"]
mod test_duckdb_source;
#[path = "loader/test_file_store.rs"]
mod test_file_store;
#[path = "loader/test_files_batches.rs"]
mod test_files_batches;
#[path = "loader/test_files_round_trip.rs"]
mod test_files_round_trip;
#[path = "loader/test_files_to_clickhouse.rs"]
mod test_files_to_clickhouse;
#[path = "loader/test_files_to_duckdb.rs"]
mod test_files_to_duckdb;
#[path = "loader/test_gcs_files.rs"]
mod test_gcs_files;
#[path = "loader/test_google_sign_in.rs"]
mod test_google_sign_in;
#[path = "loader/test_loader_edge_cases.rs"]
mod test_loader_edge_cases;
#[path = "loader/test_memory_budget.rs"]
mod test_memory_budget;
#[path = "loader/test_merge_dedupe.rs"]
mod test_merge_dedupe;
#[path = "loader/test_mixed_cursors.rs"]
mod test_mixed_cursors;
#[path = "loader/test_null_cursors.rs"]
mod test_null_cursors;
#[cfg(feature = "odbc")]
#[path = "loader/test_odbc_catalog.rs"]
mod test_odbc_catalog;
#[cfg(feature = "odbc")]
#[path = "loader/test_odbc_drivers.rs"]
mod test_odbc_drivers;
#[cfg(feature = "odbc")]
#[path = "loader/test_odbc_memory.rs"]
mod test_odbc_memory;
#[cfg(feature = "odbc")]
#[path = "loader/test_odbc_to_duckdb.rs"]
mod test_odbc_to_duckdb;
#[path = "loader/test_overlapping_files.rs"]
mod test_overlapping_files;
#[path = "loader/test_parsed_columns.rs"]
mod test_parsed_columns;
#[path = "loader/test_plan_progress.rs"]
mod test_plan_progress;
#[path = "loader/test_postgres_cdc.rs"]
mod test_postgres_cdc;
#[path = "loader/test_postgres_cdc_files.rs"]
mod test_postgres_cdc_files;
#[path = "loader/test_postgres_reader.rs"]
mod test_postgres_reader;
#[path = "loader/test_postgres_session.rs"]
mod test_postgres_session;
#[path = "loader/test_postgres_ssh.rs"]
mod test_postgres_ssh;
#[path = "loader/test_postgres_target.rs"]
mod test_postgres_target;
#[path = "loader/test_postgres_tls.rs"]
mod test_postgres_tls;
#[path = "loader/test_postgres_to_clickhouse.rs"]
mod test_postgres_to_clickhouse;
#[path = "loader/test_postgres_to_duckdb.rs"]
mod test_postgres_to_duckdb;
#[path = "loader/test_postgres_to_files.rs"]
mod test_postgres_to_files;
#[path = "loader/test_publish_checks.rs"]
mod test_publish_checks;
#[path = "loader/test_s3_profiles.rs"]
mod test_s3_profiles;
#[path = "loader/test_sftp_files.rs"]
mod test_sftp_files;
#[path = "loader/test_sheets_to_duckdb.rs"]
mod test_sheets_to_duckdb;
#[path = "loader/test_snowflake_routes.rs"]
mod test_snowflake_routes;
#[path = "loader/test_sync_deletes.rs"]
mod test_sync_deletes;
#[path = "loader/test_text_files_to_duckdb.rs"]
mod test_text_files_to_duckdb;
#[path = "loader/test_typed_cursors.rs"]
mod test_typed_cursors;
#[path = "loader/test_types.rs"]
mod test_types;
#[path = "loader/test_unnest.rs"]
mod test_unnest;
