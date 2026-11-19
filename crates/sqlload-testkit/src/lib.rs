//! Test doubles for sqlload: a fake Snowflake service, a fake GCS server, Redpanda, Postgres, MySQL and SQL Server containers, configurations and an exporter harness.

#![forbid(unsafe_code)]

pub mod clickhouse;
pub mod configs;
pub mod doc_pages;
pub mod fake_bigquery;
pub mod fake_databricks;
pub mod fake_gcs;
pub mod fake_sheets;
pub mod fake_snowflake;
pub mod harness;
pub mod odbc_servers;
pub mod parity;
pub mod postgres;
pub mod records;
pub mod redpanda;
pub mod seaweedfs;
pub mod ssh;
