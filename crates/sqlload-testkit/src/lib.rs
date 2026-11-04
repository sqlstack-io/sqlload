//! Test doubles for sqlload: a fake Snowflake service, Redpanda and Postgres containers, configurations and an exporter harness.

#![forbid(unsafe_code)]

pub mod configs;
pub mod fake_snowflake;
pub mod harness;
pub mod postgres;
pub mod records;
pub mod redpanda;
