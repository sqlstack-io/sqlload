//! What VARIANT columns need of a MotherDuck database.

use crate::adapters::duckdb::constants::{MOTHERDUCK_VERSION_QUERY, SHREDDING_DUCKDB_MAJOR};
use crate::adapters::duckdb::main::_query_text::query_text;
use crate::adapters::duckdb::models::DuckDbDestination;

/// Why a table with `VARIANT` columns cannot load into `destination` (problems), and what to know about it (warnings): MotherDuck's bridge needs Python's duckdb 1.5 or later to create one, and a server older than DuckDB 2.0 stores VARIANT unshredded, which makes queries on its fields slow. The server's version is MotherDuck's `md_version()`, else the bridge's when the server reports none. None for the bundled DuckDB.
pub(in crate::adapters) async fn variant_problems(
    destination: &DuckDbDestination,
) -> (Vec<String>, Vec<String>) {
    if !destination.motherduck {
        return (Vec::new(), Vec::new());
    }
    let client = &destination.bridge_duckdb;
    if version_parts(client) < (1, 5) {
        return (
            vec![format!(
                "VARIANT columns need Python's duckdb 1.5 or later on the MotherDuck bridge, which runs {client}: install duckdb 1.5 there (pip install 'sqlload[motherduck]'), or set json_type = \"json\" on the target"
            )],
            Vec::new(),
        );
    }
    let server = match query_text(destination, MOTHERDUCK_VERSION_QUERY.to_owned()).await {
        Ok(server) => server,
        Err(error) => {
            tracing::debug!(%error, "MotherDuck reports no server version");
            None
        }
    };
    let (version, judged) = match &server {
        Some(server) => (server.trim_start_matches('v'), String::new()),
        None => (
            client.as_str(),
            format!(
                " (judged from the bridge's duckdb {client}: MotherDuck reported no server version)"
            ),
        ),
    };
    let (major, minor) = version_parts(version);
    match major < SHREDDING_DUCKDB_MAJOR {
        true => (
            Vec::new(),
            vec![format!(
                "MotherDuck runs DuckDB {major}.{minor}, which stores VARIANT unshredded: queries on its fields can be much slower than on JSON until MotherDuck moves to DuckDB {SHREDDING_DUCKDB_MAJOR}.0; json_type = \"json\" keeps JSON{judged}"
            )],
        ),
        false => (Vec::new(), Vec::new()),
    }
}

/// The major and minor parts of a DuckDB version such as `1.5.6` or `v1.5.6-2026-11-382`; zeros for what does not read as a number.
fn version_parts(version: &str) -> (u32, u32) {
    let mut parts = version
        .trim_start_matches('v')
        .split(['.', '-'])
        .map(|part| part.parse::<u32>().unwrap_or(0));
    (parts.next().unwrap_or(0), parts.next().unwrap_or(0))
}
