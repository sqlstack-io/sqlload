//! Start the bridge and open a database on it.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::adapters::motherduck_bridge::_helpers::process::{failure, send, start};
use crate::adapters::motherduck_bridge::constants::{
    DEFAULT_PYTHON, MISSING_PACKAGE_ERRORS, PYTHON_VARIABLE, SUPPORTED_DUCKDB,
};
use crate::adapters::motherduck_bridge::errors::BridgeError;
use crate::adapters::motherduck_bridge::models::{BridgeConnection, BridgeCursor};

/// Start the bridge on `python` (else the Python `SQLLOAD_MOTHERDUCK_PYTHON` names, else `python3` on `PATH`) and open `database` there with DuckDB's `config` settings: a DuckDB path or connection string, such as `md:shop?motherduck_token=...`, which never appears in the process's arguments. Each of `hidden` (such as the token, as written and as encoded) is taken out of what the bridge prints. Returns a connection to it and the version of the duckdb package the bridge runs. Blocks until the database opens.
pub(in crate::adapters) fn open_bridge(
    database: &str,
    config: serde_json::Value,
    (python, hidden): (Option<&str>, Vec<String>),
) -> Result<(BridgeConnection, String), BridgeError> {
    let python = match python {
        Some(python) => python.to_owned(),
        None => match std::env::var(PYTHON_VARIABLE) {
            Ok(python) if !python.is_empty() => python,
            Ok(_) | Err(std::env::VarError::NotPresent) => DEFAULT_PYTHON.to_owned(),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(BridgeError::Setup(format!(
                    "{PYTHON_VARIABLE} is not valid Unicode"
                )));
            }
        },
    };
    let process = start(&python, hidden)?;
    let open = serde_json::json!({"op": "open", "database": database, "config": config});
    let reply = send(&process, open)?.recv().map_err(|_| {
        BridgeError::Stopped(format!("the MotherDuck bridge on {python} stopped"))
    })??;
    if let Some((error, kind)) = failure(&reply) {
        return Err(match MISSING_PACKAGE_ERRORS.contains(&kind.as_str()) {
            true => BridgeError::Setup(format!(
                "MotherDuck connects through Python's duckdb package ({SUPPORTED_DUCKDB}), which {python} lacks ({error}): install sqlload[motherduck] with pip or uv, or set {PYTHON_VARIABLE} to a Python that has it"
            )),
            false => BridgeError::Engine(error),
        });
    }
    let version = reply["duckdb"].as_str().unwrap_or_default().to_owned();
    let id = process.next_cursor.fetch_add(1, Ordering::Relaxed);
    let cursor = Arc::new(BridgeCursor { id, process });
    Ok((BridgeConnection { cursor }, version))
}
