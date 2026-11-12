//! Run SQL on a bridge connection.

use crate::adapters::motherduck_bridge::_helpers::process::{failure, send};
use crate::adapters::motherduck_bridge::errors::BridgeError;
use crate::adapters::motherduck_bridge::models::BridgeConnection;

/// Run `sql` (one statement or several, separated by `;`) with text `params` bound to its `?`s, after every earlier statement on `connection`, and return the last statement's rows: each value as text (numbers and booleans as their SQL text), `None` for `NULL`.
pub(in crate::adapters) async fn run_statement(
    connection: &BridgeConnection,
    sql: String,
    params: Vec<String>,
) -> Result<Vec<Vec<Option<String>>>, BridgeError> {
    let cursor = &connection.cursor;
    let request =
        serde_json::json!({"op": "run", "cursor": cursor.id, "sql": sql, "params": params});
    let received = send(&cursor.process, request)?;
    let python = cursor.process.python.clone();
    let reply = tokio::task::spawn_blocking(move || received.recv())
        .await
        .map_err(|error| BridgeError::Stopped(error.to_string()))?
        .map_err(|_| {
            BridgeError::Stopped(format!("the MotherDuck bridge on {python} stopped"))
        })??;
    if let Some((error, _)) = failure(&reply) {
        return Err(BridgeError::Engine(error));
    }
    let rows = reply["rows"].as_array().cloned().unwrap_or_default();
    Ok(rows
        .iter()
        .map(|row| {
            let values = row.as_array().map(Vec::as_slice).unwrap_or_default();
            values.iter().map(text).collect()
        })
        .collect())
}

/// A reply value as text: `None` for `NULL`, booleans as SQL spells them.
fn text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Null => None,
        serde_json::Value::String(text) => Some(text.clone()),
        serde_json::Value::Bool(true) => Some("true".to_owned()),
        serde_json::Value::Bool(false) => Some("false".to_owned()),
        other => Some(other.to_string()),
    }
}
