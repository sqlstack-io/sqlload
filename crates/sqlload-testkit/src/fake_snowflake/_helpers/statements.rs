use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::fake_snowflake::models::FakeState;

type Outcome = Result<Value, (StatusCode, Value)>;

fn result(columns: &[&str], rows: Vec<Vec<Value>>) -> Value {
    let row_type: Vec<Value> = columns.iter().map(|name| json!({ "name": name })).collect();
    json!({ "statementHandle": "fake", "resultSetMetaData": { "rowType": row_type }, "data": rows })
}

fn data_type(sql_type: &str) -> String {
    let upper = sql_type.to_ascii_uppercase();
    match upper.split('(').next().unwrap_or_default() {
        "VARCHAR" | "STRING" | "TEXT" => "TEXT".to_owned(),
        other => other.to_owned(),
    }
}

/// `CREATE TABLE IF NOT EXISTS DB.SCHEMA.NAME (A TYPE, B TYPE)` → name and columns.
fn parse_create_table(statement: &str) -> Option<(String, Vec<(String, String)>)> {
    let rest = statement.strip_prefix("CREATE TABLE IF NOT EXISTS ")?;
    let (name, columns) = rest.split_once(" (")?;
    let table = name.rsplit('.').next()?.trim_matches('"').to_owned();
    let body = columns.rsplit_once(')')?.0;
    let parsed = body
        .split(", ")
        .filter_map(|column| column.split_once(' '))
        .map(|(column, sql_type)| (column.to_owned(), data_type(sql_type)))
        .collect();
    Some((table, parsed))
}

/// Answer the statements sqlload sends; anything else succeeds with no rows.
pub(crate) fn run_statement(state: &mut FakeState, body: &[u8]) -> Outcome {
    let request: Value = serde_json::from_slice(body).unwrap_or_default();
    let statement = request["statement"].as_str().unwrap_or_default().to_owned();
    state.statements.push(statement.clone());
    if let Some((table, columns)) = parse_create_table(&statement) {
        state.tables.entry(table).or_default().columns = columns;
        return Ok(result(&["status"], vec![vec![json!("created")]]));
    }
    if statement.starts_with("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE") {
        let rows = state
            .tables
            .iter()
            .flat_map(|(table, found)| {
                found
                    .columns
                    .iter()
                    .map(|(column, data_type)| vec![json!(table), json!(column), json!(data_type)])
                    .collect::<Vec<_>>()
            })
            .collect();
        return Ok(result(&["TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"], rows));
    }
    if statement.starts_with("SELECT CURRENT_USER()") {
        return Ok(result(
            &["U", "R", "W"],
            vec![vec![
                json!("LOADER"),
                json!("LOADER_ROLE"),
                json!("LOAD_WH"),
            ]],
        ));
    }
    Ok(result(&["name"], Vec::new()))
}
