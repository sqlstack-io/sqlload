use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::fake_snowflake::_helpers::objects::{copy_files, parse_stage_pipe};
use crate::fake_snowflake::constants::NUMBER_TYPE;
use crate::fake_snowflake::models::{FakeTable, SharedState};

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
    let parsed: Vec<(String, String)> = body
        .split(", ")
        .filter_map(|column| column.split_once(' '))
        .map(|(column, sql_type)| (column.to_owned(), data_type(sql_type)))
        .collect();
    Some((table, parsed))
}

fn column_rows(table: &str, found: &FakeTable) -> Vec<Vec<Value>> {
    found
        .columns
        .iter()
        .map(|(column, data_type)| vec![json!(table), json!(column), json!(data_type)])
        .collect()
}

/// `CREATE STAGE IF NOT EXISTS DB.SCHEMA.NAME URL = 'url' STORAGE_INTEGRATION = NAME` → name, URL and integration.
fn parse_create_stage(statement: &str) -> Option<(String, String, String)> {
    let rest = statement.strip_prefix("CREATE STAGE IF NOT EXISTS ")?;
    let (name, rest) = rest.split_once(" URL = '")?;
    let (url, rest) = rest.split_once('\'')?;
    let integration = rest.split_once("STORAGE_INTEGRATION = ")?.1;
    let stage = name.rsplit('.').next()?.trim_matches('"').to_owned();
    Some((
        stage,
        url.to_owned(),
        integration.trim().trim_matches('"').to_owned(),
    ))
}

/// Whether `name` matches a `LIKE` pattern, as `SHOW` does: `_` stands for any one character.
fn like_matches(pattern: &str, name: &str) -> bool {
    pattern.len() == name.len()
        && pattern
            .chars()
            .zip(name.chars())
            .all(|(wanted, found)| wanted == '_' || wanted == found)
}

/// What one statement of an earlier multi-statement request returned.
pub(crate) fn script_result(state: &SharedState, path: &str) -> Outcome {
    let handle = path.rsplit('/').next().unwrap_or_default();
    match state.lock().script_results.get(handle) {
        Some(result) => Ok(result.clone()),
        None => Err((StatusCode::NOT_FOUND, json!({ "message": handle }))),
    }
}

/// Answer one request: a multi-statement request runs its statements in order and keeps what each returned; anything else is one statement.
pub(crate) fn run_statement(state: &SharedState, body: &[u8]) -> Outcome {
    let request: Value = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(_) => Value::Null,
    };
    if request["parameters"]["MULTI_STATEMENT_COUNT"].is_null() {
        return run_one(state, body);
    }
    let statement = request["statement"].as_str().unwrap_or_default();
    let mut handles: Vec<Value> = Vec::new();
    for part in statement.split(";\n") {
        let one = json!({ "statement": part }).to_string().into_bytes();
        let result = run_one(state, &one)?;
        let mut guard = state.lock();
        let handle = format!("fake-{}", guard.script_results.len());
        guard.script_results.insert(handle.clone(), result);
        handles.push(json!(handle));
    }
    Ok(json!({
        "statementHandle": "fake-script",
        "statementHandles": handles,
        "resultSetMetaData": { "rowType": [{ "name": "status" }] },
        "data": [["Multiple statements executed successfully."]],
    }))
}

/// Answer the statements sqlload sends; anything else succeeds with no rows.
fn run_one(state: &SharedState, body: &[u8]) -> Outcome {
    let request: Value = match serde_json::from_slice(body) {
        Ok(value) => value,
        Err(error) => {
            return Err((
                StatusCode::BAD_REQUEST,
                json!({ "message": error.to_string() }),
            ));
        }
    };
    let statement = request["statement"].as_str().unwrap_or_default().to_owned();
    let mut guard = state.lock();
    guard.statements.push(statement.clone());
    if let Some((table, columns)) = parse_create_table(&statement) {
        guard.tables.entry(table).or_default().columns = columns;
        return Ok(result(&["status"], vec![vec![json!("created")]]));
    }
    if let Some((pipe, found)) = parse_stage_pipe(&statement) {
        guard.pipes.insert(pipe, found);
        return Ok(result(&["status"], vec![vec![json!("created")]]));
    }
    if let Some((table, keys)) = copy_files(&statement) {
        let mut loaded: Vec<Vec<Value>> = Vec::new();
        for key in keys {
            if statement.contains("FORCE = TRUE") {
                guard.loaded.remove(&key);
            }
            let before = guard.tables.get(&table).map_or(0, |found| found.rows.len());
            guard.load_object(&table, &key);
            let after = guard.tables.get(&table).map_or(0, |found| found.rows.len());
            loaded.push(vec![
                json!(key),
                json!("LOADED"),
                json!((after - before).to_string()),
            ]);
        }
        return Ok(result(&["file", "status", "rows_loaded"], loaded));
    }
    if let Some((name, url, integration)) = parse_create_stage(&statement) {
        guard.stages.entry(name).or_insert((url, integration));
        return Ok(result(&["status"], vec![vec![json!("created")]]));
    }
    if statement.starts_with("SHOW STAGES") && guard.schema_missing {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            json!({ "code": "002003", "message": "Schema 'RAW' does not exist or not authorized." }),
        ));
    }
    if statement.starts_with("SHOW STAGES") {
        let rows: Vec<Vec<Value>> = guard
            .stages
            .iter()
            .map(|(name, (url, integration))| vec![json!(name), json!(url), json!(integration)])
            .collect();
        return Ok(result(&["name", "url", "storage_integration"], rows));
    }
    if statement.starts_with("SHOW INTEGRATIONS") {
        let pattern = statement
            .split('\'')
            .nth(1)
            .unwrap_or_default()
            .to_ascii_uppercase();
        let rows: Vec<Vec<Value>> = guard
            .integrations
            .iter()
            .filter(|name| like_matches(&pattern, name))
            .map(|name| vec![json!(name)])
            .collect();
        return Ok(result(&["name"], rows));
    }
    if statement.starts_with("SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION") {
        let rows: Vec<Vec<Value>> = guard
            .tables
            .iter()
            .filter(|(table, _)| statement.contains(&format!("TABLE_NAME = '{table}'")))
            .flat_map(|(_, found)| found.columns.iter())
            .map(|(column, data_type)| {
                let numeric = data_type == NUMBER_TYPE;
                vec![
                    json!(column),
                    json!(data_type),
                    numeric
                        .then_some("38")
                        .map_or(json!(null), |digits| json!(digits)),
                    numeric
                        .then_some("0")
                        .map_or(json!(null), |scale| json!(scale)),
                    json!(null),
                ]
            })
            .collect();
        return Ok(result(
            &[
                "COLUMN_NAME",
                "DATA_TYPE",
                "NUMERIC_PRECISION",
                "NUMERIC_SCALE",
                "CHARACTER_MAXIMUM_LENGTH",
            ],
            rows,
        ));
    }
    if statement.starts_with("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE") {
        let rows: Vec<Vec<Value>> = guard
            .tables
            .iter()
            .flat_map(|(table, found)| column_rows(table, found))
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
