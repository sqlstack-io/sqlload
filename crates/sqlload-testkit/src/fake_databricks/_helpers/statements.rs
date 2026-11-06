//! The SQL statements sqlload runs on a Databricks warehouse, answered from the fake's tables and volume.

use serde_json::{Value, json};

use crate::fake_databricks::_helpers::volume::Outcome;
use crate::fake_databricks::models::{FakeState, FakeTable, SharedState};
use parquet::file::reader::{FileReader, SerializedFileReader};

/// A finished statement returning `rows` of text under `columns`.
fn succeeded(columns: &[&str], rows: Vec<Vec<Value>>) -> Value {
    let described: Vec<Value> = columns.iter().map(|name| json!({ "name": name })).collect();
    json!({
        "statement_id": "fake",
        "status": { "state": "SUCCEEDED" },
        "manifest": { "schema": { "columns": described } },
        "result": { "data_array": rows },
    })
}

/// A statement that failed with `message`.
fn failed(message: &str) -> Value {
    json!({
        "statement_id": "fake",
        "status": { "state": "FAILED", "error": { "error_code": "BAD_REQUEST", "message": message } },
    })
}

/// `` `c`.`s`.`t` `` or `` `s`.`t` `` → `s.t`, lower-cased.
fn table_key(name: &str) -> String {
    let parts: Vec<String> = name
        .split("`.`")
        .map(|part| part.trim_matches('`').to_ascii_lowercase())
        .collect();
    parts
        .iter()
        .rev()
        .take(2)
        .rev()
        .cloned()
        .collect::<Vec<String>>()
        .join(".")
}

/// The first name after `prefix`, up to the next space.
fn name_after<'a>(statement: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = statement.strip_prefix(prefix)?;
    Some(rest.split(' ').next().unwrap_or(rest))
}

/// `` `a` TYPE, `b` decimal(18,2) `` → names and types; a type's own commas sit inside parentheses.
fn column_list(body: &str) -> Vec<(String, String)> {
    let mut columns: Vec<(String, String)> = Vec::new();
    let (mut depth, mut start) = (0_i32, 0_usize);
    let mut pieces: Vec<&str> = Vec::new();
    for (index, character) in body.char_indices() {
        match character {
            '(' | '<' => depth += 1,
            ')' | '>' => depth -= 1,
            ',' if depth == 0 => {
                pieces.push(&body[start..index]);
                start = index + 1;
            }
            _ => {}
        }
    }
    pieces.push(&body[start..]);
    for piece in pieces {
        let piece = piece.trim();
        let Some(rest) = piece.strip_prefix('`') else {
            continue;
        };
        let Some((name, sql_type)) = rest.split_once("` ") else {
            continue;
        };
        columns.push((name.to_owned(), sql_type.trim().to_ascii_lowercase()));
    }
    columns
}

/// The text between the first `(` and its matching `)`.
fn parenthesised(text: &str) -> Option<&str> {
    let open = text.find('(')?;
    let mut depth = 0_i32;
    for (index, character) in text[open..].char_indices() {
        match character {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&text[open + 1..open + index]);
                }
            }
            _ => {}
        }
    }
    None
}

/// The quoted text after `marker`, unescaped.
fn literal_after(statement: &str, marker: &str) -> Option<String> {
    let rest = statement.split_once(marker)?.1.strip_prefix('\'')?;
    let mut text = String::new();
    let mut characters = rest.chars();
    while let Some(character) = characters.next() {
        match character {
            '\\' => text.extend(characters.next()),
            '\'' => return Some(text),
            other => text.push(other),
        }
    }
    None
}

/// Run one statement on the fake and answer as the Statement Execution API does.
pub(crate) fn run_statement(state: &SharedState, body: &[u8]) -> Outcome {
    let request: Value = match serde_json::from_slice(body) {
        Ok(request) => request,
        Err(error) => {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                json!({ "error_code": "MALFORMED_REQUEST", "message": error.to_string() }),
            ));
        }
    };
    let statement = request["statement"].as_str().unwrap_or_default().to_owned();
    let mut state = state.lock();
    state.statements.push(statement.clone());
    Ok(state.answer(&statement))
}

impl FakeState {
    fn answer(&mut self, statement: &str) -> Value {
        let state = self;
        if let Some(name) = name_after(statement, "CREATE TABLE IF NOT EXISTS ") {
            let key = table_key(name);
            let columns = parenthesised(statement)
                .map(column_list)
                .unwrap_or_default();
            state.tables.entry(key).or_insert(FakeTable {
                columns,
                rows: Vec::new(),
            });
            return succeeded(&[], Vec::new());
        }
        if let Some(name) = name_after(statement, "CREATE OR REPLACE TABLE ") {
            return state.replace_table(statement, name);
        }
        if let Some(name) = name_after(statement, "DROP TABLE IF EXISTS ") {
            state.tables.remove(&table_key(name));
            return succeeded(&[], Vec::new());
        }
        if let Some(name) = name_after(statement, "ALTER TABLE ")
            && let Some((_, added)) = statement.split_once(" ADD COLUMNS ")
        {
            let columns = parenthesised(added).map(column_list).unwrap_or_default();
            if let Some(table) = state.tables.get_mut(&table_key(name)) {
                table.columns.extend(columns);
            }
            return succeeded(&[], Vec::new());
        }
        if let Some(name) = name_after(statement, "COPY INTO ") {
            return state.copy_into(statement, name);
        }
        if let Some(name) = name_after(statement, "INSERT INTO ")
            && let Some(source) = statement.split_once(" FROM ").map(|(_, rest)| rest.trim())
        {
            let rows = state
                .tables
                .get(&table_key(source))
                .map(|found| found.rows.clone())
                .unwrap_or_default();
            state
                .tables
                .entry(table_key(name))
                .or_default()
                .rows
                .extend(rows);
            return succeeded(&["num_affected_rows"], Vec::new());
        }
        if statement.contains("information_schema.columns") {
            return described_columns(state, statement);
        }
        succeeded(&[], Vec::new())
    }

    /// `CREATE OR REPLACE TABLE t (...)` makes an empty table; `CREATE OR REPLACE TABLE t AS SELECT * FROM b` takes `b`'s columns and rows, and a rebuild from `t` itself keeps them.
    fn replace_table(&mut self, statement: &str, name: &str) -> Value {
        let state = self;
        let key = table_key(name);
        if !statement.contains(" AS SELECT ") {
            let columns = parenthesised(statement)
                .map(column_list)
                .unwrap_or_default();
            state.tables.insert(
                key,
                FakeTable {
                    columns,
                    rows: Vec::new(),
                },
            );
            return succeeded(&[], Vec::new());
        }
        let source = statement
            .rsplit_once(" FROM ")
            .map(|(_, source)| table_key(source.trim()));
        match source.and_then(|source| state.tables.get(&source).cloned()) {
            Some(found) => {
                state.tables.insert(key, found);
                succeeded(&[], Vec::new())
            }
            None => failed(&format!("[TABLE_OR_VIEW_NOT_FOUND] {statement}")),
        }
    }

    /// `COPY INTO t FROM (SELECT ... FROM '<folder>/')`: the folder's Parquet rows join `t`, or the next scripted failure.
    fn copy_into(&mut self, statement: &str, name: &str) -> Value {
        let state = self;
        if let Some(message) = state.copy_failures.pop_front() {
            return failed(&message);
        }
        let Some(folder) = statement
            .rfind(" FROM '")
            .and_then(|at| literal_after(&statement[at..], " FROM "))
        else {
            return failed("COPY INTO reads no folder");
        };
        let paths = state.files_under(&folder);
        let key = table_key(name);
        if !state.tables.contains_key(&key) {
            return failed(&format!("[TABLE_OR_VIEW_NOT_FOUND] {name}"));
        }
        let mut rows: Vec<Value> = Vec::new();
        for path in &paths {
            let body = state
                .files
                .get(path)
                .map(|file| file.body.clone())
                .unwrap_or_default();
            match parquet_rows(body) {
                Ok(read) => rows.extend(read),
                Err(error) => return failed(&format!("{path} is not Parquet: {error}")),
            }
        }
        state.copied.push(paths);
        let loaded = rows.len();
        if let Some(table) = state.tables.get_mut(&key) {
            table.rows.extend(rows);
        }
        succeeded(
            &["num_affected_rows", "num_inserted_rows"],
            vec![vec![json!(loaded.to_string()), json!(loaded.to_string())]],
        )
    }
}

/// The columns of the table `information_schema.columns` is asked about: name, `full_data_type`, precision and scale.
fn described_columns(state: &FakeState, statement: &str) -> Value {
    let schema = literal_after(statement, "table_schema = ").unwrap_or_default();
    let table = literal_after(statement, "table_name = ").unwrap_or_default();
    let mut rows: Vec<Vec<Value>> = Vec::new();
    let columns = state
        .tables
        .get(&format!("{schema}.{table}"))
        .map(|found| &found.columns);
    for (column, sql_type) in columns.into_iter().flatten() {
        let (precision, scale) = decimal_digits(sql_type);
        rows.push(vec![json!(column), json!(sql_type), precision, scale]);
    }
    succeeded(
        &[
            "column_name",
            "full_data_type",
            "numeric_precision",
            "numeric_scale",
        ],
        rows,
    )
}

/// `decimal(18,2)` → `"18"`, `"2"`; nulls for other types.
fn decimal_digits(sql_type: &str) -> (Value, Value) {
    let digits = sql_type
        .strip_prefix("decimal(")
        .and_then(|rest| rest.strip_suffix(')'))
        .and_then(|rest| rest.split_once(','));
    match digits {
        Some((precision, scale)) => (json!(precision.trim()), json!(scale.trim())),
        None => (Value::Null, Value::Null),
    }
}

/// Every row of a Parquet file, as a JSON object.
fn parquet_rows(body: Vec<u8>) -> Result<Vec<Value>, String> {
    let reader =
        SerializedFileReader::new(bytes::Bytes::from(body)).map_err(|error| error.to_string())?;
    let mut rows: Vec<Value> = Vec::new();
    for row in reader
        .get_row_iter(None)
        .map_err(|error| error.to_string())?
    {
        rows.push(row.map_err(|error| error.to_string())?.to_json_value());
    }
    Ok(rows)
}
