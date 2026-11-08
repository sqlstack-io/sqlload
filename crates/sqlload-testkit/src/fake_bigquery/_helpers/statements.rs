//! The GoogleSQL statements sqlload runs on BigQuery, answered from the fake's tables.

use axum::http::StatusCode;
use serde_json::{Value, json};

use crate::fake_bigquery::models::{FakeState, FakeTable, SharedState};

/// A REST answer, or the HTTP status and Google error body it fails with.
pub(crate) type Outcome = Result<Value, (StatusCode, Value)>;

/// Google's `{"error": {...}}` body.
pub(crate) fn google_error(code: u16, reason: &str, message: &str) -> Value {
    json!({ "error": { "code": code, "message": message, "errors": [{ "reason": reason, "message": message }] } })
}

/// `` `p`.`d`.`t` `` or `` `d`.`t` `` → `d.t`, lower-cased.
fn table_key(name: &str) -> String {
    let parts: Vec<String> = name
        .split("`.`")
        .map(|part| part.trim_matches('`').to_ascii_lowercase())
        .collect();
    let skipped = parts.len().saturating_sub(2);
    parts[skipped..].join(".")
}

/// The first name after `prefix`, up to the next space.
fn name_after<'a>(statement: &'a str, prefix: &str) -> Option<&'a str> {
    let rest = statement.strip_prefix(prefix)?;
    Some(rest.split(' ').next().unwrap_or(rest))
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

/// `` `a` INT64 NOT NULL, `b` NUMERIC(18,2) `` → names and types; a type's own commas sit inside parentheses or angle brackets.
fn column_list(body: &str) -> Vec<(String, String)> {
    let mut pieces: Vec<&str> = Vec::new();
    let (mut depth, mut start) = (0_i32, 0_usize);
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
    pieces
        .into_iter()
        .filter_map(|piece| {
            let (name, sql_type) = piece.trim().strip_prefix('`')?.split_once("` ")?;
            let sql_type = sql_type.split(" OPTIONS").next().unwrap_or(sql_type);
            Some((name.to_owned(), sql_type.trim().to_owned()))
        })
        .collect()
}

/// A `tables.get` schema field for a column of DDL type `sql_type`, in the legacy type names BigQuery reports.
fn schema_field(name: &str, sql_type: &str) -> Value {
    let upper = sql_type.to_ascii_uppercase();
    let (required, upper) = match upper.strip_suffix(" NOT NULL") {
        Some(rest) => (true, rest.to_owned()),
        None => (false, upper),
    };
    let (repeated, inner) = match upper
        .strip_prefix("ARRAY<")
        .and_then(|rest| rest.strip_suffix('>'))
    {
        Some(inner) => (true, inner.to_owned()),
        None => (false, upper.clone()),
    };
    let (base, parameters) = match inner.split_once('(') {
        Some((base, parameters)) => (
            base.trim().to_owned(),
            parameters.trim_end_matches(')').to_owned(),
        ),
        None => (inner.clone(), String::new()),
    };
    let legacy = match base.as_str() {
        "INT64" => "INTEGER",
        "FLOAT64" => "FLOAT",
        "BOOL" => "BOOLEAN",
        "STRUCT" => "RECORD",
        other => other,
    };
    let mode = match (repeated, required) {
        (true, _) => "REPEATED",
        (false, true) => "REQUIRED",
        (false, false) => "NULLABLE",
    };
    let mut field = json!({ "name": name, "type": legacy, "mode": mode });
    match (base.as_str(), parameters.split_once(',')) {
        ("NUMERIC" | "BIGNUMERIC", Some((precision, scale))) => {
            field["precision"] = json!(precision.trim());
            field["scale"] = json!(scale.trim());
        }
        ("STRING" | "BYTES", None) if !parameters.is_empty() => {
            field["maxLength"] = json!(parameters.trim());
        }
        _ => {}
    }
    field
}

/// `tables.get` for `dataset.table`: its schema, or Google's 404.
pub(crate) fn table_metadata(state: &SharedState, dataset: &str, table: &str) -> Outcome {
    let key = format!("{dataset}.{table}").to_ascii_lowercase();
    let state = state.lock();
    let Some(found) = state.tables.get(&key) else {
        return Err((
            StatusCode::NOT_FOUND,
            google_error(404, "notFound", &format!("Not found: Table {key}")),
        ));
    };
    let fields: Vec<Value> = found
        .columns
        .iter()
        .map(|(name, sql_type)| schema_field(name, sql_type))
        .collect();
    Ok(json!({
        "tableReference": { "datasetId": dataset, "tableId": table },
        "schema": { "fields": fields },
    }))
}

/// `jobs.query`: run the statement and answer with every row at once, or fail it as an invalid query.
pub(crate) fn run_query(state: &SharedState, body: &[u8]) -> Outcome {
    let request: Value = serde_json::from_slice(body).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            google_error(400, "badRequest", &error.to_string()),
        )
    })?;
    let statement = request["query"].as_str().unwrap_or_default().to_owned();
    let mut state = state.lock();
    state.statements.push(statement.clone());
    let (columns, rows) = state.answer(&statement).map_err(|message| {
        (
            StatusCode::BAD_REQUEST,
            google_error(400, "invalidQuery", &message),
        )
    })?;
    let fields: Vec<Value> = columns.iter().map(|name| json!({ "name": name })).collect();
    let rows: Vec<Value> = rows
        .into_iter()
        .map(|row| json!({ "f": row.into_iter().map(|value| json!({ "v": value })).collect::<Vec<Value>>() }))
        .collect();
    Ok(json!({
        "jobComplete": true,
        "jobReference": { "jobId": format!("job_{}", state.statements.len()) },
        "schema": { "fields": fields },
        "rows": rows,
    }))
}

/// Column names and rows of text.
type Answer = Result<(Vec<String>, Vec<Vec<Value>>), String>;

/// A statement that returns nothing.
fn done() -> Answer {
    Ok((Vec::new(), Vec::new()))
}

impl FakeState {
    fn answer(&mut self, statement: &str) -> Answer {
        if let Some(name) = name_after(statement, "CREATE TABLE IF NOT EXISTS ") {
            let columns = parenthesised(statement)
                .map(column_list)
                .unwrap_or_default();
            self.tables.entry(table_key(name)).or_insert(FakeTable {
                columns,
                rows: Vec::new(),
            });
            return done();
        }
        if let Some(name) = name_after(statement, "CREATE OR REPLACE TABLE ") {
            return self.replace_table(statement, name);
        }
        if let Some(name) = name_after(statement, "DROP TABLE IF EXISTS ") {
            self.tables.remove(&table_key(name));
            return done();
        }
        if let Some(name) = name_after(statement, "ALTER TABLE ") {
            let added: Vec<(String, String)> = statement
                .split("ADD COLUMN ")
                .skip(1)
                .flat_map(|piece| {
                    column_list(
                        piece
                            .trim_start_matches("IF NOT EXISTS ")
                            .trim()
                            .trim_end_matches(','),
                    )
                })
                .collect();
            if let Some(table) = self.tables.get_mut(&table_key(name)) {
                table.columns.extend(added);
            }
            return done();
        }
        if let Some(name) = name_after(statement, "INSERT INTO ")
            && statement.contains(" SELECT ")
            && let Some((_, source)) = statement.rsplit_once(" FROM ")
        {
            let source = source.split(' ').next().unwrap_or(source);
            let rows = self
                .tables
                .get(&table_key(source))
                .map(|found| found.rows.clone())
                .ok_or_else(|| format!("Not found: Table {source}"))?;
            self.tables
                .entry(table_key(name))
                .or_default()
                .rows
                .extend(rows);
            return done();
        }
        if let Some(name) = name_after(statement, "DELETE FROM ")
            && statement.ends_with(" WHERE TRUE")
        {
            if let Some(table) = self.tables.get_mut(&table_key(name)) {
                table.rows.clear();
            }
            return done();
        }
        if let Some((before, _)) = statement.split_once(".INFORMATION_SCHEMA.TABLES") {
            let dataset = before
                .rsplit('.')
                .next()
                .unwrap_or(before)
                .trim_matches('`')
                .to_ascii_lowercase();
            let prefix = format!("{dataset}.");
            let rows: Vec<Vec<Value>> = self
                .tables
                .keys()
                .filter_map(|key| key.strip_prefix(&prefix))
                .filter(|table| table.contains("__sqlload_staging_"))
                .map(|table| vec![json!(table)])
                .collect();
            return Ok((vec!["table_name".to_owned()], rows));
        }
        if let Some(name) = name_after(statement, "SELECT CAST(COUNT(*) AS STRING) FROM ") {
            let rows = self
                .tables
                .get(&table_key(name))
                .map(|found| found.rows.len())
                .ok_or_else(|| format!("Not found: Table {name}"))?;
            return Ok((vec!["f0_".to_owned()], vec![vec![json!(rows.to_string())]]));
        }
        done()
    }

    /// `CREATE OR REPLACE TABLE t (...)` makes an empty table; `CREATE OR REPLACE TABLE t AS SELECT ... FROM b` takes `b`'s columns and rows.
    fn replace_table(&mut self, statement: &str, name: &str) -> Answer {
        let key = table_key(name);
        if !statement.contains(" AS SELECT ") {
            let columns = parenthesised(statement)
                .map(column_list)
                .unwrap_or_default();
            self.tables.insert(
                key,
                FakeTable {
                    columns,
                    rows: Vec::new(),
                },
            );
            return done();
        }
        let source = statement
            .rsplit_once(" FROM ")
            .map(|(_, source)| table_key(source.split(' ').next().unwrap_or(source)))
            .unwrap_or_default();
        let found = self
            .tables
            .get(&source)
            .cloned()
            .ok_or_else(|| format!("Not found: Table {source}"))?;
        self.tables.insert(key, found);
        done()
    }
}
