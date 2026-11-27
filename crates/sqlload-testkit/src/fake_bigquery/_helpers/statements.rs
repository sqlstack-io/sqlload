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

/// What a `CREATE TABLE` writes after its column list: its `PARTITION BY`, `CLUSTER BY` and `OPTIONS` clauses, `AS SELECT …` left out; empty for none.
fn layout_after_columns(statement: &str) -> String {
    let head = statement.split(" AS SELECT ").next().unwrap_or_default();
    let Some(columns) = parenthesised(head) else {
        return String::new();
    };
    let end = head
        .find(columns)
        .map_or(0, |start| start + columns.len() + 1);
    head.get(end..).unwrap_or_default().trim().to_owned()
}

/// The text of `layout`'s clause opened by `name` (such as `PARTITION BY `), up to the next clause.
fn layout_clause<'a>(layout: &'a str, name: &str) -> Option<&'a str> {
    let (_, rest) = layout.split_once(name)?;
    let end = ["PARTITION BY ", "CLUSTER BY ", "OPTIONS ("]
        .iter()
        .filter_map(|next| rest.find(next))
        .min()
        .unwrap_or(rest.len());
    Some(rest[..end].trim())
}

/// Where `statement` (lower-cased) reads `quoted`, the table's name: after `FROM` or `MERGE INTO`.
fn read_at(statement: &str, quoted: &str) -> Option<usize> {
    let reads = |at: &usize| {
        let before = statement[..*at].trim_end_matches(|character: char| character != ' ');
        before.ends_with(" from ") || before.ends_with("merge into ")
    };
    statement
        .match_indices(quoted)
        .map(|(at, _)| at)
        .find(reads)
}

/// What `statement` says from its first `WHERE` or `ON` after `start` on, its filter; empty for none.
fn filter_after(statement: &str, start: usize) -> &str {
    let after = &statement[start..];
    let found: Vec<usize> = [" where ", " on "]
        .iter()
        .filter_map(|keyword| after.find(keyword))
        .collect();
    found.iter().min().map_or("", |start| &after[*start..])
}

/// `tables.get`'s `timePartitioning`, `rangePartitioning`, `clustering` and `requirePartitionFilter` for a table laid out as `layout`.
pub(crate) fn layout_fields(layout: &str) -> serde_json::Map<String, Value> {
    let mut fields = serde_json::Map::new();
    let clause = |name: &str| layout_clause(layout, name);
    let unquoted = |text: &str| text.trim().trim_matches('`').to_owned();
    if let Some(partition) = clause("PARTITION BY ") {
        let inner = parenthesised(partition);
        let mut arguments = inner.unwrap_or(partition).splitn(2, ',');
        let column = unquoted(arguments.next().unwrap_or_default());
        let rest = arguments.next().unwrap_or_default().trim();
        if partition.starts_with("RANGE_BUCKET") {
            let numbers: Vec<&str> = parenthesised(rest)
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .collect();
            fields.insert("rangePartitioning".to_owned(), json!({ "field": column, "range": { "start": numbers.first(), "end": numbers.get(1), "interval": numbers.get(2) } }));
        } else {
            let unit = match rest {
                "" => "DAY",
                unit => unit,
            };
            fields.insert(
                "timePartitioning".to_owned(),
                json!({ "type": unit, "field": column }),
            );
        }
    }
    if let Some(clustered) = clause("CLUSTER BY ") {
        let columns: Vec<String> = clustered.split(',').map(unquoted).collect();
        fields.insert("clustering".to_owned(), json!({ "fields": columns }));
    }
    if layout.contains("require_partition_filter = TRUE") {
        fields.insert("requirePartitionFilter".to_owned(), json!(true));
    }
    fields
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
    let mut answer = json!({
        "tableReference": { "datasetId": dataset, "tableId": table },
        "type": "TABLE",
        "numRows": found.rows.len().to_string(),
        "schema": { "fields": fields },
    });
    if let Some(object) = answer.as_object_mut() {
        object.extend(layout_fields(&found.layout));
    }
    Ok(answer)
}

/// `datasets.list`: every dataset that holds a table, in one page.
pub(crate) fn list_datasets(state: &SharedState) -> Outcome {
    let state = state.lock();
    let mut datasets: Vec<&str> = Vec::new();
    for key in state.tables.keys() {
        if let Some((dataset, _)) = key.split_once('.') {
            datasets.push(dataset);
        }
    }
    datasets.dedup();
    let mut listed: Vec<Value> = Vec::new();
    for dataset in datasets {
        listed.push(json!({ "datasetReference": { "datasetId": dataset } }));
    }
    Ok(json!({ "datasets": listed }))
}

/// `tables.list` for `dataset`, in one page.
pub(crate) fn list_tables(state: &SharedState, dataset: &str) -> Outcome {
    let prefix = format!("{}.", dataset.to_ascii_lowercase());
    let state = state.lock();
    let listed: Vec<Value> = state
        .tables
        .keys()
        .filter_map(|key| key.strip_prefix(&prefix))
        .map(|table| json!({ "tableReference": { "datasetId": dataset, "tableId": table }, "type": "TABLE" }))
        .collect();
    Ok(json!({ "tables": listed }))
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
    /// BigQuery's refusal of `statement` where it reads a table with `require_partition_filter` (its name after `FROM` or `MERGE INTO`) without naming the partition column after a `WHERE` or `ON`.
    fn unfiltered(&self, statement: &str) -> Option<String> {
        self.tables.iter().find_map(|(key, table)| {
            let fields = layout_fields(&table.layout);
            fields.get("requirePartitionFilter")?;
            let partitioning = fields
                .get("timePartitioning")
                .or_else(|| fields.get("rangePartitioning"))?;
            let column = partitioning["field"].as_str()?.to_ascii_lowercase();
            let (dataset, name) = key.split_once('.')?;
            let quoted = format!("`{dataset}`.`{name}`");
            let lowered = statement.to_ascii_lowercase();
            let filter = filter_after(&lowered, read_at(&lowered, &quoted)?);
            (!filter.contains(&format!("`{column}`"))).then(|| format!(
                "Cannot query over table '{key}' without a filter over column(s) '{column}' that can be used for partition elimination"
            ))
        })
    }

    fn answer(&mut self, statement: &str) -> Answer {
        if let Some(refusal) = self.unfiltered(statement) {
            return Err(refusal);
        }
        if let Some(name) = name_after(statement, "CREATE TABLE IF NOT EXISTS ") {
            let columns = parenthesised(statement)
                .map(column_list)
                .unwrap_or_default();
            self.tables.entry(table_key(name)).or_insert(FakeTable {
                columns,
                rows: Vec::new(),
                layout: layout_after_columns(statement),
            });
            return done();
        }
        if let Some(name) = name_after(statement, "CREATE OR REPLACE TABLE ") {
            return self.replace_table(statement, name);
        }
        if let Some(name) = name_after(statement, "CREATE TABLE ") {
            if self.tables.contains_key(&table_key(name)) {
                return Err(format!("Already Exists: Table {name}"));
            }
            return self.replace_table(statement, name);
        }
        if let Some(name) = name_after(statement, "DROP TABLE IF EXISTS ") {
            self.tables.remove(&table_key(name));
            return done();
        }
        if let Some(name) = name_after(statement, "DROP TABLE ") {
            return (self.tables.remove(&table_key(name)))
                .map(|_| (Vec::new(), Vec::new()))
                .ok_or_else(|| format!("Not found: Table {name}"));
        }
        if let Some(name) = name_after(statement, "ALTER TABLE ")
            && let Some((_, renamed)) = statement.split_once(" RENAME TO ")
        {
            let key = table_key(name);
            let table =
                (self.tables.remove(&key)).ok_or_else(|| format!("Not found: Table {name}"))?;
            let dataset = key.split_once('.').map_or("", |(dataset, _)| dataset);
            self.tables
                .insert(format!("{dataset}.{}", table_key(renamed)), table);
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
        if let Some(name) = name_after(statement, "SELECT COUNT(*) FROM ")
            && let Some((_, condition)) = statement.split_once(" WHERE ")
            && let Some(column) = condition.strip_suffix(" IS NULL")
        {
            let parts: Vec<&str> = name.trim_matches('`').split('.').collect();
            let key = parts[parts.len().saturating_sub(2)..]
                .join(".")
                .to_ascii_lowercase();
            let column = column.trim_matches('`');
            let found = self
                .tables
                .get(&key)
                .ok_or_else(|| format!("Not found: Table {name}"))?;
            let nulls = found
                .rows
                .iter()
                .filter(|row| row.get(column).is_none_or(Value::is_null))
                .count();
            return Ok((vec!["f0_".to_owned()], vec![vec![json!(nulls.to_string())]]));
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

    /// `CREATE OR REPLACE TABLE t (...)` makes an empty table; `CREATE OR REPLACE TABLE t AS SELECT ... FROM b` takes `b`'s columns and rows, and with `LIKE b` its layout too, plus any clauses written after it; as BigQuery, it refuses to replace a table partitioned or clustered otherwise.
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
                    layout: layout_after_columns(statement),
                },
            );
            return done();
        }
        let source = statement
            .rsplit_once(" FROM ")
            .map(|(_, source)| table_key(source.split(' ').next().unwrap_or(source)))
            .unwrap_or_default();
        let mut found = self
            .tables
            .get(&source)
            .cloned()
            .ok_or_else(|| format!("Not found: Table {source}"))?;
        let head = statement.split(" AS SELECT ").next().unwrap_or_default();
        found.layout = match head.split_once(" LIKE ") {
            Some((_, like)) => {
                let added = like
                    .split_once(' ')
                    .map_or("", |(_, clauses)| clauses.trim());
                format!("{} {added}", found.layout).trim().to_owned()
            }
            None => layout_after_columns(head),
        };
        let partitioning = |layout: &str| {
            let fields = layout_fields(layout);
            ["timePartitioning", "rangePartitioning", "clustering"]
                .map(|field| fields.get(field).cloned())
        };
        if let Some(replaced) = self.tables.get(&key)
            && partitioning(&replaced.layout) != partitioning(&found.layout)
        {
            return Err("Cannot replace a table with a different partitioning spec. Instead, DROP the table, and then recreate it.".to_owned());
        }
        self.tables.insert(key, found);
        done()
    }
}
