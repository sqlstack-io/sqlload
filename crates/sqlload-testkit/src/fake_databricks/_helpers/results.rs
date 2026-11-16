//! A source's reads on the fake: `SELECT`s over its tables answered as Arrow stream files behind links (`EXTERNAL_LINKS`, `ARROW_STREAM`), counts, and the `information_schema` queries discovery runs.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde_json::{Value, json};

use crate::fake_databricks::models::{FakeState, FakeTable};

const STATEMENTS: &str = "/api/2.0/sql/statements";

/// Where the fake serves result files, under its own address.
pub(crate) const RESULTS: &str = "/results/";

/// A finished statement returning `rows` of text under `columns`.
fn succeeded(columns: &[&str], rows: Vec<Vec<Value>>) -> Value {
    let described: Vec<Value> = columns.iter().map(|name| json!({ "name": name })).collect();
    json!({
        "status": { "state": "SUCCEEDED" },
        "manifest": { "schema": { "columns": described } },
        "result": { "data_array": rows },
    })
}

/// A statement that failed with `message`.
fn failed(message: &str) -> Value {
    json!({
        "status": { "state": "FAILED", "error": { "error_code": "BAD_REQUEST", "message": message } },
    })
}

/// The Arrow type the fake sends a column of `sql_type` as: what Databricks sends for scalars, and JSON text for `array`, `map` and `struct`.
fn arrow_type(sql_type: &str) -> DataType {
    let lower = sql_type.to_ascii_lowercase();
    let digits = lower
        .strip_prefix("decimal(")
        .and_then(|rest| rest.strip_suffix(')'))
        .and_then(|rest| rest.split_once(','))
        .and_then(|(precision, scale)| Some((precision.trim().parse().ok()?, scale.trim().parse().ok()?)));
    match (lower.as_str(), digits) {
        (_, Some((precision, scale))) => DataType::Decimal128(precision, scale),
        ("bigint" | "long", _) => DataType::Int64,
        ("int" | "integer", _) => DataType::Int32,
        ("double", _) => DataType::Float64,
        ("boolean", _) => DataType::Boolean,
        ("date", _) => DataType::Date32,
        ("timestamp", _) => DataType::Timestamp(TimeUnit::Microsecond, Some("Etc/UTC".into())),
        ("timestamp_ntz", _) => DataType::Timestamp(TimeUnit::Microsecond, None),
        _ => DataType::Utf8,
    }
}

/// A row's value as text: strings as they are, other JSON as written, null as null.
fn text(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        other => Some(other.to_string()),
    }
}

/// `` `a`, `b` `` → `a`, `b`; `*` → every column of the table.
fn selected(list: &str, table: &FakeTable) -> Vec<(String, String)> {
    if list.trim() == "*" {
        return table.columns.clone();
    }
    list.split(", ")
        .map(|name| name.trim().trim_matches('`').replace("``", "`"))
        .map(|name| {
            let sql_type = table
                .columns
                .iter()
                .find(|(column, _)| column.eq_ignore_ascii_case(&name))
                .map_or_else(|| "string".to_owned(), |(_, sql_type)| sql_type.clone());
            (name, sql_type)
        })
        .collect()
}

/// Whether `row` passes `filter`: conditions `` `column` <op> literal `` or `` `column` IS NULL `` joined by ` AND `, a literal a bare number or a quoted string; a condition the fake cannot read is an error.
fn passes(row: &Value, filter: &str) -> Result<bool, String> {
    for condition in filter.split(" AND ") {
        let condition = condition.trim().trim_start_matches('(').trim_end_matches(')');
        if let Some(column) = condition.strip_suffix(" IS NULL") {
            if !row[column.trim_matches('`')].is_null() {
                return Ok(false);
            }
            continue;
        }
        let mut parts = condition.splitn(3, ' ');
        let (Some(column), Some(operator), Some(literal)) = (parts.next(), parts.next(), parts.next())
        else {
            return Err(format!("the fake cannot read the condition {condition:?}"));
        };
        let value = &row[column.trim_matches('`')];
        if value.is_null() {
            return Ok(false);
        }
        let order = match literal.strip_prefix('\'').and_then(|rest| rest.strip_suffix('\'')) {
            Some(quoted) => text(value).unwrap_or_default().as_str().cmp(quoted),
            None => {
                let (Some(left), Ok(right)) = (
                    text(value).and_then(|value| value.parse::<f64>().ok()),
                    literal.parse::<f64>(),
                ) else {
                    return Err(format!("the fake cannot compare {condition:?}"));
                };
                left.total_cmp(&right)
            }
        };
        let kept = match operator {
            ">" => order.is_gt(),
            ">=" => order.is_ge(),
            "<" => order.is_lt(),
            "<=" => order.is_le(),
            "=" => order.is_eq(),
            _ => return Err(format!("the fake cannot read the operator in {condition:?}")),
        };
        if !kept {
            return Ok(false);
        }
    }
    Ok(true)
}

impl FakeState {
    /// The table a `FROM` item names: `` `c`.`s`.`t` ``, or a subquery `(SELECT * FROM s.t\n)`; then the rest of the statement after it.
    fn from_item<'a>(&self, after_from: &'a str) -> Result<(&FakeTable, &'a str), String> {
        let (name, rest) = match after_from.strip_prefix('(') {
            Some(query) => {
                let (inner, rest) = query
                    .split_once("\n)")
                    .ok_or_else(|| format!("the fake cannot read the subquery {after_from:?}"))?;
                let name = inner
                    .trim()
                    .strip_prefix("SELECT * FROM ")
                    .ok_or_else(|| format!("the fake reads only SELECT * FROM a table, not {inner:?}"))?;
                (name.trim().to_owned(), rest)
            }
            None => {
                let end = after_from.find(' ').unwrap_or(after_from.len());
                (after_from[..end].to_owned(), &after_from[end..])
            }
        };
        let parts: Vec<String> = name
            .split('.')
            .map(|part| part.trim_matches('`').to_ascii_lowercase())
            .collect();
        let key = parts[parts.len().saturating_sub(2)..].join(".");
        self.tables
            .get(&key)
            .map(|table| (table, rest))
            .ok_or_else(|| format!("[TABLE_OR_VIEW_NOT_FOUND] {name}"))
    }

    /// The rows of `table` that the ` WHERE ...` in `rest` keeps, none under ` LIMIT 0`.
    fn kept_rows(table: &FakeTable, rest: &str) -> Result<Vec<Value>, String> {
        if rest.trim_end().ends_with("LIMIT 0") {
            return Ok(Vec::new());
        }
        let filter = rest.trim().strip_prefix("WHERE ").unwrap_or_default();
        let mut rows: Vec<Value> = Vec::new();
        for row in &table.rows {
            if filter.is_empty() || passes(row, filter)? {
                rows.push(row.clone());
            }
        }
        Ok(rows)
    }

    /// Answer a statement a source runs, or `None` when it is not one: `information_schema` listings, `COUNT(*)`, or with `links` a `SELECT` written as Arrow stream files.
    pub(crate) fn source_answer(&mut self, statement: &str, links: bool) -> Option<Value> {
        if links {
            return Some(self.arrow_answer(statement));
        }
        if statement.starts_with("SELECT table_schema, table_name, comment FROM ") {
            let rows: Vec<Vec<Value>> = self
                .tables
                .keys()
                .filter_map(|key| key.split_once('.'))
                .map(|(schema, table)| vec![json!(schema), json!(table), Value::Null])
                .collect();
            return Some(succeeded(&["table_schema", "table_name", "comment"], rows));
        }
        if statement.contains("full_data_type, is_nullable FROM ") {
            let mut rows: Vec<Vec<Value>> = Vec::new();
            for (key, table) in &self.tables {
                let Some((schema, name)) = key.split_once('.') else { continue };
                for (column, sql_type) in &table.columns {
                    rows.push(vec![json!(schema), json!(name), json!(column), json!(sql_type), json!("YES")]);
                }
            }
            return Some(succeeded(&["table_schema", "table_name", "column_name", "full_data_type", "is_nullable"], rows));
        }
        if statement.contains("information_schema.table_constraints") {
            let mut rows: Vec<Vec<Value>> = Vec::new();
            for (key, columns) in &self.primary_keys {
                let Some((schema, name)) = key.split_once('.') else { continue };
                for column in columns {
                    rows.push(vec![json!(schema), json!(name), json!(column)]);
                }
            }
            return Some(succeeded(&["table_schema", "table_name", "column_name"], rows));
        }
        let after = statement.strip_prefix("SELECT COUNT(*) FROM ")?;
        Some(
            match self
                .from_item(after)
                .and_then(|(table, rest)| Self::kept_rows(table, rest))
            {
                Ok(rows) => succeeded(&["count(1)"], vec![vec![json!(rows.len().to_string())]]),
                Err(message) => failed(&message),
            },
        )
    }

    /// `SELECT <columns> FROM <table or subquery> [WHERE ...] [LIMIT 0]` as Arrow stream files, one per chunk of [`FakeState::chunk_rows`] rows (one file when unset), each behind a link; its manifest describes the columns.
    fn arrow_answer(&mut self, statement: &str) -> Value {
        let Some((list, after)) = statement
            .strip_prefix("SELECT ")
            .and_then(|rest| rest.split_once(" FROM "))
        else {
            return failed(&format!("the fake reads only SELECTs as Arrow, not {statement:?}"));
        };
        let (columns, rows) = match self.from_item(after).and_then(|(table, rest)| {
            Ok((selected(list, table), Self::kept_rows(table, rest)?))
        }) {
            Ok(found) => found,
            Err(message) => return failed(&message),
        };
        self.statement_ids += 1;
        let id = format!("stmt-{}", self.statement_ids);
        let size = self.chunk_rows.filter(|size| *size > 0).unwrap_or(usize::MAX);
        let pieces: Vec<&[Value]> = match rows.is_empty() {
            true => vec![&[]],
            false => rows.chunks(size).collect(),
        };
        let count = pieces.len();
        let mut first: Option<Value> = None;
        for (index, piece) in pieces.into_iter().enumerate() {
            let body = match arrow_file(&columns, piece) {
                Ok(body) => body,
                Err(message) => return failed(&message),
            };
            let path = format!("{RESULTS}{id}/{index}");
            self.result_files.insert(path.clone(), body);
            let mut link = json!({
                "chunk_index": index,
                "row_count": piece.len(),
                "external_link": format!("{}{path}", self.base_url),
                "expiration": "2099-01-01T00:00:00.000Z",
            });
            if index + 1 < count {
                link["next_chunk_index"] = json!(index + 1);
                link["next_chunk_internal_link"] =
                    json!(format!("{STATEMENTS}/{id}/result/chunks/{}", index + 1));
            }
            let chunk = json!({ "external_links": [link] });
            self.chunks.insert((id.clone(), index), chunk.clone());
            first.get_or_insert(chunk);
        }
        let described: Vec<Value> = columns
            .iter()
            .map(|(name, sql_type)| json!({ "name": name, "type_text": sql_type.to_ascii_uppercase() }))
            .collect();
        let answer = json!({
            "statement_id": id,
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "format": "ARROW_STREAM",
                "schema": { "columns": described },
                "total_chunk_count": count,
                "total_row_count": rows.len(),
                "truncated": false,
            },
            "result": first.unwrap_or_else(|| json!({})),
        });
        self.held(id, answer)
    }

    /// The bytes of a result file, or why it can't be downloaded: the scripted expiries come first.
    pub(crate) fn download(&mut self, path: &str, authorized: bool) -> Result<Vec<u8>, u16> {
        self.downloads.push((path.to_owned(), authorized));
        if self.expire_downloads > 0 {
            self.expire_downloads -= 1;
            return Err(403);
        }
        self.result_files.get(path).cloned().ok_or(404)
    }
}

/// `rows` as one Arrow IPC stream holding `columns`, each built from text and cast to its Arrow type.
fn arrow_file(columns: &[(String, String)], rows: &[Value]) -> Result<Vec<u8>, String> {
    let mut fields: Vec<Field> = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (name, sql_type) in columns {
        let values: StringArray = rows.iter().map(|row| text(&row[name.as_str()])).collect();
        let data_type = arrow_type(sql_type);
        let array = arrow_cast::cast(&(Arc::new(values) as ArrayRef), &data_type)
            .map_err(|error| format!("{name}: {error}"))?;
        fields.push(Field::new(name, data_type, true));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = match arrays.is_empty() {
        true => RecordBatch::new_empty(schema.clone()),
        false => RecordBatch::try_new(schema.clone(), arrays).map_err(|error| error.to_string())?,
    };
    let mut body: Vec<u8> = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &schema)
        .map_err(|error| error.to_string())?;
    writer.write(&batch).map_err(|error| error.to_string())?;
    writer.finish().map_err(|error| error.to_string())?;
    drop(writer);
    Ok(body)
}
