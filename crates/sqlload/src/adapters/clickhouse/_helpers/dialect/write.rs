//! ClickHouse's writes: no transactions, so each is published by one swap of a whole table or of whole partitions, and a rerun converges.

use crate::adapters::clickhouse::_helpers::dialect::type_names::unwrapped;
use crate::adapters::clickhouse::constants::{ASIDE_SUFFIX, COPY_SUFFIX, PUBLISH_PREFIX};
use crate::adapters::clickhouse::models::{ClickHouseSql, PublishStep};
use crate::adapters::contract::constants::SWAP_SUFFIX;
use crate::adapters::contract::models::{TableName, WriteMode, WriteStatementsRequest};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;

/// `table` with `suffix` added to its name.
fn with_suffix(table: &TableName, suffix: &str) -> TableName {
    TableName {
        table: format!("{}{suffix}", table.table),
        ..table.clone()
    }
}

/// `step` as the statement the runner carries out itself.
pub(crate) fn publish_statement(step: &PublishStep) -> String {
    format!(
        "{PUBLISH_PREFIX}{}",
        serde_json::to_string(step).unwrap_or_default()
    )
}

/// `build` swapped in for `target` with `EXCHANGE TABLES`, then dropped, holding the old table.
pub(crate) fn swap_statements(
    adapter: &ClickHouseSql,
    target: &TableName,
    build: &TableName,
) -> Vec<String> {
    let build_name = adapter.render_qualified_name(build);
    vec![
        publish_statement(&PublishStep::Exchange {
            target: adapter.render_qualified_name(target),
            build: build_name.clone(),
            aside: adapter.render_qualified_name(&with_suffix(build, ASIDE_SUFFIX)),
        }),
        format!("DROP TABLE IF EXISTS {build_name}"),
    ]
}

/// The statements applying `request.mode` from `staging`. A build table is swapped in. Otherwise the staged rows, converted to the table's types, go into a delta table created `AS` the table, so it has its engine and keys; `replace` swaps that in whole, and the incremental modes publish its partitions, replacing the table's rows that `merge` matches by key or `delete_insert` covers by its range.
pub(crate) fn write_statements(
    adapter: &ClickHouseSql,
    request: &WriteStatementsRequest<'_>,
    staging: &TableName,
) -> Vec<String> {
    if request.built.is_some() {
        return swap_statements(adapter, request.table, staging);
    }
    let target = adapter.render_qualified_name(request.table);
    let delta_table = with_suffix(staging, SWAP_SUFFIX);
    let delta = adapter.render_qualified_name(&delta_table);
    let names: Vec<String> = request
        .columns
        .iter()
        .map(|column| adapter.render_identifier(&column.name))
        .collect();
    let values: Vec<String> = request
        .columns
        .iter()
        .map(|column| {
            let value = adapter.render_source_expression_cast(&column.name, &column.column_type);
            match request.hashed.contains(&column.name) {
                true => format!("lower(hex(SHA256({value})))"),
                false => value,
            }
        })
        .collect();
    let displaced = match request.mode {
        WriteMode::Replace | WriteMode::Recreate => None,
        WriteMode::Append => None,
        WriteMode::Merge { unique_key } => {
            let keys: Vec<String> = unique_key
                .iter()
                .map(|key| adapter.render_identifier(key))
                .collect();
            let keys = keys.join(", ");
            Some(format!("({keys}) IN (SELECT {keys} FROM {delta})"))
        }
        WriteMode::DeleteInsert { .. } => Some(range_condition(adapter, request)),
        WriteMode::Changes(_) => {
            return vec![
                "SELECT throwIf(1, 'captured changes are not applied to ClickHouse tables')"
                    .to_owned(),
            ];
        }
    };
    let publish = match request.mode {
        WriteMode::Replace | WriteMode::Recreate => PublishStep::Exchange {
            target: target.clone(),
            build: delta.clone(),
            aside: adapter.render_qualified_name(&with_suffix(&delta_table, ASIDE_SUFFIX)),
        },
        _ => {
            let copy = with_suffix(&delta_table, COPY_SUFFIX);
            PublishStep::Partitions {
                target: target.clone(),
                delta: delta.clone(),
                copy: adapter.render_qualified_name(&copy),
                aside: adapter.render_qualified_name(&with_suffix(&copy, ASIDE_SUFFIX)),
                displaced,
            }
        }
    };
    vec![
        format!("DROP TABLE IF EXISTS {delta}"),
        format!("CREATE TABLE {delta} AS {target}"),
        format!(
            "INSERT INTO {delta} ({}) SELECT {} FROM {}",
            names.join(", "),
            values.join(", "),
            adapter.render_qualified_name(staging)
        ),
        publish_statement(&publish),
        format!("DROP TABLE IF EXISTS {delta}"),
    ]
}

/// The rows a `delete_insert` replaces: those whose cursor is one of its values, or within its range (`1`, every row, when it has no bounds). The bounds are cast to the cursor's type without `Nullable`, a timestamp's parsed with its offset (in UTC when it has none), so a bound ClickHouse cannot read fails rather than matching nothing.
fn range_condition(adapter: &ClickHouseSql, request: &WriteStatementsRequest<'_>) -> String {
    let WriteMode::DeleteInsert {
        cursor_column,
        from,
        to,
        values,
    } = request.mode
    else {
        return "0".to_owned();
    };
    let cursor_type = request
        .columns
        .iter()
        .find(|column| column.name == *cursor_column)
        .map_or(ColumnType::Text, |column| column.column_type.clone());
    let sql_type = adapter.render_loader_logical_type(&cursor_type).sql;
    let sql_type = unwrapped(&sql_type);
    let cast = |text: &str| {
        let literal = adapter.render_loader_value_literal(text);
        match cursor_type {
            ColumnType::TimestampTz => format!("parseDateTime64BestEffort({literal}, 6, 'UTC')"),
            ColumnType::TimestampNtz => format!("parseDateTime64BestEffort({literal}, 6)"),
            _ => format!("CAST({literal} AS {sql_type})"),
        }
    };
    let column = adapter.render_identifier(cursor_column);
    if !values.is_empty() {
        let listed: Vec<String> = values.iter().map(|value| cast(value)).collect();
        return format!("{column} IN ({})", listed.join(", "));
    }
    let bounds: Vec<String> = [(from, ">="), (to, "<")]
        .into_iter()
        .filter_map(|(bound, operator)| {
            let bound = bound.as_ref()?;
            Some(format!("{column} {operator} {}", cast(bound)))
        })
        .collect();
    match bounds.is_empty() {
        true => "1".to_owned(),
        false => bounds.join(" AND "),
    }
}
