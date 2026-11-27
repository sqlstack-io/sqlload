//! A created BigQuery table's layout: its partitioning, its clustering and `require_partition_filter`.

use crate::adapters::bigquery::constants::{
    MAX_CLUSTER_COLUMNS, MAX_PARTITIONS, PARTITION_VALUE, REQUIRE_PARTITION_FILTER,
    STAGED_PARTITIONS,
};
use crate::adapters::bigquery::models::{BigQuerySql, Partitioning};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::ColumnType;
use crate::loader::models::{TableLayout, TableSpec};
use crate::project::models::LayoutSetting;
use crate::project::models::WriteStrategy;
use crate::row_batches::models::Unnest;
use crate::schema_plan::models::DesiredColumn;

/// The forms `partition_by` takes on BigQuery, for messages.
const PARTITION_FORMS: &str = "a DATE column, DATE(column), DATE_TRUNC(column, MONTH|YEAR), TIMESTAMP_TRUNC(column, HOUR|DAY|MONTH|YEAR), DATETIME_TRUNC(column, HOUR|DAY|MONTH|YEAR) or RANGE_BUCKET(column, GENERATE_ARRAY(start, end, interval))";

/// The partitioning `partition_by` names, or why BigQuery can't partition by it.
pub(crate) fn partitioning(expression: &str) -> Result<Partitioning, String> {
    let written = expression.trim();
    let refused = || format!("partition_by {written:?}: BigQuery partitions by {PARTITION_FORMS}");
    if is_column(written) {
        return Ok(Partitioning::Time {
            function: None,
            column: written.to_owned(),
            unit: None,
        });
    }
    let (function, rest) = written.split_once('(').ok_or_else(refused)?;
    let arguments = rest.trim().strip_suffix(')').ok_or_else(refused)?;
    let function = function.trim().to_ascii_uppercase();
    let (column, argument) = match arguments.split_once(',') {
        Some((column, argument)) => (column.trim(), Some(argument.trim())),
        None => (arguments.trim(), None),
    };
    if !is_column(column) {
        return Err(refused());
    }
    let units: &[&str] = match function.as_str() {
        "DATE" if argument.is_none() => {
            return Ok(Partitioning::Time {
                function: Some("DATE"),
                column: column.to_owned(),
                unit: None,
            });
        }
        "RANGE_BUCKET" => {
            return range_bucket(column, argument.unwrap_or_default()).ok_or_else(refused);
        }
        "DATE_TRUNC" => &["MONTH", "YEAR"],
        "TIMESTAMP_TRUNC" | "DATETIME_TRUNC" => &["HOUR", "DAY", "MONTH", "YEAR"],
        _ => return Err(refused()),
    };
    let unit = argument.unwrap_or_default().to_ascii_uppercase();
    let function = match function.as_str() {
        "DATE_TRUNC" => "DATE_TRUNC",
        "TIMESTAMP_TRUNC" => "TIMESTAMP_TRUNC",
        _ => "DATETIME_TRUNC",
    };
    match units.iter().find(|known| **known == unit) {
        Some(unit) => Ok(Partitioning::Time {
            function: Some(function),
            column: column.to_owned(),
            unit: Some(unit),
        }),
        None => Err(format!(
            "partition_by {written:?}: {function} partitions by {}",
            units.join(", ")
        )),
    }
}

/// `RANGE_BUCKET(column, GENERATE_ARRAY(start, end, interval))`'s partitioning, from `column` and the `GENERATE_ARRAY(…)` argument.
fn range_bucket(column: &str, argument: &str) -> Option<Partitioning> {
    let (function, rest) = argument.split_once('(')?;
    if !function.trim().eq_ignore_ascii_case("GENERATE_ARRAY") {
        return None;
    }
    let numbers: Vec<&str> = rest
        .trim()
        .strip_suffix(')')?
        .split(',')
        .map(str::trim)
        .collect();
    let [start, end, interval] = numbers.as_slice() else {
        return None;
    };
    let (Ok(start), Ok(end), Ok(interval)) = (
        start.parse::<i64>(),
        end.parse::<i64>(),
        interval.parse::<i64>(),
    ) else {
        return None;
    };
    (start < end && interval > 0).then(|| Partitioning::Range {
        column: column.to_owned(),
        start,
        end,
        interval,
    })
}

/// Whether `text` is a column's bare name.
fn is_column(text: &str) -> bool {
    let mut characters = text.chars();
    characters
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && characters.all(|character| character.is_ascii_alphanumeric() || character == '_')
}

/// The column the table is partitioned by, when `partition_by` names one BigQuery takes.
pub(crate) fn partition_column(layout: &TableLayout) -> Option<String> {
    match layout.partition_by.as_deref().map(partitioning) {
        Some(Ok(Partitioning::Time { column, .. } | Partitioning::Range { column, .. })) => {
            Some(column)
        }
        Some(Err(_)) | None => None,
    }
}

/// Whether the table must be queried with a filter on its partition column.
pub(crate) fn requires_partition_filter(layout: &TableLayout) -> bool {
    matches!(
        layout.table_settings.get(REQUIRE_PARTITION_FILTER),
        Some(LayoutSetting::Bool(true))
    )
}

/// How many ranges `GENERATE_ARRAY(start, end, interval)` makes, rounded up, in `i128` so no `i64` bounds overflow.
fn range_count(start: i64, end: i64, interval: i64) -> i128 {
    let (span, interval) = (i128::from(end) - i128::from(start), i128::from(interval));
    (span + interval - 1) / interval
}

/// Why BigQuery can't lay out a table as `layout` says, each naming the fix; nothing for a layout it takes.
pub(crate) fn layout_problems(layout: &TableLayout) -> Vec<String> {
    let mut problems: Vec<String> = Vec::new();
    if layout.engine.is_some() || layout.order_by.is_some() || layout.primary_key.is_some() {
        problems.push("engine, order_by and primary_key lay out ClickHouse tables; a BigQuery table takes partition_by, cluster_by and table_settings.require_partition_filter".to_owned());
    }
    match layout.partition_by.as_deref().map(partitioning) {
        Some(Err(problem)) => problems.push(problem),
        Some(Ok(Partitioning::Range {
            start,
            end,
            interval,
            ..
        })) if range_count(start, end, interval) > i128::from(MAX_PARTITIONS) => {
            problems.push(format!(
                "partition_by {:?} makes {} ranges; BigQuery takes at most {MAX_PARTITIONS}, so widen the interval",
                layout.partition_by.as_deref().unwrap_or_default(),
                range_count(start, end, interval)
            ));
        }
        Some(Ok(_)) | None => {}
    }
    let clustered = layout.cluster_by.as_deref().unwrap_or_default();
    if layout.cluster_by.is_some() && clustered.is_empty() {
        problems
            .push("cluster_by names no column; leave it out for an unclustered table".to_owned());
    }
    if clustered.len() > MAX_CLUSTER_COLUMNS {
        problems.push(format!(
            "cluster_by names {} columns; BigQuery clusters by at most {MAX_CLUSTER_COLUMNS}",
            clustered.len()
        ));
    }
    problems.extend(
        clustered
            .iter()
            .filter(|column| !is_column(column))
            .map(|column| {
                format!("cluster_by {column:?}: BigQuery clusters by column names, not expressions")
            }),
    );
    for (name, value) in &layout.table_settings {
        match (name.as_str(), value) {
            (REQUIRE_PARTITION_FILTER, LayoutSetting::Bool(true))
                if layout.partition_by.is_none() =>
            {
                problems.push(format!("table_settings.{REQUIRE_PARTITION_FILTER} needs partition_by: an unpartitioned table has no partition to filter on"));
            }
            (REQUIRE_PARTITION_FILTER, LayoutSetting::Bool(_)) => {}
            (REQUIRE_PARTITION_FILTER, _) => {
                problems.push(format!(
                    "table_settings.{REQUIRE_PARTITION_FILTER} is true or false"
                ));
            }
            (other, _) => problems.push(format!(
                "table_settings.{other}: a BigQuery table takes only {REQUIRE_PARTITION_FILTER}"
            )),
        }
    }
    problems
}

/// Why `spec`'s table can't load with `require_partition_filter`: a statement SQLLoad runs on the table that can't filter on the partition column. Reads of every row (the cursor's maximum, the loaded files) and a `MERGE`'s deletes carry [`whole_table_filter`], which BigQuery takes there but not in a `MERGE`'s `ON`, so a `merge` loads only by a `unique_key` holding the partition column, and a keyed `delete_insert` only by a `replace_key` holding it; `deletes`, `checks.max_row_drop`, `newest_by` and `unnest` read the table in queries that carry no filter.
fn partition_filter_problems(spec: &TableSpec) -> Vec<String> {
    let Some(column) =
        partition_column(&spec.layout).filter(|_| requires_partition_filter(&spec.layout))
    else {
        return Vec::new();
    };
    let fix = format!("set table_settings.{REQUIRE_PARTITION_FILTER} = false, or");
    let unfiltered = [
        (spec.deletes.is_some(), "deletes"),
        (
            spec.checks
                .as_ref()
                .is_some_and(|checks| checks.max_row_drop.is_some()),
            "checks.max_row_drop",
        ),
        (!spec.newest_by.is_empty(), "newest_by"),
        (spec.unnest != Unnest::None, "unnest"),
    ];
    let mut problems: Vec<String> = unfiltered
        .iter()
        .filter(|(set, _)| *set)
        .map(|(_, setting)| format!(
            "{REQUIRE_PARTITION_FILTER}: {setting} reads the table's rows in queries SQLLoad can't filter on the partition column {column}; {fix} leave {setting} out"
        ))
        .collect();
    let keys = match spec.strategy {
        WriteStrategy::Merge => Some(("merge", "unique_key", &spec.unique_key)),
        WriteStrategy::DeleteInsert if !spec.replace_key.is_empty() => {
            Some(("delete_insert", "replace_key", &spec.replace_key))
        }
        _ => None,
    };
    if let Some((strategy, setting, keys)) = keys
        && !keys.contains(&column)
    {
        problems.push(format!(
            "{REQUIRE_PARTITION_FILTER}: a {strategy} filters on the partition column only when {setting} holds it, and {setting} lacks {column}; {fix} add {column} to {setting}"
        ));
    }
    problems
}

/// Why `spec`'s loaded `columns` can't carry its layout: a partition or cluster column the table doesn't load, or of a type BigQuery won't partition or cluster by; and a [`require_partition_filter`](partition_filter_problems) its statements can't satisfy.
pub(crate) fn layout_table_problems(spec: &TableSpec, columns: &[DesiredColumn]) -> Vec<String> {
    let layout = &spec.layout;
    let typed = |name: &str| {
        columns
            .iter()
            .find(|column| column.name == name)
            .map(|column| &column.column_type)
    };
    let mut problems: Vec<String> = Vec::new();
    if let Some(Ok(partitioning)) = layout.partition_by.as_deref().map(partitioning) {
        let (column, wanted): (&str, &[ColumnType]) = match &partitioning {
            Partitioning::Time {
                function: None,
                column,
                ..
            } => (column, &[ColumnType::Date]),
            Partitioning::Time {
                function: Some("DATE"),
                column,
                ..
            } => (column, &[ColumnType::TimestampTz, ColumnType::TimestampNtz]),
            Partitioning::Time {
                function: Some("DATE_TRUNC"),
                column,
                ..
            } => (column, &[ColumnType::Date]),
            Partitioning::Time {
                function: Some("TIMESTAMP_TRUNC"),
                column,
                ..
            } => (column, &[ColumnType::TimestampTz]),
            Partitioning::Time { column, .. } => (column, &[ColumnType::TimestampNtz]),
            Partitioning::Range { column, .. } => {
                (column, &[ColumnType::Int, ColumnType::SmallInt])
            }
        };
        let names: Vec<String> = wanted.iter().map(ToString::to_string).collect();
        match typed(column) {
            None => problems.push(format!("partition_by column {column:?} is not a column the table loads")),
            Some(found) if !wanted.contains(found) => problems.push(format!(
                "partition_by {:?} needs {column} to be {}, not {found}; pin its type with a column type, or partition by another form ({PARTITION_FORMS})",
                layout.partition_by.as_deref().unwrap_or_default(),
                names.join(" or ")
            )),
            Some(_) => {}
        }
    }
    for column in layout.cluster_by.as_deref().unwrap_or_default() {
        match typed(column) {
            None => problems.push(format!(
                "cluster_by column {column:?} is not a column the table loads"
            )),
            Some(
                found @ (ColumnType::Float
                | ColumnType::Time
                | ColumnType::Json
                | ColumnType::Binary
                | ColumnType::Array(_)
                | ColumnType::Native(_)),
            ) => {
                problems.push(format!(
                    "cluster_by column {column} is {found}, which BigQuery can't cluster by; cluster by a text, number, date, timestamp or bool column"
                ));
            }
            Some(_) => {}
        }
    }
    problems.extend(partition_filter_problems(spec));
    problems
}

/// The condition matching every row of `qualifier`'s table (such as `__target.`, or `""`) that satisfies `require_partition_filter`, which refuses `WHERE TRUE` and any condition not on the partition column; `None` where the layout doesn't require one.
pub(crate) fn whole_table_filter(layout: &TableLayout, qualifier: &str) -> Option<String> {
    let column = partition_column(layout).filter(|_| requires_partition_filter(layout))?;
    let column = format!("{qualifier}{}", BigQuerySql.render_identifier(&column));
    Some(format!("({column} IS NOT NULL OR {column} IS NULL)"))
}

/// The `PARTITION BY`, `CLUSTER BY` and `OPTIONS` clauses of a table laid out as `layout`, each after a space; empty for no layout.
pub(crate) fn layout_clauses(layout: &TableLayout) -> String {
    let adapter = BigQuerySql;
    let mut clauses: Vec<String> = Vec::new();
    if let Some(Ok(partitioning)) = layout.partition_by.as_deref().map(partitioning) {
        clauses.push(format!(
            "PARTITION BY {}",
            partition_expression(&partitioning, "")
        ));
    }
    let clustered: Vec<String> = layout
        .cluster_by
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|column| adapter.render_identifier(column))
        .collect();
    if !clustered.is_empty() {
        clauses.push(format!("CLUSTER BY {}", clustered.join(", ")));
    }
    if requires_partition_filter(layout) {
        clauses.push(format!("OPTIONS ({REQUIRE_PARTITION_FILTER} = TRUE)"));
    }
    clauses.iter().map(|clause| format!(" {clause}")).collect()
}

/// The partition a row of `qualifier` (such as `__target.`, or `""` in DDL) falls in, as BigQuery's `PARTITION BY` writes it; a range partition's raw column, which is what prunes.
pub(crate) fn partition_expression(partitioning: &Partitioning, qualifier: &str) -> String {
    let adapter = BigQuerySql;
    match partitioning {
        Partitioning::Time {
            function: None,
            column,
            ..
        } => {
            format!("{qualifier}{}", adapter.render_identifier(column))
        }
        Partitioning::Time {
            function: Some(function),
            column,
            unit: None,
        } => {
            format!(
                "{function}({qualifier}{})",
                adapter.render_identifier(column)
            )
        }
        Partitioning::Time {
            function: Some(function),
            column,
            unit: Some(unit),
        } => {
            format!(
                "{function}({qualifier}{}, {unit})",
                adapter.render_identifier(column)
            )
        }
        Partitioning::Range {
            column,
            start,
            end,
            interval,
        } if qualifier.is_empty() => format!(
            "RANGE_BUCKET({}, GENERATE_ARRAY({start}, {end}, {interval}))",
            adapter.render_identifier(column)
        ),
        Partitioning::Range { column, .. } => {
            format!("{qualifier}{}", adapter.render_identifier(column))
        }
    }
}

/// The script that declares the partitions a write's staged rows fall in, and the condition on `__target` that keeps the write to them, so it scans only those partitions: for a write by `keys` (its `unique_key` or `replace_key`) of a table partitioned by a column among them, so a row a staged key matches sits in a partition a staged row does. Rows whose partition column is NULL are kept in, as a NULL key can match a NULL one. `staged` is the partition column's value in the staged rows (a cast of its staging column) and `source` the staging table. `None` where pruning isn't safe: no partitioning, or a partition column outside the keys, whose old row may sit in a partition no staged row does.
pub(crate) fn partition_filter(
    layout: &TableLayout,
    keys: &[String],
    (staged, source): (&str, &str),
) -> Option<(String, String)> {
    let Some(Ok(partitioning)) = layout.partition_by.as_deref().map(partitioning) else {
        return None;
    };
    let column = match &partitioning {
        Partitioning::Time { column, .. } | Partitioning::Range { column, .. } => column,
    };
    if !keys.contains(column) {
        return None;
    }
    let target = format!("__target.{}", BigQuerySql.render_identifier(column));
    let rows = format!("(SELECT {staged} AS {PARTITION_VALUE} FROM {source})");
    Some(match &partitioning {
        Partitioning::Range { .. } => (
            format!(
                "DECLARE {STAGED_PARTITIONS} DEFAULT (SELECT AS STRUCT MIN({PARTITION_VALUE}) AS low, MAX({PARTITION_VALUE}) AS high FROM {rows});"
            ),
            format!(
                "({target} BETWEEN {STAGED_PARTITIONS}.low AND {STAGED_PARTITIONS}.high OR {target} IS NULL)"
            ),
        ),
        Partitioning::Time { .. } => {
            let staged_partition =
                partition_expression(&with_column(&partitioning, PARTITION_VALUE), "");
            (
                format!(
                    "DECLARE {STAGED_PARTITIONS} DEFAULT ARRAY(SELECT DISTINCT {staged_partition} FROM {rows} WHERE {PARTITION_VALUE} IS NOT NULL);"
                ),
                format!(
                    "({} IN UNNEST({STAGED_PARTITIONS}) OR {target} IS NULL)",
                    partition_expression(&partitioning, "__target.")
                ),
            )
        }
    })
}

/// `partitioning` of `column` instead.
fn with_column(partitioning: &Partitioning, column: &str) -> Partitioning {
    match partitioning.clone() {
        Partitioning::Time { function, unit, .. } => Partitioning::Time {
            function,
            column: column.to_owned(),
            unit,
        },
        range @ Partitioning::Range { .. } => range,
    }
}

/// `merge`, a `MERGE … ON …`, kept to the partitions `filter` (from [`partition_filter`]) declares: its script, then the `MERGE` with the condition added to its `ON`.
pub(crate) fn pruned((declare, condition): (String, String), merge: String) -> String {
    match merge.split_once(" AS __source ON ") {
        Some((head, on)) => format!("{declare} {head} AS __source ON {condition} AND {on}"),
        None => merge,
    }
}

/// How a table is laid out, in words: from its config's `layout`, or from `tables.get`'s answer `found`.
fn described_layout(partitioned: Option<String>, clustered: &[String], filtered: bool) -> String {
    let mut parts = vec![partitioned.unwrap_or_else(|| "unpartitioned".to_owned())];
    parts.push(match clustered.is_empty() {
        true => "unclustered".to_owned(),
        false => format!("clustered by {}", clustered.join(", ")),
    });
    if filtered {
        parts.push(format!("{REQUIRE_PARTITION_FILTER} = true"));
    }
    parts.join(", ")
}

/// Why the live table `found` (`tables.get`'s answer) is laid out otherwise than `layout` says, naming both and the fix; `None` when they agree. Column names compare as BigQuery's do, ignoring case.
pub(crate) fn layout_drift(found: &serde_json::Value, layout: &TableLayout) -> Option<String> {
    let lower = |text: &str| text.to_ascii_lowercase();
    let wanted_partition = match layout.partition_by.as_deref().map(partitioning) {
        Some(Ok(Partitioning::Time { column, unit, .. })) => Some(format!(
            "partitioned by {} of {}",
            unit.unwrap_or("DAY"),
            lower(&column)
        )),
        Some(Ok(Partitioning::Range {
            column,
            start,
            end,
            interval,
        })) => Some(format!(
            "partitioned by ranges of {} from {start} to {end} by {interval}",
            lower(&column)
        )),
        _ => None,
    };
    let time = &found["timePartitioning"];
    let range = &found["rangePartitioning"];
    let number = |value: &serde_json::Value| match value {
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    let live_partition = match (time.is_object(), range.is_object()) {
        (true, _) => Some(format!(
            "partitioned by {} of {}",
            time["type"].as_str().unwrap_or("DAY"),
            time["field"]
                .as_str()
                .map_or_else(|| "ingestion time".to_owned(), lower)
        )),
        (false, true) => Some(format!(
            "partitioned by ranges of {} from {} to {} by {}",
            lower(range["field"].as_str().unwrap_or_default()),
            number(&range["range"]["start"]),
            number(&range["range"]["end"]),
            number(&range["range"]["interval"])
        )),
        (false, false) => None,
    };
    let wanted_clusters: Vec<String> = layout
        .cluster_by
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|column| lower(column))
        .collect();
    let live_clusters: Vec<String> = found["clustering"]["fields"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(serde_json::Value::as_str)
        .map(lower)
        .collect();
    let wanted_filter = requires_partition_filter(layout);
    let live_filter = found["requirePartitionFilter"].as_bool() == Some(true);
    let wanted = described_layout(wanted_partition, &wanted_clusters, wanted_filter);
    let live = described_layout(live_partition, &live_clusters, live_filter);
    (wanted != live).then(|| format!("the table is {live}, but its config lays it out {wanted}"))
}
