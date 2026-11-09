//! The statements publishing a delta table's partitions, once the server has said which partitions the write touches.

use crate::adapters::clickhouse::_helpers::dialect::write::publish_statement;
use crate::adapters::clickhouse::models::{ClickHouseSql, PublishStep};
use crate::adapters::contract::types::SqlAdapter;

/// The tables and condition of a partition publish.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartitionPublish<'a> {
    pub target: &'a str,
    pub delta: &'a str,
    pub copy: &'a str,
    pub aside: &'a str,
    /// The condition on the target's rows that the delta replaces; `None` when it replaces none.
    pub displaced: Option<&'a str>,
}

/// The query listing the partitions the delta's rows go to.
pub(crate) fn delta_partitions_query(delta: &str) -> String {
    format!("SELECT DISTINCT _partition_id FROM {delta} ORDER BY 1")
}

/// The query listing the target's partitions holding rows the delta replaces.
pub(crate) fn displaced_partitions_query(target: &str, displaced: &str) -> String {
    format!("SELECT DISTINCT _partition_id FROM {target} WHERE ifNull({displaced}, 0) ORDER BY 1")
}

/// `target`'s partition `partition` replaced by `copy`'s.
pub(crate) fn replace_partition(target: &str, copy: &str, partition: &str) -> String {
    format!(
        "ALTER TABLE {target} REPLACE PARTITION ID {} FROM {copy}",
        ClickHouseSql.render_loader_value_literal(partition)
    )
}

/// The query counting `table`'s rows in partition `partition`.
pub(crate) fn partition_rows_query(table: &str, partition: &str) -> String {
    format!(
        "SELECT count() FROM {table} WHERE _partition_id = {}",
        ClickHouseSql.render_loader_value_literal(partition)
    )
}

/// The statements publishing the delta's partitions `new` (and, replacing rows, the target's partitions `displaced` that hold them), each partition whole and every one at once: none when nothing is touched; an added single partition attached from the delta (it copies nothing); a single rebuilt partition built beside the table from its kept rows and the delta's and swapped in by `REPLACE PARTITION`; when the delta adds none of that partition's rows, it is dropped instead if the copy holds none either; several partitions rebuilt in a copy of the table that hard-links its parts, then swapped in for the table by one `EXCHANGE TABLES`. A run killed at any point leaves the table as it was or with every partition, so the next run converges.
pub(crate) fn partition_statements(
    publish: &PartitionPublish<'_>,
    (new, displaced): (&[String], &[String]),
) -> Vec<String> {
    let literal = |text: &str| ClickHouseSql.render_loader_value_literal(text);
    let mut touched: Vec<String> = new.iter().chain(displaced).cloned().collect();
    touched.sort();
    touched.dedup();
    let PartitionPublish {
        target,
        delta,
        copy,
        aside,
        displaced: condition,
    } = *publish;
    let attach = |into: &str, partition: &String| {
        format!(
            "ALTER TABLE {into} ATTACH PARTITION ID {} FROM {delta}",
            literal(partition)
        )
    };
    let kept = |partitions: &str| match condition {
        Some(condition) => format!(
            "INSERT INTO {copy} SELECT * FROM {target} WHERE _partition_id IN ({partitions}) AND NOT ifNull({condition}, 0)"
        ),
        None => format!(
            "INSERT INTO {copy} SELECT * FROM {target} WHERE _partition_id IN ({partitions})"
        ),
    };
    let listed: Vec<String> = touched.iter().map(|partition| literal(partition)).collect();
    let mut statements: Vec<String> = Vec::new();
    match (touched.as_slice(), condition) {
        ([], _) => return statements,
        ([only], None) => return vec![attach(target, only)],
        ([only], Some(_)) => {
            statements.push(format!("DROP TABLE IF EXISTS {copy}"));
            statements.push(format!("CREATE TABLE {copy} AS {target}"));
            statements.push(kept(&listed.join(", ")));
            statements.extend(new.iter().map(|partition| attach(copy, partition)));
            statements.push(match new.contains(only) {
                true => replace_partition(target, copy, only),
                false => publish_statement(&PublishStep::ReplacePartition {
                    target: target.to_owned(),
                    copy: copy.to_owned(),
                    partition: only.clone(),
                }),
            });
        }
        (_, condition) => {
            statements.push(format!("DROP TABLE IF EXISTS {copy}"));
            statements.push(format!("CREATE TABLE {copy} AS {target}"));
            statements.push(format!(
                "ALTER TABLE {copy} ATTACH PARTITION ALL FROM {target}"
            ));
            if condition.is_some() {
                let drops: Vec<String> = listed
                    .iter()
                    .map(|partition| format!("DROP PARTITION ID {partition}"))
                    .collect();
                statements.push(format!("ALTER TABLE {copy} {}", drops.join(", ")));
                statements.push(kept(&listed.join(", ")));
            }
            statements.extend(new.iter().map(|partition| attach(copy, partition)));
            statements.push(publish_statement(&PublishStep::Exchange {
                target: target.to_owned(),
                build: copy.to_owned(),
                aside: aside.to_owned(),
            }));
        }
    }
    statements.push(format!("DROP TABLE IF EXISTS {copy}"));
    statements
}
