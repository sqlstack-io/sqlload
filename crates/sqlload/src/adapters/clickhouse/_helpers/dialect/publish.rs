//! The statements publishing a delta table's partitions, once the server has said which partitions the write touches.

use crate::adapters::clickhouse::_helpers::dialect::write::publish_statement;
use crate::adapters::clickhouse::constants::LIGHTWEIGHT_DELETE_MODE;
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
    /// Whether the server can pin how a `DELETE` marks rows (ClickHouse 25.5 and later), so the displaced rows are deleted from hard-linked partitions rather than the kept rows copied.
    pub lightweight: bool,
}

/// The query listing the partitions the delta's rows go to.
pub(crate) fn delta_partitions_query(delta: &str) -> String {
    format!("SELECT DISTINCT _partition_id FROM {delta} ORDER BY 1")
}

/// The query answering a row when the server has the setting a lightweight `DELETE` is pinned with.
pub(crate) fn lightweight_delete_query() -> String {
    format!(
        "SELECT name FROM system.settings WHERE name = '{}'",
        LIGHTWEIGHT_DELETE_MODE.0
    )
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
        "SELECT count() FROM {table} WHERE _partition_id = {} FORMAT TabSeparated",
        ClickHouseSql.render_loader_value_literal(partition)
    )
}

/// The statements publishing the delta's partitions `new` (and, replacing rows, the target's partitions `displaced` that hold them), each partition whole and every one at once: none when nothing is touched; an added single partition attached from the delta (it copies nothing); otherwise a copy of the table holding the touched partitions (a single one) or all of them, and the delta's partitions attached to it, then a single partition swapped in by `REPLACE PARTITION` (or dropped when the copy holds none of its rows), several by one `EXCHANGE TABLES` of the copy. With `lightweight`, the copy hard-links the table's partitions and the displaced rows are deleted from it, so no kept row is copied; without, the touched partitions are rebuilt from their kept rows. A run killed at any point leaves the table as it was or with every partition, so the next run converges.
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
        lightweight,
    } = *publish;
    let attach = |into: &str, from: &str, partition: &String| {
        format!(
            "ALTER TABLE {into} ATTACH PARTITION ID {} FROM {from}",
            literal(partition)
        )
    };
    let listed = |partitions: &[String]| -> String {
        let literals: Vec<String> = partitions
            .iter()
            .map(|partition| literal(partition))
            .collect();
        literals.join(", ")
    };
    let single = match (touched.as_slice(), condition) {
        ([], _) => return Vec::new(),
        ([only], None) => return vec![attach(target, delta, only)],
        ([only], Some(_)) => Some(only),
        _ => None,
    };
    let mut statements = vec![
        format!("DROP TABLE IF EXISTS {copy}"),
        format!("CREATE TABLE {copy} AS {target}"),
    ];
    if single.is_none() {
        statements.push(format!(
            "ALTER TABLE {copy} ATTACH PARTITION ALL FROM {target}"
        ));
    }
    match (condition, lightweight, single) {
        (None, _, _) => {}
        (Some(condition), true, single) => {
            statements.extend(single.map(|only| attach(copy, target, only)));
            if !displaced.is_empty() {
                statements.push(publish_statement(&PublishStep::DeleteRows {
                    table: copy.to_owned(),
                    condition: format!(
                        "_partition_id IN ({}) AND ifNull({condition}, 0)",
                        listed(displaced)
                    ),
                }));
            }
        }
        (Some(condition), false, single) => {
            if single.is_none() {
                let drops: Vec<String> = touched
                    .iter()
                    .map(|partition| format!("DROP PARTITION ID {}", literal(partition)))
                    .collect();
                statements.push(format!("ALTER TABLE {copy} {}", drops.join(", ")));
            }
            statements.push(format!(
                "INSERT INTO {copy} SELECT * FROM {target} WHERE _partition_id IN ({}) AND NOT ifNull({condition}, 0)",
                listed(&touched)
            ));
        }
    }
    statements.extend(new.iter().map(|partition| attach(copy, delta, partition)));
    statements.push(match single {
        Some(only) if new.contains(only) => replace_partition(target, copy, only),
        Some(only) => publish_statement(&PublishStep::ReplacePartition {
            target: target.to_owned(),
            copy: copy.to_owned(),
            partition: only.clone(),
        }),
        None => publish_statement(&PublishStep::Exchange {
            target: target.to_owned(),
            build: copy.to_owned(),
            aside: aside.to_owned(),
        }),
    });
    statements.push(format!("DROP TABLE IF EXISTS {copy}"));
    statements
}
