//! Tie each `deletes_from` table to its planned tombstone table.

use crate::loader::models::TablePlan;
use crate::schema_plan::models::{Blocked, DesiredColumn};

/// One source's `planned` tables, each with a `deletes_from` told where its tombstone table loads and its cursor column there; blocked when that table lacks one of its `unique_key` columns, which its tombstones are matched on. A table whose tombstone table isn't planned marks nothing.
pub fn link_tombstones(planned: &mut [TablePlan]) {
    for index in 0..planned.len() {
        let table = &planned[index];
        let Some(from) = table
            .spec
            .deletes
            .as_ref()
            .and_then(|deletes| deletes.from.as_ref())
        else {
            continue;
        };
        let key = table.spec.source_table.qualified_name();
        let Some(tombstones) = planned
            .iter()
            .find(|each| each.spec.tombstones_of.as_ref() == Some(&key))
        else {
            continue;
        };
        let missing: Vec<&str> = match &tombstones.schema.blocked {
            Some(_) => Vec::new(),
            None => table
                .spec
                .unique_key
                .iter()
                .filter(|name| !holds(&tombstones.schema.write_columns, name))
                .map(String::as_str)
                .collect(),
        };
        let blocked = (!missing.is_empty()).then(|| Blocked {
            reasons: vec![format!(
                "deletes_from table {} has no column {}, so its tombstones can't be matched to rows by unique_key. Fix: name a tombstone table holding every unique_key column",
                from.source_table,
                missing.join(", ")
            )],
            fix_flag: None,
        });
        let cursor = tombstones.spec.cursor.as_ref();
        let loaded =
            cursor.map(|cursor| (tombstones.spec.destination.clone(), cursor.column.clone()));
        let table = &mut planned[index];
        if table.schema.blocked.is_none() {
            table.schema.blocked = blocked;
        }
        if let Some(from) = table
            .spec
            .deletes
            .as_mut()
            .and_then(|deletes| deletes.from.as_mut())
        {
            from.loaded = loaded;
        }
    }
}

/// Whether `columns` has one named `name`.
fn holds(columns: &[DesiredColumn], name: &str) -> bool {
    columns.iter().any(|column| column.name == name)
}
