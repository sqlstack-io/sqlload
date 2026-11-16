//! Tie each `deletes_from` table to its planned tombstone table.

use crate::loader::models::TablePlan;
use crate::project::models::Deletes;
use crate::row_deletes::constants::TOMBSTONE_TIMES;
use crate::schema_plan::models::{Blocked, DesiredColumn};

/// One source's `planned` tables, each with a `deletes_from` told where its tombstone table loads and its cursor column there; blocked when that table lacks one of its `unique_key` columns, which its tombstones are matched on, or, under `deletes = "soft"`, when its cursor isn't a timestamp, the time a mark takes. A table whose tombstone table isn't planned marks nothing.
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
        let cursor = tombstones.spec.cursor.as_ref();
        let mut reasons: Vec<String> = Vec::new();
        if !missing.is_empty() {
            reasons.push(format!(
                "deletes_from table {} has no column {}, so its tombstones can't be matched to rows by unique_key. Fix: name a tombstone table holding every unique_key column",
                from.source_table,
                missing.join(", ")
            ));
        }
        let untimed = tombstones
            .schema
            .write_columns
            .iter()
            .find(|column| cursor.is_some_and(|cursor| column.name == cursor.column))
            .filter(|column| !TOMBSTONE_TIMES.contains(&column.column_type));
        if let Some(column) = untimed
            && table.spec.deletes.as_ref().map(|deletes| deletes.apply) == Some(Deletes::Soft)
        {
            reasons.push(format!(
                "deletes_from table {} has cursor_column {} of type {}, but deletes = \"soft\" sets _sqlload_deleted_at to a tombstone's time. Fix: name a timestamp column, or set deletes = \"hard\"",
                from.source_table, column.name, column.column_type
            ));
        }
        let blocked = (!reasons.is_empty()).then_some(Blocked {
            reasons,
            fix_flag: None,
        });
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
