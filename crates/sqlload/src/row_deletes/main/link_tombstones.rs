//! Tie each `deletes_from` table to its planned tombstone table.

use crate::column_types::models::ColumnType;
use crate::loader::models::TablePlan;
use crate::project::models::Deletes;
use crate::row_deletes::constants::TOMBSTONE_TIMES;
use crate::schema_plan::models::{Blocked, DesiredColumn};

/// One source's `planned` tables, each with a `deletes_from` told where its tombstone table loads and its cursor column there; blocked when that table lacks one of its `unique_key` columns, which its tombstones are matched on, when its cursor and the table's don't compare exactly, or, under `deletes = "soft"`, when its cursor isn't a timestamp, the time a mark takes. A table whose tombstone table isn't planned marks nothing.
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
                .filter(|name| named(&tombstones.schema.write_columns, name).is_none())
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
        let deleted_at = tombstones
            .schema
            .write_columns
            .iter()
            .find(|column| cursor.is_some_and(|cursor| column.name == cursor.column));
        let untimed = deleted_at.filter(|column| !TOMBSTONE_TIMES.contains(&column.column_type));
        if let Some(column) = untimed
            && table.spec.deletes.as_ref().map(|deletes| deletes.apply) == Some(Deletes::Soft)
        {
            reasons.push(format!(
                "deletes_from table {} has cursor_column {} of type {}, but deletes = \"soft\" sets _sqlload_deleted_at to a tombstone's time. Fix: name a timestamp column, or set deletes = \"hard\"",
                from.source_table, column.name, column.column_type
            ));
        }
        let unlike = deleted_at.and_then(|deleted_at| {
            let row_cursor = table.spec.cursor.as_ref()?;
            row_cursor
                .columns()
                .filter_map(|name| named(&table.schema.write_columns, name))
                .find(|column| !compares(&column.column_type, &deleted_at.column_type))
                .map(|column| (deleted_at, column))
        });
        if let Some((deleted_at, column)) = unlike {
            reasons.push(format!(
                "deletes_from table {} has cursor_column {} of type {}, which doesn't compare exactly with the table's cursor column {} of type {}. Fix: give both the same kind of type in the source: timestamps with a time zone, timestamps or dates without one, numbers, or text",
                from.source_table,
                deleted_at.name,
                deleted_at.column_type,
                column.name,
                column.column_type
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

/// Whether a tombstone's time of type `one` and a row's cursor of type `other` compare exactly: in the same family (instants, local dates and times, numbers, or text), else of one type. An instant and a local time would compare in the session's time zone.
fn compares(one: &ColumnType, other: &ColumnType) -> bool {
    let family = |column_type: &ColumnType| match column_type {
        ColumnType::TimestampTz => Some(0),
        ColumnType::Date | ColumnType::TimestampNtz => Some(1),
        ColumnType::SmallInt | ColumnType::Int | ColumnType::Decimal { .. } | ColumnType::Float => {
            Some(2)
        }
        ColumnType::Text | ColumnType::Varchar(_) => Some(3),
        _ => None,
    };
    match (family(one), family(other)) {
        (Some(one), Some(other)) => one == other,
        _ => one == other,
    }
}

/// The column of `columns` named `name`.
fn named<'a>(columns: &'a [DesiredColumn], name: &str) -> Option<&'a DesiredColumn> {
    columns.iter().find(|column| column.name == name)
}
