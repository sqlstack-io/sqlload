//! Whether a captured table's columns changed since its snapshot record in a way the stream carries.

use crate::cdc::models::{CatalogColumn, PlannedShape, SnapshotRecord};

/// How `recorded` meets `planned`, given the table's `catalog` now: `Ok(None)` when the record stands; `Ok(Some(record))`, to be written before streaming, when every change since is one the stream carries (a dropped column, or one added after the record without a default); `Err` why the table must be copied again. A column with a number at or below the recorded highest existed then (renamed, or newly loaded), so rows hold values no change carries; so does a column with a default, an identity or a generated column. A column the table stopped loading and loads again (dropped and re-added, or excluded and included) has stale destination values, as does a recorded column numbered above the highest (dropped and re-added between loads). Under `merge` the key must not change, or soft-deleted rows could share a new key. Records without column numbers accept only unchanged columns.
pub(crate) fn reshape_problem(
    recorded: &SnapshotRecord,
    planned: PlannedShape<'_>,
    catalog: &[CatalogColumn],
) -> Result<Option<SnapshotRecord>, String> {
    let table = &recorded.table;
    let resnapshot = "run `sqlload load --resnapshot` to copy every table again";
    let was: Vec<&str> = names(&recorded.columns);
    let added: Vec<&str> = names(planned.columns)
        .into_iter()
        .filter(|name| !was.contains(name))
        .collect();
    let rekeyed = recorded
        .key
        .as_deref()
        .is_some_and(|key| key != planned.key);
    if planned.merge && rekeyed {
        return Err(format!(
            "the key of {table} changed since its snapshot, so rows deleted before could share a new key; {resnapshot}"
        ));
    }
    let known_highest = catalog
        .iter()
        .filter(|column| planned.known.contains(&column.name))
        .map(|column| column.attnum)
        .max();
    let now: Vec<&str> = names(planned.columns);
    let left: Vec<&str> = names(&recorded.left);
    let mut still_left: Vec<&str> = left.clone();
    still_left.extend(
        was.iter()
            .filter(|name| !now.contains(name) && !left.contains(name)),
    );
    let rewritten = SnapshotRecord {
        table: table.clone(),
        columns: planned.columns.to_owned(),
        max_attnum: recorded.max_attnum.max(known_highest),
        key: Some(planned.key.to_owned()),
        left: format!(
            ",{}",
            still_left
                .iter()
                .map(|name| format!("{name},"))
                .collect::<String>()
        ),
    };
    let Some(highest) = recorded.max_attnum else {
        return match recorded.columns == planned.columns {
            true => Ok(Some(rewritten)),
            false => Err(format!(
                "the columns of {table} changed since its snapshot, which predates tracking schema changes; {resnapshot}"
            )),
        };
    };
    let live = |name: &str| {
        catalog
            .iter()
            .find(|column| column.name == name && !column.dropped)
    };
    for name in &now {
        let recreated =
            was.contains(name) && live(name).is_some_and(|column| column.attnum > highest);
        if left.contains(name) || recreated {
            return Err(format!(
                "{table} loads column {name} again after it stopped loading (dropped and added back, or excluded and included), so its destination values are stale; {resnapshot}"
            ));
        }
    }
    for name in added {
        let Some(column) = live(name) else {
            return Err(format!(
                "{table} loads column {name}, which the source table no longer has: it changed shape during this load, so run it again"
            ));
        };
        if column.attnum <= highest {
            return Err(format!(
                "{table} now loads column {name}, which the table already had at its snapshot (renamed, or newly included), so its rows hold values no change carries; {resnapshot}"
            ));
        }
        if column.has_default {
            return Err(format!(
                "column {name} of {table} was added with a default (or as an identity or generated column), which fills existing rows without changes to stream; {resnapshot}, or exclude the column"
            ));
        }
    }
    Ok((rewritten != *recorded).then_some(rewritten))
}

/// The names in a `,a,b,` list.
fn names(list: &str) -> Vec<&str> {
    list.split(',').filter(|name| !name.is_empty()).collect()
}
