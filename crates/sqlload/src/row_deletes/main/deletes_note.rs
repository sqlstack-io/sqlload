//! What a table's deletes do, for a merge table's plan line.

use crate::loader::models::TableSpec;
use crate::project::models::Deletes;

/// What `deletes` does after the table's merge, as its plan line ends: `, then a full key scan of pub.stock_item soft-deletes the rows of keys it no longer holds (at most 5% of the table)` under `--sync-deletes`, and `, then soft-deletes the rows whose key audit.deleted_items lists deleted at or after the row's updated_at (its tombstones load first)` under `deletes_from`; empty with neither.
pub fn deletes_note(spec: &TableSpec) -> String {
    let Some(deletes) = spec.deletes.as_ref() else {
        return String::new();
    };
    let action = match deletes.apply {
        Deletes::Soft => "soft-deletes",
        Deletes::Hard => "deletes",
    };
    let mut note = String::new();
    if deletes.scan {
        note.push_str(&format!(
            ", then a full key scan of {} {action} the rows of keys it no longer holds (at most {}% of the table)",
            spec.source_table.qualified_name(),
            deletes.max_drop
        ));
    }
    if let (Some(from), Some(cursor)) = (&deletes.from, &spec.cursor) {
        note.push_str(&format!(
            ", then {action} the rows whose key {} lists deleted at or after the row's {} (its tombstones load first)",
            from.source_table, cursor.column
        ));
    }
    note
}
