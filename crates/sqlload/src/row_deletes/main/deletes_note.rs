//! What a key scan does, for a merge table's plan line.

use crate::loader::models::TableSpec;
use crate::project::models::Deletes;

/// What `--sync-deletes` does after the table's merge, as its plan line ends: `, then a full key scan of pub.stock_item soft-deletes the rows of keys it no longer holds (at most 5% of the table)`; empty without a scan.
pub fn deletes_note(spec: &TableSpec) -> String {
    let Some(deletes) = spec.deletes.as_ref().filter(|deletes| deletes.scan) else {
        return String::new();
    };
    let action = match deletes.apply {
        Deletes::Soft => "soft-deletes",
        Deletes::Hard => "deletes",
    };
    format!(
        ", then a full key scan of {} {action} the rows of keys it no longer holds (at most {}% of the table)",
        spec.source_table.qualified_name(),
        deletes.max_drop
    )
}
