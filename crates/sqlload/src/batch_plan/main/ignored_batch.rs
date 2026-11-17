//! Why `batch_size` has no effect.

use crate::adapters::contract::models::WriteMode;
use crate::loader::models::TablePlan;
use crate::project::models::BatchStep;

/// Why the table's `batch_size` setting has no effect, if it has none: a files source commits by whole partitions, `replace` swaps the whole table in once, as does `--reload` (whose cursor windows still cut the reads), and windows need a cursor.
pub fn ignored_batch(plan: &TablePlan) -> Option<String> {
    let spec = &plan.spec;
    let step = spec.batch?;
    let reason = match (&spec.files, &plan.mode, &spec.cursor, step) {
        (Some(_), _, _, _) => "a files source commits in batches of whole partitions",
        (None, WriteMode::Replace, _, _) => "replace swaps the whole table in once",
        (None, WriteMode::Recreate, None, _) | (None, WriteMode::Recreate, _, BatchStep::Whole) => {
            "--reload rebuilds the whole table once"
        }
        (None, _, None, BatchStep::Duration(_) | BatchStep::Integer(_)) => {
            "windows need a cursor_column"
        }
        _ => return None,
    };
    Some(format!("batch_size is ignored: {reason}"))
}
