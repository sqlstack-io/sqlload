//! The plan's words for windows read at once.

use crate::batch_plan::main::window_concurrency::window_concurrency;
use crate::loader::models::TablePlan;

/// What the plan's `batch` line adds when `table` reads several windows at once (see [`window_concurrency`]), such as `, read 4 at once`; empty when it reads one at a time.
pub fn read_at_once(table: &TablePlan) -> String {
    match window_concurrency(table) {
        1 => String::new(),
        limit => format!(", read {limit} at once"),
    }
}
