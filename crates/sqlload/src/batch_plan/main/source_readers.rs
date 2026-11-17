//! How many Postgres connections a table's read uses.

use crate::batch_plan::main::window_concurrency::window_concurrency;
use crate::loader::constants::PARALLEL_READERS;
use crate::loader::models::{CommitRule, TablePlan};
use crate::postgres_source::constants::SPLIT_READ_MIN_PAGES;

/// The Postgres connections a table's read uses: several for a plain table of `SPLIT_READ_MIN_PAGES` or more on Postgres 14+, read whole and in no order, and one per window read at once under `batch_concurrency` (other windowed and cursor-ordered reads keep one connection).
pub fn source_readers(table: &TablePlan) -> usize {
    let whole = matches!(table.commits, CommitRule::Size | CommitRule::Whole(_));
    let postgres = table.spec.python.is_none()
        && table.spec.files.is_none()
        && table.spec.sheet.is_none()
        && table.spec.warehouse_source.is_none();
    let large = table
        .spec
        .source_table
        .split_pages
        .is_some_and(|pages| pages >= SPLIT_READ_MIN_PAGES);
    match (postgres, whole && large) {
        (true, true) => PARALLEL_READERS,
        (true, false) => window_concurrency(table),
        (false, _) => 1,
    }
}
