//! Keep the files' column types when the source's fit inside them.

use crate::column_types::main::type_fit::type_fit;
use crate::column_types::models::{TypeFit, Warehouse};
use crate::schema_plan::models::{DesiredColumn, LiveColumn};

/// `columns` with each type the existing files' wider type holds replaced by that type, so new files match the old ones.
pub fn fit_live_types(columns: Vec<DesiredColumn>, live: &[LiveColumn]) -> Vec<DesiredColumn> {
    let mut fitted: Vec<DesiredColumn> = Vec::with_capacity(columns.len());
    for mut column in columns {
        let existing = live
            .iter()
            .find(|existing| existing.name.eq_ignore_ascii_case(&column.name));
        if let Some(existing) = existing
            && existing.column_type != column.column_type
            && type_fit(
                &column.column_type,
                &existing.column_type,
                Warehouse::Parquet,
            ) == TypeFit::Fits
        {
            column.column_type = existing.column_type.clone();
        }
        fitted.push(column);
    }
    fitted
}
