//! Read the columns a caller's session described.

use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::LiveColumnType;
use crate::schema_plan::models::LiveColumn;

/// The columns of described `rows`, each its name, type, numeric precision and scale, character length and fractional-second digits, as text, as many of them as the row has; a size that is not a number fails, naming the column.
pub(in crate::adapters) fn described_columns<A: SqlAdapter + ?Sized>(
    adapter: &A,
    rows: &[Vec<Option<String>>],
) -> Result<Vec<LiveColumn>, String> {
    rows.iter()
        .map(|row| {
            let name = row.first().cloned().flatten().unwrap_or_default();
            let number = |index: usize| -> Result<Option<i64>, String> {
                match row.get(index).cloned().flatten() {
                    None => Ok(None),
                    Some(text) => text.trim().parse::<i64>().map(Some).map_err(|error| {
                        format!("column {name}: its size {text:?} is not a number: {error}")
                    }),
                }
            };
            Ok(LiveColumn {
                column_type: adapter.live_type(&LiveColumnType {
                    data_type: row.get(1).cloned().flatten().unwrap_or_default(),
                    precision: number(2)?,
                    scale: number(3)?,
                    char_length: number(4)?,
                }),
                fraction_digits: number(5)?,
                name,
            })
        })
        .collect()
}
