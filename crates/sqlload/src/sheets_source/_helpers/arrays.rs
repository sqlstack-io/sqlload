//! A range's column as an Arrow array.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, StringArray};
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::{DataType, Field};

use crate::column_types::main::arrow_field::arrow_field;
use crate::column_types::models::ColumnType;
use crate::sheets_source::_helpers::cells::cell_text;
use crate::sheets_source::_helpers::layout::{cell_at, cell_name};
use crate::sheets_source::models::Grid;

/// Column `name` at `position` of `rows` as its field and an array in `column_type`; an error cell, or a value that does not convert, is an error naming its cell.
pub(crate) fn column_array(
    grid: &Grid,
    rows: &[usize],
    position: usize,
    (name, column_type): (&str, &ColumnType),
) -> Result<(Field, ArrayRef), String> {
    let mut texts: Vec<Option<String>> = Vec::with_capacity(rows.len());
    for row in rows {
        let text = cell_text(cell_at(grid, *row, position), column_type)
            .map_err(|reason| format!("{} {reason}", cell_name(grid, *row, position)))?;
        texts.push(text);
    }
    let field =
        arrow_field(name, column_type).unwrap_or_else(|| Field::new(name, DataType::Utf8, true));
    let source: ArrayRef = Arc::new(StringArray::from(texts));
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let converted = cast_with_options(&source, field.data_type(), &strict).map_err(|_| {
        let lenient = cast_with_options(&source, field.data_type(), &CastOptions::default());
        let failing = match lenient {
            Ok(converted) => first_lost(&source, &converted),
            Err(_) => None,
        };
        match failing {
            Some(index) => format!(
                "{} holds {:?}, which does not load as {column_type}; pin a type in columns that holds it",
                cell_name(grid, rows[index], position),
                source
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .map(|values| values.value(index))
                    .unwrap_or_default()
            ),
            None => format!("{}: column {name} does not load as {column_type}", grid.title),
        }
    })?;
    Ok((field, converted))
}

/// The first value of `source` that converted to NULL in `converted`: one that does not hold in the column's type.
fn first_lost(source: &ArrayRef, converted: &ArrayRef) -> Option<usize> {
    (0..source.len()).find(|index| source.is_valid(*index) && converted.is_null(*index))
}
