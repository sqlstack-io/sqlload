//! Combine the schemas of several Parquet footers.

use arrow_schema::DataType;

use crate::column_types::constants::ARRAY_ELEMENTS;
use crate::column_types::main::portable_type::portable_type;
use crate::column_types::models::ColumnType;
use crate::files_source::_helpers::plan::columns::merge_schema;
use crate::files_source::models::{FileColumn, Footer};

/// Every column in `footers` (first footer first) with its portable type, and a problem for each column whose type differs between files. A list of a type an array holds reads as that `array(T)`, as a files target writes one, so a pinned array keeps fitting the files it wrote.
pub fn footer_columns(footers: &[Footer]) -> (Vec<FileColumn>, Vec<String>) {
    let mut columns: Vec<FileColumn> = Vec::new();
    let mut problems: Vec<String> = Vec::new();
    for footer in footers {
        let (merged, conflicts) = merge_schema(columns, &footer.schema, &footer.relative_key, &[]);
        columns = merged;
        problems.extend(conflicts);
    }
    for column in &mut columns {
        if let Some(array) = written_array(footers, &column.name) {
            column.column_type = Ok(array);
        }
    }
    (columns, problems)
}

/// `array(T)` when the first footer holding column `name` writes it as a list of a type an array holds.
fn written_array(footers: &[Footer], name: &str) -> Option<ColumnType> {
    let field = footers
        .iter()
        .find_map(|footer| footer.schema.column_with_name(name))?
        .1;
    let DataType::List(item) = field.data_type() else {
        return None;
    };
    let Ok(element) = portable_type(item) else {
        return None;
    };
    (ARRAY_ELEMENTS.contains(&element) || matches!(element, ColumnType::Decimal { .. }))
        .then(|| ColumnType::Array(Box::new(element)))
}
