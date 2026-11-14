//! Convert a column to a pinned type, refusing any value the type would change.

use arrow_array::ArrayRef;
use arrow_cast::display::array_value_to_string;
use arrow_cast::{CastOptions, cast_with_options};
use arrow_schema::DataType;

use crate::column_types::_helpers::exact::{
    exact_lists, is_decimal, is_float, lossy_row, type_name,
};

/// `array` as `to`, failing on a value that does not convert or that `to` would round, truncate or cut short: decimal text, and floats (by their shortest text, which is also how a float goes into a decimal, since Arrow's float arithmetic would put `437548739.2281` into `decimal(38,9)` as `437548739.228100032`) must have no more decimal places than a decimal's scale, or none for an integer type, and other numbers must come back unchanged. Text into an integer type needs no check, as the cast already refuses a fraction. A list's elements are held to the same, against the list's element type. The error shows the first such value.
pub fn exact_cast(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, String> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let from = array.data_type();
    let elements = match (from, to) {
        (
            DataType::List(source)
            | DataType::LargeList(source)
            | DataType::FixedSizeList(source, _),
            DataType::List(item),
        ) => Some((source, item)),
        _ => None,
    };
    if let Some((source, item)) = elements {
        return exact_lists(array, (source, item), exact_cast);
    }
    let read = match is_float(from) && is_decimal(to) {
        true => {
            cast_with_options(array, &DataType::Utf8, &strict).map_err(|error| error.to_string())?
        }
        false => array.clone(),
    };
    let cast = cast_with_options(&read, to, &strict).map_err(|error| error.to_string())?;
    if from == to || !(to.is_integer() || is_decimal(to)) {
        return Ok(cast);
    }
    match lossy_row(array, (&read, &cast), to)? {
        None => Ok(cast),
        Some(row) => Err(format!(
            "holds {}, which {} can't hold without losing digits",
            array_value_to_string(array, row).map_err(|error| error.to_string())?,
            type_name(to)
        )),
    }
}
