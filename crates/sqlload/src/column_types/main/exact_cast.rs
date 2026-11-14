//! Convert a column to a pinned type, refusing any value the type would change.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_cast::display::array_value_to_string;
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::distinct;
use arrow_schema::DataType;

use crate::column_types::_helpers::exact::{is_decimal, more_places, type_name};

/// `array` as `to`, failing on a value that does not convert or that `to` would round, truncate or cut short: text and floats (by their shortest text, which is also how a float goes into a decimal, since Arrow's float arithmetic would put `437548739.2281` into `decimal(38,9)` as `437548739.228100032`) must have no more decimal places than a decimal's scale, or none for an integer type, and other numbers must come back unchanged. The error shows the first such value.
pub fn exact_cast(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, String> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let from = array.data_type();
    let float = matches!(
        from,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    );
    let read = match float && is_decimal(to) {
        true => {
            cast_with_options(array, &DataType::Utf8, &strict).map_err(|error| error.to_string())?
        }
        false => array.clone(),
    };
    let cast = cast_with_options(&read, to, &strict).map_err(|error| error.to_string())?;
    if from == to || !(to.is_integer() || is_decimal(to)) {
        return Ok(cast);
    }
    let scale = match to {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale,
        _ => 0,
    };
    let changed = match from {
        DataType::Utf8 => more_places(array.as_string::<i32>().iter(), scale),
        DataType::LargeUtf8 => more_places(array.as_string::<i64>().iter(), scale),
        DataType::Utf8View => more_places(array.as_string_view().iter(), scale),
        _ if float => {
            let texts = match read.data_type() {
                DataType::Utf8 => read.clone(),
                _ => cast_with_options(array, &DataType::Utf8, &strict)
                    .map_err(|error| error.to_string())?,
            };
            more_places(texts.as_string::<i32>().iter(), scale)
        }
        _ if from.is_numeric() => {
            let back =
                cast_with_options(&cast, from, &strict).map_err(|error| error.to_string())?;
            let differs = distinct(array, &back).map_err(|error| error.to_string())?;
            (0..array.len()).find(|row| array.is_valid(*row) && differs.value(*row))
        }
        _ => None,
    };
    match changed {
        None => Ok(cast),
        Some(row) => Err(format!(
            "holds {}, which {} can't hold without losing digits",
            array_value_to_string(array, row).map_err(|error| error.to_string())?,
            type_name(to)
        )),
    }
}
