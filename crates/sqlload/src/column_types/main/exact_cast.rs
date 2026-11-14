//! Convert a column to a pinned type, refusing any value the type would change.

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef};
use arrow_cast::display::array_value_to_string;
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::distinct;
use arrow_schema::DataType;

/// `array` as `to`, failing on a value that does not convert or that `to` would round, truncate or cut short: text and floats (by their shortest text, which is also how a float goes into a decimal) must have no more decimal places than a decimal's scale, or none for an integer type, and other numbers must come back unchanged. The error shows the first such value.
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
    // A float goes into a decimal by its shortest text: Arrow's float arithmetic would put `437548739.2281` into `decimal(38,9)` as `437548739.228100032`.
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

fn is_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

/// The first of `texts` with more decimal places than `scale`.
fn more_places<'a>(texts: impl Iterator<Item = Option<&'a str>>, scale: i8) -> Option<usize> {
    for (row, text) in texts.enumerate() {
        if let Some(text) = text
            && decimal_places(text) > i64::from(scale)
        {
            return Some(row);
        }
    }
    None
}

/// How many decimal places `text` needs to be held exactly, such as 2 for `1.250` (trailing zeros need none) and 4 for `12e-4`. Only text the cast read as a decimal comes here, so its exponent is a number.
fn decimal_places(text: &str) -> i64 {
    let unsigned = text.trim().trim_start_matches(['-', '+']);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => match exponent.parse::<i64>() {
            Ok(exponent) => (mantissa, exponent),
            Err(_) => (mantissa, 0),
        },
        None => (unsigned, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let significant = format!("{whole}{fraction}").trim_end_matches('0').len() as i64;
    if significant == 0 {
        return 0;
    }
    (significant - (whole.len() as i64 + exponent)).max(0)
}

/// `data_type` as a pinned type names it: `decimal(p,s)` for a decimal.
fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("decimal({precision},{scale})"),
        other => other.to_string().to_lowercase(),
    }
}
