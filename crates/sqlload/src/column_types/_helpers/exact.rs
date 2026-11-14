//! Helpers of `exact_cast`: which rows a conversion changed, by the decimal places a value needs, and how a pinned type names itself.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, ListArray};
use arrow_buffer::OffsetBuffer;
use arrow_cast::{CastOptions, cast_with_options};
use arrow_ord::cmp::distinct;
use arrow_schema::{DataType, FieldRef};

/// Whether `data_type` is a decimal of any width.
pub(crate) fn is_decimal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
    )
}

/// `array`, a list of `source` items, as a list of `item`s, each element its rows use (a sliced array keeps others) converted by `element_cast` (the exact cast itself), with the lists' NULLs kept; a failing element is the error.
pub(crate) fn exact_lists(
    array: &ArrayRef,
    (source, item): (&FieldRef, &FieldRef),
    element_cast: fn(&ArrayRef, &DataType) -> Result<ArrayRef, String>,
) -> Result<ArrayRef, String> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let lists = cast_with_options(array, &DataType::List(source.clone()), &strict)
        .map_err(|error| error.to_string())?;
    let lists = lists.as_list::<i32>();
    let first = lists.offsets().first();
    let last = lists.offsets().last();
    let visible = lists
        .values()
        .slice(first as usize, (last - first) as usize);
    let values =
        element_cast(&visible, item.data_type()).map_err(|error| format!("an element {error}"))?;
    let offsets = OffsetBuffer::new(
        lists
            .offsets()
            .iter()
            .map(|offset| offset - first)
            .collect(),
    );
    let rebuilt = ListArray::try_new(item.clone(), offsets, values, lists.nulls().cloned())
        .map_err(|error| error.to_string())?;
    Ok(Arc::new(rebuilt))
}

/// Whether `data_type` is a float of any width.
pub(crate) fn is_float(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

/// The first row of `array` that its conversion to `to` (`cast`, made from `read`: the array itself, or a float's shortest text) changed: decimal text and floats with more decimal places than a decimal's scale (none for an integer type), and other numbers that do not come back unchanged. Text into an integer type is not checked, as the cast already refused a fraction.
pub(crate) fn lossy_row(
    array: &ArrayRef,
    (read, cast): (&ArrayRef, &ArrayRef),
    to: &DataType,
) -> Result<Option<usize>, String> {
    let strict = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let from = array.data_type();
    let scale = match to {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale,
        _ => 0,
    };
    let row = match from {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if to.is_integer() => None,
        DataType::Utf8 => more_places(array.as_string::<i32>().iter(), scale),
        DataType::LargeUtf8 => more_places(array.as_string::<i64>().iter(), scale),
        DataType::Utf8View => more_places(array.as_string_view().iter(), scale),
        _ if is_float(from) => {
            let texts = match read.data_type() {
                DataType::Utf8 => read.clone(),
                _ => cast_with_options(array, &DataType::Utf8, &strict)
                    .map_err(|error| error.to_string())?,
            };
            more_places(texts.as_string::<i32>().iter(), scale)
        }
        _ if from.is_numeric() => {
            let back = cast_with_options(cast, from, &strict).map_err(|error| error.to_string())?;
            let differs = distinct(array, &back).map_err(|error| error.to_string())?;
            (0..array.len()).find(|row| array.is_valid(*row) && differs.value(*row))
        }
        _ => None,
    };
    Ok(row)
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
    let fraction = fraction.trim_end_matches('0');
    let significant = match fraction.is_empty() {
        true => whole.trim_end_matches('0').len(),
        false => whole.len() + fraction.len(),
    } as i64;
    if significant == 0 {
        return 0;
    }
    (significant - (whole.len() as i64 + exponent)).max(0)
}

/// `data_type` as a pinned type names it: `decimal(p,s)` for a decimal.
pub(crate) fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("decimal({precision},{scale})"),
        other => other.to_string().to_lowercase(),
    }
}
