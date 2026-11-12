//! Arrow values as orjson writes the Python values dlt reads them as.

use arrow_array::{Array, ArrayRef, Float64Array, TimestampMicrosecondArray};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{DataType, TimeUnit};

/// `array`'s values as orjson writes the Python values dlt reads them as: integers, floats (NaN as `null`) and booleans as JSON, timestamps as [`timestamp_json`], anything else as its text in a string.
pub(crate) fn json_values(array: &ArrayRef) -> Result<Vec<String>, String> {
    match array.data_type() {
        DataType::Timestamp(_, zone) => {
            let micros = arrow_cast::cast(
                array,
                &DataType::Timestamp(TimeUnit::Microsecond, zone.clone()),
            )
            .map_err(|error| error.to_string())?;
            let micros = micros
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or("a timestamp column that is not one")?;
            Ok(micros
                .iter()
                .map(|value| {
                    value.map_or_else(
                        || "null".to_owned(),
                        |value| timestamp_json(value, zone.is_some()),
                    )
                })
                .collect())
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let floats =
                arrow_cast::cast(array, &DataType::Float64).map_err(|error| error.to_string())?;
            let floats = floats
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("a float column that is not one")?;
            floats
                .iter()
                .map(|value| value.map_or_else(|| Ok("null".to_owned()), float_json))
                .collect()
        }
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Boolean => shown(array, false),
        _ => shown(array, true),
    }
}

/// `array`'s values as Arrow shows them, in JSON strings when `quoted`; `null` for a null.
fn shown(array: &ArrayRef, quoted: bool) -> Result<Vec<String>, String> {
    let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())
        .map_err(|error| error.to_string())?;
    (0..array.len())
        .map(|row| {
            let text = formatter.value(row).to_string();
            match (array.is_null(row), quoted) {
                (true, _) => Ok("null".to_owned()),
                (false, true) => serde_json::to_string(&text).map_err(|error| error.to_string()),
                (false, false) => Ok(text),
            }
        })
        .collect()
}

/// A timestamp of `micros` since the epoch as orjson writes a `datetime`: ISO 8601, microseconds only when it has some, and `+00:00` when it is `zoned` (an instant, in UTC).
fn timestamp_json(micros: i64, zoned: bool) -> String {
    let fraction = micros.rem_euclid(1_000_000);
    let time = chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
    format!(
        "\"{}{}{}\"",
        time.format("%Y-%m-%dT%H:%M:%S"),
        match fraction {
            0 => String::new(),
            _ => format!(".{fraction:06}"),
        },
        match zoned {
            true => "+00:00",
            false => "",
        }
    )
}

/// A float as orjson writes it: `null` unless finite; in decimals with at least one after the point (`100.0`, `0.00001`) from 1e-5 to below 1e16; otherwise the shortest digits that read back as it with a bare exponent (`1e16`, `1.5e-7`).
fn float_json(value: f64) -> Result<String, String> {
    if !value.is_finite() {
        return Ok("null".to_owned());
    }
    let exponent: String = format!("{value:e}");
    let power: i32 = match exponent.rsplit_once('e') {
        Some((_, power)) => power
            .parse()
            .map_err(|error| format!("{exponent}: {error}"))?,
        None => return Err(format!("{exponent}: no exponent")),
    };
    let decimal = format!("{value}");
    Ok(match ((-5..16).contains(&power), decimal.contains('.')) {
        (false, _) => exponent,
        (true, true) => decimal,
        (true, false) => format!("{decimal}.0"),
    })
}
