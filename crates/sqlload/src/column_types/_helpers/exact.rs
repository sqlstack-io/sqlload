//! Helpers of `exact_cast`: how many decimal places a value needs, and how a pinned type names itself.

use arrow_schema::DataType;

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

/// The first of `texts` with more decimal places than `scale`.
pub(crate) fn more_places<'a>(
    texts: impl Iterator<Item = Option<&'a str>>,
    scale: i8,
) -> Option<usize> {
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
pub(crate) fn decimal_places(text: &str) -> i64 {
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
pub(crate) fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => format!("decimal({precision},{scale})"),
        other => other.to_string().to_lowercase(),
    }
}
