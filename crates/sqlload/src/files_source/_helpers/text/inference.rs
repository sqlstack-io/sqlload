use chrono::DateTime;
use serde_json::Value;

use crate::column_types::models::ColumnType;

/// The type a JSON value suggests; `None` for NULL, which fits any type.
pub(crate) fn json_value_type(value: &Value) -> Option<ColumnType> {
    match value {
        Value::Null => None,
        Value::Bool(_) => Some(ColumnType::Bool),
        Value::Number(number) => Some(number_type(&number.to_string())),
        Value::String(text) => Some(string_type(text)),
        Value::Array(_) | Value::Object(_) => Some(ColumnType::Json),
    }
}

/// The type a CSV field suggests (it is never JSON: a CSV field is text).
pub(crate) fn csv_value_type(text: &str) -> ColumnType {
    match text {
        _ if is_integer(text) || is_float(text) => number_type(text),
        _ if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("false") => {
            ColumnType::Bool
        }
        _ => string_type(text),
    }
}

/// `int` for an integer that fits 64 bits, `float` for a finite decimal or exponent number, `text` for the rest (larger integers, leading zeros); the syntax is checked first, so parsing fails only on overflow.
fn number_type(text: &str) -> ColumnType {
    match (is_integer(text), is_float(text)) {
        (true, _) => match text.parse::<i64>() {
            Ok(_) => ColumnType::Int,
            Err(_) => ColumnType::Text,
        },
        (false, true) => match text.parse::<f64>() {
            Ok(value) if value.is_finite() => ColumnType::Float,
            Ok(_) | Err(_) => ColumnType::Text,
        },
        (false, false) => ColumnType::Text,
    }
}

/// `timestamp_tz` when `text` is an RFC 3339 timestamp with an offset, else `text`.
fn string_type(text: &str) -> ColumnType {
    match DateTime::parse_from_rfc3339(text) {
        Ok(_) => ColumnType::TimestampTz,
        Err(_) => ColumnType::Text,
    }
}

/// An optional minus and digits, without leading zeros (`007` is an identifier, not a number).
fn is_integer(text: &str) -> bool {
    let digits = text.strip_prefix('-').unwrap_or(text);
    !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && no_leading_zero(digits)
}

/// An optional minus, an integer part, and a fraction or exponent: `1.5`, `-0.25`, `2e10`.
fn is_float(text: &str) -> bool {
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (unsigned, None),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (mantissa, None),
    };
    let exponent_ok = exponent.is_none_or(|exponent| {
        let digits = exponent.strip_prefix(['+', '-']).unwrap_or(exponent);
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    });
    let fraction_ok = fraction.is_none_or(|fraction| {
        !fraction.is_empty() && fraction.bytes().all(|byte| byte.is_ascii_digit())
    });
    let whole_ok = !whole.is_empty()
        && whole.bytes().all(|byte| byte.is_ascii_digit())
        && no_leading_zero(whole);
    whole_ok && fraction_ok && exponent_ok && (fraction.is_some() || exponent.is_some())
}

/// Digits that do not start with a zero, or a lone zero.
fn no_leading_zero(digits: &str) -> bool {
    digits.len() == 1 || digits.as_bytes().first() != Some(&b'0')
}
