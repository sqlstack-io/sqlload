//! Order two cursor positions by their type.

use std::cmp::Ordering;

use crate::column_types::models::ColumnType;

/// How `left` compares with `right` as values of `column_type`, in the text SQLLoad writes cursors as: integers exactly (as `i128`), decimals exactly by their digits, floats as `f64`, fixed-width timestamps, dates and text as text. Text that does not read as the type compares as text.
pub fn cursor_order(left: &str, right: &str, column_type: &ColumnType) -> Ordering {
    let (left, right) = (left.trim(), right.trim());
    let typed = match column_type {
        ColumnType::Int | ColumnType::SmallInt => {
            match (left.parse::<i128>(), right.parse::<i128>()) {
                (Ok(left), Ok(right)) => Some(left.cmp(&right)),
                _ => None,
            }
        }
        ColumnType::Decimal { .. } => decimal_order(left, right),
        ColumnType::Float => match (left.parse::<f64>(), right.parse::<f64>()) {
            (Ok(left), Ok(right)) => left.partial_cmp(&right),
            _ => None,
        },
        _ => None,
    };
    typed.unwrap_or_else(|| left.cmp(right))
}

/// Exact order of two decimal texts such as `-12.50` and `3`, or `None` when either is not one.
fn decimal_order(left: &str, right: &str) -> Option<Ordering> {
    let (left_negative, left_integer, left_fraction) = decimal_parts(left)?;
    let (right_negative, right_integer, right_fraction) = decimal_parts(right)?;
    let zero = |integer: &str, fraction: &str| integer.is_empty() && fraction.is_empty();
    let left_negative = left_negative && !zero(&left_integer, &left_fraction);
    let right_negative = right_negative && !zero(&right_integer, &right_fraction);
    let width = left_fraction.len().max(right_fraction.len());
    let magnitude = left_integer
        .len()
        .cmp(&right_integer.len())
        .then_with(|| left_integer.cmp(&right_integer))
        .then_with(|| {
            format!("{left_fraction:0<width$}").cmp(&format!("{right_fraction:0<width$}"))
        });
    Some(match (left_negative, right_negative) {
        (false, false) => magnitude,
        (true, true) => magnitude.reverse(),
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
    })
}

/// The sign, integer digits without leading zeros and fraction digits without trailing zeros of a decimal text.
fn decimal_parts(text: &str) -> Option<(bool, String, String)> {
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (integer, fraction) = digits.split_once('.').unwrap_or((digits, ""));
    let valid = !(integer.is_empty() && fraction.is_empty())
        && integer
            .chars()
            .chain(fraction.chars())
            .all(|character| character.is_ascii_digit());
    valid.then(|| {
        (
            negative,
            integer.trim_start_matches('0').to_owned(),
            fraction.trim_end_matches('0').to_owned(),
        )
    })
}
