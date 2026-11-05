use crate::column_types::constants::{INT_DIGITS, SMALLINT_DIGITS, UUID_TEXT_LENGTH};
use crate::column_types::models::ColumnType;

/// Whether every value of `desired` is also a value of `live`.
pub(crate) fn holds(live: &ColumnType, desired: &ColumnType) -> bool {
    match (live, desired) {
        (ColumnType::Int, ColumnType::SmallInt) => true,
        (ColumnType::Decimal { precision, scale }, ColumnType::SmallInt) => {
            precision - scale >= SMALLINT_DIGITS
        }
        (ColumnType::Decimal { precision, scale }, ColumnType::Int) => {
            precision - scale >= INT_DIGITS
        }
        (
            ColumnType::Decimal { precision, scale },
            ColumnType::Decimal {
                precision: desired_precision,
                scale: desired_scale,
            },
        ) => scale >= desired_scale && precision - scale >= desired_precision - desired_scale,
        (ColumnType::Text, ColumnType::Varchar(_) | ColumnType::Uuid) => true,
        (ColumnType::Varchar(length), ColumnType::Varchar(desired_length)) => {
            length >= desired_length
        }
        (ColumnType::Varchar(length), ColumnType::Uuid) => *length >= UUID_TEXT_LENGTH,
        (ColumnType::Native(live), ColumnType::Native(desired)) => {
            live.eq_ignore_ascii_case(desired)
        }
        _ => false,
    }
}

/// The narrowest type holding both `live` and `desired` along SQLLoad's widening ladder.
pub(crate) fn widened_type(desired: &ColumnType, live: &ColumnType) -> Option<ColumnType> {
    match (live, desired) {
        (ColumnType::Varchar(_), ColumnType::Text | ColumnType::Varchar(_)) => {
            Some(desired.clone())
        }
        (ColumnType::SmallInt, ColumnType::Int) => Some(ColumnType::Int),
        (
            ColumnType::SmallInt | ColumnType::Int | ColumnType::Decimal { .. },
            ColumnType::Decimal { .. },
        ) => widened_decimal(live, desired),
        (ColumnType::Bool, ColumnType::Int) => Some(ColumnType::Int),
        (ColumnType::TimestampNtz, ColumnType::TimestampTz) => Some(ColumnType::TimestampTz),
        _ => None,
    }
}

fn widened_decimal(live: &ColumnType, desired: &ColumnType) -> Option<ColumnType> {
    let digits = |column_type: &ColumnType| match column_type {
        ColumnType::SmallInt => Some((SMALLINT_DIGITS, 0)),
        ColumnType::Int => Some((INT_DIGITS, 0)),
        ColumnType::Decimal { precision, scale } => Some((precision - scale, *scale)),
        _ => None,
    };
    let (live_whole, live_scale) = digits(live)?;
    let (desired_whole, desired_scale) = digits(desired)?;
    let scale = live_scale.max(desired_scale);
    let precision = live_whole.max(desired_whole) + scale;
    (precision <= crate::column_types::constants::MAX_DECIMAL_PRECISION)
        .then_some(ColumnType::Decimal { precision, scale })
}
