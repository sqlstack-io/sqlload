//! Cursor bounds as the ODBC parameters a read binds.

use arrow_odbc::odbc_api::handles::AsStatementRef;
use arrow_odbc::odbc_api::parameter::{InputParameter, WithDataType};
use arrow_odbc::odbc_api::sys::{Date, Timestamp};
use arrow_odbc::odbc_api::{DataType, IntoParameter, Prepared};
use chrono::{Datelike, NaiveDateTime, Timelike};

use crate::adapters::contract::models::BoundParameter;
use crate::odbc_source::constants::{
    LEGACY_DATETIME_DIGITS, SQL_SERVER_DBMS, TIMESTAMP_PARAMETER_PRECISION, TIMESTAMP_TEXT_DIGITS,
};

/// `parameters` as `prepared` binds them, in order: text as `VARCHAR`; a date as `DATE`; a timestamp as `TIMESTAMP` with seven fractional digits, which SQL Server compares as `datetime2(7)` (three would make it a `datetime`, rounding milliseconds), except against a SQL Server column the driver describes with one to three fractional digits, a legacy `datetime`: that compares a `datetime2` parameter with the column's exact 1/300 s ticks, which the driver rounded to milliseconds when the bound was read, so the bound goes as text, which SQL Server converts to the column's own type; when the driver can't describe the parameter, as text with seven digits, exact for `datetime2` and refused by a `datetime`.
pub(crate) fn bound_parameters<S: AsStatementRef>(
    prepared: &mut Prepared<S>,
    dbms: &str,
    parameters: &[BoundParameter],
) -> Vec<Box<dyn InputParameter>> {
    parameters
        .iter()
        .zip(1u16..)
        .map(|(parameter, number)| match parameter {
            BoundParameter::Timestamp(time) if dbms == SQL_SERVER_DBMS => {
                match prepared.describe_param(number) {
                    Ok(described) => match described.data_type {
                        DataType::Timestamp { precision }
                            if LEGACY_DATETIME_DIGITS.contains(&precision) =>
                        {
                            Box::new(
                                timestamp_text(time, precision.unsigned_abs()).into_parameter(),
                            )
                        }
                        _ => odbc_parameter(parameter),
                    },
                    Err(_) => {
                        Box::new(timestamp_text(time, TIMESTAMP_TEXT_DIGITS).into_parameter())
                    }
                }
            }
            _ => odbc_parameter(parameter),
        })
        .collect()
}

/// `time` as ISO 8601 `YYYY-MM-DDTHH:MM:SS`, which SQL Server reads alike under every `SET LANGUAGE` and `SET DATEFORMAT` (with a space it reads the month and day by the session's language), with `digits` fractional digits, or with six when it has more than `digits` (which the database then refuses rather than misreads).
pub(crate) fn timestamp_text(time: &NaiveDateTime, digits: u16) -> String {
    let nanoseconds = time.nanosecond();
    let unit = 10u32.pow(9u32.saturating_sub(u32::from(digits)));
    match (digits, nanoseconds % unit) {
        (0, 0) => time.format("%Y-%m-%dT%H:%M:%S").to_string(),
        (_, 0) => format!(
            "{}.{}",
            time.format("%Y-%m-%dT%H:%M:%S"),
            &format!("{nanoseconds:09}")[..usize::from(digits)]
        ),
        _ => time.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
    }
}

/// `parameter` bound as its own type.
fn odbc_parameter(parameter: &BoundParameter) -> Box<dyn InputParameter> {
    let small = |value: u32| u16::try_from(value).unwrap_or(u16::MAX);
    match parameter {
        BoundParameter::Text(text) => Box::new(text.clone().into_parameter()),
        BoundParameter::Timestamp(time) => Box::new(WithDataType::new(
            Timestamp {
                year: i16::try_from(time.year()).unwrap_or(i16::MAX),
                month: small(time.month()),
                day: small(time.day()),
                hour: small(time.hour()),
                minute: small(time.minute()),
                second: small(time.second()),
                fraction: time.nanosecond(),
            },
            DataType::Timestamp {
                precision: TIMESTAMP_PARAMETER_PRECISION,
            },
        )),
        BoundParameter::Date(date) => Box::new(WithDataType::new(
            Date {
                year: i16::try_from(date.year()).unwrap_or(i16::MAX),
                month: small(date.month()),
                day: small(date.day()),
            },
            DataType::Date,
        )),
    }
}
