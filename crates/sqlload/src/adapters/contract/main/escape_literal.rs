//! Cursor bounds an ODBC source writes into its query instead of binding them.

use chrono::Timelike;

use crate::adapters::contract::_helpers::identifiers::string_literal;
use crate::adapters::contract::models::BoundParameter;

/// `parameter` as the literal an ODBC driver reads as its type: a timestamp as a `{ts '…'}` escape, with the fractional digits it carries, or else the fewest of 0, 3, 6 or 9 that keep every digit; a date as a `{d '…'}` escape; text in single quotes, each quote doubled.
pub fn escape_literal(parameter: &BoundParameter) -> String {
    match parameter {
        BoundParameter::Text(text) => string_literal(text),
        BoundParameter::Date(date) => format!("{{d '{}'}}", date.format("%Y-%m-%d")),
        BoundParameter::Timestamp(time, digits) => {
            let nanoseconds = time.nanosecond();
            let digits = digits.map_or_else(
                || {
                    [0, 3, 6]
                        .into_iter()
                        .find(|digits| nanoseconds % 10u32.pow(9 - digits) == 0)
                        .unwrap_or(9)
                },
                |digits| u32::from(digits.min(9)),
            );
            let fraction = match digits {
                0 => String::new(),
                _ => format!(".{}", &format!("{nanoseconds:09}")[..digits as usize]),
            };
            format!("{{ts '{}{fraction}'}}", time.format("%Y-%m-%d %H:%M:%S"))
        }
    }
}
