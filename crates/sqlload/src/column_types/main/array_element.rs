//! The element of an array type a warehouse reports.

use crate::column_types::models::LiveColumnType;

/// The live type of the element `text` of an array column `live` reports, such as `numeric(18,2)` in `numeric(18,2)[]`: its precision and scale from its own arguments, else the column's (BigQuery reports them on the column).
pub fn array_element(live: &LiveColumnType, text: &str) -> LiveColumnType {
    match arguments(text).as_slice() {
        [precision, scale] => LiveColumnType {
            data_type: text.trim().to_owned(),
            precision: *precision,
            scale: *scale,
            char_length: None,
        },
        [length] => LiveColumnType {
            data_type: text.trim().to_owned(),
            precision: *length,
            scale: None,
            char_length: *length,
        },
        _ => LiveColumnType {
            data_type: text.trim().to_owned(),
            ..live.clone()
        },
    }
}

/// The whole-number arguments of `name(a, b)`, `None` for one that is not a number.
fn arguments(text: &str) -> Vec<Option<i64>> {
    let Some((_, rest)) = text.split_once('(') else {
        return Vec::new();
    };
    let inside = rest.split_once(')').map_or(rest, |(inside, _)| inside);
    let mut numbers: Vec<Option<i64>> = Vec::new();
    for part in inside.split(',') {
        numbers.push(match part.trim().parse::<i64>() {
            Ok(number) => Some(number),
            Err(_) => None,
        });
    }
    numbers
}
