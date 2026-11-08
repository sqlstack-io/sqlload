//! The element of an array type a warehouse reports.

use crate::column_types::models::LiveColumnType;

/// The live type of the element `text` of an array column `live` reports, such as `numeric(18,2)` in `numeric(18,2)[]`: its precision and scale from its own arguments, else the column's (BigQuery reports them on the column).
pub fn array_element(live: &LiveColumnType, text: &str) -> LiveColumnType {
    let arguments: Vec<Option<i64>> = text
        .split_once('(')
        .and_then(|(_, rest)| rest.split_once(')'))
        .map(|(inside, _)| {
            inside
                .split(',')
                .map(|part| part.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default();
    match arguments.as_slice() {
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
