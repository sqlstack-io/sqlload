use serde_json::Value;

use crate::column_parse::_helpers::setting::{epoch_parse, shape_problem, table_parse};
use crate::column_parse::models::ColumnParse;

/// One column's `parse` as written (a table's `parse` entry, `@table(parse=...)`'s, or an `sl.field(parse=...)`): `"epoch_s"`, `"epoch_ms"` or `"epoch_us"`, or a table with `timestamp` or `date` (a format or a list tried in turn: `"iso"`, an epoch word, or strftime), `tz` and `on_error`; or why it can't be, worded to follow the setting's name.
pub fn column_parse(setting: &Value) -> Result<ColumnParse, String> {
    match setting {
        Value::String(word) => epoch_parse(word).ok_or_else(|| shape_problem(setting)),
        Value::Object(fields) => table_parse(fields),
        _ => Err(shape_problem(setting)),
    }
}
