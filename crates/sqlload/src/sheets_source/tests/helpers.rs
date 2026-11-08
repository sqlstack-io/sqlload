use arrow_array::RecordBatch;
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use serde_json::{Value, json};

/// A tab as the Sheets API answers for it, its range starting at `start` (row, column; 0-based).
pub(crate) fn sheet(title: &str, start: (u32, u32), rows: Vec<Vec<Value>>) -> Value {
    let rows: Vec<Value> = rows
        .into_iter()
        .map(|cells| json!({"values": cells}))
        .collect();
    json!({"properties": {"title": title, "sheetType": "GRID"}, "data": [{"startRow": start.0, "startColumn": start.1, "rowData": rows}]})
}

/// A text cell.
pub(crate) fn text(value: &str) -> Value {
    json!({"effectiveValue": {"stringValue": value}, "formattedValue": value})
}

/// A number cell shown as `shown`, with a number format of `format` (`""` for none).
pub(crate) fn number(value: f64, shown: &str, format: &str) -> Value {
    json!({"effectiveValue": {"numberValue": value}, "formattedValue": shown, "effectiveFormat": {"numberFormat": {"type": format}}})
}

/// A checkbox or TRUE/FALSE cell.
pub(crate) fn flag(value: bool) -> Value {
    json!({"effectiveValue": {"boolValue": value}, "formattedValue": if value { "TRUE" } else { "FALSE" }})
}

/// A formula error cell.
pub(crate) fn error(shown: &str) -> Value {
    json!({"effectiveValue": {"errorValue": {"type": "DIVIDE_BY_ZERO", "message": "x"}}, "formattedValue": shown})
}

/// An empty cell.
pub(crate) fn blank() -> Value {
    json!({})
}

/// `batch`'s rows, each as its values joined by `|`, rows joined by `;`.
pub(crate) fn rows_text(batch: &RecordBatch) -> Result<String, String> {
    let options = FormatOptions::default().with_null("NULL");
    let formatters: Vec<ArrayFormatter<'_>> = batch
        .columns()
        .iter()
        .map(|column| ArrayFormatter::try_new(column.as_ref(), &options))
        .collect::<Result<_, _>>()
        .map_err(|error| error.to_string())?;
    let rows: Vec<String> = (0..batch.num_rows())
        .map(|row| {
            formatters
                .iter()
                .map(|formatter| formatter.value(row).to_string())
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect();
    Ok(rows.join(";"))
}
