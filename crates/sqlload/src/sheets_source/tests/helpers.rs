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

/// A synthetic KPI tab: a merged title row, a header row, label-only rows, KPI blocks with their measures, a note, and a formula's `""` here and there.
pub(crate) fn kpis() -> serde_json::Value {
    let percent = |value: f64, shown: &str| number(value, shown, "PERCENT");
    sheet(
        "KPIs",
        (0, 0),
        vec![
            vec![text("Service KPIs 2026")],
            vec![
                text("ID"),
                text("KPI"),
                text("Period 1"),
                text("Period 2"),
                text("Jan"),
                text("Feb"),
                text("Mar"),
            ],
            vec![blank(), text("Customer service")],
            vec![
                blank(),
                text("Targets reviewed quarterly"),
                text("note: P1 excludes returns"),
            ],
            vec![
                text("K1"),
                text("On-time delivery"),
                percent(0.9, "90.0%"),
                text(""),
                percent(0.95, "95.0%"),
                number(0.8, "0.8", ""),
                text(""),
            ],
            vec![
                blank(),
                text("Delivered on time"),
                number(9.0, "9", ""),
                blank(),
                number(19.0, "19", ""),
                number(8.0, "8", ""),
            ],
            vec![
                blank(),
                text("Delivered"),
                number(10.0, "10", ""),
                number(0.0, "0", ""),
                number(20.0, "20", ""),
                number(10.0, "10", ""),
                number(0.0, "0", ""),
            ],
            vec![
                text("K2"),
                text("First contact resolution"),
                percent(0.5, "50.0%"),
                percent(0.6, "60.0%"),
                text(""),
                percent(0.75, "75.0%"),
                percent(1.0, "100.0%"),
            ],
            vec![
                blank(),
                text("Escalations"),
                number(1.0, "1", ""),
                text("n/a - system outage"),
                blank(),
                number(0.0, "0", ""),
            ],
            vec![],
            vec![
                text("K3"),
                text("Average handle time (min)"),
                number(6.5, "6.5", ""),
                number(7.0, "7", ""),
                number(6.25, "6.25", ""),
                blank(),
                number(5.5, "5.5", ""),
            ],
        ],
    )
}
