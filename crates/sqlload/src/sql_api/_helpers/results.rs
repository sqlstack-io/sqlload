use serde_json::Value;

/// Column names from `resultSetMetaData.rowType`.
pub(crate) fn parse_columns(result: &Value) -> Vec<String> {
    let Some(columns) = result["resultSetMetaData"]["rowType"].as_array() else {
        return Vec::new();
    };
    columns.iter().map(column_name).collect()
}

fn column_name(column: &Value) -> String {
    column["name"].as_str().unwrap_or_default().to_owned()
}

/// Rows from `data`, every cell as text or null.
pub(crate) fn parse_rows(result: &Value) -> Vec<Vec<Option<String>>> {
    let Some(rows) = result["data"].as_array() else {
        return Vec::new();
    };
    rows.iter().map(parse_row).collect()
}

fn parse_row(row: &Value) -> Vec<Option<String>> {
    let Some(cells) = row.as_array() else {
        return Vec::new();
    };
    cells.iter().map(cell_text).collect()
}

fn cell_text(cell: &Value) -> Option<String> {
    cell.as_str().map(str::to_owned)
}
