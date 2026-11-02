use serde_json::Value;

/// Column names from `resultSetMetaData.rowType`.
pub(crate) fn parse_columns(result: &Value) -> Vec<String> {
    result["resultSetMetaData"]["rowType"]
        .as_array()
        .map(|columns| {
            columns
                .iter()
                .map(|column| column["name"].as_str().unwrap_or_default().to_owned())
                .collect()
        })
        .unwrap_or_default()
}

/// Rows from `data`, every cell as text or null.
pub(crate) fn parse_rows(result: &Value) -> Vec<Vec<Option<String>>> {
    result["data"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|row| {
                    row.as_array()
                        .map(|cells| {
                            cells
                                .iter()
                                .map(|cell| cell.as_str().map(str::to_owned))
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .unwrap_or_default()
}
