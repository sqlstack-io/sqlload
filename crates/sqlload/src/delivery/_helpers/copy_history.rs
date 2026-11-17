//! Auto-ingest loads are recorded only in the table's COPY_HISTORY: the Snowpipe report endpoints cover files sent with `insertFiles`.

use crate::sql_api::models::SqlRows;
use crate::stage_api::models::{FileLoad, LoadStatus};

/// One statement for every load into `table` from `start_ms`, so a check costs one warehouse query however many files wait.
pub(crate) fn copy_history_sql(database: &str, table: &str, start_ms: i64) -> String {
    format!(
        "SELECT STAGE_LOCATION, FILE_NAME, STATUS, ROW_COUNT, ROW_PARSED, FIRST_ERROR_MESSAGE FROM TABLE({database}.INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME => '{}', START_TIME => TO_TIMESTAMP_LTZ({start_ms}, 3)))",
        table.replace('\'', "''")
    )
}

/// The history rows as load reports, loads first so a stray row for a loaded file cannot fail it. `FILE_NAME` is relative to `STAGE_LOCATION`, the auto-ingest pipe's own folder, so the two are joined and made relative to `root`, the `bucket/prefix/` staged keys are relative to, whatever the URL's scheme.
pub(crate) fn history_loads(rows: &SqlRows, root: &str) -> Vec<FileLoad> {
    let index = |name: &str| rows.column(name);
    let (Some(location), Some(file), Some(status)) =
        (index("STAGE_LOCATION"), index("FILE_NAME"), index("STATUS"))
    else {
        return Vec::new();
    };
    let count = |row: &[Option<String>], name: &str| {
        index(name)
            .and_then(|column| row.get(column).cloned().flatten())
            .map_or(0, |value| value.parse().unwrap_or(0))
    };
    let mut loads: Vec<FileLoad> = rows
        .rows
        .iter()
        .filter_map(|row| {
            let name = row.get(file).cloned().flatten()?;
            let folder = row.get(location).cloned().flatten().unwrap_or_default();
            let folder = folder
                .split_once("://")
                .map_or(folder.as_str(), |(_, rest)| rest);
            let full = format!(
                "{}/{}",
                folder.trim_end_matches('/'),
                name.trim_start_matches('/')
            );
            Some(FileLoad {
                path: full.strip_prefix(root).unwrap_or(&name).to_owned(),
                status: load_status(row.get(status).cloned().flatten().as_deref()),
                rows_inserted: count(row, "ROW_COUNT"),
                rows_parsed: count(row, "ROW_PARSED"),
                first_error: index("FIRST_ERROR_MESSAGE")
                    .and_then(|column| row.get(column).cloned().flatten()),
            })
        })
        .collect();
    loads.sort_by_key(|load| load.status != LoadStatus::Loaded);
    loads
}

/// COPY_HISTORY's statuses; a skipped file loaded nothing, so it counts as failed.
fn load_status(status: Option<&str>) -> LoadStatus {
    match status.unwrap_or_default().to_ascii_lowercase().as_str() {
        "loaded" => LoadStatus::Loaded,
        "load failed" | "load skipped" => LoadStatus::Failed,
        "partially loaded" => LoadStatus::PartiallyLoaded,
        _ => LoadStatus::InProgress,
    }
}
