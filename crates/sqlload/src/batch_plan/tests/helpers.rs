use std::sync::Arc;

use arrow_array::Int64Array;
use chrono::{DateTime, Utc};

use crate::batch_plan::main::cut_index::cut_index;
use crate::batch_plan::main::file_groups::file_groups;
use crate::batch_plan::main::last_value::last_value;
use crate::batch_plan::main::split_windows::split_windows;
use crate::batch_plan::models::{CursorWindow, WindowSplit};
use crate::batch_plan::tests::test_types::{FileGroupsTestCase, SplitWindowsTestCase};
use crate::file_store::models::StoredFile;
use crate::files_source::models::DataFile;

/// 2026-11-05 10:30 UTC.
fn now() -> DateTime<Utc> {
    DateTime::parse_from_rfc3339("2026-11-05T10:30:00Z")
        .map(|time| time.with_timezone(&Utc))
        .unwrap_or_default()
}

/// The case's windows as `from..to` text, `(` marking a start that is not read.
pub(crate) fn windows(test_case: &SplitWindowsTestCase) -> Result<Vec<String>, String> {
    let split = WindowSplit {
        from: test_case.from,
        to: test_case.to,
        inclusive: test_case.inclusive,
        step: test_case.step,
        cursor_type: test_case.cursor_type.as_ref(),
        now: now(),
    };
    Ok(split_windows(&split)?.iter().map(window_text).collect())
}

fn window_text(window: &CursorWindow) -> String {
    let open = match window.inclusive {
        true => "",
        false => "(",
    };
    format!(
        "{open}{}..{}",
        window.from.as_deref().unwrap_or("-"),
        window.to.as_deref().unwrap_or("-")
    )
}

/// The case's groups as file counts and values.
pub(crate) fn groups(test_case: &FileGroupsTestCase) -> Vec<(usize, Vec<String>)> {
    let files: Vec<DataFile> = test_case
        .partitions
        .iter()
        .flat_map(|(day, count)| (0..*count).map(move |index| data_file(day, index)))
        .collect();
    file_groups(&files, test_case.by_value)
        .into_iter()
        .map(|group| (group.files.len(), group.values))
        .collect()
}

fn data_file(day: &str, index: usize) -> DataFile {
    let key = format!("day={day}/part-{index:05}.parquet");
    DataFile {
        file: StoredFile {
            path: object_store::path::Path::from(key.as_str()),
            relative_key: key,
            size: 1,
            etag: None,
            modified_ms: 0,
        },
        partitions: vec![("day".to_owned(), day.to_owned())],
    }
}

/// Where a batch of `values` may end a commit after `last`, and its last value.
pub(crate) fn cut(values: &[i64], last: Option<&str>) -> Result<(usize, Option<String>), String> {
    let array = Arc::new(Int64Array::from(values.to_vec()));
    Ok((
        cut_index(array.as_ref(), last)?,
        last_value(array.as_ref())?,
    ))
}
