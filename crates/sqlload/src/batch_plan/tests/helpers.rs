use std::sync::Arc;

use arrow_cast::display::{ArrayFormatter, FormatOptions};

use arrow_array::Int64Array;
use chrono::{DateTime, Utc};

use crate::batch_plan::_helpers::bound_array::bound_array;
use crate::batch_plan::main::cut_index::cut_index;
use crate::batch_plan::main::file_groups::file_groups;
use crate::batch_plan::main::last_value::last_value;
use crate::batch_plan::main::split_windows::split_windows;
use crate::batch_plan::main::window_at::window_at;
use crate::batch_plan::models::{CursorWindow, WindowSplit};
use crate::batch_plan::tests::test_types::{
    BoundArrayTestCase, FileGroupsTestCase, PartitionFiles, SplitWindowsTestCase,
};
use crate::column_types::main::arrow_field::arrow_field;
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
    let plan = split_windows(&split)?;
    (0..plan.count)
        .map(|index| window_at(&plan, index).map(|window| window_text(&window)))
        .collect()
}

fn window_text(window: &CursorWindow) -> String {
    let open = ["(", ""][usize::from(window.inclusive)];
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
        .flat_map(|partition| (0..partition.files).map(move |index| data_file(partition, index)))
        .collect();
    file_groups(&files, test_case.by_value)
        .into_iter()
        .map(|group| (group.files.len(), group.values))
        .collect()
}

fn data_file(partition: &PartitionFiles, index: usize) -> DataFile {
    let key = format!(
        "region={}/day={}/part-{index:05}.parquet",
        partition.region, partition.day
    );
    DataFile {
        file: StoredFile {
            path: object_store::path::Path::from(key.as_str()),
            relative_key: key,
            size: partition.file_bytes,
            etag: None,
            modified_ms: 0,
            member: None,
        },
        partitions: vec![
            ("region".to_owned(), partition.region.to_owned()),
            ("day".to_owned(), partition.day.to_owned()),
        ],
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

/// `expected` as the owned values [`windows`] returns.
pub(crate) fn owned_windows(expected: Result<&[&str], &str>) -> Result<Vec<String>, String> {
    expected
        .map(|windows| windows.iter().map(|window| (*window).to_owned()).collect())
        .map_err(str::to_owned)
}

/// `expected` as the owned values [`groups`] returns.
pub(crate) fn owned_groups(expected: &[(usize, &[&str])]) -> Vec<(usize, Vec<String>)> {
    expected
        .iter()
        .map(|(count, values)| (*count, owned(values)))
        .collect()
}

fn owned(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

/// `files` tiny files of `day` in region `eu`.
pub(crate) const fn eu(day: &'static str, files: usize) -> PartitionFiles {
    PartitionFiles {
        region: "eu",
        day,
        files,
        file_bytes: 1,
    }
}

/// The bound built for a cursor column of the case's type, as Arrow displays it.
pub(crate) fn bound_text(test_case: &BoundArrayTestCase) -> Result<String, String> {
    let field = arrow_field("c", &test_case.cursor_type).ok_or("no Arrow type")?;
    let array = bound_array(test_case.text, &test_case.cursor_type, field.data_type())
        .map_err(|error| error.to_string())?;
    let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
        .map_err(|error| error.to_string())?;
    Ok(format!("{} {}", array.data_type(), formatter.value(0)))
}
