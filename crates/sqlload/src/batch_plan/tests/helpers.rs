use std::sync::Arc;

use arrow_cast::display::{ArrayFormatter, FormatOptions};

use arrow_array::Int64Array;
use chrono::{DateTime, Utc};

use crate::batch_plan::_helpers::bound_array::bound_array;
use crate::batch_plan::main::cut_index::cut_index;
use crate::batch_plan::main::file_groups::file_groups;
use crate::batch_plan::main::last_value::last_value;
use crate::batch_plan::main::read_in_order::read_in_order;
use crate::batch_plan::main::split_windows::split_windows;
use crate::batch_plan::main::window_at::window_at;
use crate::batch_plan::models::{CursorWindow, WindowSplit};
use crate::batch_plan::tests::test_types::{
    BoundArrayTestCase, FileGroupsTestCase, InOrderOutcome, PartitionFiles, ReadInOrderTestCase,
    SplitWindowsTestCase, WindowRowsTestCase,
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
        zone: test_case.zone,
        most: test_case.most,
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
    let array = bound_array(
        test_case.text,
        &test_case.cursor_type,
        field.data_type(),
        chrono_tz::Tz::UTC,
    )
    .map_err(|error| error.to_string())?;
    let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
        .map_err(|error| error.to_string())?;
    Ok(format!("{} {}", array.data_type(), formatter.value(0)))
}

/// 2026-03-01 00:00 UTC, in microseconds.
const MARCH_FIRST_MICROS: i64 = 1_772_323_200_000_000;

/// The case's rows read as one window in two batches: how many load, how many the bounds skipped for a NULL cursor, and what the load says of those outside.
pub(crate) fn window_report(
    test_case: &WindowRowsTestCase,
) -> Result<(usize, usize, String), String> {
    use crate::batch_plan::main::outside_text::outside_text;
    use crate::batch_plan::main::window_rows::window_rows;
    use crate::batch_plan::models::OutsideRows;
    use crate::column_types::models::ColumnType;
    use crate::loader::models::ReadWindow;
    use crate::project::models::OnCursorOutOfRange;
    use arrow_array::{Array, RecordBatch, TimestampMicrosecondArray};
    let micros: Vec<Option<i64>> = test_case
        .hours
        .iter()
        .map(|hours| hours.map(|hours| MARCH_FIRST_MICROS + hours * 3_600_000_000))
        .collect();
    let column = TimestampMicrosecondArray::from(micros).with_timezone("UTC");
    let field = arrow_schema::Field::new("updated_at", column.data_type().clone(), true);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![field])),
        vec![Arc::new(column)],
    )
    .map_err(|error| error.to_string())?;
    let window = ReadWindow {
        column: "updated_at".to_owned(),
        from: test_case.from.map(str::to_owned),
        to: test_case.to.map(str::to_owned),
        inclusive: test_case.inclusive,
        maximum: None,
    };
    let keep_outside = test_case.rule == OnCursorOutOfRange::Warn;
    let rules = (&ColumnType::TimestampTz, test_case.include, keep_outside);
    let split = test_case.split_at;
    let first = window_rows(
        &batch.slice(0, split),
        &window,
        rules,
        (test_case.zone, 0, OutsideRows::default()),
    )
    .map_err(|error| error.to_string())?;
    let second = window_rows(
        &batch.slice(split, batch.num_rows() - split),
        &window,
        rules,
        (test_case.zone, split as u64, first.outside),
    )
    .map_err(|error| error.to_string())?;
    let text = outside_text(
        &second.outside,
        &window,
        (&ColumnType::TimestampTz, test_case.zone),
        test_case.rule,
    )
    .map_err(|error| error.to_string())?;
    Ok((
        first.kept.num_rows() + second.kept.num_rows(),
        first.nulls + second.nulls,
        text,
    ))
}

/// Run the case's reads through `read_in_order` on a paused clock: read `index` sleeps `(count - index) * 10` ms, so later reads finish first, and each consumption sleeps 50 ms. Returns the results consumed; the most reads running at once, the most results held at once (from a read's start to the end of its consumption) and the first read to finish; and the error.
pub(crate) fn in_order(test_case: &ReadInOrderTestCase) -> Result<InOrderOutcome, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .map_err(|error| error.to_string())?;
    let (running, most_running) = (std::cell::Cell::new(0_usize), std::cell::Cell::new(0_usize));
    let (held, most_held) = (std::cell::Cell::new(0_usize), std::cell::Cell::new(0_usize));
    let finished: std::cell::RefCell<Vec<u64>> = std::cell::RefCell::new(Vec::new());
    let mut consumed: Vec<u64> = Vec::new();
    let count = test_case.count;
    let read = |index: u64| {
        let (running, most_running, finished) = (&running, &most_running, &finished);
        let (held, most_held) = (&held, &most_held);
        async move {
            running.set(running.get() + 1);
            most_running.set(most_running.get().max(running.get()));
            held.set(held.get() + 1);
            most_held.set(most_held.get().max(held.get()));
            tokio::time::sleep(std::time::Duration::from_millis((count - index) * 10)).await;
            running.set(running.get() - 1);
            finished.borrow_mut().push(index);
            (test_case.failing_read == Some(index))
                .then(|| format!("read {index} failed"))
                .map_or(Ok(index), Err)
        }
    };
    let result = runtime.block_on(read_in_order(
        (count, test_case.limit),
        read,
        async |index: u64| {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            held.set(held.get() - 1);
            (test_case.failing_consume == Some(index))
                .then(|| format!("commit {index} failed"))
                .map_or(Ok(()), Err)
                .map(|()| consumed.push(index))
        },
    ));
    let first = finished.borrow().first().copied().unwrap_or(u64::MAX);
    Ok((
        consumed,
        (most_running.get(), most_held.get(), first),
        result.err(),
    ))
}
