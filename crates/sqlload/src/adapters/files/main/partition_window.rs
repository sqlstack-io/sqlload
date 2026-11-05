//! Choose the cursor window of a `delete_insert` into partition folders.

use crate::adapters::files::_helpers::layout::bounds::{
    align_down, align_up, minus_lookback, partition_start,
};
use crate::adapters::files::models::{PartitionWindow, PartitionWindowRequest};

/// The window: from `--start-cursor`, the newest partition minus the lookback, or `cursor_start`; both ends widened to whole partitions.
pub fn partition_window(request: &PartitionWindowRequest<'_>) -> Result<PartitionWindow, String> {
    let (by, cursor_type) = (request.by, request.cursor_type);
    let column = request.cursor_column;
    let label = match request.reload {
        true => "reload",
        false => "first run",
    };
    let (from, note) = match (request.start, request.newest, request.cursor_start) {
        (Some(start), _, _) => (
            Some(align_down(start, by, cursor_type)?),
            format!("{column} from {start} (--start-cursor)"),
        ),
        (None, Some(newest), _) => {
            let start = partition_start(newest, by, cursor_type);
            let back = minus_lookback(start, cursor_type, request.lookback)?;
            let lookback = request
                .lookback
                .map(|lookback| format!(" minus lookback {}", humantime::format_duration(lookback)))
                .unwrap_or_default();
            (
                Some(align_down(&back, by, cursor_type)?),
                format!("{column} from partition {newest} (the newest{lookback})"),
            )
        }
        (None, None, Some(start)) => (
            Some(align_down(start, by, cursor_type)?),
            format!("{column} {label}, from cursor_start {start}"),
        ),
        (None, None, None) => (None, format!("{column} {label}, everything")),
    };
    let to = request
        .end
        .map(|end| align_up(end, by, cursor_type))
        .transpose()?;
    let note = match (&from, &to) {
        (Some(from), Some(to)) => format!("{note}; rewrites whole partitions {from} to below {to}"),
        (Some(from), None) => format!("{note}; rewrites whole partitions from {from}"),
        (None, Some(to)) => format!("{note}; rewrites whole partitions below {to}"),
        (None, None) => note,
    };
    Ok(PartitionWindow { from, to, note })
}
