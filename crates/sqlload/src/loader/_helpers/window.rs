use crate::column_types::models::ColumnType;
use crate::loader::_helpers::destination::single_text;
use crate::loader::errors::LoaderError;
use crate::loader::models::{CursorSpec, Destination, PlanOptions, TableSpec};
use crate::project::models::WriteStrategy;
use crate::write_sql::main::cursor_bound::cursor_bound;
use crate::write_sql::main::cursor_query::cursor_query;
use crate::write_sql::main::cursor_text::cursor_text;
use crate::write_sql::models::{CursorQuery, WriteMode};

/// What decides one table's read window.
pub(crate) struct WindowRequest<'a> {
    pub(crate) spec: &'a TableSpec,
    pub(crate) destination: &'a Destination,
    pub(crate) table_exists: bool,
    pub(crate) cursor_type: Option<&'a ColumnType>,
    /// The cursor's select expression in the source, which filters compare against.
    pub(crate) cursor_expression: Option<&'a str>,
    pub(crate) options: &'a PlanOptions,
}

/// The source filter, the write mode and a note for `plan`.
pub(crate) struct Window {
    pub(crate) filter: Option<String>,
    /// The cursor range read: `from <= cursor < to`, each open when `None`.
    pub(crate) bounds: (Option<String>, Option<String>),
    pub(crate) mode: WriteMode,
    pub(crate) note: Option<String>,
    /// Whether the lower bound itself is read.
    pub(crate) inclusive: bool,
    /// The destination's cursor maximum, without lookback, when it was read.
    pub(crate) maximum: Option<String>,
}

fn write_mode(
    spec: &TableSpec,
    reload: bool,
    from: Option<String>,
    to: Option<String>,
) -> WriteMode {
    match (reload, spec.strategy) {
        (true, _) => WriteMode::Recreate,
        (false, WriteStrategy::Append) => WriteMode::Append,
        (false, WriteStrategy::Replace) => WriteMode::Replace,
        (false, WriteStrategy::Merge) => WriteMode::Merge {
            unique_key: spec.unique_key.clone(),
        },
        (false, WriteStrategy::DeleteInsert) => WriteMode::DeleteInsert {
            cursor_column: spec
                .cursor
                .as_ref()
                .map(|cursor| cursor.column.clone())
                .unwrap_or_default(),
            from,
            to,
        },
    }
}

/// `"column" >= 'from' AND "column" < 'to'` for Postgres, from the bounds that are set.
pub(crate) fn source_filter(
    column: &str,
    lower: Option<(&str, &str)>,
    upper: Option<&str>,
) -> Option<String> {
    let literal = |value: &str| format!("'{}'", value.replace('\'', "''"));
    let bounds: Vec<String> = lower
        .map(|(operator, value)| format!("{column} {operator} {}", literal(value)))
        .into_iter()
        .chain(upper.map(|value| format!("{column} < {}", literal(value))))
        .collect();
    (!bounds.is_empty()).then(|| bounds.join(" AND "))
}

/// Where the read starts, and why: a `--start-cursor`, the destination's maximum, `cursor_start`, or nothing; with the destination's maximum (without lookback) when it was read.
async fn lower_bound(
    request: &WindowRequest<'_>,
    cursor: &CursorSpec,
    cursor_type: &ColumnType,
    start_flag: Option<&str>,
) -> Result<Result<(Option<String>, String, Option<String>), String>, LoaderError> {
    let options = request.options;
    if let Some(start) = start_flag {
        return Ok(Ok((
            Some(start.to_owned()),
            format!("{} from {start} (--start-cursor)", cursor.column),
            None,
        )));
    }
    if options.reload || !request.table_exists {
        let label = if options.reload {
            "reload"
        } else {
            "first run"
        };
        return Ok(Ok(match &cursor.start {
            Some(start) => (
                Some(start.clone()),
                format!("{} {label}, from cursor_start {start}", cursor.column),
                None,
            ),
            None => (None, format!("{} {label}, everything", cursor.column), None),
        }));
    }
    let spec = request.spec;
    let query = cursor_query(&CursorQuery {
        table: &spec.destination,
        column: &cursor.column,
        column_type: cursor_type,
        lookback: cursor.lookback,
        warehouse: spec.warehouse,
    });
    let Some(query) = query else {
        return Ok(Err(format!(
            "lookback needs a timestamp or date cursor, but {} is {cursor_type}",
            cursor.column
        )));
    };
    let lookback = cursor
        .lookback
        .map(|lookback| format!(" minus lookback {}", humantime::format_duration(lookback)))
        .unwrap_or_default();
    let raw = single_text(request.destination, query.sql).await?;
    let bounds = raw.as_deref().map(|raw| {
        Ok::<(String, String), String>((
            cursor_bound(raw, query.value, cursor.lookback)?,
            cursor_bound(raw, query.value, None)?,
        ))
    });
    let (maximum, data_maximum) = match bounds {
        Some(Ok((bound, data))) => (Some(bound), Some(data)),
        Some(Err(problem)) => return Ok(Err(problem)),
        None => (None, None),
    };
    Ok(Ok(match (maximum, &cursor.start) {
        (Some(maximum), _) => {
            let note = format!(
                "{} from {maximum} (destination maximum{lookback})",
                cursor.column
            );
            (Some(maximum), note, data_maximum)
        }
        (None, Some(start)) => (
            Some(start.clone()),
            format!("{} empty table, from cursor_start {start}", cursor.column),
            None,
        ),
        (None, None) => (
            None,
            format!("{} empty table, everything", cursor.column),
            None,
        ),
    }))
}

/// The read window for one table, or why it cannot be read as asked.
pub(crate) async fn cursor_window(
    request: &WindowRequest<'_>,
) -> Result<Result<Window, String>, LoaderError> {
    let spec = request.spec;
    let options = request.options;
    let backfill = options.start_cursor.is_some() || options.end_cursor.is_some();
    let (Some(cursor), Some(cursor_type)) = (&spec.cursor, request.cursor_type) else {
        if backfill {
            return Ok(Err(
                "--start-cursor/--end-cursor need a cursor_column".to_owned()
            ));
        }
        return Ok(Ok(Window {
            filter: None,
            bounds: (None, None),
            mode: write_mode(spec, options.reload, None, None),
            note: None,
            inclusive: true,
            maximum: None,
        }));
    };
    if backfill && options.reload {
        return Ok(Err(
            "--reload rebuilds the whole table, so it takes no --start-cursor/--end-cursor"
                .to_owned(),
        ));
    }
    let flag = |value: &Option<String>| {
        value
            .as_deref()
            .map(|text| cursor_text(text, cursor_type))
            .transpose()
    };
    let (start_flag, to) = match (flag(&options.start_cursor), flag(&options.end_cursor)) {
        (Ok(start), Ok(end)) => (start, end),
        (Err(problem), _) | (_, Err(problem)) => return Ok(Err(problem)),
    };
    let (from, note, maximum) =
        match lower_bound(request, cursor, cursor_type, start_flag.as_deref()).await? {
            Ok(bound) => bound,
            Err(problem) => return Ok(Err(problem)),
        };
    let inclusive = !matches!(
        (spec.strategy, cursor.inclusive, &options.start_cursor),
        (WriteStrategy::Append, false, None)
    );
    let operator = match inclusive {
        true => ">=",
        false => ">",
    };
    let note = match &to {
        Some(end) => format!("{note}, below {end} (--end-cursor)"),
        None => note,
    };
    Ok(Ok(Window {
        filter: source_filter(
            request.cursor_expression.unwrap_or_default(),
            from.as_deref().map(|from| (operator, from)),
            to.as_deref(),
        ),
        bounds: (from.clone(), to.clone()),
        mode: write_mode(spec, options.reload, from, to),
        note: Some(note),
        inclusive,
        maximum,
    }))
}
