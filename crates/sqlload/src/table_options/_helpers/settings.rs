use std::collections::BTreeMap;

use crate::adapters::contract::models::ChangeApply;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::cdc::constants::{BATCH_INTERVAL, FILES_BATCH_INTERVAL};
use crate::column_parse::main::column_parse::column_parse;
use crate::column_parse::main::time_zone::time_zone;
use crate::column_parse::models::ColumnParse;
use crate::loader::models::{CdcSpec, CursorSpec, SettingOrigin, TableLayout};
use crate::project::models::{
    CdcMode, CursorBounds, Deletes, Lookback, LookbackFrom, SourceKind, TableOverride,
    TableSettings,
};
use crate::table_options::main::layered::layered;
use crate::table_options::models::OptionContext;

/// The cursor a table's settings describe, if any.
pub(crate) fn cursor(table: &TableSettings, source: &TableSettings) -> Option<CursorSpec> {
    let (columns, _) = layered(&table.cursor_column, &source.cursor_column)?;
    let (column, coalesce) = columns.split_first()?;
    Some(CursorSpec {
        column: column.clone(),
        coalesce: coalesce.to_vec(),
        null_cursor: layered(&table.null_cursor, &source.null_cursor)
            .map(|(value, _)| value)
            .unwrap_or_default(),
        out_of_range: layered(
            &table.on_cursor_out_of_range,
            &source.on_cursor_out_of_range,
        )
        .map(|(value, _)| value)
        .unwrap_or_default(),
        start: layered(&table.cursor_start, &source.cursor_start).map(|(value, _)| value),
        lookback: match layered(&table.lookback, &source.lookback) {
            Some((Lookback::Duration(duration), _)) => Some(duration),
            _ => None,
        },
        lookback_units: match layered(&table.lookback, &source.lookback) {
            Some((Lookback::Units(units), _)) => Some(units),
            _ => None,
        },
        lookahead: layered(&table.lookahead, &source.lookahead).map(|(value, _)| value),
        from_now: layered(&table.lookback_from, &source.lookback_from)
            .is_some_and(|(value, _)| value == LookbackFrom::Now),
        inclusive: layered(
            &table.append_cursor_inclusive,
            &source.append_cursor_inclusive,
        )
        .is_some_and(|(value, _)| value),
        literals: layered(&table.cursor_bounds, &source.cursor_bounds)
            .is_some_and(|(value, _)| value == CursorBounds::Literals),
    })
}

/// A `postgres_cdc` source's stream settings, with the default slot and publication names.
pub(crate) fn cdc_spec(context: &OptionContext<'_>, deletes: Option<Deletes>) -> Option<CdcSpec> {
    let source = context.source;
    (source.kind == SourceKind::PostgresCdc).then(|| {
        let default_name = format!("sqlload_{}", context.name.to_lowercase());
        CdcSpec {
            apply: match (source.cdc_mode, deletes.unwrap_or(Deletes::Soft)) {
                (CdcMode::Changelog, _) => ChangeApply::Changelog,
                (CdcMode::Merge, Deletes::Soft) => ChangeApply::SoftDeletes,
                (CdcMode::Merge, Deletes::Hard) => ChangeApply::HardDeletes,
            },
            slot: source.slot.clone().unwrap_or_else(|| default_name.clone()),
            publication: source.publication.clone().unwrap_or(default_name),
            batch_interval: source.batch_interval.unwrap_or(
                match sql_adapter(context.target.warehouse).runs_sql() {
                    true => BATCH_INTERVAL,
                    false => FILES_BATCH_INTERVAL,
                },
            ),
        }
    })
}

/// The table's `parse` entries read, or their problems; a column cannot be both pinned in `columns` and parsed, as `parse` gives its type.
pub(crate) fn table_parses(
    overrides: &TableOverride,
) -> Result<BTreeMap<String, ColumnParse>, Vec<String>> {
    let mut problems: Vec<String> = Vec::new();
    let mut parses: BTreeMap<String, ColumnParse> = BTreeMap::new();
    for (column, setting) in &overrides.parse {
        match column_parse(setting) {
            Ok(_) if overrides.columns.contains_key(column) => problems.push(format!(
                "parse {column}: the column is also pinned in columns, and parse gives its type; drop the pin"
            )),
            Ok(parse) => {
                parses.insert(column.clone(), parse);
            }
            Err(problem) => problems.push(format!("parse {column}: {problem}")),
        }
    }
    match problems.is_empty() {
        true => Ok(parses),
        false => Err(problems),
    }
}

/// The table's `timezone` (its own, else its source's) and where it is set, or why it is not an IANA zone.
pub(crate) fn table_timezone(
    (settings, defaults): (&TableSettings, &TableSettings),
) -> Result<Option<(chrono_tz::Tz, SettingOrigin)>, String> {
    match layered(&settings.timezone, &defaults.timezone) {
        Some((name, origin)) => time_zone(&name)
            .map(|zone| Some((zone, origin)))
            .map_err(|problem| format!("timezone: {problem}")),
        None => Ok(None),
    }
}

/// The table's layout, each field layered table over source; `table_settings` merged, the table's value winning per setting.
pub(crate) fn table_layout(table: &TableSettings, source: &TableSettings) -> TableLayout {
    let mut table_settings = source.table_settings.clone().unwrap_or_default();
    table_settings.extend(table.table_settings.clone().unwrap_or_default());
    TableLayout {
        engine: layered(&table.engine, &source.engine).map(|(value, _)| value),
        order_by: layered(&table.order_by, &source.order_by).map(|(value, _)| value),
        partition_by: layered(&table.partition_by, &source.partition_by).map(|(value, _)| value),
        cluster_by: layered(&table.cluster_by, &source.cluster_by).map(|(value, _)| value),
        primary_key: layered(&table.primary_key, &source.primary_key).map(|(value, _)| value),
        table_settings,
        relayout: false,
    }
}
