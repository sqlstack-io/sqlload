//! Where each captured table's snapshot came from, kept in the destination beside the tables: a slot's snapshot point when SQLLoad created it, and each table once its snapshot is in, so a snapshot that stopped part-way is never mistaken for a finished one.

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
use crate::cdc::models::SnapshotRecord;
use crate::column_types::models::{ColumnType, Warehouse};
use crate::loader::errors::LoaderError;
use crate::time_text::main::now_utc_text::now_utc_text;

/// The table holding snapshot records, in the captured tables' schema.
const SNAPSHOTS_TABLE: &str = "_sqlload_cdc_snapshots";

/// Separates a record's fields when it is read back as one text value.
const FIELD_SEPARATOR: char = '\t';

/// The record columns, in order.
const RECORD_COLUMNS: [&str; 6] = [
    "source",
    "slot",
    "table_name",
    "snapshot_lsn",
    "columns",
    "recorded_at",
];

/// One source's slot and where its records live.
pub(crate) struct SnapshotLog<'a> {
    pub(crate) destination: &'a Destination,
    pub(crate) warehouse: Warehouse,
    /// A captured table's name, whose database and schema the records share.
    pub(crate) beside: &'a TableName,
    pub(crate) source: &'a str,
    pub(crate) slot: &'a str,
}

impl SnapshotLog<'_> {
    fn table(&self) -> TableName {
        TableName {
            table: SNAPSHOTS_TABLE.to_owned(),
            ..self.beside.clone()
        }
    }

    /// Create the records table when it is missing.
    pub(crate) async fn ensure(&self) -> Result<(), LoaderError> {
        let adapter = sql_adapter(self.warehouse);
        let text = adapter.render_loader_logical_type(&ColumnType::Text).sql;
        let integer = adapter.render_loader_logical_type(&ColumnType::Int).sql;
        let columns: Vec<String> = RECORD_COLUMNS
            .iter()
            .map(|name| {
                let sql_type = match *name {
                    "snapshot_lsn" => &integer,
                    _ => &text,
                };
                format!("{} {sql_type}", adapter.render_identifier(name))
            })
            .collect();
        execute(
            self.destination,
            vec![format!(
                "CREATE TABLE IF NOT EXISTS {} ({})",
                adapter.render_qualified_name(&self.table()),
                columns.join(", ")
            )],
        )
        .await
    }

    /// Record that `record`'s table (empty: the slot itself) holds the snapshot taken at `lsn`, with its columns, highest column number and key.
    pub(crate) async fn record(
        &self,
        record: &SnapshotRecord,
        lsn: i64,
    ) -> Result<(), LoaderError> {
        let adapter = sql_adapter(self.warehouse);
        let text = |value: &str| adapter.render_loader_value_literal(value);
        let names: Vec<String> = RECORD_COLUMNS
            .iter()
            .map(|name| adapter.render_identifier(name))
            .collect();
        execute(
            self.destination,
            vec![format!(
                "INSERT INTO {} ({}) VALUES ({}, {}, {}, {lsn}, {}, {})",
                adapter.render_qualified_name(&self.table()),
                names.join(", "),
                text(self.source),
                text(self.slot),
                text(&record.table),
                text(&record_columns(record)),
                text(&now_utc_text()),
            )],
        )
        .await
    }

    /// The tables holding the slot's newest snapshot, each with its newest record, and that snapshot's LSN; `None` when SQLLoad never recorded creating the slot.
    pub(crate) async fn current(&self) -> Result<Option<(i64, Vec<SnapshotRecord>)>, LoaderError> {
        let adapter = sql_adapter(self.warehouse);
        let column = |name: &str| adapter.render_identifier(name);
        let text_type = adapter.render_loader_logical_type(&ColumnType::Text).sql;
        let separator = adapter.render_loader_value_literal(&FIELD_SEPARATOR.to_string());
        let rows = text_values(
            self.destination,
            format!(
                "SELECT CAST({} AS {text_type}) || {separator} || {} || {separator} || {} || {separator} || {} FROM {} WHERE {} = {} AND {} = {}",
                column("snapshot_lsn"),
                column("recorded_at"),
                column("table_name"),
                column("columns"),
                adapter.render_qualified_name(&self.table()),
                column("source"),
                adapter.render_loader_value_literal(self.source),
                column("slot"),
                adapter.render_loader_value_literal(self.slot),
            ),
        )
        .await?;
        let mut records: Vec<(i64, String, SnapshotRecord)> = Vec::with_capacity(rows.len());
        for row in &rows {
            records.push(read_record(row).map_err(LoaderError::Invalid)?);
        }
        Ok(newest_snapshot(records))
    }
}

/// A record's `columns` text: the data columns, then, tab-separated, the highest column number and the key when it has them.
fn record_columns(record: &SnapshotRecord) -> String {
    match (record.max_attnum, &record.key) {
        (Some(highest), Some(key)) => {
            format!(
                "{}{FIELD_SEPARATOR}{highest}{FIELD_SEPARATOR}{key}",
                record.columns
            )
        }
        _ => record.columns.clone(),
    }
}

/// One record read back as `lsn, recorded_at, table, columns`: its snapshot LSN, when it was written, and the record; an error for a row in neither form.
pub(crate) fn read_record(row: &str) -> Result<(i64, String, SnapshotRecord), String> {
    let unreadable = || format!("an unreadable snapshot record: {row:?}");
    let fields: Vec<&str> = row.split(FIELD_SEPARATOR).collect();
    let (lsn, recorded_at, table, columns, numbered) = match fields.as_slice() {
        [lsn, recorded_at, table, columns] => (lsn, recorded_at, table, columns, None),
        [lsn, recorded_at, table, columns, highest, key] => {
            (lsn, recorded_at, table, columns, Some((highest, key)))
        }
        _ => return Err(unreadable()),
    };
    let lsn = lsn.parse::<i64>().map_err(|_| unreadable())?;
    let (max_attnum, key) = match numbered {
        Some((highest, key)) => (
            Some(highest.parse::<i16>().map_err(|_| unreadable())?),
            Some((*key).to_owned()),
        ),
        None => (None, None),
    };
    Ok((
        lsn,
        (*recorded_at).to_owned(),
        SnapshotRecord {
            table: (*table).to_owned(),
            columns: (*columns).to_owned(),
            max_attnum,
            key,
        },
    ))
}

/// The newest slot record's LSN and, of the tables recorded under it, each one's newest record; `None` without a slot record.
pub(crate) fn newest_snapshot(
    records: Vec<(i64, String, SnapshotRecord)>,
) -> Option<(i64, Vec<SnapshotRecord>)> {
    let newest = records
        .iter()
        .filter(|(_, _, record)| record.table.is_empty())
        .map(|(lsn, _, _)| *lsn)
        .max()?;
    let mut tables: std::collections::BTreeMap<String, (String, SnapshotRecord)> =
        std::collections::BTreeMap::new();
    for (lsn, recorded_at, record) in records {
        if lsn != newest || record.table.is_empty() {
            continue;
        }
        let newer = tables
            .get(&record.table)
            .is_none_or(|(seen, _)| recorded_at >= *seen);
        if newer {
            tables.insert(record.table.clone(), (recorded_at, record));
        }
    }
    Some((
        newest,
        tables.into_values().map(|(_, record)| record).collect(),
    ))
}
