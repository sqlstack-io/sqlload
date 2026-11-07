//! Where each captured table's snapshot came from, kept in the destination beside the tables: a slot's snapshot point when SQLLoad created it, and each table once its snapshot is in, so a snapshot that stopped part-way is never mistaken for a finished one.

use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::dispatch::main::session::execute::execute;
use crate::adapters::dispatch::main::session::text_values::text_values;
use crate::adapters::dispatch::models::Destination;
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

    /// Record that `table_name` (empty: the slot itself) holds the snapshot taken at `lsn`, copied with the data `columns` (`,a,b,`).
    pub(crate) async fn record(
        &self,
        table_name: &str,
        lsn: i64,
        columns: &str,
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
                text(table_name),
                text(columns),
                text(&now_utc_text()),
            )],
        )
        .await
    }

    /// The tables holding the slot's newest snapshot with the data columns each was copied with, and that snapshot's LSN; `None` when SQLLoad never recorded creating the slot.
    pub(crate) async fn current(
        &self,
    ) -> Result<Option<(i64, Vec<(String, String)>)>, LoaderError> {
        let adapter = sql_adapter(self.warehouse);
        let column = |name: &str| adapter.render_identifier(name);
        let text_type = adapter.render_loader_logical_type(&ColumnType::Text).sql;
        let separator = adapter.render_loader_value_literal(&FIELD_SEPARATOR.to_string());
        let rows = text_values(
            self.destination,
            format!(
                "SELECT CAST({} AS {text_type}) || {separator} || {} || {separator} || {} FROM {} WHERE {} = {} AND {} = {}",
                column("snapshot_lsn"),
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
        let mut records: Vec<(i64, String, String)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut fields = row.splitn(3, FIELD_SEPARATOR);
            let unreadable =
                || LoaderError::Invalid(format!("an unreadable snapshot record: {row:?}"));
            let (Some(lsn), Some(table), Some(columns)) =
                (fields.next(), fields.next(), fields.next())
            else {
                return Err(unreadable());
            };
            let lsn = lsn.parse::<i64>().map_err(|_| unreadable())?;
            records.push((lsn, table.to_owned(), columns.to_owned()));
        }
        let newest = records
            .iter()
            .filter(|(_, table, _)| table.is_empty())
            .map(|(lsn, _, _)| *lsn)
            .max();
        let Some(newest) = newest else {
            return Ok(None);
        };
        let tables: Vec<(String, String)> = records
            .into_iter()
            .filter(|(lsn, table, _)| *lsn == newest && !table.is_empty())
            .map(|(_, table, columns)| (table, columns))
            .collect();
        Ok(Some((newest, tables)))
    }
}
