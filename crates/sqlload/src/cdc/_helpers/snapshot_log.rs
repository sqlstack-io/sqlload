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
        let columns = [
            ("source", &text),
            ("slot", &text),
            ("table_name", &text),
            ("snapshot_lsn", &integer),
            ("recorded_at", &text),
        ]
        .map(|(name, sql_type)| format!("{} {sql_type}", adapter.render_identifier(name)));
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

    /// Record that `table_name` (empty: the slot itself) holds the snapshot taken at `lsn`.
    pub(crate) async fn record(&self, table_name: &str, lsn: i64) -> Result<(), LoaderError> {
        let adapter = sql_adapter(self.warehouse);
        let text = |value: &str| adapter.render_loader_value_literal(value);
        let names = [
            "source",
            "slot",
            "table_name",
            "snapshot_lsn",
            "recorded_at",
        ]
        .map(|name| adapter.render_identifier(name));
        execute(
            self.destination,
            vec![format!(
                "INSERT INTO {} ({}) VALUES ({}, {}, {}, {lsn}, {})",
                adapter.render_qualified_name(&self.table()),
                names.join(", "),
                text(self.source),
                text(self.slot),
                text(table_name),
                text(&now_utc_text()),
            )],
        )
        .await
    }

    /// The tables holding the slot's newest snapshot, and that snapshot's LSN; `None` when SQLLoad never recorded creating the slot.
    pub(crate) async fn current(&self) -> Result<Option<(i64, Vec<String>)>, LoaderError> {
        let adapter = sql_adapter(self.warehouse);
        let column = |name: &str| adapter.render_identifier(name);
        let text_type = adapter.render_loader_logical_type(&ColumnType::Text).sql;
        let rows = text_values(
            self.destination,
            format!(
                "SELECT CAST({} AS {text_type}) || ' ' || {} FROM {} WHERE {} = {} AND {} = {}",
                column("snapshot_lsn"),
                column("table_name"),
                adapter.render_qualified_name(&self.table()),
                column("source"),
                adapter.render_loader_value_literal(self.source),
                column("slot"),
                adapter.render_loader_value_literal(self.slot),
            ),
        )
        .await?;
        let mut records: Vec<(i64, String)> = Vec::with_capacity(rows.len());
        for row in &rows {
            let (lsn, table) = row.split_once(' ').unwrap_or((row.as_str(), ""));
            let lsn = lsn.parse::<i64>().map_err(|_| {
                LoaderError::Invalid(format!("an unreadable snapshot record: {row:?}"))
            })?;
            records.push((lsn, table.to_owned()));
        }
        let newest = records
            .iter()
            .filter(|(_, table)| table.is_empty())
            .map(|(lsn, _)| *lsn)
            .max();
        let Some(newest) = newest else {
            return Ok(None);
        };
        let tables: Vec<String> = records
            .into_iter()
            .filter(|(lsn, table)| *lsn == newest && !table.is_empty())
            .map(|(_, table)| table)
            .collect();
        Ok(Some((newest, tables)))
    }
}
