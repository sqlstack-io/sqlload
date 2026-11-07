//! The `_sqlload_*` columns a captured table carries after its data columns.

use crate::adapters::contract::models::ChangeApply;
use crate::cdc::constants::SNAPSHOT_OP;
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{ReadColumn, WireType};
use crate::schema_plan::constants::{
    CDC_COMMITTED_AT_COLUMN, CDC_DELETED_AT_COLUMN, CDC_LSN_COLUMN, CDC_OP_COLUMN, CDC_SEQ_COLUMN,
    CDC_UNCHANGED_COLUMN,
};

/// The change columns, in the order a change batch carries them, read from the snapshot as constants: every snapshot row has `_sqlload_lsn = snapshot_lsn` and `_sqlload_op = 'snapshot'`.
pub fn metadata_read_columns(apply: ChangeApply, snapshot_lsn: i64) -> Vec<ReadColumn> {
    let column = |name: &str, expression: String, wire: WireType, output: ColumnType| ReadColumn {
        name: name.to_owned(),
        expression,
        wire,
        output,
    };
    let null_timestamp = || "NULL::timestamptz".to_owned();
    let mut columns = vec![
        column(
            CDC_LSN_COLUMN,
            format!("{snapshot_lsn}::int8"),
            WireType::Int8,
            ColumnType::Int,
        ),
        column(
            CDC_OP_COLUMN,
            format!("'{SNAPSHOT_OP}'::text"),
            WireType::Text,
            ColumnType::Text,
        ),
        column(
            CDC_COMMITTED_AT_COLUMN,
            null_timestamp(),
            WireType::Timestamp,
            ColumnType::TimestampTz,
        ),
    ];
    let fourth = match apply {
        ChangeApply::Changelog => Some(column(
            CDC_SEQ_COLUMN,
            "0::int8".to_owned(),
            WireType::Int8,
            ColumnType::Int,
        )),
        ChangeApply::SoftDeletes => Some(column(
            CDC_DELETED_AT_COLUMN,
            null_timestamp(),
            WireType::Timestamp,
            ColumnType::TimestampTz,
        )),
        ChangeApply::HardDeletes => None,
    };
    columns.extend(fourth);
    columns.push(column(
        CDC_UNCHANGED_COLUMN,
        "NULL::text".to_owned(),
        WireType::Text,
        ColumnType::Text,
    ));
    columns
}
