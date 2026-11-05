//! Load a load's staged Parquet from the external stage.

use crate::column_types::models::Warehouse;
use crate::external_stage::constants::{
    COPY_OPTIONS, MAX_FILES_PER_COPY, PARQUET_COPY_FORMAT, STAGED_FOLDER,
};
use crate::write_sql::main::qualified_name::qualified_name;
use crate::write_sql::main::sql_literal::sql_literal;
use crate::write_sql::models::TableName;

/// `COPY` statements loading parts `1..=parts` of load `load_id` from `stage` into `staging` by column name, at most 1,000 files each.
pub fn staged_copies(staging: &TableName, stage: &str, load_id: &str, parts: usize) -> Vec<String> {
    let names: Vec<String> = (1..=parts)
        .map(|part| sql_literal(&format!("part-{part:05}.parquet"), Warehouse::Snowflake))
        .collect();
    names
        .chunks(MAX_FILES_PER_COPY)
        .map(|chunk| {
            format!(
                "COPY INTO {} FROM {stage}/{STAGED_FOLDER}{load_id}/ FILES = ({}) {PARQUET_COPY_FORMAT} MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE {COPY_OPTIONS}",
                qualified_name(staging, Warehouse::Snowflake),
                chunk.join(", ")
            )
        })
        .collect()
}
