use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::loader::_helpers::python::inferred_table;
use crate::loader::tests::test_types::InferredTableTestCase;
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};
use crate::schema_plan::models::LiveColumn;

/// The columns of the inferred table, as `name:type` joined by commas.
pub(crate) fn inferred_columns(test_case: &InferredTableTestCase) -> String {
    let planned = SourceTable {
        schema: String::new(),
        name: "t".to_owned(),
        columns: test_case
            .declared
            .iter()
            .map(|name| SourceColumn {
                name: (*name).to_owned(),
                postgres_type: "python".to_owned(),
                pg_type: PgType::Other,
                column_type: Ok(crate::column_types::models::ColumnType::Text),
                nullable: true,
            })
            .collect(),
        primary_key: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
    };
    let live: Vec<LiveColumn> = test_case
        .live
        .iter()
        .map(|(name, column_type)| LiveColumn {
            name: (*name).to_owned(),
            column_type: column_type.clone(),
        })
        .collect();
    let live = Some(live.as_slice()).filter(|live| !live.is_empty());
    inferred_table(&planned, &(test_case.schema)(), live)
        .columns
        .iter()
        .map(|column| {
            format!(
                "{}:{}",
                column.name,
                column
                    .column_type
                    .as_ref()
                    .map_or_else(Clone::clone, ToString::to_string)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// A crossing's schema: an int, a float, text, a UTC timestamp, a JSON column, an all-NULL column and a metadata column.
pub(crate) fn crossed_schema() -> Schema {
    let json = HashMap::from([(ARROW_TYPE_METADATA_KEY.to_owned(), "json".to_owned())]);
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("code", DataType::Utf8, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("meta", DataType::Utf8, true).with_metadata(json),
        Field::new("_sqlload_load_id", DataType::Utf8, true),
    ])
}

/// A schema of one integer column, `code`.
pub(crate) fn integer_code() -> Schema {
    Schema::new(vec![Field::new("code", DataType::Int64, true)])
}
