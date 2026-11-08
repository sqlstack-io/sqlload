//! Tables as the driver lists them, and their columns as a prepared `SELECT` describes them.

use arrow_odbc::arrow_schema_from;
use arrow_odbc::odbc_api::Connection;
use arrow_schema::{Field, Schema};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

use crate::column_types::main::portable_type::portable_type;
use crate::odbc_source::errors::OdbcSourceError;
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};

/// One glob set for `schema.table` patterns, matched ignoring case as most drivers fold names.
pub(crate) fn table_matcher(patterns: &[String]) -> Result<GlobSet, OdbcSourceError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = GlobBuilder::new(pattern)
            .case_insensitive(true)
            .build()
            .map_err(|error| {
                OdbcSourceError::Invalid(format!("invalid table pattern {pattern:?}: {error}"))
            })?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|error| OdbcSourceError::Invalid(error.to_string()))
}

/// The columns of `SELECT * FROM <from>`, as the driver describes the prepared statement; nothing is run.
pub(crate) fn described(
    connection: &Connection<'_>,
    from: &str,
) -> Result<Vec<SourceColumn>, OdbcSourceError> {
    let dbms: Option<String> = connection.database_management_system_name().ok();
    let mut prepared = connection.prepare(&format!("SELECT * FROM {from}"))?;
    let schema: Schema = arrow_schema_from(&mut prepared, dbms.as_deref(), false)?;
    Ok(schema
        .fields()
        .iter()
        .map(|field| source_column(field))
        .collect())
}

/// A described column: its Arrow type shown as its source type, and the portable type that holds it.
fn source_column(field: &Field) -> SourceColumn {
    SourceColumn {
        name: field.name().clone(),
        postgres_type: field.data_type().to_string(),
        pg_type: PgType::Other,
        column_type: portable_type(field),
        nullable: field.is_nullable(),
        source_spelling: None,
    }
}

/// A source table of `columns`, its key as the driver reports it.
pub(crate) fn source_table(
    (schema, name): (String, String),
    columns: Vec<SourceColumn>,
    primary_key: Vec<String>,
) -> SourceTable {
    SourceTable {
        schema,
        name,
        columns,
        primary_key,
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
        comment: None,
    }
}
