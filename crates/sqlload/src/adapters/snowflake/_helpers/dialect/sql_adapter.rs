//! Snowflake's SQL, where it differs from the contract's shared rendering.

use arrow_array::RecordBatch;
use chrono::NaiveDateTime;

use crate::adapters::contract::constants::{STAGING_SUFFIX, STALE_STAGING_HOURS};
use crate::adapters::contract::main::_session_parquet::default_session_parquet;
use crate::adapters::contract::models::{
    CursorValue, SessionRows, SessionStage, TableName, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::snowflake::_helpers::dialect::type_names::{
    logical_type, portable_type, widens_in_place,
};
use crate::adapters::snowflake::_helpers::write::table_writer::lists_as_json;
use crate::adapters::snowflake::constants::{
    MERGE_KEYWORD, RUN_LOG_FALLBACK_RETENTION_DAYS, RUN_LOG_RETENTION_DAYS, STAGE_FOLDER,
};
use crate::adapters::snowflake::main::copy::_copy_staged::copy_staged;
use crate::adapters::snowflake::models::{CopyStaged, SnowflakeSql};
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};
use crate::loader::models::TableLayout;
use crate::schema_plan::models::{DesiredColumn, LiveColumn};

/// Whether `name` is lower snake_case, which Snowflake stores upper-cased like an unquoted name.
fn is_plain(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|first| first.is_ascii_lowercase() || first == '_')
        && name.chars().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}

impl SqlAdapter for SnowflakeSql {
    fn warehouse(&self) -> Warehouse {
        Warehouse::Snowflake
    }

    fn bills_while_running(&self) -> bool {
        true
    }

    /// The mark statements of `deletes` and `deletes_from` ran on a Snowflake warehouse through every step of the container tests' key scans and tombstones, soft and hard.
    fn marks_deleted_rows(&self) -> bool {
        true
    }

    fn max_identifier_length(&self) -> Option<usize> {
        Some(255)
    }

    /// The table's database's `INFORMATION_SCHEMA`, which need not be the session's.
    fn render_schema_columns(&self, table: &TableName) -> String {
        let catalog = table
            .database
            .as_deref()
            .map(|name| format!("{}.", self.render_identifier(name)))
            .unwrap_or_default();
        format!("{catalog}information_schema.columns")
    }

    fn builds_like_target(&self) -> bool {
        true
    }

    fn render_loader_logical_type(&self, column_type: &ColumnType) -> WarehouseType {
        logical_type(column_type)
    }

    /// Snowflake doesn't order semi-structured VARIANT, OBJECT and ARRAY values consistently across their types (https://docs.snowflake.com/en/sql-reference/constructs/order-by); structured ARRAY(...), OBJECT(...) and MAP(...) compare (https://docs.snowflake.com/en/sql-reference/data-types-structured).
    fn orders_native(&self, family: &str, parameterised: bool) -> bool {
        !(matches!(family, "VARIANT") || (matches!(family, "OBJECT" | "ARRAY") && !parameterised))
    }

    fn live_type(&self, live: &LiveColumnType) -> ColumnType {
        portable_type(live).unwrap_or_else(|| ColumnType::Native(live.data_type.clone()))
    }

    fn widens_in_place(&self, live: &ColumnType, target: &ColumnType) -> bool {
        widens_in_place(live, target)
    }

    fn stored_name(&self, name: &str) -> String {
        match is_plain(name) {
            true => name.to_ascii_uppercase(),
            false => name.to_owned(),
        }
    }

    fn is_stored_as(&self, live: &str, stored: &str) -> bool {
        live == stored
    }

    fn render_loader_value_literal(&self, text: &str) -> String {
        format!("'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
    }

    fn render_read_words(&self) -> (&'static str, &'static str) {
        ("COPYs", "COPY")
    }

    fn render_display_name(&self, table: &TableName) -> String {
        format!("{}.{}", table.schema, table.table).to_ascii_uppercase()
    }

    /// JSON and arrays are staged as their JSON text, UUIDs as text.
    fn render_staging_type(&self, column_type: &ColumnType) -> String {
        match column_type {
            ColumnType::Json | ColumnType::Uuid | ColumnType::Array(_) => {
                self.render_loader_logical_type(&ColumnType::Text).sql
            }
            other => self.render_loader_logical_type(other).sql,
        }
    }

    fn render_source_expression_cast(&self, name: &str, column_type: &ColumnType) -> String {
        let column = self.render_identifier(name);
        match column_type {
            ColumnType::Json => format!("PARSE_JSON({column})"),
            ColumnType::Array(_) => format!("CAST(PARSE_JSON({column}) AS ARRAY)"),
            ColumnType::Uuid => format!(
                "CAST({column} AS {})",
                self.render_loader_logical_type(column_type).sql
            ),
            _ => column,
        }
    }

    fn staging_table_kind(&self) -> &'static str {
        "TRANSIENT TABLE"
    }

    fn render_create_build_table(
        &self,
        build: &TableName,
        table: &TableName,
        (columns, differing, _): (&[DesiredColumn], Option<&[LiveColumn]>, Option<&str>),
    ) -> Vec<String> {
        let name = self.render_qualified_name(build);
        let sql_type = |column_type: &ColumnType| self.render_loader_logical_type(column_type).sql;
        let mut statements = vec![format!(
            "CREATE OR REPLACE TABLE {name} LIKE {} COPY GRANTS",
            self.render_qualified_name(table)
        )];
        let stored = |column: &DesiredColumn| self.stored_name(&column.name);
        let same = |known: &LiveColumn, column: &DesiredColumn| {
            sql_type(&known.column_type) == sql_type(&column.column_type)
        };
        let live = differing.unwrap_or_default();
        for known in live {
            let kept = columns.iter().find(|column| stored(column) == known.name);
            if kept.is_none_or(|column| !same(known, column)) {
                statements.push(format!(
                    "ALTER TABLE {name} DROP COLUMN {}",
                    self.render_exact_identifier(&known.name)
                ));
            }
        }
        for column in columns.iter().filter(|_| differing.is_some()) {
            let known = live.iter().find(|known| known.name == stored(column));
            if known.is_none_or(|known| !same(known, column)) {
                statements.push(format!(
                    "ALTER TABLE {name} ADD COLUMN {} {}",
                    self.render_identifier(&column.name),
                    sql_type(&column.column_type)
                ));
            }
        }
        statements
    }

    fn render_alter_column_type(&self, target: &str, column: &str, sql_type: &str) -> String {
        format!("ALTER TABLE {target} ALTER COLUMN {column} SET DATA TYPE {sql_type}")
    }

    fn widens_staging_by_rebuild(&self) -> bool {
        true
    }

    fn render_swap(
        &self,
        target: &TableName,
        build: &TableName,
        _indexes: &[String],
        _layout: &TableLayout,
    ) -> Vec<String> {
        let (old, new) = (
            self.render_qualified_name(target),
            self.render_qualified_name(build),
        );
        vec![
            format!("ALTER TABLE {old} SWAP WITH {new}"),
            format!("DROP TABLE IF EXISTS {new}"),
        ]
    }

    fn render_replace_table_from_relation(
        &self,
        request: &WriteStatementsRequest<'_>,
        source: &str,
        values: &[String],
    ) -> Vec<String> {
        let definitions: Vec<String> = request
            .columns
            .iter()
            .map(|column| {
                format!(
                    "{} {}",
                    self.render_identifier(&column.name),
                    self.render_loader_logical_type(&column.column_type).sql
                )
            })
            .collect();
        vec![format!(
            "CREATE OR REPLACE TABLE {} ({}) COPY GRANTS AS SELECT {} FROM {source}",
            self.render_qualified_name(request.table),
            definitions.join(", "),
            values.join(", ")
        )]
    }

    fn commits_alone(&self, statement: &str) -> bool {
        !statement.starts_with(MERGE_KEYWORD)
    }

    fn render_cursor_text(&self, column: &str, value: CursorValue) -> String {
        match value {
            CursorValue::EpochMicros { .. } => {
                format!("DATE_PART(epoch_microsecond, MAX({column}))")
            }
            CursorValue::Date => format!("TO_VARCHAR(MAX({column}), 'YYYY-MM-DD')"),
            CursorValue::Plain => format!("TO_VARCHAR(MAX({column}))"),
        }
    }

    fn render_binary_text(&self, expression: &str) -> String {
        format!("COLLATE({expression}, 'utf8')")
    }

    fn render_staging_filter(&self, suffix: &str) -> String {
        format!(
            "CONTAINS(table_name, {}) AND last_altered < DATEADD(hour, -{STALE_STAGING_HOURS}, CURRENT_TIMESTAMP())",
            self.render_loader_value_literal(&self.stored_name(suffix)),
        )
    }

    fn is_stale_staging(&self, name: &str, _before: NaiveDateTime) -> bool {
        name.to_ascii_lowercase()
            .contains(&format!("{STAGING_SUFFIX}_"))
    }

    fn render_create_run_log_table_sql(
        &self,
        runs: &TableName,
        columns: &[DesiredColumn],
    ) -> (String, Option<String>) {
        let create = self.render_create_table(runs, columns);
        (
            format!("{create} DATA_RETENTION_TIME_IN_DAYS = {RUN_LOG_RETENTION_DAYS}"),
            Some(format!(
                "{create} DATA_RETENTION_TIME_IN_DAYS = {RUN_LOG_FALLBACK_RETENTION_DAYS}"
            )),
        )
    }

    /// In UTC, as SQLLoad's own Snowflake sessions are, so `TIMESTAMP_TZ` values are stored at `+00:00`.
    fn render_session_setup(&self) -> Vec<String> {
        vec!["ALTER SESSION SET TIMEZONE = 'UTC'".to_owned()]
    }

    fn render_autocommit_check(&self) -> Option<String> {
        Some("SHOW PARAMETERS LIKE 'AUTOCOMMIT' IN SESSION".to_owned())
    }

    /// A caller's session uploads each batch's Parquet files to the user's stage with `PUT`, then `COPY`s them in together.
    fn session_rows(&self) -> SessionRows {
        SessionRows::StagedParquet
    }

    /// From `database`'s `INFORMATION_SCHEMA` (the table's own database first), which need not be the session's.
    fn render_describe_table(&self, table: &TableName, database: Option<&str>) -> Option<String> {
        let database = table.database.as_deref().or(database)?;
        let literal = |text: &str| self.render_loader_value_literal(text);
        Some(format!(
            "SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE, CHARACTER_MAXIMUM_LENGTH, DATETIME_PRECISION FROM \"{}\".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = {} AND TABLE_NAME = {} ORDER BY ORDINAL_POSITION",
            self.stored_name(database).replace('"', "\"\""),
            literal(&self.stored_name(&table.schema)),
            literal(&self.stored_name(&table.table)),
        ))
    }

    /// Lists and arrays as JSON text, which the `COPY` parses, as Snowflake's own writer stages them.
    fn session_parquet(
        &self,
        batch: &RecordBatch,
        columns: &[DesiredColumn],
    ) -> Result<Vec<u8>, String> {
        let batch = lists_as_json(batch.clone())?;
        default_session_parquet(&batch, columns)
    }

    /// `PUT` into, `COPY` from and `REMOVE` of `@~/sqlload/<load id>/<batch>/`, the folder Snowflake's own writer stages a batch in.
    fn render_session_stage(&self, stage: SessionStage<'_>) -> Vec<String> {
        let folder = |load_id: &str, batch: u64| format!("{STAGE_FOLDER}/{load_id}/{batch:05}/");
        match stage {
            SessionStage::Put {
                load_id,
                batch,
                file,
            } => vec![format!(
                "PUT {} @~/{} AUTO_COMPRESS = FALSE OVERWRITE = TRUE",
                self.render_loader_value_literal(&format!("file://{file}")),
                folder(load_id, batch)
            )],
            SessionStage::Copy {
                load_id,
                batch,
                table,
                names,
                selected,
            } => copy_staged(&CopyStaged {
                table,
                stage: "@~",
                folder: &folder(load_id, batch),
                names,
                selected,
            }),
            SessionStage::Remove { load_id, batch } => {
                vec![format!("REMOVE @~/{}", folder(load_id, batch))]
            }
        }
    }
}
