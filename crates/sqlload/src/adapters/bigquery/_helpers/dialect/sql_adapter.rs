//! BigQuery GoogleSQL, where it differs from the contract's shared rendering.

use crate::adapters::bigquery::_helpers::dialect::type_names::{
    decimal_digits, logical_type, portable_type, widens_in_place,
};
use crate::adapters::bigquery::_helpers::dialect::write::write_statements;
use crate::adapters::bigquery::models::BigQuerySql;
use crate::adapters::contract::constants::STAGING_SUFFIX;
use crate::adapters::contract::models::{
    ChangeApply, CursorValue, TableName, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::column_types::models::{ColumnType, LiveColumnType, Warehouse, WarehouseType};

impl SqlAdapter for BigQuerySql {
    fn warehouse(&self) -> Warehouse {
        Warehouse::BigQuery
    }

    fn max_identifier_length(&self) -> Option<usize> {
        Some(300)
    }

    /// The mark statements of `deletes` and `deletes_from` ran on BigQuery through every step of the container tests' key scans and tombstones, soft and hard.
    fn marks_deleted_rows(&self) -> bool {
        true
    }

    /// Not yet: a later release applies changes to BigQuery once a live round has run them.
    fn captures_changes(&self, _apply: ChangeApply) -> bool {
        false
    }

    fn keeps_nanoseconds(&self) -> bool {
        false
    }

    fn render_loader_logical_type(&self, column_type: &ColumnType) -> WarehouseType {
        logical_type(column_type)
    }

    /// The Write API refuses arrays of zoned timestamps.
    fn type_problem(&self, column_type: &ColumnType) -> Option<&'static str> {
        (*column_type == ColumnType::Array(Box::new(ColumnType::TimestampTz))).then_some(
            "BigQuery cannot load array(timestamp_tz) yet, as its write API refuses arrays of zoned timestamps; pin array(timestamp_ntz), which holds UTC times, or json",
        )
    }

    /// GoogleSQL can't order ARRAY, STRUCT, GEOGRAPHY, JSON or graph values: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#orderable_data_types
    fn orders_native(&self, family: &str, _parameterised: bool) -> bool {
        !matches!(
            family,
            "ARRAY" | "STRUCT" | "RECORD" | "GEOGRAPHY" | "JSON" | "GRAPH_ELEMENT" | "GRAPH_PATH"
        )
    }

    fn live_type(&self, live: &LiveColumnType) -> ColumnType {
        portable_type(live).unwrap_or_else(|| ColumnType::Native(live.data_type.clone()))
    }

    fn widens_in_place(&self, live: &ColumnType, target: &ColumnType) -> bool {
        widens_in_place(live, target)
    }

    /// Backticks, with `\` and `` ` `` escaped by a backslash, as GoogleSQL reads quoted identifiers.
    fn render_exact_identifier(&self, name: &str) -> String {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }

    fn render_identifier(&self, name: &str) -> String {
        self.render_exact_identifier(&self.stored_name(name))
    }

    /// Backslash escapes: GoogleSQL has no `''` escape inside a string literal, and a quoted string holds no raw line break.
    fn render_loader_value_literal(&self, text: &str) -> String {
        format!(
            "'{}'",
            text.replace('\\', "\\\\")
                .replace('\'', "\\'")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
        )
    }

    /// A decimal bound as a `BIGNUMERIC`, which `NUMERIC` and `BIGNUMERIC` columns compare with exactly; a bare decimal is a `FLOAT64` literal, which rounds.
    fn render_cursor_number(&self, value: &str, column_type: &ColumnType) -> String {
        match column_type {
            ColumnType::Decimal { .. } => format!("CAST('{value}' AS BIGNUMERIC)"),
            _ => value.to_owned(),
        }
    }

    /// JSON is parsed from its staged text; a UUID is stored as the text it arrived as.
    fn render_source_expression_cast(&self, name: &str, column_type: &ColumnType) -> String {
        let column = self.render_identifier(name);
        match column_type {
            ColumnType::Json => format!("PARSE_JSON({column})"),
            _ => column,
        }
    }

    /// BigQuery has no `COMMENT ON`: a table's description is one of its options.
    fn render_table_comment(&self, table: &TableName, description: &str) -> Option<String> {
        (!description.is_empty()).then(|| {
            format!(
                "ALTER TABLE {} SET OPTIONS (description = {})",
                self.render_qualified_name(table),
                self.render_loader_value_literal(description)
            )
        })
    }

    fn render_alter_column_type(&self, target: &str, column: &str, sql_type: &str) -> String {
        format!("ALTER TABLE {target} ALTER COLUMN {column} SET DATA TYPE {sql_type}")
    }

    /// A decimal is cast to `NUMERIC` or `BIGNUMERIC` without parameters, which every parameterized column of the same digits takes: a column reads back as `decimal(p,s)` whether it was made `NUMERIC(p,s)` or `BIGNUMERIC(p,s)`, it is declared again as the one that fits those digits, and BigQuery won't put a `BIGNUMERIC` value into a `NUMERIC` column. A cast can't name the parameters.
    fn render_typed_rebuild_column(&self, name: &str, column_type: &ColumnType) -> String {
        match column_type {
            ColumnType::Decimal { precision, scale } => {
                let base = match decimal_digits(*precision, *scale).2 {
                    true => "BIGNUMERIC",
                    false => "NUMERIC",
                };
                format!("CAST({name} AS {base}) AS {name}")
            }
            _ => name.to_owned(),
        }
    }

    /// The columns are listed with their types: a query's result types carry no precision, scale or length, so without the list every `NUMERIC(20, 2)` and `STRING(10)` of the table would come back unparameterized. A table with a column whose type can't be named again, such as a `STRUCT`, gets no list.
    fn render_rebuild_table(&self, target: &str, (select, columns): (&str, &str)) -> String {
        match columns.is_empty() {
            true => format!("CREATE OR REPLACE TABLE {target} AS SELECT {select} FROM {target}"),
            false => format!(
                "CREATE OR REPLACE TABLE {target} ({columns}) AS SELECT {select} FROM {target}"
            ),
        }
    }

    /// BigQuery's `DELETE` needs a `WHERE`.
    fn render_empty_staging(&self, staging: &TableName) -> String {
        format!(
            "DELETE FROM {} WHERE TRUE",
            self.render_qualified_name(staging)
        )
    }

    /// BigQuery has no rename into place: the table is replaced by the build table's rows in one statement, so the rows are copied once. `LIKE` the build table, as a query's result types carry no precision, scale or length (`NUMERIC(20, 2)` would come back as `NUMERIC`); a query rather than a table copy, which leaves out rows the Write API committed moments ago. `LIKE` also takes the build table's options, so a build table must never carry an expiration, or the table it replaces would expire with it. The build table carries a load's own name, so no write stream ever meets a table recreated under the same name.
    fn render_swap(
        &self,
        target: &TableName,
        build: &TableName,
        _indexes: &[String],
    ) -> Vec<String> {
        let (old, new) = (
            self.render_qualified_name(target),
            self.render_qualified_name(build),
        );
        vec![
            format!("CREATE OR REPLACE TABLE {old} LIKE {new} AS SELECT * FROM {new}"),
            format!("DROP TABLE IF EXISTS {new}"),
        ]
    }

    fn render_write(
        &self,
        request: &WriteStatementsRequest<'_>,
        staging: &TableName,
    ) -> Vec<String> {
        write_statements(self, request, staging)
    }

    fn commits_alone(&self, _statement: &str) -> bool {
        true
    }

    fn render_text_cast(&self, expression: &str) -> String {
        format!("CAST({expression} AS STRING)")
    }

    /// A key column that may be NULL compared as `=` or both NULL: BigQuery runs a join on `IS NOT DISTINCT FROM` alone as a cross join, and refuses it in a full outer join, but still hashes on the `=` branch of this form.
    fn render_key_equality(&self, (left, right): (&str, &str), nullable: bool) -> String {
        match nullable {
            true => format!("({left} = {right} OR ({left} IS NULL AND {right} IS NULL))"),
            false => format!("{left} = {right}"),
        }
    }

    /// BigQuery refuses FLOAT64 and JSON in `PARTITION BY`, so those keys are partitioned by their text: 17 significant digits tell any two doubles apart, which `CAST` to STRING does not promise, adding `0.0` first turns `-0.0` into `0.0`, as `MERGE ... ON` compares them equal, and each NaN key gets a partition of its own, as no NaN matches another there.
    fn render_partition_key(&self, value: &str, column_type: &ColumnType) -> String {
        match column_type {
            ColumnType::Float => {
                format!("IF(IS_NAN({value}), GENERATE_UUID(), FORMAT('%.17g', {value} + 0.0))")
            }
            ColumnType::Json => format!("TO_JSON_STRING({value})"),
            _ => value.to_owned(),
        }
    }

    fn render_cursor_text(&self, column: &str, value: CursorValue) -> String {
        match value {
            CursorValue::EpochMicros { zoned: true } => {
                format!("CAST(UNIX_MICROS(MAX({column})) AS STRING)")
            }
            CursorValue::EpochMicros { zoned: false } => {
                format!("CAST(UNIX_MICROS(TIMESTAMP(MAX({column}), 'UTC')) AS STRING)")
            }
            CursorValue::Date => format!("FORMAT_DATE('%F', MAX({column}))"),
            CursorValue::Plain => format!("CAST(MAX({column}) AS STRING)"),
        }
    }

    /// BigQuery has no `ILIKE`.
    fn render_staging_filter(&self, suffix: &str) -> String {
        format!(
            "LOWER(table_name) LIKE {}",
            self.render_loader_value_literal(&format!("%{}%", suffix.to_ascii_lowercase()))
        )
    }

    /// The dataset's own `INFORMATION_SCHEMA`, qualified by its project.
    fn render_schema_columns(&self, table: &TableName) -> String {
        let project = table
            .database
            .as_deref()
            .map(|name| format!("{}.", self.render_identifier(name)))
            .unwrap_or_default();
        format!(
            "{project}{}.INFORMATION_SCHEMA.COLUMNS",
            self.render_identifier(&table.schema)
        )
    }

    /// A dataset's own `INFORMATION_SCHEMA`, qualified by its project.
    fn list_staging_relations_sql(&self, table: &TableName, database: Option<&str>) -> String {
        let project = table
            .database
            .as_deref()
            .or(database)
            .map(|name| format!("{}.", self.render_identifier(name)))
            .unwrap_or_default();
        format!(
            "SELECT table_name FROM {project}{}.INFORMATION_SCHEMA.TABLES WHERE {}",
            self.render_identifier(&table.schema),
            self.render_staging_filter(&format!("{STAGING_SUFFIX}_"))
        )
    }
}
