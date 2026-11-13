//! Postgres's SQL: the contract's shared rendering, with Postgres's types, a temporary staging table, `replace` in place and `merge` by `ON CONFLICT`.

use crate::adapters::contract::constants::{BEGIN_STATEMENT, COMMIT_STATEMENT, STAGING_SUFFIX};
use crate::adapters::contract::main::_ranked_source::ranked_source;
use crate::adapters::contract::models::{
    ChangeApply, CursorValue, TableName, WriteStatementsRequest,
};
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::postgres::_helpers::type_names::{logical_type, portable_type, staging_type};
use crate::adapters::postgres::constants::{BLOCK_QUOTE, TEMPORARY_SCHEMA, UNBOUNDED_NUMERIC};
use crate::adapters::postgres::models::PostgresSql;
use crate::column_types::models::{ColumnType, LiveColumnType, TypeFit, Warehouse, WarehouseType};
use crate::schema_plan::models::DesiredColumn;

/// Whether `name` is how an unquoted name is written on the warehouses that fold to upper case (`ORDER_ID`): Postgres stores it folded to lower case, as it would unquoted.
fn is_folded(name: &str) -> bool {
    name.chars()
        .next()
        .is_some_and(|first| first.is_ascii_uppercase() || first == '_')
        && name.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        })
}

impl SqlAdapter for PostgresSql {
    fn merges_files(&self) -> bool {
        true
    }

    fn warehouse(&self) -> Warehouse {
        Warehouse::Postgres
    }

    fn max_identifier_length(&self) -> Option<usize> {
        Some(63)
    }

    /// Not yet: Postgres targets take `postgres` and other sources, and a later release applies captured changes.
    fn captures_changes(&self, _apply: ChangeApply) -> bool {
        false
    }

    fn keeps_tables(&self) -> bool {
        true
    }

    fn keeps_nanoseconds(&self) -> bool {
        false
    }

    fn render_loader_logical_type(&self, column_type: &ColumnType) -> WarehouseType {
        logical_type(column_type)
    }

    fn live_type(&self, live: &LiveColumnType) -> ColumnType {
        portable_type(live).unwrap_or_else(|| ColumnType::Native(live.data_type.clone()))
    }

    /// Also a narrower integer, a shorter `varchar`, or a `numeric` with no fewer digits after the point: Postgres refuses a value those cannot hold rather than rounding it.
    fn assigns_exactly(&self, desired: &ColumnType, live: &ColumnType) -> bool {
        let integer = matches!(desired, ColumnType::SmallInt | ColumnType::Int);
        match (desired, live) {
            _ if self.type_fit(desired, live) == TypeFit::Fits => true,
            (_, ColumnType::SmallInt | ColumnType::Int | ColumnType::Decimal { .. }) if integer => {
                true
            }
            (ColumnType::Text | ColumnType::Varchar(_), ColumnType::Varchar(_)) => true,
            (ColumnType::Decimal { scale: from, .. }, ColumnType::Decimal { scale: to, .. }) => {
                from <= to
            }
            (ColumnType::Decimal { .. }, ColumnType::Native(name)) => name == UNBOUNDED_NUMERIC,
            (_, ColumnType::Native(name)) if integer => name == UNBOUNDED_NUMERIC,
            _ => false,
        }
    }

    /// `ALTER COLUMN ... TYPE` changes any column in place (rewriting it where it must).
    fn widens_in_place(&self, _live: &ColumnType, _target: &ColumnType) -> bool {
        true
    }

    fn stored_name(&self, name: &str) -> String {
        match is_folded(name) {
            true => name.to_ascii_lowercase(),
            false => name.to_owned(),
        }
    }

    /// `schema.table` only: a Postgres session reaches the tables of the database it connected to, which is the target's.
    fn render_qualified_name(&self, table: &TableName) -> String {
        format!(
            "{}.{}",
            self.render_identifier(&table.schema),
            self.render_identifier(&table.table)
        )
    }

    /// `CREATE SCHEMA IF NOT EXISTS` needs `CREATE` on the database even when the schema exists, so the schema is created only when it is missing. A name holding the block's quote tag falls back to the plain statement.
    fn render_create_schema(&self, table: &TableName) -> String {
        let create = format!("CREATE SCHEMA {}", self.render_identifier(&table.schema));
        if table.schema.contains(BLOCK_QUOTE) {
            return format!(
                "CREATE SCHEMA IF NOT EXISTS {}",
                self.render_identifier(&table.schema)
            );
        }
        format!(
            "DO {BLOCK_QUOTE} BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_namespace WHERE nspname = {}) THEN {create}; END IF; END {BLOCK_QUOTE}",
            self.render_loader_value_literal(&self.stored_name(&table.schema)),
        )
    }

    fn render_staging_type(&self, column_type: &ColumnType) -> String {
        staging_type(column_type)
    }

    /// JSON, UUIDs, arrays and native types are converted from their staged text by their type's own input, which refuses what it cannot read; the rest by the table's assignment casts, which refuse a value that does not fit.
    fn render_source_expression_cast(&self, name: &str, column_type: &ColumnType) -> String {
        let column = self.render_identifier(name);
        match column_type {
            ColumnType::Json | ColumnType::Uuid | ColumnType::Native(_) | ColumnType::Array(_) => {
                format!(
                    "CAST({column} AS {})",
                    self.render_loader_logical_type(column_type).sql
                )
            }
            _ => column,
        }
    }

    /// The staging table lives in the session's temporary schema: it needs no privilege on the target's schema, and goes with the session if the load dies.
    fn staging_relation_name(&self, table: &TableName, staging_id: &str) -> TableName {
        let safe_id: String = staging_id
            .chars()
            .filter(|character| character.is_ascii_alphanumeric() || *character == '_')
            .collect();
        TableName {
            database: None,
            schema: TEMPORARY_SCHEMA.to_owned(),
            table: format!("{}{STAGING_SUFFIX}_{safe_id}", table.table),
        }
    }

    fn render_create_staging_table(
        &self,
        table: &TableName,
        columns: &[DesiredColumn],
        staging_id: &str,
    ) -> (TableName, String) {
        let staging = self.staging_relation_name(table, staging_id);
        let definitions: Vec<String> = columns
            .iter()
            .map(|column| {
                format!(
                    "{} {}",
                    self.render_identifier(&column.name),
                    self.render_staging_type(&column.column_type)
                )
            })
            .collect();
        let create = format!(
            "CREATE TEMPORARY TABLE {} ({})",
            self.render_qualified_name(&staging),
            definitions.join(", ")
        );
        (staging, create)
    }

    fn render_alter_column_type(&self, target: &str, column: &str, sql_type: &str) -> String {
        format!(
            "ALTER TABLE {target} ALTER COLUMN {column} TYPE {sql_type} USING {column}::{sql_type}"
        )
    }

    fn render_empty_staging(&self, staging: &TableName) -> String {
        format!("TRUNCATE {}", self.render_qualified_name(staging))
    }

    /// `INSERT ... ON CONFLICT (key) DO UPDATE`, which needs a unique index or constraint on exactly the key; the staged rows are first kept to one per key when the mode dedupes; otherwise a key repeated within them fails the statement, changing nothing.
    fn render_merge(
        &self,
        request: &WriteStatementsRequest<'_>,
        unique_key: &[String],
        source: &str,
        values: &[String],
    ) -> String {
        let ranked = ranked_source(self, request, (source, values));
        let (source, values) = ranked.as_ref().map_or((source, values), |(source, names)| {
            (source.as_str(), names.as_slice())
        });
        let names: Vec<String> = request
            .columns
            .iter()
            .map(|column| self.render_identifier(&column.name))
            .collect();
        let keys: Vec<String> = unique_key
            .iter()
            .map(|key| self.render_identifier(key))
            .collect();
        let updates: Vec<String> = names
            .iter()
            .filter(|name| !keys.contains(name))
            .map(|name| format!("{name} = EXCLUDED.{name}"))
            .collect();
        let action = match updates.is_empty() {
            true => "DO NOTHING".to_owned(),
            false => format!("DO UPDATE SET {}", updates.join(", ")),
        };
        format!(
            "INSERT INTO {} ({}) SELECT {} FROM {source} ON CONFLICT ({}) {action}",
            self.render_qualified_name(request.table),
            names.join(", "),
            values.join(", "),
            keys.join(", ")
        )
    }

    /// The table emptied and refilled from the staged rows in one transaction: it keeps its grants, indexes, defaults, triggers and dependent views, and readers see the old rows until the commit. `DELETE`, not `TRUNCATE`, which a reader holding an older snapshot would see as an empty table.
    fn render_replace(
        &self,
        request: &WriteStatementsRequest<'_>,
        _staging: &TableName,
        insert_from: &str,
    ) -> Vec<String> {
        let target = self.render_qualified_name(request.table);
        vec![
            BEGIN_STATEMENT.to_owned(),
            format!("DELETE FROM {target}"),
            format!("INSERT INTO {target} {insert_from}"),
            COMMIT_STATEMENT.to_owned(),
        ]
    }

    /// The unique index `merge`'s `ON CONFLICT` needs, on a table SQLLoad creates.
    fn render_key_index(&self, table: &TableName, unique_key: &[String]) -> Option<String> {
        let keys: Vec<String> = unique_key
            .iter()
            .map(|key| self.render_identifier(key))
            .collect();
        (!keys.is_empty()).then(|| {
            format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({})",
                self.render_identifier(&format!("{}_sqlload_key", table.table)),
                self.render_qualified_name(table),
                keys.join(", ")
            )
        })
    }

    /// Microseconds from whole seconds and the seconds' microseconds, both exact integers on every Postgres version.
    fn render_cursor_text(&self, column: &str, value: CursorValue) -> String {
        match value {
            CursorValue::EpochMicros { .. } => format!(
                "CAST(CAST(EXTRACT(EPOCH FROM date_trunc('second', MAX({column}))) AS bigint) * 1000000 + CAST(EXTRACT(MICROSECONDS FROM MAX({column})) AS bigint) % 1000000 AS text)"
            ),
            CursorValue::Date => format!("to_char(MAX({column}), 'YYYY-MM-DD')"),
            CursorValue::Plain => format!("CAST(MAX({column}) AS text)"),
        }
    }

    fn render_binary_text(&self, expression: &str) -> String {
        format!("{expression} COLLATE \"C\"")
    }
}
