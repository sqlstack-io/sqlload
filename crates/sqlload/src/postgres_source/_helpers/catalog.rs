use std::collections::BTreeMap;

use globset::{Glob, GlobSet, GlobSetBuilder};
use tokio_postgres::Row;

use crate::postgres_source::_helpers::type_map::{TypeDetails, natural_type, pg_type};
use crate::postgres_source::models::{SourceColumn, SourceTable};

/// A matcher for `schema.table` patterns such as `public.*`.
pub(crate) fn table_matcher(patterns: &[String]) -> Result<GlobSet, String> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob = Glob::new(pattern)
            .map_err(|error| format!("invalid table pattern {pattern:?}: {error}"))?;
        builder.add(glob);
    }
    builder.build().map_err(|error| error.to_string())
}

/// Group catalog column rows into tables, keeping those `include` matches and `exclude` does not.
pub(crate) fn tables_from_rows(
    column_rows: &[Row],
    (key_rows, index_rows): (&[Row], &[Row]),
    include: &GlobSet,
    exclude: &GlobSet,
) -> Vec<SourceTable> {
    let by_table = |rows: &[Row]| {
        let mut grouped: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();
        for row in rows {
            grouped
                .entry((row.get(0), row.get(1)))
                .or_default()
                .push(row.get(2));
        }
        grouped
    };
    let keys = by_table(key_rows);
    let indexes = by_table(index_rows);
    let mut tables: BTreeMap<(String, String), SourceTable> = BTreeMap::new();
    for row in column_rows {
        let schema: String = row.get(0);
        let name: String = row.get(1);
        let qualified = format!("{schema}.{name}");
        if !include.is_match(&qualified) || exclude.is_match(&qualified) {
            continue;
        }
        let table = tables
            .entry((schema.clone(), name.clone()))
            .or_insert_with(|| SourceTable {
                primary_key: keys
                    .get(&(schema.clone(), name.clone()))
                    .cloned()
                    .unwrap_or_default(),
                indexed_columns: indexes
                    .get(&(schema.clone(), name.clone()))
                    .cloned()
                    .unwrap_or_default(),
                schema,
                name,
                columns: Vec::new(),
                estimated_rows: row.get(10),
                estimated_bytes: row.get(11),
                comment: row.get(12),
            });
        table.columns.push(column_from_row(row));
    }
    tables.into_values().collect()
}

fn column_from_row(row: &Row) -> SourceColumn {
    let type_name: String = row.get(4);
    let category: String = row.get(8);
    let pg_type = pg_type(&type_name, &category);
    let details = TypeDetails {
        precision: row.get(5),
        scale: row.get(6),
        char_length: row.get(7),
    };
    SourceColumn {
        name: row.get(2),
        postgres_type: row.get(3),
        pg_type,
        column_type: natural_type(pg_type, details),
        nullable: row.get(9),
        source_spelling: None,
    }
}
