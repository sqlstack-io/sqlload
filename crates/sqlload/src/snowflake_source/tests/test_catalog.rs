use crate::column_types::models::ColumnType;
use crate::snowflake_source::_helpers::catalog::{described_columns, listed_tables, table_matcher};
use crate::snowflake_source::constants::SHOW_ROWS_MAX;
use crate::sql_api::models::SqlRows;

fn rows(columns: &[&str], rows: &[&[Option<&str>]]) -> SqlRows {
    SqlRows {
        columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        rows: rows
            .iter()
            .map(|row| row.iter().map(|value| value.map(str::to_owned)).collect())
            .collect(),
    }
}

#[test]
fn given_show_listings_when_listing_tables_then_matching_tables_and_views_come_sorted_without_catalog_views()
 {
    let tables = rows(
        &["name", "schema_name", "kind", "rows", "bytes", "comment"],
        &[
            &[
                Some("ORDERS"),
                Some("MARTS"),
                Some("TABLE"),
                Some("120"),
                Some("4096"),
                Some("Order lines"),
            ],
            &[
                Some("EVENTS"),
                Some("MARTS"),
                Some("TABLE"),
                Some("7"),
                Some("512"),
                Some(" "),
            ],
            &[
                Some("SCRATCH"),
                Some("MARTS"),
                Some("TABLE"),
                Some("1"),
                Some("1"),
                None,
            ],
            &[
                Some("USERS"),
                Some("RAW"),
                Some("TABLE"),
                Some("3"),
                Some("64"),
                None,
            ],
        ],
    );
    let views = rows(
        &["name", "schema_name", "comment", "is_materialized"],
        &[
            &[Some("DAILY"), Some("MARTS"), Some("Per day"), Some("false")],
            &[
                Some("TABLES"),
                Some("INFORMATION_SCHEMA"),
                Some("The tables"),
                Some("false"),
            ],
        ],
    );
    let include = table_matcher(&["marts.*".to_owned(), "*.tables".to_owned()]).unwrap();
    let exclude = table_matcher(&["MARTS.SCRATCH".to_owned()]).unwrap();
    let listed = listed_tables(&[tables, views], &include, &exclude).unwrap();
    let summary: Vec<(String, i64, i64, Option<String>)> = listed
        .into_iter()
        .map(|table| {
            (
                format!("{}.{}", table.schema, table.name),
                table.estimated_rows,
                table.estimated_bytes,
                table.comment,
            )
        })
        .collect();
    assert_eq!(
        summary,
        vec![
            ("MARTS.DAILY".to_owned(), -1, -1, Some("Per day".to_owned())),
            ("MARTS.EVENTS".to_owned(), 7, 512, None),
            (
                "MARTS.ORDERS".to_owned(),
                120,
                4096,
                Some("Order lines".to_owned())
            ),
        ]
    );
}

#[test]
fn given_a_listing_at_the_show_row_cap_when_listing_tables_then_it_is_not_trusted() {
    let name = Some("T");
    let row: &[Option<&str>] = &[name, Some("S")];
    let capped = rows(&["name", "schema_name"], &vec![row; SHOW_ROWS_MAX]);
    let include = table_matcher(&["*".to_owned()]).unwrap();
    let exclude = table_matcher(&[]).unwrap();
    assert!(listed_tables(&[capped], &include, &exclude).is_none());
}

#[test]
fn given_a_described_table_when_reading_columns_then_order_nullability_and_types_follow_describe() {
    let described = rows(
        &["name", "type", "kind", "null?"],
        &[
            &[Some("ID"), Some("NUMBER(18,0)"), Some("COLUMN"), Some("N")],
            &[
                Some("CODE"),
                Some("VARCHAR(10) COLLATE 'en-ci'"),
                Some("COLUMN"),
                Some("Y"),
            ],
        ],
    );
    let columns: Vec<(String, bool, Result<ColumnType, String>)> = described_columns(&described)
        .into_iter()
        .map(|column| (column.name, column.nullable, column.column_type))
        .collect();
    assert_eq!(
        columns,
        vec![
            ("id".to_owned(), false, Ok(ColumnType::Int)),
            ("code".to_owned(), true, Ok(ColumnType::Varchar(10))),
        ]
    );
}
