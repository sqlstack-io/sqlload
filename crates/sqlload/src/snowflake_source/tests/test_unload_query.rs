use std::collections::BTreeMap;

use crate::column_types::models::ColumnType;
use crate::snowflake_source::_helpers::unload_query::unload_query;
use crate::snowflake_source::main::read_columns::read_columns;
use crate::snowflake_source::models::UnloadRequest;
use crate::snowflake_source::tests::helpers::events;
use crate::snowflake_source::tests::test_types::UnloadQueryTestCase;

#[test]
fn given_a_table_or_query_when_unloading_then_each_column_is_selected_by_its_snowflake_spelling_under_its_loaded_name()
 {
    let test_cases = [
        UnloadQueryTestCase {
            description: "a table is read by name, timestamps as microseconds, filtered",
            sql: None,
            filter: Some("\"SEEN_AT\" >= '2026-11-01'"),
            pinned: &[],
            expected_query: Ok(
                "SELECT \"EVENT_ID\"::NUMBER(19,0) AS \"event_id\", \"MixedCase\" AS \"MixedCase\", \
DATE_PART(EPOCH_MICROSECOND, \"SEEN_AT\")::NUMBER(18,0) AS \"seen_at\" \
FROM \"MARTS\".\"EVENTS\" WHERE \"SEEN_AT\" >= '2026-11-01'",
            ),
        },
        UnloadQueryTestCase {
            description: "a query is the FROM, without its semicolon, closed after a trailing comment",
            sql: Some("  SELECT * FROM MARTS.EVENTS -- all of it\n;"),
            filter: None,
            pinned: &[("seen_at", ColumnType::Date)],
            expected_query: Ok(
                "SELECT \"EVENT_ID\"::NUMBER(19,0) AS \"event_id\", \"MixedCase\" AS \"MixedCase\", \
CONVERT_TIMEZONE('UTC', \"SEEN_AT\")::DATE AS \"seen_at\" \
FROM (SELECT * FROM MARTS.EVENTS -- all of it\n)",
            ),
        },
        UnloadQueryTestCase {
            description: "a pin the column cannot take names the column",
            sql: None,
            filter: None,
            pinned: &[("MixedCase", ColumnType::TimestampTz)],
            expected_query: Err(vec![
                "MixedCase has type VARCHAR(10), which cannot be read as timestamp_tz",
            ]),
        },
    ];
    let table = events();
    for test_case in test_cases {
        let pinned: BTreeMap<String, ColumnType> = test_case
            .pinned
            .iter()
            .map(|(name, pinned)| ((*name).to_owned(), pinned.clone()))
            .collect();
        let query = read_columns(&table, &pinned).and_then(|columns| {
            unload_query(&UnloadRequest {
                table: &table,
                sql: test_case.sql,
                columns: &columns,
                filter: test_case.filter,
                batch_rows: 1,
            })
            .map_err(|error| vec![error])
        });
        assert_eq!(
            query,
            test_case
                .expected_query
                .map(str::to_owned)
                .map_err(|problems| problems.into_iter().map(str::to_owned).collect()),
            "{}",
            test_case.description
        );
    }
}
