use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use sqlload::postgres_source::main::copy_table::copy_table;
use sqlload::postgres_source::main::plan_read::plan_read;
use sqlload::postgres_source::main::read_catalog::read_catalog;
use sqlload::postgres_source::models::CopyRequest;
use sqlload_testkit::postgres::main::connect::connect;
use sqlload_testkit::postgres::main::start_postgres::start_postgres;

use crate::helpers::{column_values, run_async};
use crate::test_types::{PostgresColumnTestCase, SplitReadTestCase};

const SPLIT_ROWS: i64 = 70_000;

const SPLIT_SETUP: &str = "
CREATE TABLE public.padded (id int8 NOT NULL, pad text NOT NULL);
ALTER TABLE public.padded ALTER COLUMN pad SET STORAGE PLAIN;
INSERT INTO public.padded SELECT i, repeat(md5(i::text), 59) FROM generate_series(1, 70000) AS s(i);
";

const ACTIVE_COPIES: &str = "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE 'COPY (SELECT%padded%'";

const SETUP: &str = "
CREATE TYPE mood AS ENUM ('calm', 'busy');
CREATE TABLE public.type_matrix (
    id int8 PRIMARY KEY,
    flag bool,
    small int2,
    medium int4,
    ratio float4,
    amount numeric(38,10),
    price numeric(10,2),
    label varchar(20),
    note text,
    day date,
    at_time time,
    local_at timestamp,
    instant timestamptz,
    payload jsonb,
    tags text[],
    ident uuid,
    raw bytea,
    feeling mood,
    span interval
);
INSERT INTO public.type_matrix VALUES
  (1, true, -32768, 2147483647, 1.5, 1234567890123456789012345678.0123456789, 12.34, 'café', 'line one',
   '2026-11-04', '10:12:03.123456', '2026-11-04 10:12:03.123456', '2026-11-04 10:12:03.123456+02',
   '{\"b\": [1, 2], \"a\": null}', ARRAY['x', 'y'], 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\\xdeadbeef', 'calm', '1 day 02:00:00'),
  (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (3, false, 7, -1, -0.25, -0.0000000001, -0.5, '', '', '1970-01-01', '00:00:00', '1999-12-31 23:59:59', '2000-01-01 00:00:00+00',
   '[]', ARRAY[]::text[], '00000000-0000-0000-0000-000000000000', '\\x', 'busy', '-3 months');
";

#[test]
fn given_a_postgres_table_of_every_type_when_copied_then_each_value_arrives_exactly()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        client
            .batch_execute(SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let tables = read_catalog(&client, &["public.*".to_owned()], &[])
            .await
            .map_err(|error| error.to_string())?;
        let table = tables.first().ok_or("no table found")?;
        let columns = plan_read(table, &BTreeMap::new()).map_err(|problems| problems.join("; "))?;
        let mut batches: Vec<RecordBatch> = Vec::new();
        let request = CopyRequest {
            table,
            columns,
            filter: None,
            order_by: None,
            batch_rows: 2,
            batch_bytes: usize::MAX,
            helpers: &[],
        };
        let rows = copy_table(&client, request, async |batch| {
            batches.push(batch);
            Ok(())
        })
        .await
        .map_err(|error| error.to_string())?;
        assert_eq!(rows, 3);
        assert_eq!(
            batches.len(),
            2,
            "batch_rows = 2 splits three rows in two batches"
        );
        assert_eq!(table.primary_key, ["id"]);
        let test_cases = [
            PostgresColumnTestCase {
                description: "int8 key",
                column: "id",
                expected_type: "int",
                expected_values: &["1", "2", "3"],
            },
            PostgresColumnTestCase {
                description: "bool",
                column: "flag",
                expected_type: "bool",
                expected_values: &["true", "", "false"],
            },
            PostgresColumnTestCase {
                description: "int2 at its minimum",
                column: "small",
                expected_type: "smallint",
                expected_values: &["-32768", "", "7"],
            },
            PostgresColumnTestCase {
                description: "int4 at its maximum widens to int",
                column: "medium",
                expected_type: "int",
                expected_values: &["2147483647", "", "-1"],
            },
            PostgresColumnTestCase {
                description: "float4 widens to float",
                column: "ratio",
                expected_type: "float",
                expected_values: &["1.5", "", "-0.25"],
            },
            PostgresColumnTestCase {
                description: "a 38-digit numeric keeps every digit",
                column: "amount",
                expected_type: "decimal(38,10)",
                expected_values: &[
                    "1234567890123456789012345678.0123456789",
                    "",
                    "-0.0000000001",
                ],
            },
            PostgresColumnTestCase {
                description: "a money-like numeric",
                column: "price",
                expected_type: "decimal(10,2)",
                expected_values: &["12.34", "", "-0.50"],
            },
            PostgresColumnTestCase {
                description: "varchar keeps its length and UTF-8",
                column: "label",
                expected_type: "varchar(20)",
                expected_values: &["café", "", ""],
            },
            PostgresColumnTestCase {
                description: "date, including the Unix epoch",
                column: "day",
                expected_type: "date",
                expected_values: &["2026-11-04", "", "1970-01-01"],
            },
            PostgresColumnTestCase {
                description: "time with microseconds",
                column: "at_time",
                expected_type: "time",
                expected_values: &["10:12:03.123456", "", "00:00:00"],
            },
            PostgresColumnTestCase {
                description: "timestamp without a zone, before the Postgres epoch",
                column: "local_at",
                expected_type: "timestamp_ntz",
                expected_values: &["2026-11-04T10:12:03.123456", "", "1999-12-31T23:59:59"],
            },
            PostgresColumnTestCase {
                description: "timestamptz arrives as the UTC instant",
                column: "instant",
                expected_type: "timestamp_tz",
                expected_values: &["2026-11-04T08:12:03.123456Z", "", "2000-01-01T00:00:00Z"],
            },
            PostgresColumnTestCase {
                description: "jsonb arrives as JSON text",
                column: "payload",
                expected_type: "json",
                expected_values: &["{\"a\": null, \"b\": [1, 2]}", "", "[]"],
            },
            PostgresColumnTestCase {
                description: "an array arrives as a JSON array",
                column: "tags",
                expected_type: "json",
                expected_values: &["[\"x\", \"y\"]", "", "[]"],
            },
            PostgresColumnTestCase {
                description: "uuid arrives hyphenated",
                column: "ident",
                expected_type: "uuid",
                expected_values: &[
                    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
                    "",
                    "00000000-0000-0000-0000-000000000000",
                ],
            },
            PostgresColumnTestCase {
                description: "bytea, including empty",
                column: "raw",
                expected_type: "binary",
                expected_values: &["deadbeef", "", ""],
            },
            PostgresColumnTestCase {
                description: "an enum arrives as text",
                column: "feeling",
                expected_type: "text",
                expected_values: &["calm", "", "busy"],
            },
            PostgresColumnTestCase {
                description: "an interval arrives as Postgres text",
                column: "span",
                expected_type: "text",
                expected_values: &["1 day 02:00:00", "", "-3 mons"],
            },
        ];
        for test_case in test_cases {
            let source = table
                .columns
                .iter()
                .find(|column| column.name == test_case.column)
                .ok_or_else(|| format!("{}: no column", test_case.description))?;
            let natural = source
                .column_type
                .as_ref()
                .map(ToString::to_string)
                .map_err(Clone::clone)?;
            assert_eq!(
                natural, test_case.expected_type,
                "{}",
                test_case.description
            );
            assert_eq!(
                column_values(&batches, test_case.column)?,
                test_case.expected_values,
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}

#[test]
fn given_a_table_over_the_split_size_when_copied_then_each_reader_takes_a_range_and_every_row_arrives_once()
-> Result<(), String> {
    run_async(async {
        let postgres = start_postgres().await?;
        let client = connect(&postgres).await?;
        let watcher = connect(&postgres).await?;
        client
            .batch_execute(SPLIT_SETUP)
            .await
            .map_err(|error| error.to_string())?;
        let tables = read_catalog(&client, &["public.padded".to_owned()], &[])
            .await
            .map_err(|error| error.to_string())?;
        let table = tables.first().ok_or("no table found")?;
        let test_cases = [
            SplitReadTestCase {
                description: "a helper splits the read in two ranges",
                helpers: 1,
                filter: None,
                expected_rows: SPLIT_ROWS,
                expected_sum: SPLIT_ROWS * (SPLIT_ROWS + 1) / 2,
                expected_copies: 2,
            },
            SplitReadTestCase {
                description: "a filter narrows each range",
                helpers: 1,
                filter: Some("id % 2 = 0"),
                expected_rows: SPLIT_ROWS / 2,
                expected_sum: SPLIT_ROWS / 2 * (SPLIT_ROWS / 2 + 1),
                expected_copies: 2,
            },
            SplitReadTestCase {
                description: "no helper reads in one pass",
                helpers: 0,
                filter: None,
                expected_rows: SPLIT_ROWS,
                expected_sum: SPLIT_ROWS * (SPLIT_ROWS + 1) / 2,
                expected_copies: 1,
            },
        ];
        for test_case in test_cases {
            let mut helpers = Vec::new();
            for _ in 0..test_case.helpers {
                helpers.push(connect(&postgres).await?);
            }
            let request = CopyRequest {
                table,
                columns: plan_read(table, &BTreeMap::new())
                    .map_err(|problems| problems.join("; "))?,
                filter: test_case.filter.map(str::to_owned),
                order_by: None,
                batch_rows: 1_000,
                batch_bytes: usize::MAX,
                helpers: &helpers,
            };
            let mut ids: Vec<String> = Vec::new();
            let mut copies: Vec<i64> = Vec::new();
            let rows = copy_table(&client, request, async |batch| {
                let row = watcher
                    .query_one(ACTIVE_COPIES, &[])
                    .await
                    .map_err(|error| error.to_string())?;
                copies.push(row.get(0));
                ids.extend(column_values(std::slice::from_ref(&batch), "id")?);
                Ok(())
            })
            .await
            .map_err(|error| error.to_string())?;
            let distinct: std::collections::BTreeSet<&String> = ids.iter().collect();
            let sum: i64 = ids
                .iter()
                .map(|id| id.parse::<i64>().map_err(|error| error.to_string()))
                .sum::<Result<i64, String>>()?;
            assert_eq!(
                rows, test_case.expected_rows as u64,
                "{}",
                test_case.description
            );
            assert_eq!(
                ids.len(),
                test_case.expected_rows as usize,
                "{}",
                test_case.description
            );
            assert_eq!(
                distinct.len(),
                test_case.expected_rows as usize,
                "{}",
                test_case.description
            );
            assert_eq!(sum, test_case.expected_sum, "{}", test_case.description);
            assert_eq!(
                copies.first(),
                Some(&test_case.expected_copies),
                "{}",
                test_case.description
            );
        }
        Ok(())
    })
}
