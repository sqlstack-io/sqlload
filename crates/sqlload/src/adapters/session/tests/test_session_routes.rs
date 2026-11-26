use crate::adapters::contract::models::TableName;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::adapters::session::tests::helpers::{
    autocommit_refusal, caller_parse_problem, reference_checks, written_batch,
};
use crate::adapters::session::tests::test_types::{
    AttachedNameTestCase, AutocommitTestCase, DescribeQueryTestCase, DescribedTypeTestCase,
    ReferenceCheckTestCase, SessionRouteTestCase,
};
use crate::column_types::models::Warehouse;

#[test]
fn given_a_session_on_each_warehouse_when_a_batch_loads_then_its_rows_reach_the_staging_table_by_the_warehouses_route()
 {
    let test_cases = [
        SessionRouteTestCase {
            description: "Snowflake checks autocommit, runs in UTC, puts the file on the user stage, copies it into staging and removes it",
            warehouse: Warehouse::Snowflake,
            expected_calls: &[
                "SHOW PARAMETERS LIKE 'AUTOCOMMIT' IN SESSION",
                "ALTER SESSION SET TIMEZONE = 'UTC'",
                "PUT 'file://<folder>/part-00001.parquet' @~/sqlload/load1/00001/ AUTO_COMPRESS = FALSE OVERWRITE = TRUE",
                "COPY INTO \"ANALYTICS\".\"RAW\".\"ORDERS__SQLLOAD_STAGING_1\" FROM @~/sqlload/load1/00001/ FILES = ('part-00001.parquet')",
                "REMOVE @~/sqlload/load1/00001/",
            ],
        },
        SessionRouteTestCase {
            description: "Postgres copies the rows in as binary COPY data",
            warehouse: Warehouse::Postgres,
            expected_calls: &[
                "COPY \"raw\".\"orders__sqlload_staging_1\" (\"id\", \"name\") FROM STDIN (FORMAT binary) <- PGCOPY",
            ],
        },
        SessionRouteTestCase {
            description: "BigQuery's session loads the Parquet file itself",
            warehouse: Warehouse::BigQuery,
            expected_calls: &["load_parquet raw.orders__sqlload_staging_1 exists=true"],
        },
        SessionRouteTestCase {
            description: "ClickHouse's session loads the Parquet file itself",
            warehouse: Warehouse::ClickHouse,
            expected_calls: &["load_parquet raw.orders__sqlload_staging_1 exists=true"],
        },
        SessionRouteTestCase {
            description: "Databricks' session loads the Parquet file itself",
            warehouse: Warehouse::Databricks,
            expected_calls: &["load_parquet raw.orders__sqlload_staging_1 exists=true"],
        },
    ];
    for test_case in test_cases {
        let calls = written_batch(test_case.warehouse)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        let starts: Vec<bool> = calls
            .iter()
            .zip(test_case.expected_calls)
            .map(|(call, expected)| call.starts_with(expected))
            .collect();
        assert_eq!(
            (calls.len(), starts.iter().all(|starts| *starts)),
            (test_case.expected_calls.len(), true),
            "{}: {calls:?}",
            test_case.description
        );
    }
}

#[test]
fn given_a_table_when_a_callers_session_describes_it_then_the_query_names_it_with_literals() {
    let test_cases = [
        DescribeQueryTestCase {
            description: "DuckDB fills both placeholders of its describe query",
            warehouse: Warehouse::DuckDb,
            schema: "raw",
            table: "it's",
            expected_parts: &["table_schema = 'raw'", "table_name = 'it''s'"],
        },
        DescribeQueryTestCase {
            description: "Postgres fills $1 and $2 with the stored names",
            warehouse: Warehouse::Postgres,
            schema: "Raw",
            table: "orders",
            expected_parts: &["n.nspname = 'Raw'", "c.relname = 'orders'"],
        },
        DescribeQueryTestCase {
            description: "BigQuery reads the dataset's INFORMATION_SCHEMA in the session's project",
            warehouse: Warehouse::BigQuery,
            schema: "raw",
            table: "orders",
            expected_parts: &[
                "`analytics`.`raw`.INFORMATION_SCHEMA.COLUMNS",
                "table_name = 'orders'",
            ],
        },
    ];
    for test_case in test_cases {
        let table = TableName {
            database: None,
            schema: test_case.schema.to_owned(),
            table: test_case.table.to_owned(),
        };
        let query = sql_adapter(test_case.warehouse)
            .render_describe_table(&table, Some("analytics"))
            .unwrap_or_else(|| panic!("{}: no query", test_case.description));
        assert!(
            test_case
                .expected_parts
                .iter()
                .all(|part| query.contains(part)),
            "{}: {query}",
            test_case.description
        );
    }
}

#[test]
fn given_a_bigquery_column_type_as_information_schema_writes_it_when_read_then_its_sizes_come_apart()
 {
    let test_cases = [
        DescribedTypeTestCase {
            description: "a sized NUMERIC keeps its precision and scale",
            written: "NUMERIC(10, 2)",
            expected_type: "Decimal { precision: 10, scale: 2 }",
        },
        DescribedTypeTestCase {
            description: "a sized STRING is a varchar",
            written: "STRING(20)",
            expected_type: "Varchar(20)",
        },
        DescribedTypeTestCase {
            description: "an unsized STRING is text",
            written: "STRING",
            expected_type: "Text",
        },
        DescribedTypeTestCase {
            description: "an array of integers is an array",
            written: "ARRAY<INT64>",
            expected_type: "Array(Int)",
        },
        DescribedTypeTestCase {
            description: "a record reads as a struct, as tables.get gives it",
            written: "STRUCT<id INT64, name STRING>",
            expected_type: "Native(\"STRUCT\")",
        },
    ];
    for test_case in test_cases {
        let rows = vec![vec![
            Some("c".to_owned()),
            Some(test_case.written.to_owned()),
        ]];
        let columns = sql_adapter(Warehouse::BigQuery)
            .describe_text_rows(&rows)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        assert_eq!(
            columns
                .first()
                .map(|column| format!("{:?}", column.column_type)),
            Some(test_case.expected_type.to_owned()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_callers_connection_attached_to_a_project_when_it_parses_then_targets_use_it_by_name() {
    let test_cases = [
        AttachedNameTestCase {
            description: "a target loads into the attached connection as into any DuckDB connection",
            project: "[targets.dev]\nconnection = \"caller\"\nloader_schema = \"raw\"\n",
            expected_problem: None,
        },
        AttachedNameTestCase {
            description: "a project connection of the same name is refused",
            project: "[connections.caller]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n",
            expected_problem: Some(
                "[connections.caller] is also a connection the caller attaches; give one of them another name",
            ),
        },
        AttachedNameTestCase {
            description: "a target on it is checked as a warehouse target",
            project: "[targets.dev]\nconnection = \"caller\"\n",
            expected_problem: Some("[targets.dev] needs a loader_schema"),
        },
    ];
    for test_case in test_cases {
        let problem = caller_parse_problem(test_case.project);
        assert_eq!(
            problem.is_some(),
            test_case.expected_problem.is_some(),
            "{}: {problem:?}",
            test_case.description
        );
        assert!(
            problem
                .unwrap_or_default()
                .contains(test_case.expected_problem.unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_callers_session_with_autocommit_off_when_it_opens_then_sqlload_refuses_it() {
    let test_cases = [
        AutocommitTestCase {
            description: "a Snowflake session with autocommit off is refused",
            warehouse: Warehouse::Snowflake,
            autocommit: Some("false"),
            expected_refusal: Some("the caller's Snowflake session has autocommit off"),
        },
        AutocommitTestCase {
            description: "a Snowflake session with autocommit on opens",
            warehouse: Warehouse::Snowflake,
            autocommit: Some("true"),
            expected_refusal: None,
        },
        AutocommitTestCase {
            description: "a Postgres session is not asked, as the server does not hold it",
            warehouse: Warehouse::Postgres,
            autocommit: Some("false"),
            expected_refusal: None,
        },
    ];
    for test_case in test_cases {
        let refusal = autocommit_refusal(test_case.warehouse, test_case.autocommit)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        assert_eq!(
            refusal.is_some(),
            test_case.expected_refusal.is_some(),
            "{}: {refusal:?}",
            test_case.description
        );
        assert!(
            refusal
                .unwrap_or_default()
                .contains(test_case.expected_refusal.unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_foreign_key_referencing_a_postgres_table_when_a_write_through_a_callers_session_deletes_rows_then_it_is_checked_as_on_sqlloads_own_session()
 {
    let test_cases = [
        ReferenceCheckTestCase {
            description: "ON DELETE CASCADE would change another table, so replace is refused",
            reference: ("c", false, false),
            replace: true,
            expected_problem: Some("ON DELETE CASCADE deletes its rows when replace deletes"),
            expected_warning: None,
        },
        ReferenceCheckTestCase {
            description: "a restricting key is warned of, as delete_insert fails while it holds rows",
            reference: ("r", false, false),
            replace: false,
            expected_problem: None,
            expected_warning: Some(
                "so delete_insert fails while any of its rows reference a row it deletes",
            ),
        },
        ReferenceCheckTestCase {
            description: "replace ignores the table's reference to itself",
            reference: ("c", false, true),
            replace: true,
            expected_problem: None,
            expected_warning: None,
        },
    ];
    for test_case in test_cases {
        let (problems, warnings) = reference_checks(test_case.reference, test_case.replace)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        let found = |lines: &[String], part: Option<&str>| {
            part.map_or(lines.is_empty(), |part| {
                lines.len() == 1 && lines[0].contains(part)
            })
        };
        assert!(
            found(&problems, test_case.expected_problem)
                && found(&warnings, test_case.expected_warning),
            "{}: {problems:?} {warnings:?}",
            test_case.description
        );
    }
}
