use crate::copy::tests::helpers::{PROJECT, outcome};
use crate::copy::tests::test_types::CopyProjectTestCase;

#[test]
fn given_copy_endpoints_when_building_the_project_then_secrets_stay_out_of_its_text() {
    let test_cases = [
        CopyProjectTestCase {
            description: "a literal URI password becomes a reference with its value kept aside, and a warning",
            source: "postgresql://app:p%40ss@db:6543/shop#public.accounts",
            destination: "duckdb://wh.duckdb#RAW.ACCOUNTS",
            project: "",
            expected_lines: &[
                "password = \"${ENV:SQLLOAD_COPY_SOURCE_PASSWORD}\"",
                "include = [\"public.accounts\"]",
                "name = \"ACCOUNTS\"",
                "loader_schema = \"RAW\"",
                "SQLLOAD_COPY_SOURCE_PASSWORD=p@ss",
                "postgresql://app:***@db:6543/shop#public.accounts has a literal password; use ${ENV:NAME} in the URI so it stays out of shell history",
            ],
        },
        CopyProjectTestCase {
            description: "an ${ENV} password in a URI stays a reference, with no warning",
            source: "postgresql://app:${ENV:PG}@db/shop#public.accounts",
            destination: "duckdb://wh.duckdb#RAW.ACCOUNTS",
            project: "",
            expected_lines: &[
                "password = \"${ENV:PG}\"",
                "include = [\"public.accounts\"]",
                "name = \"ACCOUNTS\"",
                "loader_schema = \"RAW\"",
            ],
        },
        CopyProjectTestCase {
            description: "a named connection is copied as written, and an s3 folder target holds the source's table name",
            source: "app:public.orders",
            destination: "s3://lake/raw/?region=eu-west-2",
            project: PROJECT,
            expected_lines: &[
                "password = \"${ENV:APP_PASSWORD}\"",
                "bucket = \"lake\"",
                "include = [\"public.orders\"]",
                "name = \"orders\"",
                "path = \"raw/{table}/\"",
            ],
        },
        CopyProjectTestCase {
            description: "a local folder source is a files source named by its folder",
            source: "./exports/orders",
            destination: "duckdb://wh.duckdb#RAW.ORDERS",
            project: "",
            expected_lines: &[
                "root = \"./exports/orders\"",
                "path = \"\"",
                "table_name = \"ORDERS\"",
                "loader_schema = \"RAW\"",
            ],
        },
        CopyProjectTestCase {
            description: "a connection sqlload.toml lacks is refused with the ones it has",
            source: "warehouse:public.orders",
            destination: "duckdb://wh.duckdb#RAW.ORDERS",
            project: PROJECT,
            expected_lines: &[
                "the source warehouse:public.orders names connection warehouse, which is not in sqlload.toml (it has: app); name one of those, or give a URI such as postgresql://user@host/db#schema.table",
            ],
        },
        CopyProjectTestCase {
            description: "a warehouse destination without SCHEMA.TABLE is refused",
            source: "app:public.orders",
            destination: "duckdb://wh.duckdb",
            project: PROJECT,
            expected_lines: &[
                "the destination duckdb://wh.duckdb names no table; end it with SCHEMA.TABLE",
            ],
        },
        CopyProjectTestCase {
            description: "a DuckDB source is refused rather than read some other way",
            source: "duckdb://wh.duckdb#main.orders",
            destination: "./out",
            project: "",
            expected_lines: &[
                "the source duckdb://wh.duckdb#main.orders is a duckdb connection; copy reads Postgres tables and files (s3://, file://, a folder)",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            outcome(test_case.source, test_case.destination, test_case.project),
            test_case.expected_lines,
            "{}",
            test_case.description
        );
    }
}
