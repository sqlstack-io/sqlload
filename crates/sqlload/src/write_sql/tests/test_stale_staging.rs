use crate::column_types::models::Warehouse;
use crate::write_sql::main::staging_id::staging_id;
use crate::write_sql::main::staging_table::staging_table;
use crate::write_sql::main::staging_tables_query::staging_tables_query;
use crate::write_sql::main::stale_staging::stale_staging;
use crate::write_sql::tests::helpers::{accounts, columns, ten_oclock};
use crate::write_sql::tests::test_types::{StagingIdTestCase, StaleStagingTestCase};

#[test]
fn given_staging_tables_when_a_load_starts_then_only_those_named_older_than_the_cut_are_dropped()
-> Result<(), String> {
    let test_cases = [
        StaleStagingTestCase {
            description: "an old staging table and its swap table go; a newer one and other tables stay",
            warehouse: Warehouse::DuckDb,
            found: &[
                "app__accounts__sqlload_staging_20261101000000_abcd1234",
                "app__accounts__sqlload_staging_20261101000000_abcd1234_swap",
                "app__accounts__sqlload_staging_20261105090000_ffff0000",
                "app__accounts",
            ],
            expected_statements: &[
                "DROP TABLE IF EXISTS \"raw\".\"app__accounts__sqlload_staging_20261101000000_abcd1234\"",
                "DROP TABLE IF EXISTS \"raw\".\"app__accounts__sqlload_staging_20261101000000_abcd1234_swap\"",
            ],
        },
        StaleStagingTestCase {
            description: "Snowflake's upper-case names read the same",
            warehouse: Warehouse::Snowflake,
            found: &["APP__ACCOUNTS__SQLLOAD_STAGING_20261101000000_ABCD1234"],
            expected_statements: &[
                "DROP TABLE IF EXISTS \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_20261101000000_ABCD1234\"",
            ],
        },
        StaleStagingTestCase {
            description: "a name without a readable time is never guessed at",
            warehouse: Warehouse::DuckDb,
            found: &[
                "app__accounts__sqlload_staging_abcd1234",
                "app__accounts__sqlload_staging_99999999999999_abcd1234",
            ],
            expected_statements: &[],
        },
    ];
    let before = ten_oclock()? - chrono::Duration::hours(24);
    for test_case in test_cases {
        let found: Vec<String> = test_case
            .found
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        assert_eq!(
            stale_staging(&accounts(), &found, before, test_case.warehouse),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_load_when_naming_its_staging_table_then_the_name_holds_its_start_time()
-> Result<(), String> {
    let test_cases = [StagingIdTestCase {
        description: "the time, then the start of the load id",
        load_id: "abcd-1234-5678",
        expected_name: "app__accounts__sqlload_staging_20261105100000_abcd1234",
    }];
    for test_case in test_cases {
        let id = staging_id(test_case.load_id, ten_oclock()?);
        let (staging, _) = staging_table(&accounts(), &columns(), &id, Warehouse::DuckDb);
        assert_eq!(
            staging.table, test_case.expected_name,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_schema_when_listing_its_staging_tables_then_snowflake_names_its_database() {
    let test_cases = [
        StagingIdTestCase {
            description: "DuckDB reads its own catalog",
            load_id: "",
            expected_name: "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw' AND table_name ILIKE '%__sqlload_staging_%'",
        },
        StagingIdTestCase {
            description: "Snowflake names the connection's database",
            load_id: "analytics",
            expected_name: "SELECT table_name FROM \"ANALYTICS\".information_schema.tables WHERE table_schema = 'RAW' AND table_name ILIKE '%__sqlload_staging_%'",
        },
    ];
    for test_case in test_cases {
        let warehouse =
            [Warehouse::DuckDb, Warehouse::Snowflake][usize::from(!test_case.load_id.is_empty())];
        let database = Some(test_case.load_id).filter(|name| !name.is_empty());
        assert_eq!(
            staging_tables_query(&accounts(), database, warehouse),
            test_case.expected_name,
            "{}",
            test_case.description
        );
    }
}
