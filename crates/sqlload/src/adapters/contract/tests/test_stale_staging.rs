use crate::adapters::contract::main::staging_id::staging_id;
use crate::adapters::contract::tests::helpers::{accounts, columns, ten_oclock};
use crate::adapters::contract::tests::test_types::{StagingIdTestCase, StaleStagingTestCase};
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;

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
            description: "Snowflake's query chose by last_altered, so the name's time does not matter; a table without the suffix still never goes",
            warehouse: Warehouse::Snowflake,
            found: &[
                "APP__ACCOUNTS__SQLLOAD_STAGING_20261105095959_ABCD1234",
                "APP__ACCOUNTS__SQLLOAD_STAGING_ABCD1234",
                "APP__ACCOUNTSXXSQLLOADXSTAGINGX",
            ],
            expected_statements: &[
                "DROP TABLE IF EXISTS \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_20261105095959_ABCD1234\"",
                "DROP TABLE IF EXISTS \"RAW\".\"APP__ACCOUNTS__SQLLOAD_STAGING_ABCD1234\"",
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
            sql_adapter(test_case.warehouse).render_stale_staging(&accounts(), &found, before),
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
        let (staging, _) = sql_adapter(Warehouse::DuckDb).render_create_staging_table(
            &accounts(),
            &columns(),
            &id,
        );
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
            description: "Snowflake names the connection's database and chooses by last_altered, with no wildcards",
            load_id: "analytics",
            expected_name: "SELECT table_name FROM \"ANALYTICS\".information_schema.tables WHERE table_schema = 'RAW' AND CONTAINS(table_name, '__SQLLOAD_STAGING_') AND last_altered < DATEADD(hour, -24, CURRENT_TIMESTAMP())",
        },
    ];
    for test_case in test_cases {
        let warehouse =
            [Warehouse::DuckDb, Warehouse::Snowflake][usize::from(!test_case.load_id.is_empty())];
        let database = Some(test_case.load_id).filter(|name| !name.is_empty());
        assert_eq!(
            sql_adapter(warehouse).list_staging_relations_sql(&accounts(), database),
            test_case.expected_name,
            "{}",
            test_case.description
        );
    }
}
