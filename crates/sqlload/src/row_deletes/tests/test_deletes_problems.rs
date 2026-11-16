use crate::column_types::models::Warehouse;
use crate::project::models::{Deletes, SourceKind, WriteStrategy};
use crate::row_deletes::main::deletes_problems::deletes_problems;
use crate::row_deletes::tests::test_types::DeletesProblemsTestCase;

#[test]
fn given_a_tables_deletes_when_validating_it_then_only_merges_of_listed_keys_into_targets_that_ran_the_marks_pass()
 {
    let test_cases = [
        DeletesProblemsTestCase {
            description: "a merge of a database table into DuckDB takes either kind",
            deletes: Some(Deletes::Hard),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Odbc,
            warehouse: Warehouse::DuckDb,
            motherduck: false,
            unnests: false,
            expected_problems: &[],
        },
        DeletesProblemsTestCase {
            description: "a captured table's deletes are its changes' to apply",
            deletes: Some(Deletes::Hard),
            strategy: WriteStrategy::Append,
            kind: SourceKind::PostgresCdc,
            warehouse: Warehouse::ClickHouse,
            motherduck: false,
            unnests: true,
            expected_problems: &[],
        },
        DeletesProblemsTestCase {
            description: "an append has no key, a files source lists no keys, ClickHouse mutates later, and child rows would stay",
            deletes: Some(Deletes::Soft),
            strategy: WriteStrategy::Append,
            kind: SourceKind::Files,
            warehouse: Warehouse::ClickHouse,
            motherduck: false,
            unnests: true,
            expected_problems: &[
                "deletes marks the rows whose unique_key the source no longer holds, which needs write_strategy = \"merge\"; append keeps no key to match. Fix: remove deletes, or merge on a unique_key",
                "deletes reads every key of the source table to find the ones it no longer holds, which only database and warehouse sources list; a files source can't. Fix: remove deletes",
                "deletes marks rows of the table in place with statements SQLLoad has run only on DuckDB and Postgres so far, so it refuses them on ClickHouse rather than run them untested. Fix: remove deletes, or load into DuckDB or Postgres",
                "deletes would leave the child tables' rows of each row it removes or marks. Fix: drop unnest, or remove deletes",
            ],
        },
        DeletesProblemsTestCase {
            description: "files targets change no rows in place",
            deletes: Some(Deletes::Soft),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Postgres,
            warehouse: Warehouse::Parquet,
            motherduck: false,
            unnests: false,
            expected_problems: &[
                "deletes marks rows of the table in place with statements SQLLoad has run only on DuckDB and Postgres so far, so it refuses them on Parquet files rather than run them untested. Fix: remove deletes, or load into DuckDB or Postgres",
            ],
        },
        DeletesProblemsTestCase {
            description: "Snowflake hasn't run the mark statements yet, so plan refuses them",
            deletes: Some(Deletes::Hard),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Snowflake,
            warehouse: Warehouse::Snowflake,
            motherduck: false,
            unnests: false,
            expected_problems: &[
                "deletes marks rows of the table in place with statements SQLLoad has run only on DuckDB and Postgres so far, so it refuses them on Snowflake rather than run them untested. Fix: remove deletes, or load into DuckDB or Postgres",
            ],
        },
        DeletesProblemsTestCase {
            description: "Databricks hasn't run the mark statements yet, so plan refuses them",
            deletes: Some(Deletes::Hard),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Snowflake,
            warehouse: Warehouse::Databricks,
            motherduck: false,
            unnests: false,
            expected_problems: &[
                "deletes marks rows of the table in place with statements SQLLoad has run only on DuckDB and Postgres so far, so it refuses them on Databricks rather than run them untested. Fix: remove deletes, or load into DuckDB or Postgres",
            ],
        },
        DeletesProblemsTestCase {
            description: "BigQuery hasn't run the mark statements yet, so plan refuses them",
            deletes: Some(Deletes::Hard),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Snowflake,
            warehouse: Warehouse::BigQuery,
            motherduck: false,
            unnests: false,
            expected_problems: &[
                "deletes marks rows of the table in place with statements SQLLoad has run only on DuckDB and Postgres so far, so it refuses them on BigQuery rather than run them untested. Fix: remove deletes, or load into DuckDB or Postgres",
            ],
        },
        DeletesProblemsTestCase {
            description: "MotherDuck runs statements itself and hasn't run them",
            deletes: Some(Deletes::Soft),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Postgres,
            warehouse: Warehouse::DuckDb,
            motherduck: true,
            unnests: false,
            expected_problems: &[
                "deletes marks rows of the table in place with statements SQLLoad has run only on DuckDB and Postgres so far, so it refuses them on MotherDuck rather than run them untested. Fix: remove deletes, or load into DuckDB or Postgres",
            ],
        },
        DeletesProblemsTestCase {
            description: "Postgres has run them",
            deletes: Some(Deletes::Soft),
            strategy: WriteStrategy::Merge,
            kind: SourceKind::Postgres,
            warehouse: Warehouse::Postgres,
            motherduck: false,
            unnests: false,
            expected_problems: &[],
        },
    ];
    for test_case in test_cases {
        let problems = deletes_problems(
            test_case.deletes,
            (test_case.strategy, test_case.kind),
            (test_case.warehouse, test_case.motherduck),
            test_case.unnests,
        );
        assert_eq!(
            problems, test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
}
