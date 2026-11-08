use crate::adapters::contract::models::CheckQuery;
use crate::adapters::contract::tests::helpers::accounts;
use crate::adapters::contract::tests::test_types::CheckQueryTestCase;
use crate::adapters::dispatch::main::dialect::sql_adapter::sql_adapter;
use crate::column_types::models::Warehouse;

#[test]
fn given_a_check_when_rendering_its_query_then_it_reads_text_in_each_dialect() {
    let key = ["id".to_owned(), "Region".to_owned()];
    let test_cases = [
        CheckQueryTestCase {
            description: "the row count is cast to text in the shared SQL",
            warehouse: Warehouse::Snowflake,
            check: CheckQuery::Rows,
            expected_query: "SELECT CAST(COUNT(*) AS VARCHAR) FROM \"RAW\".\"APP__ACCOUNTS\"",
        },
        CheckQueryTestCase {
            description: "duplicates count the key values more than one row holds, skipping NULL keys, with the dialect's quoting and case",
            warehouse: Warehouse::DuckDb,
            check: CheckQuery::Duplicates(&key),
            expected_query: "SELECT CAST(COUNT(*) AS VARCHAR) FROM (SELECT 1 AS repeated FROM \"raw\".\"app__accounts\" WHERE \"id\" IS NOT NULL AND \"Region\" IS NOT NULL GROUP BY \"id\", \"Region\" HAVING COUNT(*) > 1) repeated_keys",
        },
        CheckQueryTestCase {
            description: "examples join each key column's text, in a stable order, at most five, with BigQuery's STRING cast",
            warehouse: Warehouse::BigQuery,
            check: CheckQuery::DuplicateExamples(&key),
            expected_query: "SELECT CAST(`id` AS STRING) || ', ' || CAST(`Region` AS STRING) AS example FROM `raw`.`app__accounts` WHERE `id` IS NOT NULL AND `Region` IS NOT NULL GROUP BY `id`, `Region` HAVING COUNT(*) > 1 ORDER BY 1 LIMIT 5",
        },
        CheckQueryTestCase {
            description: "nulls count the rows missing the column, with Databricks' STRING cast",
            warehouse: Warehouse::Databricks,
            check: CheckQuery::Nulls("email"),
            expected_query: "SELECT CAST(COUNT(*) AS STRING) FROM `raw`.`app__accounts` WHERE `email` IS NULL",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            sql_adapter(test_case.warehouse).render_check_query(&accounts(), test_case.check),
            test_case.expected_query,
            "{}",
            test_case.description
        );
    }
}
