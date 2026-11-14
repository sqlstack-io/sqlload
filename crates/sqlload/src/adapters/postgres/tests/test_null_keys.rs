use crate::adapters::postgres::_helpers::null_keys::{index_problem, version_problem};
use crate::adapters::postgres::tests::helpers::orders;
use crate::adapters::postgres::tests::test_types::NullKeysProblemTestCase;

#[test]
fn given_a_merge_key_that_may_be_null_when_planning_on_postgres_then_it_needs_version_15_and_an_index_treating_nulls_as_equal()
 {
    let test_cases = [
        NullKeysProblemTestCase {
            description: "Postgres 14 cannot create an index NULLS NOT DISTINCT, so a key that may be NULL is refused, naming the version and the fixes",
            nullable: &["site"],
            version: (140_011, "14.11"),
            indexes: None,
            expected_problem: Some(
                "app.orders: merge matches a NULL in a key column with a NULL (null_keys = \"match\"), and site can be NULL; INSERT ... ON CONFLICT does that only through a unique index created NULLS NOT DISTINCT, which Postgres has from 15, and this server is Postgres 14.11. Fix: set null_keys = \"fail\" to refuse a row with a NULL key, make site NOT NULL in the source, or upgrade the target to Postgres 15 or later",
            ),
        },
        NullKeysProblemTestCase {
            description: "Postgres 14 merges a key that is never NULL as before",
            nullable: &[],
            version: (140_011, "14.11"),
            indexes: Some((1, 0, "app.orders_sqlload_key")),
            expected_problem: None,
        },
        NullKeysProblemTestCase {
            description: "Postgres 15 creates the index itself on a new table",
            nullable: &["site"],
            version: (150_000, "15.0"),
            indexes: None,
            expected_problem: None,
        },
        NullKeysProblemTestCase {
            description: "an existing index treating NULLs as distinct would let a NULL key in again on every load, so it is refused with the index to create",
            nullable: &["site"],
            version: (170_002, "17.2"),
            indexes: Some((1, 0, "app.orders_sqlload_key")),
            expected_problem: Some(
                "but its unique index on (id, site), app.orders_sqlload_key, treats NULLs as distinct, so INSERT ... ON CONFLICT would insert a row with a NULL key again on every load. Fix: create one that treats them as equal, CREATE UNIQUE INDEX ON \"app\".\"orders\" (\"id\", \"site\") NULLS NOT DISTINCT, then drop app.orders_sqlload_key",
            ),
        },
        NullKeysProblemTestCase {
            description: "an existing index treating NULLs as equal lets the merge run, whatever other indexes there are",
            nullable: &["site"],
            version: (170_002, "17.2"),
            indexes: Some((2, 1, "app.orders_old_key")),
            expected_problem: None,
        },
        NullKeysProblemTestCase {
            description: "no unique index on the key is refused as before, the example NULLS NOT DISTINCT when a key column may be NULL",
            nullable: &["site"],
            version: (170_002, "17.2"),
            indexes: Some((0, 0, "")),
            expected_problem: Some(
                "and app.orders has none; add one, for example CREATE UNIQUE INDEX ON \"app\".\"orders\" (\"id\", \"site\") NULLS NOT DISTINCT",
            ),
        },
    ];
    let unique_key = ["id".to_owned(), "site".to_owned()];
    for test_case in test_cases {
        let table = orders();
        let problem =
            version_problem(&table, test_case.nullable, test_case.version).or_else(|| {
                test_case.indexes.and_then(|(count, equal, others)| {
                    index_problem(
                        &table,
                        (&unique_key, test_case.nullable),
                        (count, equal, others),
                    )
                })
            });
        assert_eq!(
            problem.as_deref().map(|problem| test_case
                .expected_problem
                .is_some_and(|expected| problem.contains(expected))),
            test_case.expected_problem.map(|_| true),
            "{}: {problem:?}",
            test_case.description
        );
    }
}
