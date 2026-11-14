use crate::loader::tests::helpers::not_null_keys;
use crate::loader::tests::test_types::KeyNullsTestCase;

#[test]
fn given_a_merge_key_when_resolving_the_table_then_columns_never_null_compare_with_equals_and_a_reported_key_that_may_be_null_is_refused()
 {
    let test_cases = [
        KeyNullsTestCase {
            description: "a named key compares its NOT NULL columns with =, and matches NULLs in the rest",
            columns: &[("company", false), ("item", false), ("site", true)],
            primary_key: &[],
            settings: "write_strategy = \"merge\"\nunique_key = [\"company\", \"item\", \"site\"]",
            expected_keys: Ok("company, item"),
        },
        KeyNullsTestCase {
            description: "under null_keys = \"fail\" no key column holds a NULL once the commit passes, so each compares with =",
            columns: &[("company", false), ("item", false), ("site", true)],
            primary_key: &[],
            settings: "write_strategy = \"merge\"\nunique_key = [\"company\", \"item\", \"site\"]\nnull_keys = \"fail\"",
            expected_keys: Ok("company, item, site"),
        },
        KeyNullsTestCase {
            description: "a reported primary key whose columns are all NOT NULL merges as before",
            columns: &[("id", false), ("site", true)],
            primary_key: &["id"],
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"",
            expected_keys: Ok("id"),
        },
        KeyNullsTestCase {
            description: "a reported primary key with a column that may be NULL (a unique index MySQL Connector/ODBC reports as the key) is refused, naming the column",
            columns: &[("code", true), ("at", false)],
            primary_key: &["code"],
            settings: "write_strategy = \"merge\"\nunique_key = \"{primary_key}\"",
            expected_keys: Err(
                "erp.stock (kind = \"postgres\"): unique_key = \"{primary_key}\" takes the key the source reports, (code), and code can be NULL there, so the source can hold different rows with a NULL in it, which a merge matching NULLs would merge into one. Fix: name the unique_key columns",
            ),
        },
        KeyNullsTestCase {
            description: "null_keys on a table that doesn't merge is refused",
            columns: &[("id", false)],
            primary_key: &[],
            settings: "write_strategy = \"append\"\nnull_keys = \"fail\"",
            expected_keys: Err("null_keys is for write_strategy = \"merge\""),
        },
    ];
    for test_case in test_cases {
        let found = not_null_keys(&test_case);
        assert_eq!(
            found
                .as_ref()
                .map(String::as_str)
                .map_err(|problem| test_case
                    .expected_keys
                    .err()
                    .is_some_and(|expected| problem.contains(expected))),
            test_case.expected_keys.map_err(|_| true),
            "{}: {found:?}",
            test_case.description
        );
    }
}
