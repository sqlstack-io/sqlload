use crate::adapters::session::tests::helpers::{
    after_new_column, owned, refused_commit, twice_loaded,
};
use crate::adapters::session::tests::test_types::{
    RefusedCommitTestCase, SchemaChangeTestCase, SessionLoadTestCase,
};

#[test]
fn given_each_write_strategy_when_two_loads_go_through_a_callers_session_then_the_table_holds_what_the_strategy_writes()
 {
    let test_cases = [
        SessionLoadTestCase {
            description: "replace rewrites the table from the source",
            settings: "write_strategy = \"replace\"",
            change: "DELETE FROM orders WHERE id = 2; INSERT INTO orders VALUES (3, 'third', '2026-01-03 00:00:00');",
            expected_rows: &["1:first", "3:third"],
        },
        SessionLoadTestCase {
            description: "append adds the rows past the cursor",
            settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"",
            change: "INSERT INTO orders VALUES (3, 'third', '2026-01-03 00:00:00');",
            expected_rows: &["1:first", "2:second", "3:third"],
        },
        SessionLoadTestCase {
            description: "merge updates rows by key and adds new ones",
            settings: "write_strategy = \"merge\"\ncursor_column = \"updated_at\"\nunique_key = \"id\"",
            change: "UPDATE orders SET name = 'first again', updated_at = '2026-01-04 00:00:00' WHERE id = 1; INSERT INTO orders VALUES (3, 'third', '2026-01-03 00:00:00');",
            expected_rows: &["1:first again", "2:second", "3:third"],
        },
        SessionLoadTestCase {
            description: "delete_insert replaces the rows of the cursor's range",
            settings: "write_strategy = \"delete_insert\"\ncursor_column = \"updated_at\"",
            change: "INSERT INTO orders VALUES (3, 'third', '2026-01-03 00:00:00');",
            expected_rows: &["1:first", "2:second", "3:third"],
        },
    ];
    for test_case in test_cases {
        let rows = twice_loaded(test_case.settings, test_case.change)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        assert_eq!(rows, test_case.expected_rows, "{}", test_case.description);
    }
}

#[test]
fn given_a_source_gaining_a_column_when_it_loads_through_a_callers_session_then_on_schema_change_decides()
 {
    let test_cases = [
        SchemaChangeTestCase {
            description: "append_new_columns, the default, adds the column",
            settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"",
            expected_loaded: true,
            expected_columns: &["id", "name", "updated_at", "status"],
        },
        SchemaChangeTestCase {
            description: "ignore loads the rows without it",
            settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"\non_schema_change = \"ignore\"",
            expected_loaded: true,
            expected_columns: &["id", "name", "updated_at"],
        },
        SchemaChangeTestCase {
            description: "fail refuses to load the table",
            settings: "write_strategy = \"append\"\ncursor_column = \"updated_at\"\non_schema_change = \"fail\"",
            expected_loaded: false,
            expected_columns: &["id", "name", "updated_at"],
        },
    ];
    for test_case in test_cases {
        let (loaded, columns) = after_new_column(test_case.settings)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        assert_eq!(
            (loaded, columns),
            (test_case.expected_loaded, owned(test_case.expected_columns)),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_commit_the_callers_session_refuses_when_a_load_writes_then_sqlload_rolls_back_and_the_table_is_unchanged()
 {
    let test_cases = [RefusedCommitTestCase {
        description: "a merge whose commit fails leaves the rows of the load before",
        settings: "write_strategy = \"merge\"\ncursor_column = \"updated_at\"\nunique_key = \"id\"",
        change: "UPDATE orders SET name = 'changed', updated_at = '2026-01-04 00:00:00' WHERE id = 1;",
        expected_next: "ROLLBACK",
        expected_rows: &["1:first", "2:second"],
    }];
    for test_case in test_cases {
        let (next, rows) = refused_commit(test_case.settings, test_case.change)
            .unwrap_or_else(|error| panic!("{}: {error}", test_case.description));
        assert_eq!(
            (next, rows),
            (
                test_case.expected_next.to_owned(),
                owned(test_case.expected_rows)
            ),
            "{}",
            test_case.description
        );
    }
}
