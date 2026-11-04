use crate::project::main::parse_project::parse_project;
use crate::project::tests::helpers::{CONNECTIONS, PROJECT, environment};
use crate::project::tests::test_types::{ParseProjectErrorTestCase, ParseProjectTestCase};

#[test]
fn given_a_full_project_when_parsing_then_sources_and_table_settings_are_read() -> Result<(), String>
{
    let project = parse_project(PROJECT, &environment).map_err(|error| error.to_string())?;
    let test_cases = [
        ParseProjectTestCase {
            description: "references are resolved",
            read: |project| {
                format!("{:?}", project.connections["warehouse"])
                    .contains("<KEY_FILE>")
                    .to_string()
            },
            expected_value: "true",
        },
        ParseProjectTestCase {
            description: "passwords are redacted in debug output",
            read: |project| {
                format!("{:?}", project.connections["app_pg"])
                    .contains("<PG_PASSWORD>")
                    .to_string()
            },
            expected_value: "false",
        },
        ParseProjectTestCase {
            description: "new_tables is read",
            read: |project| format!("{:?}", project.sources["app"].new_tables),
            expected_value: "Skip",
        },
        ParseProjectTestCase {
            description: "source-level table settings are defaults",
            read: |project| format!("{:?}", project.sources["app"].defaults.write_strategy),
            expected_value: "Some(Merge)",
        },
        ParseProjectTestCase {
            description: "a single unique key becomes a list",
            read: |project| format!("{:?}", project.sources["app"].defaults.unique_key),
            expected_value: "Some([\"id\"])",
        },
        ParseProjectTestCase {
            description: "lookback is a duration",
            read: |project| format!("{:?}", project.sources["app"].defaults.lookback),
            expected_value: "Some(3600s)",
        },
        ParseProjectTestCase {
            description: "the table name template defaults to source and table",
            read: |project| project.sources["app"].table_name.clone(),
            expected_value: "{source}__{table}",
        },
        ParseProjectTestCase {
            description: "pinned columns are read per table",
            read: |project| {
                project.sources["app"].tables["public.accounts"].columns["balance"].clone()
            },
            expected_value: "decimal(18,2)",
        },
        ParseProjectTestCase {
            description: "a table policy overrides the default",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["app"].tables["public.accounts"]
                        .settings
                        .type_change_rebuild
                )
            },
            expected_value: "Some(Allow)",
        },
        ParseProjectTestCase {
            description: "a composite unique key is kept in order",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["app"].tables["public.events"]
                        .settings
                        .unique_key
                )
            },
            expected_value: "Some([\"event_id\", \"line_no\"])",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            (test_case.read)(&project),
            test_case.expected_value,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_an_invalid_project_when_parsing_then_every_problem_is_named() {
    let test_cases = [
        ParseProjectErrorTestCase {
            description: "a misspelt source key suggests the right one",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\ncursr_column = \"x\"\n",
            expected_message: "unknown key \"cursr_column\" in [sources.app]; did you mean \"cursor_column\"?",
        },
        ParseProjectErrorTestCase {
            description: "a misspelt table key is rejected",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\ncolums = {}\n",
            expected_message: "unknown key \"colums\" in [sources.app.tables.\"public.a\"]",
        },
        ParseProjectErrorTestCase {
            description: "a source's connection must exist",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"missing\"\ninclude = [\"*\"]\n",
            expected_message: "connection \"missing\" is not a [connections] entry",
        },
        ParseProjectErrorTestCase {
            description: "a postgres source needs a postgres connection",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"wh\"\ninclude = [\"*\"]\n",
            expected_message: "but connection \"wh\" is a duckdb connection",
        },
        ParseProjectErrorTestCase {
            description: "a destination must be a target",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ndestination = \"prod\"\ninclude = [\"*\"]\n",
            expected_message: "destination \"prod\" is not a [targets] entry",
        },
        ParseProjectErrorTestCase {
            description: "a table name template needs the table",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\ntable_name = \"ALL\"\n",
            expected_message: "must contain {table}",
        },
        ParseProjectErrorTestCase {
            description: "an unknown write strategy is a syntax error",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nwrite_strategy = \"upsert\"\n",
            expected_message: "unknown variant `upsert`",
        },
    ];
    for test_case in test_cases {
        let text = format!("{CONNECTIONS}{}", test_case.text);
        let error = parse_project(&text, &environment)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert!(
            error.contains(test_case.expected_message),
            "{}: {error}",
            test_case.description
        );
    }
}
