use crate::project::main::config_at::config_at;
use crate::project::main::config_lines::config_lines;
use crate::project::main::parse_project::parse_project;
use crate::project::tests::test_types::{ConfigAtTestCase, ConnectionVariablesTestCase};

const PROJECT: &str = r#"[connections.pg]
kind = "postgres"
host = "$${not a variable}"
user = "${ENV:PG_USER}"
password = "${ENV:PG_PASSWORD}${ENV:PG_USER}"
database = "app"

[sources.app]
kind = "postgres"
connection = "pg"
unique_key = ["id"]

[sources.app.tables."public.orders"]
description = "d"
unique_key = ["tenant_id", "id"]

[sources.app.tables."public.events"]
description = "d"

[sources.crm]
kind = "postgres"
connection = "pg"
tables."public.leads".unique_key = ["lead_id"]
"#;

#[test]
fn given_a_project_file_when_finding_where_a_setting_is_written_then_its_line_is_named() {
    let lines = config_lines("sqlload.toml", PROJECT);
    let test_cases = [
        ConfigAtTestCase {
            description: "a source's section",
            path: &["sources", "app"],
            expected_at: Some("sqlload.toml:8"),
        },
        ConfigAtTestCase {
            description: "a source's unique_key",
            path: &["sources", "app", "unique_key"],
            expected_at: Some("sqlload.toml:11"),
        },
        ConfigAtTestCase {
            description: "a table entry",
            path: &["sources", "app", "tables", "public.orders"],
            expected_at: Some("sqlload.toml:13"),
        },
        ConfigAtTestCase {
            description: "a table's own unique_key, not its source's",
            path: &["sources", "app", "tables", "public.orders", "unique_key"],
            expected_at: Some("sqlload.toml:15"),
        },
        ConfigAtTestCase {
            description: "a table entry without a unique_key",
            path: &["sources", "app", "tables", "public.events", "unique_key"],
            expected_at: None,
        },
        ConfigAtTestCase {
            description: "a dotted key written under its source",
            path: &["sources", "crm", "tables", "public.leads", "unique_key"],
            expected_at: Some("sqlload.toml:23"),
        },
        ConfigAtTestCase {
            description: "a table the file doesn't list",
            path: &["sources", "app", "tables", "public.items"],
            expected_at: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            config_at(&lines, test_case.path).as_deref(),
            test_case.expected_at,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_connection_reading_variables_when_parsing_then_each_is_named_once_and_an_escaped_one_is_not()
-> Result<(), String> {
    let test_cases = [ConnectionVariablesTestCase {
        description: "a connection reading PG_USER twice, PG_PASSWORD once and an escaped $${...}",
        connection: "pg",
        expected_variables: &["PG_PASSWORD", "PG_USER"],
    }];
    let connections = PROJECT.split("[sources.").next().unwrap_or_default();
    let project =
        parse_project(connections, &|_| Some("x".to_owned())).map_err(|error| error.to_string())?;
    for test_case in test_cases {
        assert_eq!(
            project
                .connection_variables
                .get(test_case.connection)
                .cloned(),
            Some(
                test_case
                    .expected_variables
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect::<Vec<String>>()
            ),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
