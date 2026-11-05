use crate::project::main::parse_project::parse_project;
use crate::project::tests::helpers::{CONNECTIONS, FILES_PROJECT, PROJECT, environment};
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
            read: |project| project.sources["app"].table_name_template().to_owned(),
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
fn given_a_files_target_when_parsing_then_paths_and_defaults_are_read() -> Result<(), String> {
    let project = parse_project(FILES_PROJECT, &environment).map_err(|error| error.to_string())?;
    let test_cases = [
        ParseProjectTestCase {
            description: "a files target needs no loader_schema",
            read: |project| format!("{:?}", project.targets["lake"].loader_schema),
            expected_value: "None",
        },
        ParseProjectTestCase {
            description: "the table folder defaults to source and table",
            read: |project| project.targets["lake"].path_template().to_owned(),
            expected_value: "{source}/{table}/",
        },
        ParseProjectTestCase {
            description: "footer reads are capped at 100 by default",
            read: |project| project.targets["lake"].footer_read_limit().to_string(),
            expected_value: "100",
        },
        ParseProjectTestCase {
            description: "a source sets the destination path of its tables",
            read: |project| format!("{:?}", project.sources["app"].destination_path),
            expected_value: "Some(\"shop/{table}/\")",
        },
        ParseProjectTestCase {
            description: "a table overrides the source's destination path",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["app"].tables["public.events"].destination_path
                )
            },
            expected_value: "Some(\"shop/{table}/day={cursor:date}/\")",
        },
        ParseProjectTestCase {
            description: "a files source reads its format, compression and CSV options; unset options keep their defaults",
            read: |project| format!("{:?}", project.sources["events"].files_encoding()),
            expected_value: "FilesEncoding { format: Csv, compression: Zstd, csv: CsvOptions { delimiter: ';', header: true, quote: '\"', null_values: [\"NA\", \"\"] } }",
        },
        ParseProjectTestCase {
            description: "a files source without settings reads Parquet",
            read: |project| project.sources["app"].files_encoding().to_string(),
            expected_value: "parquet",
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
    let snowflake = |stage: &str| {
        format!(
            "[connections.wh]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = {{ method = \"pat\", token = \"t\" }}\ndatabase = \"D\"\nload_method = {stage}\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n"
        )
    };
    let stage_cases = [
        (
            "an s3 stage needs an integration or a name",
            snowflake("{ type = \"external_stage\", url = \"s3://lake/x/\" }"),
            "needs exactly one of storage_integration",
        ),
        (
            "an s3 stage takes only one of them",
            snowflake(
                "{ type = \"external_stage\", url = \"s3://lake/x/\", storage_integration = \"I\", name = \"D.S.N\" }",
            ),
            "needs exactly one of storage_integration",
        ),
        (
            "an existing stage's name is fully qualified",
            snowflake("{ type = \"external_stage\", url = \"s3://lake/x/\", name = \"N\" }"),
            "must be DATABASE.SCHEMA.STAGE",
        ),
        (
            "an s3 stage's url is s3",
            snowflake(
                "{ type = \"external_stage\", url = \"/tmp/x/\", storage_integration = \"I\" }",
            ),
            "must be an s3:// URL",
        ),
        (
            "keep_staged_files is only for an external stage",
            snowflake("{ type = \"internal_user_stage\", keep_staged_files = true }"),
            "keep_staged_files",
        ),
        (
            "an unknown type is refused",
            snowflake("{ type = \"auto\" }"),
            "auto",
        ),
        (
            "load_method is required, and the error names the fix",
            snowflake("{ type = \"insert\" }").replace("load_method = { type = \"insert\" }\n", ""),
            "load_method is required: how should data get into Snowflake? Add one of these to [connections.wh]: load_method = { type = \"external_stage\"",
        ),
    ];
    for (description, text, expected_message) in stage_cases {
        let error = parse_project(&text, &|_| None)
            .map(|_| String::new())
            .unwrap_or_else(|error| error.to_string());
        assert!(error.contains(expected_message), "{description}: {error}");
    }
    let test_cases = [
        ParseProjectErrorTestCase {
            description: "shaping takes only loader",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nshaping = \"sql\"\n",
            expected_message: "sql",
        },
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
        ParseProjectErrorTestCase {
            description: "a warehouse target needs a loader_schema",
            text: "[targets.other]\nconnection = \"wh\"\n",
            expected_message: "[targets.other] needs a loader_schema",
        },
        ParseProjectErrorTestCase {
            description: "a warehouse target takes no path",
            text: "[targets.other]\nconnection = \"wh\"\nloader_schema = \"RAW\"\npath = \"{table}/\"\n",
            expected_message: "[targets.other] path, format and max_footer_reads are for targets on an s3 or filesystem connection",
        },
        ParseProjectErrorTestCase {
            description: "a files target takes no loader_schema",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[targets.lake]\nconnection = \"lake\"\nloader_schema = \"RAW\"\n",
            expected_message: "[targets.lake] writes files, so it takes no loader_schema or database",
        },
        ParseProjectErrorTestCase {
            description: "only a parquet format is known",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[targets.lake]\nconnection = \"lake\"\nformat = \"json\"\n",
            expected_message: "unknown variant `json`",
        },
        ParseProjectErrorTestCase {
            description: "compression is for text formats",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\ncompression = \"gzip\"\n",
            expected_message: "[sources.raw] compression is for ndjson, json and csv files; Parquet files compress inside the file",
        },
        ParseProjectErrorTestCase {
            description: "csv options need format = csv",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"ndjson\"\ncsv = { header = false }\n",
            expected_message: "[sources.raw] csv is for format = \"csv\", not \"ndjson\"",
        },
        ParseProjectErrorTestCase {
            description: "a csv delimiter is one ASCII character",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\ncsv = { delimiter = \"\u{e9}\" }\n",
            expected_message: "[sources.raw] csv delimiter must be one ASCII character other than a line break",
        },
        ParseProjectErrorTestCase {
            description: "an unknown csv option is rejected",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\ncsv = { seperator = \";\" }\n",
            expected_message: "unknown field `seperator`",
        },
        ParseProjectErrorTestCase {
            description: "an unknown files format is rejected",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"avro\"\n",
            expected_message: "unknown variant `avro`",
        },
        ParseProjectErrorTestCase {
            description: "format settings are for files sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nformat = \"csv\"\n",
            expected_message: "[sources.app] format, compression and csv are for kind = \"files\"",
        },
        ParseProjectErrorTestCase {
            description: "a target cannot load into a source database",
            text: "[targets.other]\nconnection = \"pg\"\nloader_schema = \"RAW\"\n",
            expected_message: "is a postgres connection; targets load into snowflake or duckdb or write files to s3 or filesystem",
        },
        ParseProjectErrorTestCase {
            description: "destination_path needs a files target",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\ndestination_path = \"{table}/\"\n",
            expected_message: "[sources.app] destination_path is for targets on an s3 or filesystem connection",
        },
        ParseProjectErrorTestCase {
            description: "a table's destination_path needs a files target too",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\ndestination_path = \"{table}/\"\n",
            expected_message: "[sources.app] destination_path is for targets on an s3 or filesystem connection",
        },
        ParseProjectErrorTestCase {
            description: "a source schema is for warehouse targets",
            text: concat!(
                "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[targets.lake]\nconnection = \"lake\"\n",
                "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ndestination = \"lake\"\ninclude = [\"*\"]\nschema = \"RAW\"\n"
            ),
            expected_message: "[sources.app] schema is for warehouse targets",
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
