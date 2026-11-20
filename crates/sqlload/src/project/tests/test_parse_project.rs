use crate::auth::models::Secret;
use crate::project::main::motherduck_stage::motherduck_stage;
use crate::project::main::parse_project::parse_project;
use crate::project::tests::helpers::{
    CONNECTIONS, FILES_PROJECT, PROJECT, SNOWFLAKE_SOURCE, environment,
};
use crate::project::tests::helpers::{LAKE, md_stage};
use crate::project::tests::test_types::MotherDuckStageTestCase;
use crate::project::tests::test_types::{ParseProjectErrorTestCase, ParseProjectTestCase};
use crate::store_paths::models::{HmacKey, Location, StoreKind};

const CONSTANT_SOURCES: &str = r#"
[sources.app]
kind = "postgres"
connection = "pg"
include = ["public.*"]
constant_columns = { tenant_id = "${ENV:TENANT}", region = "eu" }

[sources.app.tables."public.orders"]
description = "d"
constant_columns = { tenant_id = { value = "7", type = "int" }, as_of = 2026-11-08 }

[sources.app.tables."public.events"]
description = "d"
unique_key = ["tenant_id", "id"]
"#;

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
            description: "a bigquery connection reads its location and key, the key redacted, and no project until the key gives one",
            read: |project| format!("{:?}", project.connections["bq"]),
            expected_value: "BigQuery { project: None, location: \"europe-west2\", service_account_key: <redacted>, base_url: None, storage_url: None }",
        },
        ParseProjectTestCase {
            description: "new_tables is read",
            read: |project| format!("{:?}", project.sources["app"].new_tables),
            expected_value: "Skip",
        },
        ParseProjectTestCase {
            description: "a table's checks are read, the row-drop limit as a percentage",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["app"].tables["public.accounts"]
                        .settings
                        .checks
                )
            },
            expected_value: "Some(TableChecks { unique: Some([\"id\"]), not_null: [\"id\"], min_rows: None, max_row_drop: Some(20), warn: [MaxRowDrop] })",
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
            description: "lookback written as text is a duration",
            read: |project| format!("{:?}", project.sources["app"].defaults.lookback),
            expected_value: "Some(Duration(3600s))",
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
            expected_value: "FilesEncoding { format: Csv, compression: Zstd, csv: CsvOptions { delimiter: ';', header: true, quote: Char('\"'), escape: None, encoding_check: true, null_values: [\"NA\", \"\"], encoding: \"utf-8\", skip_rows: 0, column_names: None }, xlsx: XlsxOptions { sheet: None, header: true, skip_rows: 0, range: None, header_row: None, column_names: None }, xml: XmlOptions { record_path: \"\", lists: [] }, members: None, pattern: None, record: None, flatten: Flatten { objects: None, keep: [], pinned: [], max_depth: 0, naming: SnakeCase, excluded: ExcludePatterns { patterns: [], set: GlobSet { len: 0, strats: [] } }, as_text: false, nonfinite_null: false } }",
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
            "load_method is required: how should data get into Snowflake? Fix: add one of these to [connections.wh]:\nload_method = { type = \"external_stage\"",
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
            description: "shaping is gone: nothing chooses SQLLoad's route over the warehouse's own read",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nshaping = \"loader\"\n",
            expected_message: "unknown key \"shaping\"",
        },
        ParseProjectErrorTestCase {
            description: "a row-drop limit must be a percentage",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nchecks = { max_row_drop = \"120%\" }\n",
            expected_message: "\"120%\" is not a percentage",
        },
        ParseProjectErrorTestCase {
            description: "an unknown check is rejected",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nchecks = { uniq = [\"id\"] }\n",
            expected_message: "unknown field `uniq`",
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
            description: "reading windows at once needs the project to opt in",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\nbatch_concurrency = 4\n",
            expected_message: "batch_concurrency reads several windows at once, which needs microbatch_concurrency = true at the top of the project",
        },
        ParseProjectErrorTestCase {
            description: "batch_concurrency holds a source connection per window, so it is capped",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nbatch_concurrency = 17\n",
            expected_message: "batch_concurrency = 17 would hold 17 source connections; read at most 16 windows at once",
        },
        ParseProjectErrorTestCase {
            description: "batch_concurrency reads at least one window",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nbatch_concurrency = 0\n",
            expected_message: "nonzero",
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
            expected_message: "[targets.other] path, format, compression, csv, single_file and max_footer_reads are for targets on an s3, gcs or filesystem connection",
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
            expected_message: "[sources.raw] compression is for ndjson, json, csv, xlsx and xml files; Parquet files compress inside the file",
        },
        ParseProjectErrorTestCase {
            description: "csv options need format = csv",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"ndjson\"\ncsv = { header = false }\n",
            expected_message: "[sources.raw] csv is for format = \"csv\", not \"ndjson\"",
        },
        ParseProjectErrorTestCase {
            description: "xml files need a record path",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"xml\"\n",
            expected_message: "[sources.raw] format = \"xml\" needs xml.record_path",
        },
        ParseProjectErrorTestCase {
            description: "a transform is for files and google_sheets tables",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\ntransform = \"fix_rows\"\n",
            expected_message: "[sources.app] transform is for kind = \"files\" and kind = \"google_sheets\" tables",
        },
        ParseProjectErrorTestCase {
            description: "a transform names the function it runs",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\ntransform = \" \"\n",
            expected_message: "[sources.raw] transform is empty; give the name @sl.transform registered the function under",
        },
        ParseProjectErrorTestCase {
            description: "a transform is found on python_paths",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\ntransform = \"metrics\"\n",
            expected_message: "[sources.raw] transform needs python_paths, naming the folder that holds the @sl.transform function",
        },
        ParseProjectErrorTestCase {
            description: "xlsx options need format = xlsx",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\nxlsx = { sheet = \"Orders\" }\n",
            expected_message: "[sources.raw] xlsx is for format = \"xlsx\"",
        },
        ParseProjectErrorTestCase {
            description: "after_load on ZIP archives another source on the connection also reads in an overlapping folder",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\ncompression = \"zip\"\nafter_load = \"delete\"\n[sources.orders]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/orders/\"\nformat = \"csv\"\ncompression = \"zip\"\n",
            expected_message: "[sources.raw] after_load moves or deletes each ZIP archive once its members load, and [sources.orders] reads ZIP archives in \"raw/orders/\" on the same connection, which may still need them",
        },
        ParseProjectErrorTestCase {
            description: "after_load is for files sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"public.*\"]\nafter_load = \"delete\"\n",
            expected_message: "copy_to_internal_stage, after_load, order and on_empty are for kind = \"files\"",
        },
        ParseProjectErrorTestCase {
            description: "an unknown after_load is refused",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\nafter_load = \"archive\"\n",
            expected_message: "unknown variant `archive`",
        },
        ParseProjectErrorTestCase {
            description: "members need compression = zip",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\nmembers = \"*.csv\"\n",
            expected_message: "[sources.raw] members is for compression = \"zip\"",
        },
        ParseProjectErrorTestCase {
            description: "whole records are not for spreadsheets",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"xlsx\"\nshape = \"record\"\n",
            expected_message: "[sources.raw] shape = \"record\" is for ndjson, json and csv files, not xlsx",
        },
        ParseProjectErrorTestCase {
            description: "ZIP archives' members never go to a warehouse stage",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\ncompression = \"zip\"\ncopy_to_internal_stage = true\n",
            expected_message: "[sources.raw] copy_to_internal_stage is for files Snowflake reads itself",
        },
        ParseProjectErrorTestCase {
            description: "an unknown xlsx option is rejected",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"xlsx\"\nxlsx = { tab = \"Orders\" }\n",
            expected_message: "unknown field `tab`",
        },
        ParseProjectErrorTestCase {
            description: "a csv delimiter is one ASCII character",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\ncsv = { delimiter = \"\u{e9}\" }\n",
            expected_message: "[sources.raw] csv delimiter must be one ASCII character other than a line break",
        },
        ParseProjectErrorTestCase {
            description: "a pattern that isn't a glob is refused",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\npattern = \"file_[1.txt\"\n",
            expected_message: "[sources.raw] pattern: error parsing glob 'file_[1.txt': unclosed character class",
        },
        ParseProjectErrorTestCase {
            description: "pattern is for files sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\npattern = \"*.txt\"\n",
            expected_message: "pattern, shape and record_column are for kind = \"files\"",
        },
        ParseProjectErrorTestCase {
            description: "csv quote = true names no character",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\ncsv = { quote = true }\n",
            expected_message: "[sources.raw] csv quote = true names no character",
        },
        ParseProjectErrorTestCase {
            description: "csv escape needs quoted fields",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\ncsv = { quote = false, escape = \"\\\\\" }\n",
            expected_message: "[sources.raw] csv escape works inside quoted fields, and quote = false has none; remove escape",
        },
        ParseProjectErrorTestCase {
            description: "csv escape is one ASCII character, not the delimiter",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\ncsv = { escape = \",\" }\n",
            expected_message: "[sources.raw] csv delimiter must differ from quote and escape",
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
            description: "a query table is for snowflake, odbc, bigquery, databricks, duckdb and motherduck sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\nsql = \"SELECT 1\"\n",
            expected_message: "[sources.app] sql is for kind = \"snowflake\", \"odbc\", \"bigquery\", \"databricks\", \"duckdb\" and \"motherduck\"",
        },
        ParseProjectErrorTestCase {
            description: "a databricks connection a target loads into needs a volume to stage files in",
            text: "[connections.dbx]\nkind = \"databricks\"\nhost = \"h\"\nclient_id = \"i\"\nclient_secret = \"s\"\nwarehouse_id = \"w\"\ncatalog = \"main\"\n[targets.cloud]\nconnection = \"dbx\"\nloader_schema = \"raw\"\n",
            expected_message: "[connections.dbx] needs volume = \"CATALOG.SCHEMA.VOLUME\"",
        },
        ParseProjectErrorTestCase {
            description: "where narrows a database table's SELECT, so a files source refuses it",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\nwhere = \"a = 1\"\n",
            expected_message: "[sources.raw] where is for kind = \"postgres\", \"snowflake\", \"odbc\", \"bigquery\", \"databricks\", \"duckdb\" and \"motherduck\"",
        },
        ParseProjectErrorTestCase {
            description: "a postgres_cdc source captures every change, so it refuses where on a table",
            text: "[sources.app]\nkind = \"postgres_cdc\"\nconnection = \"pg\"\ninclude = [\"public.a\"]\n[sources.app.tables.\"public.a\"]\nwhere = \"a = 1\"\n",
            expected_message: "[sources.app] where is for kind = \"postgres\", \"snowflake\", \"odbc\", \"bigquery\", \"databricks\", \"duckdb\" and \"motherduck\"",
        },
        ParseProjectErrorTestCase {
            description: "a query table takes its condition in its sql",
            text: "[sources.erp]\nkind = \"odbc\"\nconnection = \"erp\"\n[sources.erp.tables.recent]\nsql = \"SELECT 1 AS a\"\nwhere = \"a = 1\"\n",
            expected_message: "[sources.erp] [tables.recent] where narrows a table SQLLoad selects from, and recent is a query; put the condition in its sql",
        },
        ParseProjectErrorTestCase {
            description: "a source's project is for bigquery sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nproject = \"other\"\n",
            expected_message: "[sources.app] project is for kind = \"bigquery\"",
        },
        ParseProjectErrorTestCase {
            description: "an ODBC source's memory settings are refused on another kind, rather than ignored",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nmax_batch_memory = \"2GB\"\n",
            expected_message: "[sources.app] long_column_threshold and max_batch_memory are for kind = \"odbc\"",
        },
        ParseProjectErrorTestCase {
            description: "a fetch budget of nothing could read no row",
            text: "[sources.erp]\nkind = \"odbc\"\nconnection = \"erp\"\ninclude = [\"*\"]\nmax_batch_memory = 0\n",
            expected_message: "[sources.erp] max_batch_memory must be more than 0",
        },
        ParseProjectErrorTestCase {
            description: "an empty AWS profile is never read as a profile named \"\"",
            text: "[connections.lake]\nkind = \"s3\"\nbucket = \"b\"\nprofile = \"\"\n",
            expected_message: "[connections.lake] profile is empty; omit it",
        },
        ParseProjectErrorTestCase {
            description: "an HMAC key's ID without its secret signs nothing",
            text: "[connections.lake]\nkind = \"gcs\"\nbucket = \"b\"\nhmac_key_id = \"GOOG1E\"\n",
            expected_message: "[connections.lake] hmac_key_id and hmac_secret go together",
        },
        ParseProjectErrorTestCase {
            description: "an http connection's rate is checked when the project is read",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nrate_limit = \"fast\"\n",
            expected_message: "[connections.api] rate_limit \"fast\" is not a rate such as \"10/s\", \"600/m\" or \"1000/h\"",
        },
        ParseProjectErrorTestCase {
            description: "an in-flight cap of none would never send a request",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nmax_in_flight = 0\n",
            expected_message: "[connections.api] max_in_flight must be from 1 to 10000",
        },
        ParseProjectErrorTestCase {
            description: "an API key goes in a header or a query parameter, not both",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nauth = { kind = \"api_key\", header = \"X-Key\", query = \"key\" }\n",
            expected_message: "[connections.api] auth api_key takes exactly one of header or query",
        },
        ParseProjectErrorTestCase {
            description: "a token URL the client secret would reach in clear text is refused",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nauth = { kind = \"oauth2_client_credentials\", token_url = \"http://auth.example.com/token\" }\n",
            expected_message: "[connections.api] token_url http://auth.example.com/token must be https",
        },
        ParseProjectErrorTestCase {
            description: "a misspelt auth setting is never silently ignored",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nauth = { kind = \"oauth2_password\", token_url = \"https://a.example.com/t\", scopes = \"x\" }\n",
            expected_message: "unknown field `scopes`",
        },
        ParseProjectErrorTestCase {
            description: "an unknown auth scheme is refused",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nauth = { kind = \"digest\" }\n",
            expected_message: "unknown variant `digest`",
        },
        ParseProjectErrorTestCase {
            description: "a rate limit on a connection that sends no HTTP request is refused",
            text: "[connections.sf]\nkind = \"snowflake\"\naccount = \"a\"\nrate_limit = \"10/s\"\n",
            expected_message: "unknown field `rate_limit`",
        },
        ParseProjectErrorTestCase {
            description: "a token written as auth is refused without showing it",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nauth = \"${ENV:API_TOKEN}\"\n",
            expected_message: "auth is a table such as { kind = \"bearer\" }; credentials such as token go in their own keys",
        },
        ParseProjectErrorTestCase {
            description: "a credential written inside a bearer auth is never silently dropped",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nauth = { kind = \"bearer\", token = \"t\" }\n",
            expected_message: "unknown field `token`",
        },
        ParseProjectErrorTestCase {
            description: "an in-flight cap past what one process can hold is refused",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nmax_in_flight = 1000000000000\n",
            expected_message: "[connections.api] max_in_flight must be from 1 to 10000",
        },
        ParseProjectErrorTestCase {
            description: "a retry count that would overflow is refused",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\nretries = 4294967295\n",
            expected_message: "[connections.api] retries must be at most 100",
        },
        ParseProjectErrorTestCase {
            description: "an odbc connection needs a connection string or a DSN",
            text: "[connections.erp]\nkind = \"odbc\"\n",
            expected_message: "[connections.erp] needs connection_string, or dsn",
        },
        ParseProjectErrorTestCase {
            description: "an odbc connection takes one of them",
            text: "[connections.erp]\nkind = \"odbc\"\nconnection_string = \"Driver=X\"\ndsn = \"erp\"\n",
            expected_message: "[connections.erp] takes connection_string or dsn, not both",
        },
        ParseProjectErrorTestCase {
            description: "user and password beside a connection string are never silently ignored",
            text: "[connections.erp]\nkind = \"odbc\"\nconnection_string = \"Driver=X\"\nuser = \"u\"\n",
            expected_message: "[connections.erp] user and password go with dsn",
        },
        ParseProjectErrorTestCase {
            description: "a DSN name is written unbraced, so it cannot hold a separator",
            text: "[connections.erp]\nkind = \"odbc\"\ndsn = \"erp;Server=x\"\n",
            expected_message: "[connections.erp] dsn and user cannot contain",
        },
        ParseProjectErrorTestCase {
            description: "a snowflake source selects tables or has a query table",
            text: "[connections.sf]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"D\"\n[sources.marts]\nkind = \"snowflake\"\nconnection = \"sf\"\n",
            expected_message: "[sources.marts] include is empty, so it selects no tables; list tables in include, or give a table a sql query",
        },
        ParseProjectErrorTestCase {
            description: "an unload url needs the storage integration Snowflake writes with",
            text: "[connections.sf]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"D\"\n[sources.marts]\nkind = \"snowflake\"\nconnection = \"sf\"\ninclude = [\"MARTS.*\"]\nstage = { url = \"s3://b/unload/\" }\n",
            expected_message: "[sources.marts] stage url needs the storage_integration Snowflake writes to it with",
        },
        ParseProjectErrorTestCase {
            description: "an unload stage is a url or a name, not both",
            text: "[connections.sf]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"D\"\n[sources.marts]\nkind = \"snowflake\"\nconnection = \"sf\"\ninclude = [\"MARTS.*\"]\nstage = { url = \"s3://b/u/\", storage_integration = \"I\", name = \"D.S.X\" }\n",
            expected_message: "[sources.marts] stage takes either url and storage_integration (an S3 folder) or name (an existing stage)",
        },
        ParseProjectErrorTestCase {
            description: "a snowflake connection a target loads into still needs a load_method",
            text: "[connections.sf]\nkind = \"snowflake\"\naccount = \"a\"\nuser = \"u\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"D\"\n[targets.sf]\nconnection = \"sf\"\nloader_schema = \"RAW\"\n",
            expected_message: "connection sf: load_method is required",
        },
        ParseProjectErrorTestCase {
            description: "format settings are for files sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nformat = \"csv\"\n",
            expected_message: "[sources.app] format, compression, csv, xlsx, xml, members, pattern, shape and record_column are for kind = \"files\"",
        },
        ParseProjectErrorTestCase {
            description: "batch_interval is for postgres_cdc sources, never silently ignored",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nbatch_interval = \"1m\"\n",
            expected_message: "[sources.app] publication, slot, cdc_mode and batch_interval are for kind = \"postgres_cdc\"",
        },
        ParseProjectErrorTestCase {
            description: "a batch_interval just under a second is refused",
            text: "[sources.app]\nkind = \"postgres_cdc\"\nconnection = \"pg\"\ninclude = [\"*\"]\nbatch_interval = \"999ms\"\n",
            expected_message: "[sources.app] batch_interval must be at least 1s",
        },
        ParseProjectErrorTestCase {
            description: "copy_to_internal_stage is for files sources, never silently ignored",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\ncopy_to_internal_stage = true\n",
            expected_message: "[sources.app] path, partition_columns, path_columns, unmatched_paths, copy_to_internal_stage, after_load, order and on_empty are for kind = \"files\"",
        },
        ParseProjectErrorTestCase {
            description: "a postgres_cdc source refuses constant_columns, at source level",
            text: "[sources.app]\nkind = \"postgres_cdc\"\nconnection = \"pg\"\ninclude = [\"*\"]\nconstant_columns = { tenant_id = \"a\" }\n",
            expected_message: "[sources.app] constant_columns is not supported for kind = \"postgres_cdc\"",
        },
        ParseProjectErrorTestCase {
            description: "a postgres_cdc source refuses constant_columns on one table too",
            text: "[sources.app]\nkind = \"postgres_cdc\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\nconstant_columns = { tenant_id = \"a\" }\n",
            expected_message: "[sources.app] constant_columns is not supported for kind = \"postgres_cdc\"",
        },
        ParseProjectErrorTestCase {
            description: "a table's constant that does not convert to its type fails parsing, before any source is read",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\nconstant_columns = { n = { value = \"x\", type = \"int\" } }\n",
            expected_message: "[sources.app] constant column n: \"x\" is not a int",
        },
        ParseProjectErrorTestCase {
            description: "a constant cannot take a name SQLLoad keeps for its own columns, in any case",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nconstant_columns = { _SQLLOAD_load_id = \"x\" }\n",
            expected_message: "[sources.app] constant column _SQLLOAD_load_id: names starting with _sqlload_ are SQLLoad's own",
        },
        ParseProjectErrorTestCase {
            description: "a files table's own constant clashing with a Hive folder is caught like the source's",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\npartition_columns = { day = \"date\" }\n[sources.events.tables.events]\nconstant_columns = { DAY = 2026-11-08 }\n",
            expected_message: "[sources.events] column DAY is in both partition_columns and constant_columns",
        },
        ParseProjectErrorTestCase {
            description: "a column filled by both a Hive folder and a constant has no single value",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\npartition_columns = { day = \"date\" }\nconstant_columns = { day = 2026-11-08 }\n",
            expected_message: "[sources.events] column day is in both partition_columns and constant_columns",
        },
        ParseProjectErrorTestCase {
            description: "names differing only in case clash: destinations fold them together",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\npath_columns = \"{Region}/*\"\nconstant_columns = { region = \"eu\" }\n",
            expected_message: "[sources.events] column region is in both path_columns and constant_columns",
        },
        ParseProjectErrorTestCase {
            description: "a malformed path template fails parsing, before any file is listed",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\npath_columns = \"{region/*\"\n",
            expected_message: "[sources.events] path_columns \"{region/*\": { in \"{region\" is never closed",
        },
        ParseProjectErrorTestCase {
            description: "the record shape is for text files: Parquet files already have typed columns",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\nshape = \"record\"\n",
            expected_message: "[sources.events] shape = \"record\" is for ndjson, json and csv files",
        },
        ParseProjectErrorTestCase {
            description: "record_column without the record shape is never silently ignored",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\nformat = \"ndjson\"\nrecord_column = \"raw\"\n",
            expected_message: "[sources.events] record_column is for shape = \"record\", which is not set",
        },
        ParseProjectErrorTestCase {
            description: "the record column clashes with a constant column of another case",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\nformat = \"csv\"\nshape = \"record\"\nconstant_columns = { Record = \"x\" }\n",
            expected_message: "[sources.events] column Record is in both record_column and constant_columns",
        },
        ParseProjectErrorTestCase {
            description: "the record column cannot take a name SQLLoad adds itself, which would drop the record",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\nformat = \"ndjson\"\nshape = \"record\"\nrecord_column = \"_SQLLOAD_SOURCE_ROW\"\n",
            expected_message: "[sources.events] record_column _SQLLOAD_SOURCE_ROW is a column SQLLoad adds itself",
        },
        ParseProjectErrorTestCase {
            description: "a record-shape source pins no typed columns, which would load NULL or the whole record",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\nformat = \"ndjson\"\nshape = \"record\"\n[sources.events.tables.events]\ncolumns = { id = \"int\" }\n",
            expected_message: "[sources.events] shape = \"record\" loads each record into one json column, record, so [tables.events] columns cannot pin id",
        },
        ParseProjectErrorTestCase {
            description: "shape is for files sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nshape = \"record\"\n",
            expected_message: "[sources.app] format, compression, csv, xlsx, xml, members, pattern, shape and record_column are for kind = \"files\"",
        },
        ParseProjectErrorTestCase {
            description: "unmatched_paths without path_columns is never silently ignored",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\nunmatched_paths = \"skip\"\n",
            expected_message: "[sources.events] unmatched_paths is for path_columns, which is not set",
        },
        ParseProjectErrorTestCase {
            description: "a target cannot load into an HTTP API",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\n[targets.other]\nconnection = \"api\"\nloader_schema = \"RAW\"\n",
            expected_message: "is a http connection; targets load into snowflake, databricks, bigquery, clickhouse, duckdb, motherduck, postgres or write files to s3, gcs or filesystem",
        },
        ParseProjectErrorTestCase {
            description: "destination_path needs a files target",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\ndestination_path = \"{table}/\"\n",
            expected_message: "[sources.app] destination_path is for targets on an s3, gcs or filesystem connection",
        },
        ParseProjectErrorTestCase {
            description: "a table's destination_path needs a files target too",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\ndestination_path = \"{table}/\"\n",
            expected_message: "[sources.app] destination_path is for targets on an s3, gcs or filesystem connection",
        },
        ParseProjectErrorTestCase {
            description: "a source's destination schema is for warehouse targets",
            text: concat!(
                "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[targets.lake]\nconnection = \"lake\"\n",
                "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ndestination = \"lake\"\ninclude = [\"*\"]\ndestination_schema = \"RAW\"\n"
            ),
            expected_message: "[sources.app] destination_schema is for warehouse targets",
        },
        ParseProjectErrorTestCase {
            description: "a source's schema key is gone: the error names destination_schema, and include for the source schema",
            text: "[sources.erp]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nschema = \"raw_erp\"\n",
            expected_message: "[sources.erp]: schema was renamed destination_schema (the schema tables land in); the source schema goes in include, such as \"dbo.*\"",
        },
        ParseProjectErrorTestCase {
            description: "a sheet range the source's column_names reach that keeps its header row is refused",
            text: "[connections.sheets]\nkind = \"google_sheets\"\nservice_account_json = \"{}\"\n[sources.ref]\nkind = \"google_sheets\"\nconnection = \"sheets\"\nspreadsheet_id = \"s\"\nwrite_strategy = \"replace\"\nranges = { a = \"A!A:C\", b = \"B!A:C\" }\ncolumn_names = [\"x\", \"y\", \"z\"]\n[sources.ref.tables.a]\nheader = false\n",
            expected_message: "[sources.ref] column_names names the columns of data without a header row; set header = false",
        },
        ParseProjectErrorTestCase {
            description: "column_names on whole CSV records is refused",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"csv\"\nshape = \"record\"\ncsv = { header = false }\ncolumn_names = [\"a\"]\n",
            expected_message: "column_names is for columns of data without a header row",
        },
        ParseProjectErrorTestCase {
            description: "naming takes snake_case or source; dlt is no naming",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nnaming = \"dlt\"\n",
            expected_message: "unknown variant `dlt`, expected `snake_case` or `source`",
        },
        ParseProjectErrorTestCase {
            description: "deletes_from needs deletes to say what a tombstone does",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"public.items\"]\n[sources.app.tables.\"public.items\"]\nwrite_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"updated_at\"\ndeletes_from = { table = \"audit.deleted_items\", cursor_column = \"deleted_at\" }\n",
            expected_message: "[sources.app] table \"public.items\": deletes_from marks the rows its tombstones name, which needs deletes = \"soft\" or \"hard\" to say how",
        },
        ParseProjectErrorTestCase {
            description: "a tombstone table loads only as one table's tombstones, not listed itself",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"public.items\"]\n[sources.app.tables.\"public.items\"]\nwrite_strategy = \"merge\"\nunique_key = [\"id\"]\ncursor_column = \"updated_at\"\ndeletes = \"hard\"\ndeletes_from = { table = \"audit.deleted_items\", cursor_column = \"deleted_at\" }\n[sources.app.tables.\"audit.deleted_items\"]\nwrite_strategy = \"append\"\n",
            expected_message: "deletes_from table \"audit.deleted_items\" is also listed under tables",
        },
        ParseProjectErrorTestCase {
            description: "an xlsx header_row past the range's rows is refused",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/data\"\n[sources.raw]\nkind = \"files\"\nconnection = \"lake\"\npath = \"raw/\"\nformat = \"xlsx\"\nxlsx = { range = \"A1:C5\", header_row = 10 }\n",
            expected_message: "[sources.raw] xlsx.header_row 10 is past the range's 5 rows",
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

#[test]
fn given_a_snowflake_source_when_parsing_then_its_query_tables_and_window_are_read_and_no_load_method_is_needed()
-> Result<(), String> {
    let text = format!("{CONNECTIONS}{SNOWFLAKE_SOURCE}");
    let project = parse_project(&text, &environment).map_err(|error| error.to_string())?;
    let test_cases = [
        ParseProjectTestCase {
            description: "the source's warehouse is read",
            read: |project| format!("{:?}", project.sources["marts"].warehouse),
            expected_value: "Some(\"EXPORT_WH\")",
        },
        ParseProjectTestCase {
            description: "a query table keeps its query",
            read: |project| format!("{:?}", project.sources["marts"].tables["current"].sql),
            expected_value: "Some(\"SELECT * FROM MARTS.EVENTS\")",
        },
        ParseProjectTestCase {
            description: "lookahead is a duration",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["marts"].tables["current"]
                        .settings
                        .lookahead
                )
            },
            expected_value: "Some(604800s)",
        },
        ParseProjectTestCase {
            description: "lookback_from is read",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["marts"].tables["current"]
                        .settings
                        .lookback_from
                )
            },
            expected_value: "Some(Now)",
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
fn given_constant_columns_on_a_source_and_its_tables_when_parsing_then_a_tables_map_replaces_the_sources()
-> Result<(), String> {
    let text = format!("{CONNECTIONS}{CONSTANT_SOURCES}");
    let project = parse_project(&text, &environment).map_err(|error| error.to_string())?;
    let test_cases = [
        ParseProjectTestCase {
            description: "the source's constants are its tables' default, with ${ENV:NAME} resolved",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["app"].table_constants("public.events")
                )
            },
            expected_value: "{\"region\": String(\"eu\"), \"tenant_id\": String(\"<TENANT>\")}",
        },
        ParseProjectTestCase {
            description: "a table's own constants replace the source's whole map, a TOML date typed as a date",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["app"].table_constants("public.orders")
                )
            },
            expected_value: "{\"as_of\": Table({\"type\": String(\"date\"), \"value\": String(\"2026-11-08\")}), \"tenant_id\": Table({\"type\": String(\"int\"), \"value\": String(\"7\")})}",
        },
        ParseProjectTestCase {
            description: "a table the configuration does not name takes the source's",
            read: |project| {
                format!(
                    "{:?}",
                    project.sources["app"].table_constants("public.other")
                )
            },
            expected_value: "{\"region\": String(\"eu\"), \"tenant_id\": String(\"<TENANT>\")}",
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
fn given_column_names_on_a_sheets_source_when_every_table_using_them_is_headerless_then_it_parses()
{
    let sheets = "[connections.sheets]\nkind = \"google_sheets\"\nservice_account_json = \"{}\"\n[sources.ref]\nkind = \"google_sheets\"\nconnection = \"sheets\"\nspreadsheet_id = \"s\"\nwrite_strategy = \"replace\"\nranges = { a = \"A!A:C\", b = \"B!A:C\" }\ncolumn_names = [\"x\", \"y\", \"z\"]\n";
    let test_cases = [
        ParseProjectErrorTestCase {
            description: "each range's table entry sets header = false",
            text: "[sources.ref.tables.a]\nheader = false\n[sources.ref.tables.b]\nheader = false\n",
            expected_message: "",
        },
        ParseProjectErrorTestCase {
            description: "the source sets header = false for every table",
            text: "header = false\n",
            expected_message: "",
        },
    ];
    for test_case in test_cases {
        let text = format!("{CONNECTIONS}{sheets}{}", test_case.text);
        let error = parse_project(&text, &environment)
            .err()
            .map(|error| error.to_string())
            .unwrap_or_default();
        assert_eq!(
            error, test_case.expected_message,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_motherduck_external_stage_when_parsing_then_it_stages_in_a_bucket_motherduck_reads() {
    let s3 = |bucket: &str, region: Option<&str>| StoreKind::S3 {
        bucket: bucket.to_owned(),
        region: region.map(str::to_owned),
        endpoint: None,
        profile: None,
    };
    let at = |store: StoreKind| Location {
        store,
        prefix: "md/".to_owned(),
    };
    let test_cases = [
        MotherDuckStageTestCase {
            description: "an s3 prefix, keys from the environment",
            text: md_stage("{ type = \"external_stage\", url = \"s3://lake/md/\" }", ""),
            expected_stage: Some(Some((at(s3("lake", None)), false))),
            expected_error: "",
        },
        MotherDuckStageTestCase {
            description: "an s3 prefix with an s3 connection's settings, kept",
            text: md_stage(
                "{ type = \"external_stage\", url = \"s3://lake/md/\", connection = \"lake\", keep_staged_files = true }",
                LAKE,
            ),
            expected_stage: Some(Some((at(s3("lake", Some("eu-west-1"))), true))),
            expected_error: "",
        },
        MotherDuckStageTestCase {
            description: "a gs prefix with a gcs connection's HMAC key",
            text: md_stage(
                "{ type = \"external_stage\", url = \"gs://lake/md/\", connection = \"gcs\" }",
                "[connections.gcs]\nkind = \"gcs\"\nbucket = \"lake\"\nhmac_key_id = \"K\"\nhmac_secret = \"S\"\n",
            ),
            expected_stage: Some(Some((
                at(StoreKind::Gcs {
                    bucket: "lake".to_owned(),
                    service_account_key: None,
                    endpoint: None,
                    hmac: Some(HmacKey {
                        key_id: "K".to_owned(),
                        secret: Secret("S".to_owned()),
                    }),
                }),
                false,
            ))),
            expected_error: "",
        },
        MotherDuckStageTestCase {
            description: "local sends from this machine",
            text: md_stage("{ type = \"local\" }", ""),
            expected_stage: Some(None),
            expected_error: "",
        },
        MotherDuckStageTestCase {
            description: "gs needs a gcs connection's HMAC key",
            text: md_stage("{ type = \"external_stage\", url = \"gs://lake/md/\" }", ""),
            expected_stage: None,
            expected_error: "[connections.md] load_method url \"gs://lake/md/\": MotherDuck reads GCS only with an HMAC key; set connection to a gcs connection with hmac_key_id and hmac_secret",
        },
        MotherDuckStageTestCase {
            description: "a custom endpoint is out of MotherDuck's reach",
            text: md_stage(
                "{ type = \"external_stage\", url = \"s3://lake/md/\", connection = \"lake\" }",
                "[connections.lake]\nkind = \"s3\"\nbucket = \"lake\"\nendpoint = \"http://localhost:9000\"\n",
            ),
            expected_stage: None,
            expected_error: "[connections.md] load_method stages in a bucket with a custom endpoint, which MotherDuck's servers don't reach: stage in AWS S3 or GCS",
        },
        MotherDuckStageTestCase {
            description: "a local folder is no stage",
            text: md_stage("{ type = \"external_stage\", url = \"/tmp/md/\" }", ""),
            expected_stage: None,
            expected_error: "[connections.md] load_method url \"/tmp/md/\" must be an s3:// or gs:// URL, which MotherDuck's servers read",
        },
        MotherDuckStageTestCase {
            description: "the connection must be a bucket's",
            text: md_stage(
                "{ type = \"external_stage\", url = \"s3://lake/md/\", connection = \"files\" }",
                "[connections.files]\nkind = \"filesystem\"\nroot = \"/data\"\n",
            ),
            expected_stage: None,
            expected_error: "[connections.md] load_method connection \"files\" must name an s3 or gcs connection to the bucket of \"s3://lake/md/\"",
        },
        MotherDuckStageTestCase {
            description: "the connection's bucket must be the url's",
            text: md_stage(
                "{ type = \"external_stage\", url = \"s3://other/md/\", connection = \"lake\" }",
                LAKE,
            ),
            expected_stage: None,
            expected_error: "[connections.md] load_method url \"s3://other/md/\":",
        },
        MotherDuckStageTestCase {
            description: "an unknown setting is refused",
            text: md_stage(
                "{ type = \"external_stage\", url = \"s3://lake/md/\", name = \"N\" }",
                "",
            ),
            expected_stage: None,
            expected_error: "unknown field `name`",
        },
    ];
    for test_case in test_cases {
        let found = parse_project(&test_case.text, &|_| None)
            .map_err(|error| error.to_string())
            .and_then(|project| motherduck_stage(&project, "md"));
        assert_eq!(
            found.as_ref().ok(),
            test_case.expected_stage.as_ref(),
            "{}: {found:?}",
            test_case.description
        );
        assert!(
            found
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_error),
            "{}",
            test_case.description
        );
    }
}
