use crate::project::main::parse_project::parse_project;
use crate::project::tests::helpers::{
    CONNECTIONS, FILES_PROJECT, PROJECT, SNOWFLAKE_SOURCE, environment,
};
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
            expected_value: "FilesEncoding { format: Csv, compression: Zstd, csv: CsvOptions { delimiter: ';', header: true, quote: '\"', null_values: [\"NA\", \"\"] }, record: None }",
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
            expected_message: "[targets.other] path, format, compression, csv, single_file and max_footer_reads are for targets on an s3 or filesystem connection",
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
            description: "a query table is for snowflake, odbc and bigquery sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n[sources.app.tables.\"public.a\"]\nsql = \"SELECT 1\"\n",
            expected_message: "[sources.app] sql is for kind = \"snowflake\", \"odbc\" and \"bigquery\"",
        },
        ParseProjectErrorTestCase {
            description: "a source's project is for bigquery sources",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nproject = \"other\"\n",
            expected_message: "[sources.app] project is for kind = \"bigquery\"",
        },
        ParseProjectErrorTestCase {
            description: "an empty AWS profile is never read as a profile named \"\"",
            text: "[connections.lake]\nkind = \"s3\"\nbucket = \"b\"\nprofile = \"\"\n",
            expected_message: "[connections.lake] profile is empty; omit it",
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
            expected_message: "[sources.app] format, compression, csv, shape and record_column are for kind = \"files\"",
        },
        ParseProjectErrorTestCase {
            description: "batch_interval is for postgres_cdc sources, never silently ignored",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\nbatch_interval = \"1m\"\n",
            expected_message: "[sources.app] publication, slot, cdc_mode, deletes and batch_interval are for kind = \"postgres_cdc\"",
        },
        ParseProjectErrorTestCase {
            description: "a batch_interval just under a second is refused",
            text: "[sources.app]\nkind = \"postgres_cdc\"\nconnection = \"pg\"\ninclude = [\"*\"]\nbatch_interval = \"999ms\"\n",
            expected_message: "[sources.app] batch_interval must be at least 1s",
        },
        ParseProjectErrorTestCase {
            description: "copy_to_internal_stage is for files sources, never silently ignored",
            text: "[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\ncopy_to_internal_stage = true\n",
            expected_message: "[sources.app] path, partition_columns, path_columns, unmatched_paths, constant_columns and copy_to_internal_stage are for kind = \"files\"",
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
            expected_message: "[sources.app] format, compression, csv, shape and record_column are for kind = \"files\"",
        },
        ParseProjectErrorTestCase {
            description: "unmatched_paths without path_columns is never silently ignored",
            text: "[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"events/\"\nunmatched_paths = \"skip\"\n",
            expected_message: "[sources.events] unmatched_paths is for path_columns, which is not set",
        },
        ParseProjectErrorTestCase {
            description: "a target cannot load into an HTTP API",
            text: "[connections.api]\nkind = \"http\"\nbase_url = \"https://api.example.com\"\n[targets.other]\nconnection = \"api\"\nloader_schema = \"RAW\"\n",
            expected_message: "is a http connection; targets load into snowflake, databricks, bigquery, clickhouse, duckdb, postgres or write files to s3 or filesystem",
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
