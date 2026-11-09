use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::dispatch::main::route::writer_accepts::writer_accepts;
use crate::adapters::dispatch::main::session::drop_stale_staging::drop_stale_staging;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::duckdb::_helpers::motherduck_connection::{
    chosen_token, connection_string, path_text, refused,
};
use crate::adapters::duckdb::main::_describe_table::describe_table;
use crate::adapters::duckdb::main::_open_duckdb::open_duckdb;
use crate::adapters::duckdb::main::_open_motherduck::open_motherduck;
use crate::adapters::duckdb::models::DuckDbSql;
use crate::adapters::duckdb::tests::test_types::{
    AttachedDatabaseTestCase, ConnectionStringTestCase, DuckDbPathTestCase, MotherDuckOpenTestCase,
    MotherDuckPathTestCase, MotherDuckRefusalTestCase, MotherDuckRouteTestCase,
    MotherDuckTokenTestCase, ResolvedDatabaseTestCase,
};
use crate::auth::models::Secret;
use crate::column_types::models::Warehouse;
use crate::loader::_helpers::resolve::source_target;
use crate::project::main::parse_project::parse_project;
use crate::project::models::ConnectionConfig;

/// What DuckDB 2.0.0 reports when MotherDuck's extension refuses to load on it.
const UNSUPPORTED_VERSION: &str = "Invalid Input Error: Initialization function \"motherduck_duckdb_cpp_init\" from file \"/home/u/.duckdb/extensions/v2.0.0/linux_amd64/motherduck.duckdb_extension\" threw an exception: \"\nYour DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported version is v1.5.6. Please downgrade to use MotherDuck.\n\nSee https://motherduck.com/docs/getting-started/connect-query-from-duckdb-cli for more information.\"";

/// The refusal of a database setting that holds a token.
const TOKEN_IN_DATABASE: &str =
    "MotherDuck: a motherduck connection's database holds a token; set it in token instead";

#[test]
fn given_a_motherduck_database_when_naming_its_path_then_one_md_prefix_leads_and_tokens_are_refused()
 {
    let test_cases = [
        MotherDuckPathTestCase {
            description: "a bare name gains md:",
            database: "shop",
            expected_path: Ok("md:shop"),
        },
        MotherDuckPathTestCase {
            description: "a name already starting with md: keeps one",
            database: "md:shop",
            expected_path: Ok("md:shop"),
        },
        MotherDuckPathTestCase {
            description: "a prefix in any case is MotherDuck's, as DuckDB matches it",
            database: "MotherDuck:shop",
            expected_path: Ok("md:shop"),
        },
        MotherDuckPathTestCase {
            description: "options are kept",
            database: "MD:shop?attach_mode=single",
            expected_path: Ok("md:shop?attach_mode=single"),
        },
        MotherDuckPathTestCase {
            description: "no name opens the account's default database",
            database: "",
            expected_path: Ok("md:"),
        },
        MotherDuckPathTestCase {
            description: "a token option under any spelling is refused without repeating it",
            database: "md:shop?attach_mode=single&TOKEN=tok.en-1",
            expected_path: Err(TOKEN_IN_DATABASE),
        },
    ];
    for test_case in test_cases {
        let path = path_text(test_case.database).map_err(|error| error.to_string());
        assert_eq!(
            path.as_deref().map_err(String::as_str),
            test_case.expected_path,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_tokens_in_the_connection_and_environment_when_choosing_then_the_first_non_empty_one_signs_in()
 {
    let test_cases = [
        MotherDuckTokenTestCase {
            description: "the connection's token wins over the environment",
            token: Some("from-config"),
            variables: &[("motherduck_token", "lower"), ("MOTHERDUCK_TOKEN", "upper")],
            expected_token: Some("from-config"),
        },
        MotherDuckTokenTestCase {
            description: "motherduck_token comes before MOTHERDUCK_TOKEN",
            token: None,
            variables: &[("motherduck_token", "lower"), ("MOTHERDUCK_TOKEN", "upper")],
            expected_token: Some("lower"),
        },
        MotherDuckTokenTestCase {
            description: "an empty motherduck_token falls through to MOTHERDUCK_TOKEN",
            token: None,
            variables: &[("motherduck_token", ""), ("MOTHERDUCK_TOKEN", "upper")],
            expected_token: Some("upper"),
        },
        MotherDuckTokenTestCase {
            description: "an empty connection token is no token",
            token: Some(""),
            variables: &[("MOTHERDUCK_TOKEN", "upper")],
            expected_token: None,
        },
        MotherDuckTokenTestCase {
            description: "nothing set is no token",
            token: None,
            variables: &[],
            expected_token: None,
        },
    ];
    for test_case in test_cases {
        let token = test_case.token.map(|token| Secret(token.to_owned()));
        let variables = test_case.variables;
        let chosen = chosen_token(token.as_ref(), |name| {
            variables
                .iter()
                .find(|(variable, _)| *variable == name)
                .map(|(_, value)| (*value).to_owned())
        });
        assert_eq!(
            chosen.as_deref(),
            test_case.expected_token,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_path_and_a_token_when_building_the_connection_string_then_the_token_is_one_encoded_option()
 {
    let test_cases = [
        ConnectionStringTestCase {
            description: "query characters in the token are percent-encoded",
            path: "md:shop",
            token: "eyJ.a-b_c~d&e=f#g h/+",
            expected_string: "md:shop?motherduck_token=eyJ.a-b_c~d%26e%3Df%23g%20h%2F%2B",
        },
        ConnectionStringTestCase {
            description: "a path with options gains the token after &",
            path: "md:shop?attach_mode=single",
            token: "t",
            expected_string: "md:shop?attach_mode=single&motherduck_token=t",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            connection_string(test_case.path, test_case.token),
            test_case.expected_string,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_motherduck_does_not_open_when_reporting_why_then_its_own_words_are_relayed_without_the_token()
 {
    let test_cases = [
        MotherDuckRefusalTestCase {
            description: "the extension refusing this DuckDB version is relayed on one line",
            error: UNSUPPORTED_VERSION.to_owned(),
            token: "tok.en-1",
            expected_message: "MotherDuck: cannot open md:shop: MotherDuck doesn't accept the DuckDB version this SQLLoad bundles yet, so no SQLLoad release can connect to it until it does. MotherDuck says: Your DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported version is v1.5.6. Please downgrade to use MotherDuck. See https://motherduck.com/docs/getting-started/connect-query-from-duckdb-cli for more information.",
        },
        MotherDuckRefusalTestCase {
            description: "another error that quotes the connection string loses the token",
            error: "IO Error: Cannot open md:shop?motherduck_token=tok.en-1".to_owned(),
            token: "tok.en-1",
            expected_message: "MotherDuck: cannot open md:shop: IO Error: Cannot open md:shop?motherduck_token=<redacted>",
        },
        MotherDuckRefusalTestCase {
            description: "the token as sent, percent-encoded, is taken out too",
            error: "IO Error: Cannot open md:shop?motherduck_token=tok.en-1%26x".to_owned(),
            token: "tok.en-1&x",
            expected_message: "MotherDuck: cannot open md:shop: IO Error: Cannot open md:shop?motherduck_token=<redacted>",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            refused(&test_case.error, test_case.token, "md:shop").to_string(),
            test_case.expected_message,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_motherduck_connection_when_opening_then_the_targets_database_wins_and_no_token_is_refused_first()
 {
    let test_cases = [
        MotherDuckOpenTestCase {
            description: "the connection's database opens when the target names none",
            connection_database: Some("shop"),
            target_database: None,
            expected_error: "MotherDuck: md:shop needs an access token: set token in the connection, or the motherduck_token or MOTHERDUCK_TOKEN environment variable",
        },
        MotherDuckOpenTestCase {
            description: "the target's database overrides the connection's",
            connection_database: Some("shop"),
            target_database: Some("md:shop_prod"),
            expected_error: "MotherDuck: md:shop_prod needs an access token: set token in the connection, or the motherduck_token or MOTHERDUCK_TOKEN environment variable",
        },
        MotherDuckOpenTestCase {
            description: "a token in the database is refused before anything else",
            connection_database: Some("md:shop?motherduck_token=tok.en-1"),
            target_database: None,
            expected_error: TOKEN_IN_DATABASE,
        },
    ];
    for test_case in test_cases {
        let connection = ConnectionConfig::MotherDuck {
            database: test_case.connection_database.map(str::to_owned),
            token: Some(Secret(String::new())),
        };
        let error = open_motherduck(&connection, test_case.target_database)
            .err()
            .map(|error| error.to_string());
        assert_eq!(
            error.as_deref(),
            Some(test_case.expected_error),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_motherduck_target_when_resolving_its_tables_then_they_are_named_with_the_bare_database()
{
    let test_cases = [
        ResolvedDatabaseTestCase {
            description: "the target's database, without md: and options, names the tables",
            connection_line: "database = \"shop\"",
            target_line: "database = \"md:shop_prod?attach_mode=single\"",
            expected_database: Ok("shop_prod"),
        },
        ResolvedDatabaseTestCase {
            description: "the connection's database names them when the target names none",
            connection_line: "database = \"MotherDuck:shop\"",
            target_line: "",
            expected_database: Ok("shop"),
        },
        ResolvedDatabaseTestCase {
            description: "a target naming no database is refused, as MotherDuck attaches every database",
            connection_line: "",
            target_line: "",
            expected_database: Err(
                "invalid project:\n  - [targets.prod] loads into MotherDuck, which attaches every database in the account: name the database, in the target or its connection",
            ),
        },
    ];
    for test_case in test_cases {
        let text = format!(
            "[connections.cloud]\nkind = \"motherduck\"\n{}\n\n[connections.files]\nkind = \"filesystem\"\nroot = \"/tmp\"\n\n[targets.prod]\nconnection = \"cloud\"\nloader_schema = \"raw\"\n{}\n\n[sources.orders]\nkind = \"files\"\ndescription = \"orders\"\nconnection = \"files\"\npath = \"orders/\"\n",
            test_case.connection_line, test_case.target_line
        );
        let resolved = parse_project(&text, &|_| None)
            .map_err(|error| error.to_string())
            .and_then(|project| {
                let source = project.sources.get("orders").ok_or("no source")?;
                source_target(&project, source)
            })
            .map(|target| target.database.unwrap_or_default());
        assert_eq!(
            resolved.as_deref().map_err(String::as_str),
            test_case.expected_database,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_duckdb_connection_when_its_database_is_a_motherduck_path_then_it_is_refused() {
    let refusal = "MotherDuck: a duckdb connection's database is a file or :memory:; for a MotherDuck database use kind = \"motherduck\"";
    let test_cases = [
        DuckDbPathTestCase {
            description: "an in-memory database opens",
            path: ":memory:",
            expected_error: None,
        },
        DuckDbPathTestCase {
            description: "md: is MotherDuck's",
            path: "md:shop",
            expected_error: Some(refusal),
        },
        DuckDbPathTestCase {
            description: "its motherduck: alias too",
            path: "motherduck:shop",
            expected_error: Some(refusal),
        },
        DuckDbPathTestCase {
            description: "in any case, as DuckDB matches it",
            path: "MD:shop",
            expected_error: Some(refusal),
        },
    ];
    for test_case in test_cases {
        let error = open_duckdb(test_case.path)
            .err()
            .map(|error| error.to_string());
        assert_eq!(
            error.as_deref(),
            test_case.expected_error,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_motherduck_destination_when_routing_then_it_takes_only_arrow_batches()
-> Result<(), String> {
    let test_cases = [
        MotherDuckRouteTestCase {
            description: "local DuckDB reads Parquet itself",
            motherduck: false,
            expected_reads_files: true,
        },
        MotherDuckRouteTestCase {
            description: "MotherDuck never reads files itself",
            motherduck: true,
            expected_reads_files: false,
        },
    ];
    for test_case in test_cases {
        let mut destination = open_duckdb(":memory:").map_err(|error| error.to_string())?;
        destination.motherduck = test_case.motherduck;
        let accepts = writer_accepts(&Destination::DuckDb(destination), None);
        assert!(accepts.arrow, "{}", test_case.description);
        assert_eq!(
            !accepts.pulls_from.is_empty() || accepts.reads_files_itself.is_some(),
            test_case.expected_reads_files,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_other_attached_databases_when_reading_the_schema_and_staging_then_only_the_current_database_counts()
-> Result<(), String> {
    let test_cases = [AttachedDatabaseTestCase {
        description: "another database holding the table, a stale staging table, and a database named like the schema",
        setup: "ATTACH ':memory:' AS other;
                CREATE SCHEMA other.raw;
                CREATE TABLE other.raw.orders (id INTEGER, extra INTEGER);
                CREATE TABLE other.raw.orders__sqlload_staging_20200101000000 (id INTEGER);
                CREATE TABLE raw.orders__sqlload_staging_20200101000000 (id INTEGER);
                ATTACH ':memory:' AS raw;",
        expected_columns: &["id"],
        expected_dropped: 1,
    }];
    let runtime = tokio::runtime::Runtime::new().map_err(|error| error.to_string())?;
    for test_case in test_cases {
        let destination = open_duckdb(":memory:").map_err(|error| error.to_string())?;
        let table = TableName {
            database: Some("memory".to_owned()),
            schema: "raw".to_owned(),
            table: "orders".to_owned(),
        };
        let insert = format!(
            "INSERT INTO {} VALUES (1)",
            DuckDbSql.render_qualified_name(&table)
        );
        {
            let connection = destination
                .connection
                .lock()
                .map_err(|error| error.to_string())?;
            connection
                .execute_batch("CREATE SCHEMA raw; CREATE TABLE raw.orders (id INTEGER);")
                .and_then(|()| connection.execute_batch(test_case.setup))
                .and_then(|()| connection.execute(&insert, []))
                .map_err(|error| format!("{}: {error}", test_case.description))?;
        }
        let columns = runtime
            .block_on(describe_table(&destination, &table))
            .map_err(|error| error.to_string())?
            .unwrap_or_default();
        let names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
        let dropped = runtime
            .block_on(drop_stale_staging(
                &Destination::DuckDb(destination.clone()),
                &table,
                Warehouse::DuckDb,
            ))
            .map_err(|error| format!("{}: {error}", test_case.description))?;
        assert_eq!(
            names, test_case.expected_columns,
            "{}",
            test_case.description
        );
        assert_eq!(
            dropped, test_case.expected_dropped,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
