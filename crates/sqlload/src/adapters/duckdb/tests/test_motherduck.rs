use crate::adapters::contract::models::TableName;
use crate::adapters::contract::types::SqlAdapter;
use crate::adapters::dispatch::main::route::writer_accepts::writer_accepts;
use crate::adapters::dispatch::main::session::connect_destination::connect_destination;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::duckdb::main::_describe_table::describe_table;
use crate::adapters::duckdb::main::_open_duckdb::open_duckdb;
use crate::adapters::duckdb::main::_open_motherduck::{
    chosen_token, connection_string, motherduck_path, query_value, refused,
};
use crate::adapters::duckdb::models::DuckDbSql;
use crate::adapters::duckdb::tests::test_types::{
    MotherDuckPathTestCase, MotherDuckRefusalTestCase, MotherDuckTokenTestCase,
};
use crate::auth::models::Secret;
use crate::loader::_helpers::resolve::source_target;
use crate::project::main::parse_project::parse_project;

/// What DuckDB 2.0.0 reports when MotherDuck's extension refuses to load on it.
const UNSUPPORTED_VERSION: &str = "Invalid Input Error: Initialization function \"motherduck_duckdb_cpp_init\" from file \"/home/u/.duckdb/extensions/v2.0.0/linux_amd64/motherduck.duckdb_extension\" threw an exception: \"\nYour DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported version is v1.5.6. Please downgrade to use MotherDuck.\n\nSee https://motherduck.com/docs/getting-started/connect-query-from-duckdb-cli for more information.\"";

#[test]
fn given_a_motherduck_database_when_naming_its_path_then_one_md_prefix_leads_and_tokens_are_refused()
 {
    let test_cases = [
        MotherDuckPathTestCase {
            description: "a bare name gains md:",
            database: "shop",
            expected: Ok("md:shop"),
        },
        MotherDuckPathTestCase {
            description: "a name already starting with md: keeps one",
            database: "md:shop",
            expected: Ok("md:shop"),
        },
        MotherDuckPathTestCase {
            description: "DuckDB's motherduck: alias becomes md:",
            database: "motherduck:shop",
            expected: Ok("md:shop"),
        },
        MotherDuckPathTestCase {
            description: "a prefix in any case is MotherDuck's, as DuckDB matches it",
            database: "MotherDuck:shop",
            expected: Ok("md:shop"),
        },
        MotherDuckPathTestCase {
            description: "a token option under another spelling is refused too",
            database: "MD:shop?attach_mode=single&TOKEN=tok.en-1",
            expected: Err(
                "MotherDuck: a motherduck connection's database holds a token; set it in token instead",
            ),
        },
        MotherDuckPathTestCase {
            description: "no name opens the account's default database",
            database: "",
            expected: Ok("md:"),
        },
        MotherDuckPathTestCase {
            description: "a token in the name is refused without repeating it",
            database: "md:shop?motherduck_token=tok.en-1",
            expected: Err(
                "MotherDuck: a motherduck connection's database holds a token; set it in token instead",
            ),
        },
    ];
    for test_case in test_cases {
        let path = motherduck_path(test_case.database).map_err(|error| error.to_string());
        assert_eq!(
            path,
            test_case.expected.map(str::to_owned).map_err(str::to_owned),
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
            expected: Some("from-config"),
        },
        MotherDuckTokenTestCase {
            description: "motherduck_token comes before MOTHERDUCK_TOKEN",
            token: None,
            variables: &[("motherduck_token", "lower"), ("MOTHERDUCK_TOKEN", "upper")],
            expected: Some("lower"),
        },
        MotherDuckTokenTestCase {
            description: "an empty motherduck_token falls through to MOTHERDUCK_TOKEN",
            token: None,
            variables: &[("motherduck_token", ""), ("MOTHERDUCK_TOKEN", "upper")],
            expected: Some("upper"),
        },
        MotherDuckTokenTestCase {
            description: "an empty connection token is no token",
            token: Some(""),
            variables: &[("MOTHERDUCK_TOKEN", "upper")],
            expected: None,
        },
        MotherDuckTokenTestCase {
            description: "nothing set is no token",
            token: None,
            variables: &[],
            expected: None,
        },
    ];
    for test_case in test_cases {
        let token = test_case.token.map(|token| Secret(token.to_owned()));
        let chosen = chosen_token(token.as_ref(), |name| {
            test_case
                .variables
                .iter()
                .find(|(variable, _)| *variable == name)
                .map(|(_, value)| (*value).to_owned())
        });
        assert_eq!(
            chosen.as_deref(),
            test_case.expected,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_path_and_a_token_when_building_the_connection_string_then_the_token_is_one_encoded_option()
 {
    assert_eq!(
        connection_string("md:shop", "a&b=c"),
        "md:shop?motherduck_token=a%26b%3Dc"
    );
    assert_eq!(
        connection_string("md:shop?attach_mode=single", "t"),
        "md:shop?attach_mode=single&motherduck_token=t"
    );
}

#[test]
fn given_a_motherduck_target_when_connecting_then_its_database_overrides_the_connection_and_tables_carry_no_catalog()
-> Result<(), String> {
    let project = parse_project(
        r#"
[connections.cloud]
kind = "motherduck"
database = "shop"
token = ""

[connections.files]
kind = "filesystem"
root = "/tmp"

[targets.prod]
connection = "cloud"
database = "md:shop_prod"
loader_schema = "raw"

[sources.orders]
kind = "files"
description = "orders"
connection = "files"
path = "orders/"
"#,
        &|_| None,
    )
    .map_err(|error| error.to_string())?;
    let error = tokio::runtime::Runtime::new()
        .map_err(|error| error.to_string())?
        .block_on(connect_destination(&project, "prod"))
        .err()
        .map(|error| error.to_string())
        .unwrap_or_default();
    assert!(
        error.contains("md:shop_prod needs an access token"),
        "the target's database is opened: {error}"
    );
    let source = project
        .sources
        .get("orders")
        .ok_or("the source is parsed")?;
    let target = source_target(&project, source)?;
    assert_eq!(
        target.database.as_deref(),
        Some("shop_prod"),
        "tables are named with the bare name of the target's database"
    );
    let unnamed = parse_project(
        r#"
[connections.cloud]
kind = "motherduck"

[targets.prod]
connection = "cloud"
loader_schema = "raw"
"#,
        &|_| None,
    )
    .err()
    .map(|error| error.to_string())
    .unwrap_or_default();
    assert!(
        unnamed.contains("[targets.prod] loads into MotherDuck, which attaches every database in the account: name the database, in the target or its connection"),
        "a target naming no database is refused: {unnamed}"
    );
    Ok(())
}

#[test]
fn given_motherduck_does_not_open_when_reporting_why_then_its_own_words_are_relayed_without_the_token()
 {
    let test_cases = [
        MotherDuckRefusalTestCase {
            description: "the extension refusing this DuckDB version is relayed on one line",
            error: UNSUPPORTED_VERSION.to_owned(),
            token: "tok.en-1",
            expected: "MotherDuck: cannot open md:shop: MotherDuck doesn't accept the DuckDB version this SQLLoad bundles yet, so no SQLLoad release can connect to it until it does. MotherDuck says: Your DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported version is v1.5.6. Please downgrade to use MotherDuck. See https://motherduck.com/docs/getting-started/connect-query-from-duckdb-cli for more information.",
        },
        MotherDuckRefusalTestCase {
            description: "another error that quotes the connection string loses the token",
            error: "IO Error: Cannot open md:shop?motherduck_token=tok.en-1".to_owned(),
            token: "tok.en-1",
            expected: "MotherDuck: cannot open md:shop: IO Error: Cannot open md:shop?motherduck_token=<redacted>",
        },
        MotherDuckRefusalTestCase {
            description: "the token as sent, percent-encoded, is taken out too",
            error: "IO Error: Cannot open md:shop?motherduck_token=tok.en-1%26x".to_owned(),
            token: "tok.en-1&x",
            expected: "MotherDuck: cannot open md:shop: IO Error: Cannot open md:shop?motherduck_token=<redacted>",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            refused(&test_case.error, test_case.token, "md:shop").to_string(),
            test_case.expected,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_motherduck_destination_when_routing_then_it_takes_only_arrow_batches()
-> Result<(), String> {
    let local = open_duckdb(":memory:").map_err(|error| error.to_string())?;
    let mut motherduck = local.clone();
    motherduck.motherduck = true;
    let local = writer_accepts(&Destination::DuckDb(local), None);
    let motherduck = writer_accepts(&Destination::DuckDb(motherduck), None);
    assert!(
        !local.pulls_from.is_empty(),
        "local DuckDB reads Parquet itself"
    );
    assert!(motherduck.arrow, "MotherDuck takes Arrow batches");
    assert!(
        motherduck.pulls_from.is_empty() && motherduck.reads_files_itself.is_none(),
        "MotherDuck never reads files itself: {motherduck:?}"
    );
    Ok(())
}

#[test]
fn given_a_token_with_query_characters_when_opening_then_it_is_percent_encoded() {
    assert_eq!(
        query_value("eyJ.a-b_c~d&e=f#g h/+"),
        "eyJ.a-b_c~d%26e%3Df%23g%20h%2F%2B"
    );
}

#[test]
fn given_a_duckdb_connection_when_its_database_is_a_motherduck_path_then_it_is_refused() {
    for path in ["md:shop", "motherduck:shop", "MD:shop"] {
        let error = open_duckdb(path).err().map(|error| error.to_string());
        assert_eq!(
            error.as_deref(),
            Some(
                "MotherDuck: a duckdb connection's database is a file or :memory:; for a MotherDuck database use kind = \"motherduck\""
            ),
            "{path}"
        );
    }
}

#[test]
fn given_another_attached_database_with_the_same_table_when_reading_the_schema_then_only_the_current_database_counts()
-> Result<(), String> {
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
    let staging_query = DuckDbSql.list_staging_relations_sql(&table, None);
    let staged = {
        let connection = destination
            .connection
            .lock()
            .map_err(|error| error.to_string())?;
        connection
            .execute_batch(
                "ATTACH ':memory:' AS other;
                 CREATE SCHEMA raw; CREATE TABLE raw.orders (id INTEGER);
                 CREATE SCHEMA other.raw; CREATE TABLE other.raw.orders (id INTEGER, extra INTEGER);
                 CREATE TABLE other.raw.orders__sqlload_staging_20260101 (id INTEGER);
                 ATTACH ':memory:' AS raw;",
            )
            .map_err(|error| error.to_string())?;
        connection
            .execute(&insert, [])
            .map_err(|error| format!("the qualified name binds to this database: {error}"))?;
        let mut statement = connection
            .prepare(&staging_query)
            .map_err(|error| error.to_string())?;
        statement
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?
    };
    let columns = tokio::runtime::Runtime::new()
        .map_err(|error| error.to_string())?
        .block_on(describe_table(&destination, &table))
        .map_err(|error| error.to_string())?
        .unwrap_or_default();
    let names: Vec<&str> = columns.iter().map(|column| column.name.as_str()).collect();
    assert_eq!(
        names,
        ["id"],
        "the other database's extra column is not ours"
    );
    assert!(
        staged.is_empty(),
        "the other database's staging tables are not listed: {staged:?}"
    );
    Ok(())
}
