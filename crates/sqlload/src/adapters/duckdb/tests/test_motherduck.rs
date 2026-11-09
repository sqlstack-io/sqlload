use crate::adapters::dispatch::main::route::writer_accepts::writer_accepts;
use crate::adapters::dispatch::models::Destination;
use crate::adapters::duckdb::main::_open_duckdb::open_duckdb;
use crate::adapters::duckdb::main::_open_motherduck::{open_motherduck, refused};
use crate::adapters::duckdb::tests::test_types::{
    MotherDuckRefusalTestCase, MotherDuckTokenTestCase,
};
use crate::auth::models::Secret;

/// What DuckDB 2.0.0 reports when MotherDuck's extension refuses to load on it.
const UNSUPPORTED_VERSION: &str = "Invalid Input Error: Initialization function \"motherduck_duckdb_cpp_init\" from file \"/home/u/.duckdb/extensions/v2.0.0/linux_amd64/motherduck.duckdb_extension\" threw an exception: \"\nYour DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported version is v1.5.6. Please downgrade to use MotherDuck.\n\nSee https://motherduck.com/docs/getting-started/connect-query-from-duckdb-cli for more information.\"";

#[test]
fn given_a_motherduck_connection_when_its_token_is_empty_then_it_is_refused_before_connecting() {
    let test_cases = [
        MotherDuckTokenTestCase {
            description: "a bare name gains md:",
            database: Some("shop"),
            expected_error: "MotherDuck: md:shop needs an access token: set token in the connection, or the motherduck_token environment variable",
        },
        MotherDuckTokenTestCase {
            description: "a name already starting with md: keeps one",
            database: Some("md:shop"),
            expected_error: "MotherDuck: md:shop needs an access token: set token in the connection, or the motherduck_token environment variable",
        },
        MotherDuckTokenTestCase {
            description: "no name opens the account's default database",
            database: None,
            expected_error: "MotherDuck: md: needs an access token: set token in the connection, or the motherduck_token environment variable",
        },
    ];
    for test_case in test_cases {
        let error = open_motherduck(test_case.database, Some(&Secret(String::new())))
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
fn given_motherduck_does_not_open_when_reporting_why_then_its_own_words_are_relayed_without_the_token()
 {
    let test_cases = [
        MotherDuckRefusalTestCase {
            description: "the extension refusing this DuckDB version is relayed on one line",
            error: UNSUPPORTED_VERSION.to_owned(),
            expected: "MotherDuck: cannot open md:shop: MotherDuck doesn't accept the DuckDB version this SQLLoad bundles yet, so no SQLLoad release can connect to it until it does. MotherDuck says: Your DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported version is v1.5.6. Please downgrade to use MotherDuck. See https://motherduck.com/docs/getting-started/connect-query-from-duckdb-cli for more information.",
        },
        MotherDuckRefusalTestCase {
            description: "another error that quotes the connection string loses the token",
            error: "IO Error: Cannot open md:shop?motherduck_token=tok.en-1".to_owned(),
            expected: "MotherDuck: cannot open md:shop: IO Error: Cannot open md:shop?motherduck_token=<redacted>",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            refused(&test_case.error, "tok.en-1", "md:shop").to_string(),
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
