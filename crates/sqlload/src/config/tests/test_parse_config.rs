use crate::config::main::parse_config::parse_config;
use crate::config::tests::helpers::{BASE, lookup};
use crate::config::tests::test_types::{
    DestinationNameTestCase, ParseConfigErrorTestCase, ParseConfigTestCase,
};

#[test]
fn given_valid_config_text_when_parsing_then_defaults_fill_the_envelope() -> Result<(), String> {
    let test_cases = [
        ParseConfigTestCase {
            description: "a minimal file gets the nine default envelope columns",
            text: "[staging]\nkind = \"none\"\n",
            expected_columns: 9,
        },
        ParseConfigTestCase {
            description: "custom columns replace the defaults",
            text: "[staging]\nkind = \"internal\"\n[[envelope.columns]]\nname = \"PAYLOAD\"\nfield = \"value\"\ntype = \"VARIANT\"\n",
            expected_columns: 1,
        },
        ParseConfigTestCase {
            description: "sizes accept units",
            text: "[staging]\nkind = \"none\"\n[batching]\nmax_bytes = \"3.5MiB\"\nmax_request_bytes = 1000000\n",
            expected_columns: 9,
        },
    ];
    for test_case in &test_cases {
        let config = parse_config(&format!("{BASE}{}", test_case.text), &lookup)
            .map_err(|error| error.to_string())?;
        assert_eq!(
            config.envelope.columns.len(),
            test_case.expected_columns,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_invalid_config_text_when_parsing_then_the_error_names_the_problem() {
    let test_cases = [
        ParseConfigErrorTestCase {
            description: "unknown keys are rejected",
            text: "[staging]\nkind = \"none\"\n[batching]\nmax_bites = 1\n",
            expected_error: "unknown field",
        },
        ParseConfigErrorTestCase {
            description: "an unset environment variable is an error",
            text: "[staging]\nkind = \"none\"\n[metrics]\nlisten = \"${MISSING}\"\n",
            expected_error: "MISSING is not set",
        },
        ParseConfigErrorTestCase {
            description: "s3 staging needs its bucket section",
            text: "[staging]\nkind = \"s3\"\n",
            expected_error: "needs a [staging.s3] section",
        },
        ParseConfigErrorTestCase {
            description: "streaming requests cannot exceed the service limit",
            text: "[staging]\nkind = \"none\"\n[batching]\nmax_request_bytes = \"5MiB\"\n",
            expected_error: "cannot exceed 4MiB",
        },
        ParseConfigErrorTestCase {
            description: "duplicate column names are rejected",
            text: "[staging]\nkind = \"none\"\n[[envelope.columns]]\nname = \"A\"\nfield = \"topic\"\ntype = \"VARCHAR\"\n[[envelope.columns]]\nname = \"a\"\nfield = \"key\"\ntype = \"VARCHAR\"\n",
            expected_error: "appears twice",
        },
        ParseConfigErrorTestCase {
            description: "unknown size units are rejected",
            text: "[staging]\nkind = \"none\"\n[batching]\nmax_bytes = \"3 parsecs\"\n",
            expected_error: "unknown size unit",
        },
    ];
    for test_case in &test_cases {
        let error = parse_config(&format!("{BASE}{}", test_case.text), &lookup)
            .expect_err(test_case.description);
        assert!(
            error.to_string().contains(test_case.expected_error),
            "{}: {error}",
            test_case.description
        );
    }
}

#[test]
fn given_destination_names_when_parsing_then_plain_names_are_upper_cased_and_others_rejected() {
    let test_cases = [
        DestinationNameTestCase {
            description: "a lower-case schema is stored as Snowflake resolves it",
            schema: "raw_kafka",
            expected_outcome: "RAW_KAFKA",
        },
        DestinationNameTestCase {
            description: "a schema that would need quoting is rejected",
            schema: "raw-kafka",
            expected_outcome: "must be an unquoted identifier",
        },
    ];
    for test_case in &test_cases {
        let text = format!(
            "{}[staging]\nkind = \"none\"\n",
            BASE.replace(
                "schema = \"RAW\"",
                &format!("schema = \"{}\"", test_case.schema)
            )
        );
        let outcome = parse_config(&text, &lookup)
            .map(|config| config.destination.schema)
            .unwrap_or_else(|error| error.to_string());
        assert!(
            outcome.contains(test_case.expected_outcome),
            "{}: {outcome}",
            test_case.description
        );
    }
}
