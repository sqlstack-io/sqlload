use crate::loader::tests::helpers::{gcs_reader, transform_reader};
use crate::loader::tests::test_types::{GcsReaderTestCase, TransformReaderTestCase};

#[test]
fn given_a_files_table_with_a_transform_or_a_parse_when_routing_then_sqlload_reads_the_files_itself()
-> Result<(), String> {
    let test_cases = [
        TransformReaderTestCase {
            description: "a transform runs on each file, so no destination reads the files itself",
            transform: Some("metrics"),
            parse: None,
            expected_reason: Some("transform metrics runs on each file"),
        },
        TransformReaderTestCase {
            description: "without one, Parquet files on a local folder need no reader of SQLLoad's",
            transform: None,
            parse: None,
            expected_reason: None,
        },
        TransformReaderTestCase {
            description: "a parse runs in SQLLoad, so no destination reads the files itself",
            transform: None,
            parse: Some("updated"),
            expected_reason: Some("parse runs on updated"),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            transform_reader(&test_case)?.as_deref(),
            test_case.expected_reason,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_parquet_files_in_gcs_when_routing_into_duckdb_then_duckdb_reads_them_only_with_the_connections_hmac_key()
-> Result<(), String> {
    let test_cases = [
        GcsReaderTestCase {
            description: "with an HMAC key DuckDB reads the bucket itself",
            hmac: true,
            endpoint: None,
            expected_reason: None,
        },
        GcsReaderTestCase {
            description: "without one SQLLoad reads the files, as DuckDB takes no service-account key",
            hmac: false,
            endpoint: None,
            expected_reason: Some(
                "the gcs connection sets no hmac_key_id, which DuckDB and MotherDuck read GCS with",
            ),
        },
        GcsReaderTestCase {
            description: "an emulator's endpoint is SQLLoad's to follow, not DuckDB's",
            hmac: true,
            endpoint: Some("http://127.0.0.1:4443"),
            expected_reason: Some("the GCS connection sets a custom endpoint"),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            gcs_reader(&test_case)?.as_deref(),
            test_case.expected_reason,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
