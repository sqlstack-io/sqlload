use crate::loader::tests::helpers::transform_reader;
use crate::loader::tests::test_types::TransformReaderTestCase;

#[test]
fn given_a_files_table_with_a_transform_when_routing_then_sqlload_reads_the_files_itself()
-> Result<(), String> {
    let test_cases = [
        TransformReaderTestCase {
            description: "a transform runs on each file, so no destination reads the files itself",
            transform: Some("transforms.metrics:transform"),
            expected_reason: Some("transform transforms.metrics:transform runs on each file"),
        },
        TransformReaderTestCase {
            description: "without one, Parquet files on a local folder need no reader of SQLLoad's",
            transform: None,
            expected_reason: None,
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
