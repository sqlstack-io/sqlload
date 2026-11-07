use crate::contract::tests::helpers::other_warehouse_sources;
use crate::contract::tests::test_types::OtherWarehouseTestCase;

#[test]
fn given_a_from_target_when_generating_then_sources_whose_own_warehouse_differs_are_named() {
    let test_cases = [
        OtherWarehouseTestCase {
            description: "a Snowflake --from names the source loading into DuckDB",
            from: Some("prod"),
            expected_sources: &["orders"],
        },
        OtherWarehouseTestCase {
            description: "another DuckDB target names the source loading into Snowflake",
            from: Some("test"),
            expected_sources: &["events"],
        },
        OtherWarehouseTestCase {
            description: "without --from every table reads its own target",
            from: None,
            expected_sources: &[],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            other_warehouse_sources(test_case.from),
            test_case.expected_sources,
            "{}",
            test_case.description
        );
    }
}
