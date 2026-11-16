use crate::odbc_source::_helpers::catalog::search_patterns;
use crate::odbc_source::tests::test_types::SearchPatternsTestCase;

#[test]
fn given_an_include_pattern_when_looking_it_up_then_it_becomes_the_driver_s_search_patterns_or_none_when_no_lookup_stands_for_it()
 {
    let test_cases = [
        SearchPatternsTestCase {
            description: "an exact schema.table is the schema and the table",
            pattern: "PUB.action",
            expected_patterns: Some(("PUB", "action")),
        },
        SearchPatternsTestCase {
            description: "a schema with a table pattern: * is %",
            pattern: "PUB.*",
            expected_patterns: Some(("PUB", "%")),
        },
        SearchPatternsTestCase {
            description: "* is % and ? is _ on either side",
            pattern: "sales_?.tmp_*",
            expected_patterns: Some(("sales__", "tmp_%")),
        },
        SearchPatternsTestCase {
            description: "a literal _ or % stays the driver's wildcard, which lists more tables include then drops",
            pattern: "erp.order_line%",
            expected_patterns: Some(("erp", "order_line%")),
        },
        SearchPatternsTestCase {
            description: "a table without a schema, as a driver without schemas names it, in any schema",
            pattern: "events",
            expected_patterns: Some(("%", "events")),
        },
        SearchPatternsTestCase {
            description: "a * without a schema could match across a schema's dot",
            pattern: "erp*",
            expected_patterns: None,
        },
        SearchPatternsTestCase {
            description: "a ? without a schema could match the dot itself",
            pattern: "erp?items",
            expected_patterns: None,
        },
        SearchPatternsTestCase {
            description: "a character class",
            pattern: "erp.item[s]",
            expected_patterns: None,
        },
        SearchPatternsTestCase {
            description: "alternatives",
            pattern: "erp.{items,notes}",
            expected_patterns: None,
        },
        SearchPatternsTestCase {
            description: "an escaped character",
            pattern: "erp.a\\*b",
            expected_patterns: None,
        },
        SearchPatternsTestCase {
            description: "more than one dot",
            pattern: "db.erp.items",
            expected_patterns: None,
        },
    ];
    for test_case in test_cases {
        let patterns = search_patterns(test_case.pattern);
        assert_eq!(
            patterns
                .as_ref()
                .map(|(schema, table)| (schema.as_str(), table.as_str())),
            test_case.expected_patterns,
            "{}",
            test_case.description
        );
    }
}
