use crate::load_naming::main::dlt_identifier::dlt_identifier;
use crate::load_naming::main::dlt_table_path::dlt_table_path;
use crate::load_naming::tests::test_types::DltNameTestCase;

// Every expected name is what dlt 1.32.0 gives (`dlt.common.normalizers.naming.snake_case.NamingConvention`, with no limit or `max_length = 63`, Postgres's), run with Python.

#[test]
fn given_names_when_normalising_as_dlt_then_they_match_dlts_snake_case() {
    let test_cases = [
        DltNameTestCase {
            description: "an acronym at the end splits from the word before it",
            normalise: dlt_identifier,
            name: "OrderID",
            max_length: None,
            expected_name: "order_id",
        },
        DltNameTestCase {
            description: "an acronym inside splits on both sides, and a capitalised word after a consumed one does not",
            normalise: dlt_identifier,
            name: "getHTTPResponseCode",
            max_length: None,
            expected_name: "get_http_response_code",
        },
        DltNameTestCase {
            description: "a digit before a capital splits, a capital after a capital does not",
            normalise: dlt_identifier,
            name: "a1B",
            max_length: None,
            expected_name: "a1_b",
        },
        DltNameTestCase {
            description: "surrounding spaces are trimmed, and one inside becomes _",
            normalise: dlt_identifier,
            name: " a b ",
            max_length: None,
            expected_name: "a_b",
        },
        DltNameTestCase {
            description: "each trailing _ becomes x, before runs of _ are merged",
            normalise: dlt_identifier,
            name: "trailing__",
            max_length: None,
            expected_name: "trailingxx",
        },
        DltNameTestCase {
            description: "a replaced character after a literal _ counts as a second trailing _",
            normalise: dlt_identifier,
            name: "a_%",
            max_length: None,
            expected_name: "axx",
        },
        DltNameTestCase {
            description: "leading underscores merge but stay",
            normalise: dlt_identifier,
            name: "__leading",
            max_length: None,
            expected_name: "_leading",
        },
        DltNameTestCase {
            description: "a leading digit gets _ before it",
            normalise: dlt_identifier,
            name: "1st",
            max_length: None,
            expected_name: "_1st",
        },
        DltNameTestCase {
            description: "+ * @ | become x x a l and a run of - one _",
            normalise: dlt_identifier,
            name: "a+b*c@d|e--f",
            max_length: None,
            expected_name: "axbxcadle_f",
        },
        DltNameTestCase {
            description: "letters outside ASCII become _, and so does a superscript digit",
            normalise: dlt_identifier,
            name: "Ünïcödé x²",
            max_length: None,
            expected_name: "_n_c_d_xx",
        },
        DltNameTestCase {
            description: "a decimal digit of another script is kept, and counts as a leading digit",
            normalise: dlt_identifier,
            name: "٣rd",
            max_length: None,
            expected_name: "_٣rd",
        },
        DltNameTestCase {
            description: "dlt's own column names stay as they are",
            normalise: dlt_identifier,
            name: "_dlt_id",
            max_length: None,
            expected_name: "_dlt_id",
        },
        DltNameTestCase {
            description: "an empty or blank key is named _empty",
            normalise: dlt_identifier,
            name: "  ",
            max_length: None,
            expected_name: "_empty",
        },
        DltNameTestCase {
            description: "a name that fits the limit is not shortened",
            normalise: dlt_identifier,
            name: "UnitPrice",
            max_length: Some(63),
            expected_name: "unit_price",
        },
        DltNameTestCase {
            description: "past the limit, the head (a character longer) and tail are kept around a tag hashed from the original name",
            normalise: dlt_identifier,
            name: "LongCamelCaseIdentifierThatGoesOnAndOnBeyondTheSixtyThreeCharacterLimitOfPostgres",
            max_length: Some(63),
            expected_name: "long_camel_case_identifier_thvtajng_character_limit_of_postgres",
        },
        DltNameTestCase {
            description: "without a limit nothing is shortened",
            normalise: dlt_identifier,
            name: "LongCamelCaseIdentifierThatGoesOnAndOnBeyondTheSixtyThreeCharacterLimitOfPostgres",
            max_length: None,
            expected_name: "long_camel_case_identifier_that_goes_on_and_on_beyond_the_sixty_three_character_limit_of_postgres",
        },
        DltNameTestCase {
            description: "each table in a path is normalised, the __ between them kept",
            normalise: dlt_table_path,
            name: "Orders__line_items__SubParts",
            max_length: None,
            expected_name: "orders__line_items__sub_parts",
        },
        DltNameTestCase {
            description: "a path past the limit is shortened as a whole, with a tag of the original path",
            normalise: dlt_table_path,
            name: "orders__LineItemPartLineItemPartLineItemPartLineItemPartLineItemPartLineItemPart",
            max_length: Some(63),
            expected_name: "orders__line_item_part_line_iwskgbgine_item_part_line_item_part",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            (test_case.normalise)(test_case.name, test_case.max_length),
            test_case.expected_name,
            "{}",
            test_case.description
        );
    }
}
