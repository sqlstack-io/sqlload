use crate::postgres_source::_helpers::values::read_numeric;
use crate::postgres_source::tests::helpers::numeric_bytes;
use crate::postgres_source::tests::test_types::ReadNumericTestCase;

#[test]
fn given_a_binary_numeric_when_reading_at_a_scale_then_it_is_exact_or_rejected() {
    let test_cases = [
        ReadNumericTestCase {
            description: "12.34 at scale 2",
            digits: &[12, 3400],
            weight: 0,
            sign: 0x0000,
            precision: 10,
            scale: 2,
            expected_result: Ok(1234),
        },
        ReadNumericTestCase {
            description: "-0.5 at scale 2",
            digits: &[5000],
            weight: -1,
            sign: 0x4000,
            precision: 10,
            scale: 2,
            expected_result: Ok(-50),
        },
        ReadNumericTestCase {
            description: "123456789 at scale 0 spans three digits",
            digits: &[1, 2345, 6789],
            weight: 2,
            sign: 0x0000,
            precision: 18,
            scale: 0,
            expected_result: Ok(123_456_789),
        },
        ReadNumericTestCase {
            description: "20000 with trailing zero digits omitted",
            digits: &[2],
            weight: 1,
            sign: 0x0000,
            precision: 10,
            scale: 3,
            expected_result: Ok(20_000_000),
        },
        ReadNumericTestCase {
            description: "zero has no digits",
            digits: &[],
            weight: 0,
            sign: 0x0000,
            precision: 5,
            scale: 2,
            expected_result: Ok(0),
        },
        ReadNumericTestCase {
            description: "1.234 does not fit scale 2",
            digits: &[1, 2340],
            weight: 0,
            sign: 0x0000,
            precision: 10,
            scale: 2,
            expected_result: Err("without losing digits"),
        },
        ReadNumericTestCase {
            description: "1000.00 does not fit precision 5",
            digits: &[1000],
            weight: 0,
            sign: 0x0000,
            precision: 5,
            scale: 2,
            expected_result: Err("too large for decimal(5,2)"),
        },
        ReadNumericTestCase {
            description: "NaN is rejected",
            digits: &[],
            weight: 0,
            sign: 0xC000,
            precision: 5,
            scale: 2,
            expected_result: Err("NaN and infinite"),
        },
    ];
    for test_case in test_cases {
        let bytes = numeric_bytes(test_case.digits, test_case.weight, test_case.sign, 0);
        let result = read_numeric(&bytes, test_case.precision, test_case.scale);
        assert_eq!(
            result.as_ref().ok(),
            test_case.expected_result.as_ref().ok(),
            "{}",
            test_case.description
        );
        assert!(
            result
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_result.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
