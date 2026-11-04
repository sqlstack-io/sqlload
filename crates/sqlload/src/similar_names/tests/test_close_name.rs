use crate::similar_names::main::close_name::close_name;
use crate::similar_names::tests::test_types::CloseNameTestCase;

#[test]
fn given_a_name_when_looking_for_a_close_candidate_then_typos_and_case_changes_match() {
    let test_cases = [
        CloseNameTestCase {
            description: "two swapped letters are close",
            name: "emial",
            candidates: &["id", "email"],
            expected_close: Some("email"),
        },
        CloseNameTestCase {
            description: "a different case is close",
            name: "Email",
            candidates: &["email"],
            expected_close: Some("email"),
        },
        CloseNameTestCase {
            description: "an unrelated name is not close",
            name: "coupon_code",
            candidates: &["id", "email"],
            expected_close: None,
        },
        CloseNameTestCase {
            description: "two edits in a short name are not close",
            name: "ab",
            candidates: &["id"],
            expected_close: None,
        },
        CloseNameTestCase {
            description: "the name itself is no suggestion",
            name: "email",
            candidates: &["email"],
            expected_close: None,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            close_name(test_case.name, test_case.candidates.iter().copied()),
            test_case.expected_close,
            "{}",
            test_case.description
        );
    }
}
