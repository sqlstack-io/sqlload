use crate::external_stage::tests::helpers::{rust_hash, snowflake_hash_sql};
use crate::external_stage::tests::test_types::HashParityTestCase;

#[test]
fn given_fixed_values_when_hashing_in_rust_and_in_sql_then_both_give_snowflakes_sha2_digest()
-> Result<(), String> {
    let test_cases = [
        HashParityTestCase {
            description: "empty text",
            value: "",
            expected_digest: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        },
        HashParityTestCase {
            description: "ASCII",
            value: "abc",
            expected_digest: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        },
        HashParityTestCase {
            description: "two-byte UTF-8",
            value: "é",
            expected_digest: "4a99557e4033c3539de2eb65472017cad5f9557f7a0625a09f1c3f6e2ba69c4c",
        },
        HashParityTestCase {
            description: "multi-byte UTF-8 and a space",
            value: "Ünïcødé 日本",
            expected_digest: "583235591ae91429902eb7cb7528bf5da63a05f70432a24f5366be015a7b0303",
        },
        HashParityTestCase {
            description: "a quote and a backslash, which SQL literals escape",
            value: "o'brien \\ x",
            expected_digest: "20f151849d72328a55e2d0ccf39711aac534ebb84b2bf91612e902afe18378d4",
        },
    ];
    let sql = snowflake_hash_sql();
    for test_case in test_cases {
        assert_eq!(
            rust_hash(test_case.value)?,
            test_case.expected_digest,
            "{}",
            test_case.description
        );
        assert!(
            sql.contains("SELECT SHA2(\"NAME\", 256) FROM"),
            "{}: SQL hashes with SHA2(text, 256), the lowercase hex SHA-256 of the UTF-8 text: {sql}",
            test_case.description
        );
    }
    Ok(())
}
