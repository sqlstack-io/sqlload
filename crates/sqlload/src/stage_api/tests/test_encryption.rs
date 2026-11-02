use base64::Engine;
use base64::engine::general_purpose::STANDARD;

use crate::auth::models::Secret;
use crate::stage_api::_helpers::encryption::{decrypt, encrypt};
use crate::stage_api::models::EncryptionMaterial;
use crate::stage_api::tests::test_types::EncryptionTestCase;

#[test]
fn given_stage_material_when_encrypting_then_the_file_round_trips_with_driver_metadata()
-> Result<(), String> {
    let test_cases = [
        EncryptionTestCase {
            description: "an empty file still gets one padding block",
            key_bytes: 32,
            plaintext_bytes: 0,
            expected_ciphertext_bytes: 16,
            expected_matdesc: r#"{"queryId":"01b2c3d4-0000-0000-0000-000000000000","smkId":"12345","keySize":"256"}"#,
        },
        EncryptionTestCase {
            description: "a whole number of blocks gains a full padding block",
            key_bytes: 32,
            plaintext_bytes: 32,
            expected_ciphertext_bytes: 48,
            expected_matdesc: r#"{"queryId":"01b2c3d4-0000-0000-0000-000000000000","smkId":"12345","keySize":"256"}"#,
        },
        EncryptionTestCase {
            description: "a 128-bit master key works too",
            key_bytes: 16,
            plaintext_bytes: 100_003,
            expected_ciphertext_bytes: 100_016,
            expected_matdesc: r#"{"queryId":"01b2c3d4-0000-0000-0000-000000000000","smkId":"12345","keySize":"128"}"#,
        },
    ];
    for test_case in &test_cases {
        let material = EncryptionMaterial {
            query_stage_master_key: Secret(STANDARD.encode(vec![7u8; test_case.key_bytes])),
            query_id: "01b2c3d4-0000-0000-0000-000000000000".to_owned(),
            smk_id: 12345,
        };
        let plaintext: Vec<u8> = (0..test_case.plaintext_bytes)
            .map(|index| (index % 251) as u8)
            .collect();
        let (ciphertext, metadata) = encrypt(&material, &plaintext)?;
        assert_eq!(
            ciphertext.len(),
            test_case.expected_ciphertext_bytes,
            "{}",
            test_case.description
        );
        assert_eq!(
            decrypt(&material, &metadata, &ciphertext)?,
            plaintext,
            "{}",
            test_case.description
        );
        assert_eq!(
            metadata.matdesc, test_case.expected_matdesc,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
