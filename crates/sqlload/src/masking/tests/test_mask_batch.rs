use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Int64Array, RecordBatch, StringArray};

use crate::auth::models::Secret;
use crate::masking::main::mask_batch::mask_batch;
use crate::masking::models::MaskRules;
use crate::masking::tests::test_types::MaskBatchTestCase;
use crate::project::models::MaskMethod;

#[test]
fn given_a_masked_text_column_when_masking_then_values_are_digested_and_others_kept()
-> Result<(), String> {
    let test_cases = [
        MaskBatchTestCase {
            description: "hash gives the value's SHA-256 hex",
            method: MaskMethod::Hash,
            value: Some("a@example.com"),
            expected_value: Some(
                "08168cd80dfd534ab0f10af10f1303fe00af2d43ab5c1432360d137f8197e17a",
            ),
        },
        MaskBatchTestCase {
            description: "hmac gives the value's HMAC-SHA256 hex with the source key",
            method: MaskMethod::Hmac,
            value: Some("a@example.com"),
            expected_value: Some(
                "383b4dc91a515d13e32b8b9bdf8532309b8a89e727f4977fad749522153886e6",
            ),
        },
        MaskBatchTestCase {
            description: "NULL stays NULL",
            method: MaskMethod::Hmac,
            value: None,
            expected_value: None,
        },
    ];
    for test_case in test_cases {
        let rules = MaskRules {
            columns: BTreeMap::from([("email".to_owned(), test_case.method)]),
            key: Some(Secret("secret-key".to_owned())),
        };
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1])) as _),
            (
                "email",
                Arc::new(StringArray::from(vec![test_case.value])) as _,
            ),
        ])
        .map_err(|error| error.to_string())?;
        let masked = mask_batch(batch, &rules)?;
        assert_eq!(
            masked.column(1).as_string::<i32>().iter().next().flatten(),
            test_case.expected_value,
            "{}",
            test_case.description
        );
        assert_eq!(masked.column(0).len(), 1, "{}", test_case.description);
    }
    Ok(())
}
