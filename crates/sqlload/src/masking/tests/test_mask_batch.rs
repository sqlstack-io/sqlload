use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Int64Array, RecordBatch, StringArray};

use crate::masking::main::mask_batch::mask_batch;
use crate::masking::tests::test_types::MaskBatchTestCase;
use crate::project::models::MaskMethod;

#[test]
fn given_a_masked_text_column_when_masking_then_values_are_hashed_and_others_kept()
-> Result<(), String> {
    let test_cases = [
        MaskBatchTestCase {
            description: "a value becomes its SHA-256 hex",
            value: Some("a@example.com"),
            expected_value: Some(
                "08168cd80dfd534ab0f10af10f1303fe00af2d43ab5c1432360d137f8197e17a",
            ),
        },
        MaskBatchTestCase {
            description: "NULL stays NULL",
            value: None,
            expected_value: None,
        },
    ];
    let masks = BTreeMap::from([("email".to_owned(), MaskMethod::Hash)]);
    for test_case in test_cases {
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int64Array::from(vec![1])) as _),
            (
                "email",
                Arc::new(StringArray::from(vec![test_case.value])) as _,
            ),
        ])
        .map_err(|error| error.to_string())?;
        let masked = mask_batch(batch, &masks)?;
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
