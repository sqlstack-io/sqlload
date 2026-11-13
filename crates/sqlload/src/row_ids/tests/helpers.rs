use std::sync::Arc;

use arrow_array::{
    ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap_or_else(|error| panic!("{error}"))
}

pub(crate) fn one_integer() -> RecordBatch {
    batch(vec![(
        "id",
        Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
    )])
}

pub(crate) fn two_texts() -> RecordBatch {
    batch(vec![
        (
            "region",
            Arc::new(StringArray::from(vec!["eu"])) as ArrayRef,
        ),
        ("id", Arc::new(StringArray::from(vec!["a"])) as ArrayRef),
    ])
}

pub(crate) fn instant_and_negative() -> RecordBatch {
    batch(vec![
        ("id", Arc::new(Int64Array::from(vec![-7])) as ArrayRef),
        (
            "at",
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_767_323_045_000_006]).with_timezone("UTC"),
            ) as ArrayRef,
        ),
    ])
}

pub(crate) fn escaped_text() -> RecordBatch {
    batch(vec![(
        "id",
        Arc::new(StringArray::from(vec!["é\"x"])) as ArrayRef,
    )])
}

pub(crate) fn floats() -> RecordBatch {
    batch(vec![(
        "id",
        Arc::new(Float64Array::from(vec![
            1.0,
            1e20,
            f64::NAN,
            1e-5,
            1.5e-7,
            9_999_999_999_999_998.0,
            -0.0,
        ])) as ArrayRef,
    )])
}
