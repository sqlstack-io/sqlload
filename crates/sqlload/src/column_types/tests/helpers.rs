use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{Float64Builder, ListBuilder};

/// A list array with one row per item of `rows`.
pub(crate) fn float_lists(rows: &[&[f64]]) -> ArrayRef {
    let mut lists = ListBuilder::new(Float64Builder::new());
    for values in rows {
        lists.values().append_slice(values);
        lists.append(true);
    }
    Arc::new(lists.finish())
}
