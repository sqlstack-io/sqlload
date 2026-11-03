//! Order records.

/// `count` small JSON order records numbered from `first`.
pub fn order_values(first: usize, count: usize) -> Vec<String> {
    (first..first + count)
        .map(|index| {
            format!(
                r#"{{"order_id":{index},"customer":"customer-{}"}}"#,
                index % 11
            )
        })
        .collect()
}
