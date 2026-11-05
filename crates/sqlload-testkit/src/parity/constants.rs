/// The only folder of the bucket this test may write to.
pub const ALLOWED_PREFIX: &str = "sqlload/";

/// What a table's type must be compared as text for: `MINUS` cannot compare it.
pub const TEXT_COMPARED_TYPES: &[&str] = &["VARIANT", "OBJECT", "ARRAY"];

/// The outcome of the comparison's self-test when it works.
pub const SELF_TEST_PASSED: &str = "detected";

/// A count of no rows, as the comparison's text shows it.
pub const NO_ROWS: &str = "0";
