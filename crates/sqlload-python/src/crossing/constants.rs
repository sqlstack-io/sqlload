/// The module of numpy's scalar and array types, which orjson writes by their own rules.
pub(crate) const NUMPY_MODULE: &str = "numpy";

/// numpy's array type, written as a list when its values are numbers or booleans.
pub(crate) const NUMPY_ARRAY: &str = "ndarray";

/// `date(1970, 1, 1).toordinal()`.
pub(crate) const EPOCH_ORDINAL: i64 = 719_163;

/// Microseconds in a day.
pub(crate) const MICROS_PER_DAY: i64 = 86_400_000_000;

/// Microseconds in a second.
pub(crate) const MICROS_PER_SECOND: i64 = 1_000_000;

/// The `sqlload._runtime` function orjson calls for values it cannot write itself.
pub(crate) const CROSSING_DEFAULT: &str = "crossing_default";

/// How long a page `transform`'s crossing is waited on at a time; it polls again until the function's output ends.
pub(crate) const PAGE_POLL: std::time::Duration = std::time::Duration::from_secs(3600);
