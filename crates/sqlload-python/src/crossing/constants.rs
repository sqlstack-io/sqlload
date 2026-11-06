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

/// The method of an `sl.http` job that returns its definition as JSON.
pub(crate) const HTTP_JOB_METHOD: &str = "__sqlload_http_job__";

/// The method of an `sl.checkpoint` that returns its state as JSON.
pub(crate) const CHECKPOINT_METHOD: &str = "__sqlload_checkpoint__";
