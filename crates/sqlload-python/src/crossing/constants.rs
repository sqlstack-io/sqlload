/// The module of numpy's scalar and array types, which orjson writes by their own rules.
pub(crate) const NUMPY_MODULE: &str = "numpy";

/// numpy's array type, written as a list when its values are numbers or booleans.
pub(crate) const NUMPY_ARRAY: &str = "ndarray";
