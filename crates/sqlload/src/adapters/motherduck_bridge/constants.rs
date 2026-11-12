/// The bridge's Python program, run with `python -c`.
pub const BRIDGE_SCRIPT: &str = include_str!("_helpers/bridge.py");
/// The environment variable naming the Python the bridge runs on; the `sqlload` Python package sets it to its own interpreter unless it is set already.
pub const PYTHON_VARIABLE: &str = "SQLLOAD_MOTHERDUCK_PYTHON";
/// The Python the bridge runs on when `PYTHON_VARIABLE` is unset: the first on `PATH`.
#[cfg(not(windows))]
pub const DEFAULT_PYTHON: &str = "python3";
/// The Python the bridge runs on when `PYTHON_VARIABLE` is unset: the first on `PATH`.
#[cfg(windows)]
pub const DEFAULT_PYTHON: &str = "python";
/// The duckdb package versions MotherDuck accepts, as the bridge's install hints name them.
pub const SUPPORTED_DUCKDB: &str = "duckdb 1.4.1 to 1.5.6";
/// How many bytes of the bridge's last stderr output a failure quotes.
pub const STDERR_TAIL_BYTES: usize = 2048;
/// The error types the bridge replies with when its Python has no duckdb package.
pub const MISSING_PACKAGE_ERRORS: &[&str] = &["ModuleNotFoundError", "ImportError"];
