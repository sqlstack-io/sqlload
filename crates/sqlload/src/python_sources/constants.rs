//! The methods by which what a table function yields crosses into the engine as plain data, so the Python that builds it (the `sqlload_api` package) needs no compiled code.

/// The method of an `sl.http` job that returns its definition as JSON, which [`run_http_job`](crate::http_source::main::run_http_job::run_http_job) runs.
pub const HTTP_JOB_METHOD: &str = "__sqlload_http_job__";

/// The method of an `sl.checkpoint` that returns its state as JSON.
pub const CHECKPOINT_METHOD: &str = "__sqlload_checkpoint__";

/// The method of an `sl.json(...)` or `sl.ndjson(...)` that returns its bytes and their read spec as JSON, which [`json_read_spec`](crate::row_batches::main::json_read_spec::json_read_spec) reads.
pub const JSON_DATA_METHOD: &str = "__sqlload_json__";
