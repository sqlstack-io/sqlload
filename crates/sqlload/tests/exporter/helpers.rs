use sqlload::config::models::Config;
use sqlload_testkit::configs::main::streaming_config::streaming_config;
use sqlload_testkit::harness::models::Stack;

pub(crate) const PARTITIONS: i32 = 3;
pub(crate) const FAST: &str = "[batching]\nmax_age = \"1s\"\nmax_rows = 40\n";

/// Streaming configuration for topics matching `include` on this stack, plus `extra` TOML.
pub(crate) fn config(stack: &Stack, include: &str, extra: &str) -> Result<Config, String> {
    streaming_config(
        &stack.redpanda.brokers,
        &stack.fake.base_url,
        include,
        extra,
    )
}

/// Run an async test body on a fresh multi-threaded runtime.
pub(crate) fn run_async<F: std::future::Future<Output = Result<(), String>>>(
    body: F,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    runtime.block_on(body)
}
