use tracing_subscriber::EnvFilter;

use crate::command::models::LogFormat;

/// Log to stderr at `info` unless `RUST_LOG` says otherwise.
pub(crate) fn init_logging(format: LogFormat) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,odbc_api=error"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    let _ = match format {
        LogFormat::Text => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
}
