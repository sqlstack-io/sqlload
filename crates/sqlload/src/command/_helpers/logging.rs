use std::io::Write;
use std::sync::Mutex;

use tracing_subscriber::EnvFilter;

use crate::command::_helpers::load_output::above_live_block;
use crate::command::models::LogFormat;

/// `load`'s live block while it is drawn, so a log line prints above it instead of through it. Process-wide like the tracing subscriber it serves, which is installed once per process (the Python package can run several commands in one), so a handle passed to one run would never reach it.
pub(crate) static LIVE_BLOCK: Mutex<Option<indicatif::ProgressBar>> = Mutex::new(None);

/// Stderr for log lines: each written above `load`'s live block when one is drawn, instead of through it.
struct LogWriter;

impl Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        above_live_block(|| std::io::stderr().write(buf))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stderr().flush()
    }
}

/// Log to stderr at `info`, without the ODBC crates' per-column buffer notes (`arrow_odbc` at `warn`, `odbc_api` at `error`), unless `RUST_LOG` says otherwise; above `load`'s live block when it is drawn.
pub(crate) fn init_logging(format: LogFormat) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,odbc_api=error,arrow_odbc=warn"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(|| LogWriter);
    let _ = match format {
        LogFormat::Text => builder.try_init(),
        LogFormat::Json => builder.json().try_init(),
    };
}
