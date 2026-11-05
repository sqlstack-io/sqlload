use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};

/// Load data into warehouses: the Kafka exporter, and the loader for projects with [sources].
#[derive(Debug, Parser)]
#[command(name = "sqlload", version, about)]
pub struct Cli {
    /// The configuration file.
    #[arg(
        long,
        short,
        global = true,
        default_value = "sqlload.toml",
        env = "SQLLOAD_CONFIG"
    )]
    pub config: PathBuf,
    /// Log output format.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text)]
    pub log_format: LogFormat,
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate the configuration and test every connection (`check` is an old name for it).
    #[command(alias = "check")]
    Debug {
        /// Only validate the configuration; connect to nothing.
        #[arg(long)]
        no_connection: bool,
    },
    /// Show what `apply` and `load` would do (exporter: each topic's table and objects).
    Plan(LoaderArgs),
    /// Print `topic<TAB>table<TAB>stream<TAB>group` per topic (`-` when skipped), without Snowflake.
    Tables {
        /// Read topic names, one per line, from this file instead of Kafka.
        #[arg(long)]
        topics_file: Option<PathBuf>,
    },
    /// Create or change tables (exporter: create missing tables, stages and pipes); never loads data.
    Apply(LoaderArgs),
    /// Load the selected tables of a loader project.
    Load(LoaderArgs),
    /// Export until stopped.
    Run {
        /// Stop after this long, e.g. `10m`.
        #[arg(long, value_parser = humantime::parse_duration)]
        max_duration: Option<Duration>,
        /// This instance's index below `source.shards`.
        #[arg(long, env = "SQLLOAD_SHARD")]
        shard: Option<u32>,
    },
    /// Show per-partition lag and committed offsets.
    Status,
    /// Inspect or change the run log of `state = "logged"` tables.
    State {
        #[command(subcommand)]
        action: StateAction,
    },
}

/// What `sqlload state` does.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum StateAction {
    /// Show each logged table's run log and current state; read-only.
    Show(StateArgs),
    /// Compare each logged table's logged cursor with the data; read-only, exits 1 when they disagree.
    Verify(StateArgs),
    /// Append a reconciliation adopting the data's position where the log disagrees (with --yes; otherwise only shows it).
    Reconcile(StateArgs),
    /// Append a reset: the next run reads from --to, or from the beginning with --clear (with --yes; otherwise only shows it).
    Reset(ResetArgs),
}

/// Table selection for `sqlload state`, and confirmation for the commands that append.
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Args)]
pub struct StateArgs {
    /// Sources or tables to select; all when empty.
    pub select: Vec<String>,
    /// Append the events instead of only showing them.
    #[arg(long)]
    pub yes: bool,
}

/// `sqlload state reset`'s position.
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Args)]
pub struct ResetArgs {
    #[command(flatten)]
    pub tables: StateArgs,
    /// The position to read from next: a cursor value for tables with a cursor_column, else the `ctx.state` value as JSON.
    #[arg(long, required_unless_present = "clear", conflicts_with = "clear")]
    pub to: Option<String>,
    /// Forget the position: the next run reads from the beginning (cursor_start, or everything).
    #[arg(long)]
    pub clear: bool,
}

/// Table selection and policy flags for loader commands.
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Args)]
pub struct LoaderArgs {
    /// Sources or tables to select (source names, destination table names or `schema.table`); all when empty.
    pub select: Vec<String>,
    /// Allow table rebuilds that `type_change_rebuild = "require_confirmation"` would otherwise block.
    #[arg(long)]
    pub allow_type_change_rebuild: bool,
    /// Rebuild the selected tables from the whole source, with the current columns and types.
    #[arg(long)]
    pub reload: bool,
    /// Backfill from this timestamp (RFC 3339 or a date) instead of the destination's maximum.
    #[arg(long, conflicts_with = "start_cursor_int")]
    pub start_cursor_ts: Option<String>,
    /// Backfill only cursor values below this timestamp (RFC 3339 or a date).
    #[arg(long, conflicts_with = "end_cursor_int")]
    pub end_cursor_ts: Option<String>,
    /// Backfill from this integer cursor value instead of the destination's maximum.
    #[arg(long)]
    pub start_cursor_int: Option<i64>,
    /// Backfill only integer cursor values below this one.
    #[arg(long)]
    pub end_cursor_int: Option<i64>,
    /// The most tables loading at once (default: the project's `threads`, else 4); one table's batches always commit one at a time.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    pub threads: Option<u16>,
    /// The bytes a batch holds before it may commit, instead of about 1 GB (for tests).
    #[arg(long, hide = true, value_parser = clap::value_parser!(u64).range(1..))]
    pub batch_bytes: Option<u64>,
}

/// How logs are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

/// A loader command to run on a project's text.
#[derive(Debug)]
pub struct LoaderRequest<'a> {
    pub text: &'a str,
    /// The project file, whose folder `python_paths` are relative to.
    pub config: &'a std::path::Path,
    pub command: Command,
    /// Runs Python sources; `None` in the standalone executable.
    pub host: Option<std::sync::Arc<dyn crate::python_sources::types::PythonHost>>,
}
