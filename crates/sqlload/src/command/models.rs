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
    /// Validate the configuration and connect to Kafka and Snowflake.
    Check,
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
}

/// Table selection and policy flags for loader commands.
#[derive(Debug, Clone, Default, clap::Args)]
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
}

/// How logs are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}
