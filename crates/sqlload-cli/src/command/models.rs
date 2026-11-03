use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand, ValueEnum};

/// Export Kafka topics to Snowflake.
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
    /// Show each topic's table and the objects that would be created.
    Plan,
    /// Print `topic<TAB>table<TAB>stream<TAB>group` per topic (`-` when skipped), without Snowflake.
    Tables {
        /// Read topic names, one per line, from this file instead of Kafka.
        #[arg(long)]
        topics_file: Option<PathBuf>,
    },
    /// Create missing tables, stages and pipes; never alters or drops.
    Apply,
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

/// How logs are written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}
