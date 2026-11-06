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
    /// Print without colour (also when `NO_COLOR` is set or the output is not a terminal).
    #[arg(long, global = true)]
    pub no_color: bool,
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Validate the configuration and test every connection.
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
    /// Compare each table's declared columns with its live table; read-only, exits 1 when they differ.
    Contract {
        #[command(subcommand)]
        action: ContractAction,
    },
    /// Copy one table once, between connections named in sqlload.toml or URIs (sqlload.toml is optional).
    Copy(CopyArgs),
}

/// What `sqlload contract` does.
#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ContractAction {
    /// Report each selected table's declared columns that differ from its live table, and on `contract = "enforced"` tables its undeclared live columns.
    Diff(ContractArgs),
}

/// Table selection and output for `sqlload contract`.
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Args)]
pub struct ContractArgs {
    /// Sources or tables to compare, as for `plan`; all when neither this nor --select is given.
    #[arg(value_name = "SELECTOR")]
    pub positional_select: Vec<String>,
    /// Sources or tables to compare, combined with any given positionally.
    #[arg(long, short = 's', num_args = 1.., value_name = "SELECTOR")]
    pub select: Vec<String>,
    /// Sources or tables to leave out; wins over --select.
    #[arg(long, num_args = 1.., value_name = "SELECTOR")]
    pub exclude: Vec<String>,
    /// Read every table as if its source loaded into this target, in that target's database and schema.
    #[arg(long, value_name = "TARGET")]
    pub from: Option<String>,
    /// Print the report as one JSON document.
    #[arg(long)]
    pub json: bool,
}

/// `sqlload copy`'s endpoints and write settings.
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Args)]
pub struct CopyArgs {
    /// What to copy: `connection:schema.table`, `postgresql://user:${ENV:PGPASSWORD}@host/db#schema.table`, `s3://bucket/prefix/`, `file:///folder` or a folder.
    pub source: String,
    /// Where to: `connection:SCHEMA.TABLE`, `duckdb://file.duckdb#SCHEMA.TABLE`, `s3://bucket/prefix/`, `file:///folder` or a folder.
    pub destination: String,
    /// append, merge, delete_insert or replace (default: the loader's).
    #[arg(long)]
    pub write_strategy: Option<String>,
    /// The key columns for merge and delete_insert.
    #[arg(long, num_args = 1.., value_delimiter = ',')]
    pub unique_key: Vec<String>,
    /// The column incremental copies continue from.
    #[arg(long)]
    pub cursor_column: Option<String>,
    /// The format of a files source's files: parquet, ndjson, json or csv.
    #[arg(long)]
    pub format: Option<String>,
    /// Print the copy as a project entry for sqlload.toml instead of running it.
    #[arg(long)]
    pub print_toml: bool,
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
    /// Sources or tables to select (source names, destination table names or `schema.table`), the same as --select; all when neither is given.
    #[arg(value_name = "SELECTOR")]
    pub positional_select: Vec<String>,
    /// Sources or tables to select, combined with any given positionally.
    #[arg(long, short = 's', num_args = 1.., value_name = "SELECTOR")]
    pub select: Vec<String>,
    /// Sources or tables to leave out, named as for --select; wins over --select.
    #[arg(long, num_args = 1.., value_name = "SELECTOR")]
    pub exclude: Vec<String>,
    /// Allow table rebuilds that `type_change_rebuild = "require_confirmation"` would otherwise block.
    #[arg(long)]
    pub allow_type_change_rebuild: bool,
    /// Rebuild the selected tables from the whole source, with the current columns and types; asks first unless --yes.
    #[arg(long)]
    pub reload: bool,
    /// Rebuild with --reload without asking.
    #[arg(long)]
    pub yes: bool,
    /// Backfill from this cursor value instead of the destination's maximum, typed from each selected table's cursor (an integer, or RFC 3339 or a date).
    #[arg(long, value_name = "VALUE")]
    pub start_cursor: Option<String>,
    /// Backfill only cursor values below this one, typed from each selected table's cursor.
    #[arg(long, value_name = "VALUE")]
    pub end_cursor: Option<String>,
    /// The most tables loading at once (default: the project's `concurrency`, else 4); one table's batches always commit one at a time.
    #[arg(long, value_parser = clap::value_parser!(u16).range(1..))]
    pub concurrency: Option<u16>,
    /// `plan` only: print the plan as one JSON document.
    #[arg(long)]
    pub json: bool,
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
    /// `--no-color`, for the plan a `--reload` prompt shows.
    pub no_color: bool,
    /// Values for `${ENV:NAME}` references, read before the process environment (`sqlload copy`'s literal URI passwords).
    pub variables: std::collections::BTreeMap<String, String>,
}
