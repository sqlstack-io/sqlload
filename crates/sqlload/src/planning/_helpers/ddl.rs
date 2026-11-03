use crate::config::models::Config;
use crate::file_format::main::copy_into::copy_into;
use crate::naming::main::quote_name::quote_name;
use crate::planning::constants::FILE_FORMAT;
use crate::planning::models::StagePipe;

/// `DATABASE.SCHEMA.NAME`, quoting parts that are not plain upper-case identifiers.
pub(crate) fn qualified(config: &Config, name: &str) -> String {
    format!(
        "{}.{}.{}",
        quote_name(&config.destination.database),
        quote_name(&config.destination.schema),
        quote_name(name)
    )
}

pub(crate) fn create_table(config: &Config, table: &str, error_logging: bool) -> String {
    let columns: Vec<String> = config
        .envelope
        .columns
        .iter()
        .map(|column| format!("{} {}", quote_name(&column.name), column.sql_type))
        .collect();
    let logging = match error_logging {
        true => " ERROR_LOGGING = TRUE",
        false => "",
    };
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({}){logging}",
        qualified(config, table),
        columns.join(", ")
    )
}

pub(crate) fn create_stage(config: &Config, stage: &str) -> String {
    format!(
        "CREATE STAGE IF NOT EXISTS {} {FILE_FORMAT}",
        qualified(config, stage)
    )
}

pub(crate) fn create_streaming_pipe(config: &Config, pipe: &str, table: &str) -> String {
    format!(
        "CREATE PIPE IF NOT EXISTS {} AS COPY INTO {} FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING')) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        qualified(config, pipe),
        qualified(config, table)
    )
}

pub(crate) fn create_stage_pipe(config: &Config, pipe: &StagePipe<'_>) -> String {
    let ingest = match pipe.auto_ingest {
        true => " AUTO_INGEST = TRUE",
        false => "",
    };
    format!(
        "CREATE PIPE IF NOT EXISTS {}{ingest} AS {}",
        qualified(config, pipe.pipe),
        copy_into(config, &qualified(config, pipe.table), pipe.source, "")
    )
}

/// A single-quoted SQL string literal.
pub(crate) fn literal(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "''"))
}
