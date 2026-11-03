use crate::config::models::Config;
use crate::file_format::_helpers::columns::{is_semi_structured, parquet_kind, timestamp_family};
use crate::file_format::models::{LoadSelect, ParquetKind};
use crate::naming::main::quote_name::quote_name;

/// Each column cast to its type; JSON text parsed for semi-structured columns, and `exported_at` labelled with `envelope.time_zone` so tables match JSON loads.
pub(crate) fn load_select(config: &Config) -> LoadSelect {
    let zone = sql_literal(&config.envelope.time_zone);
    let mut columns: Vec<String> = Vec::new();
    let mut expressions: Vec<String> = Vec::new();
    for column in &config.envelope.columns {
        let source = format!("$1:\"{}\"", column.name.replace('"', "\"\""));
        let sql_type = column.sql_type.trim();
        let expression = match (parquet_kind(column), timestamp_family(sql_type)) {
            (ParquetKind::Text, _) if is_semi_structured(sql_type) => {
                format!("PARSE_JSON({source}::VARCHAR)")
            }
            (ParquetKind::Timestamp, Some("TIMESTAMP_TZ")) => {
                format!("CONVERT_TIMEZONE({zone}, {source}::TIMESTAMP_TZ)")
            }
            (ParquetKind::Timestamp, Some("TIMESTAMP_NTZ")) => {
                format!("CONVERT_TIMEZONE({zone}, {source}::TIMESTAMP_TZ)::TIMESTAMP_NTZ")
            }
            _ => format!("{source}::{sql_type}"),
        };
        columns.push(quote_name(&column.name));
        expressions.push(expression);
    }
    LoadSelect {
        columns: format!("({})", columns.join(", ")),
        expressions: expressions.join(", "),
    }
}

fn sql_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}
