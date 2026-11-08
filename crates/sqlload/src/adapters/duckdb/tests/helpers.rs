use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// A column read from the source, as a load writes it.
pub(crate) fn column(name: &str, column_type: ColumnType) -> DesiredColumn {
    DesiredColumn {
        name: name.to_owned(),
        column_type,
        origin: ColumnOrigin::Source,
    }
}

/// One row unloaded as a warehouse writes it (the times and timestamps as whole microseconds in `DECIMAL(18,0)`, a decimal as itself), loaded by DuckDB reading the file itself, with ICU loaded and the session in `time_zone` when one is given (otherwise without ICU, as a fresh connection is), and read back as text: the timestamp, the timestamp with time zone's microseconds since the epoch, the time, the decimal, the load id, and a column the files lack.
pub(crate) fn unloaded_row_loaded(
    micros: [i64; 3],
    time_zone: Option<&str>,
) -> Result<Vec<String>, String> {
    use crate::adapters::contract::models::TableName;
    use crate::adapters::duckdb::_helpers::pull_sql::unloaded_insert;
    use crate::postgres_source::models::{ReadColumn, WireType};

    let failed = |error: duckdb::Error| error.to_string();
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let file = folder.path().join("data_0_0_0.snappy.parquet");
    let file = file.display().to_string();
    let [ntz, tz, time] = micros;
    let connection = duckdb::Connection::open_in_memory().map_err(failed)?;
    connection
        .execute_batch(&format!(
            "{}
             COPY (SELECT CAST({ntz} AS DECIMAL(18,0)) AS \"NTZ\", CAST({tz} AS DECIMAL(18,0)) AS \"TZ\", CAST({time} AS DECIMAL(18,0)) AS \"T\", CAST(12.50 AS DECIMAL(10,2)) AS \"AMOUNT\") TO '{file}' (FORMAT parquet);
             CREATE TABLE loaded (\"NTZ\" TIMESTAMP, \"TZ\" TIMESTAMPTZ, \"T\" TIME, \"AMOUNT\" DECIMAL(10,2), \"_sqlload_load_id\" VARCHAR, \"missing\" VARCHAR);",
            time_zone.map_or_else(String::new, |zone| format!("SET TimeZone = '{zone}';"))
        ))
        .map_err(failed)?;
    let typed = [
        ("NTZ", ColumnType::TimestampNtz),
        ("TZ", ColumnType::TimestampTz),
        ("T", ColumnType::Time),
        (
            "AMOUNT",
            ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
        ),
    ];
    let read: Vec<ReadColumn> = typed
        .iter()
        .map(|(name, output)| ReadColumn {
            name: (*name).to_owned(),
            expression: format!("\"{name}\""),
            wire: WireType::Text,
            output: output.clone(),
        })
        .collect();
    let mut columns: Vec<DesiredColumn> = typed
        .iter()
        .map(|(name, column_type)| column(name, column_type.clone()))
        .collect();
    columns.push(column("_sqlload_load_id", ColumnType::Text));
    columns.push(column("missing", ColumnType::Text));
    let table = TableName {
        database: None,
        schema: "main".to_owned(),
        table: "loaded".to_owned(),
    };
    let insert = unloaded_insert(
        &table,
        (&columns, &read),
        &[file],
        ("L1", "2026-11-08T00:00:00Z"),
    );
    connection.execute(&insert, []).map_err(failed)?;
    connection
        .query_row(
            "SELECT CAST(\"NTZ\" AS VARCHAR), CAST(epoch_us(\"TZ\") AS VARCHAR), CAST(\"T\" AS VARCHAR), CAST(\"AMOUNT\" AS VARCHAR), \"_sqlload_load_id\", coalesce(\"missing\", 'NULL') FROM loaded",
            [],
            |row| (0..6).map(|index| row.get::<_, String>(index)).collect(),
        )
        .map_err(failed)
}
