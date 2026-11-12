use std::sync::{Arc, Mutex};

use crate::adapters::duckdb::models::{DuckDbConnection, DuckDbDestination};
use crate::column_types::models::ColumnType;
use crate::schema_plan::models::{ColumnOrigin, DesiredColumn};

/// The connection of a destination on SQLLoad's own DuckDB.
pub(crate) fn local(
    destination: &DuckDbDestination,
) -> Result<Arc<Mutex<duckdb::Connection>>, String> {
    let DuckDbConnection::Local(connection) = &destination.connection else {
        return Err("the destination is MotherDuck's".to_owned());
    };
    Ok(connection.clone())
}

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
    use crate::adapters::duckdb::_helpers::pull_sql::{UnloadedColumns, unloaded_insert};
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
        UnloadedColumns {
            columns: &columns,
            read: &read,
            constants: &[],
        },
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

/// An unload of two rows written as one Parquet file in a local folder (no file at all when not `with_files`), said to hold `reported` rows, loaded by DuckDB reading it itself straight into the table; then any open batch rolled back, as a load's close does. What the load returned (`failed` for an error), and the rows the table holds afterwards.
pub(crate) fn pulled_unloaded(reported: u64, with_files: bool) -> Result<(String, u64), String> {
    use crate::adapters::contract::models::{TableName, UnloadedBatch, WriteTarget};
    use crate::adapters::contract::types::TableWriter;
    use crate::adapters::duckdb::models::DuckDbWriter;
    use crate::postgres_source::models::{ReadColumn, WireType};
    use crate::snowflake_source::models::UnloadedFiles;
    use crate::store_paths::models::{Location, StoreKind};

    let failed = |error: duckdb::Error| error.to_string();
    let folder = tempfile::tempdir().map_err(|error| error.to_string())?;
    let root = folder.path().display().to_string();
    let connection = duckdb::Connection::open_in_memory().map_err(failed)?;
    connection
        .execute_batch(&format!(
            "COPY (SELECT * FROM (VALUES (1), (2)) AS v(\"ID\")) TO '{root}/data_0.parquet' (FORMAT parquet);
             CREATE TABLE loaded (\"ID\" BIGINT, \"_sqlload_load_id\" VARCHAR);"
        ))
        .map_err(failed)?;
    let mut writer = DuckDbWriter {
        destination: DuckDbDestination {
            connection: DuckDbConnection::Local(Arc::new(Mutex::new(connection))),
            ddl: Arc::new(tokio::sync::Mutex::new(())),
            motherduck: false,
        },
        open_transaction: false,
    };
    let files = UnloadedFiles {
        folder: Location {
            store: StoreKind::Filesystem { root },
            prefix: String::new(),
        },
        keys: [vec![], vec!["data_0.parquet".to_owned()]][usize::from(with_files)].clone(),
        rows: reported,
    };
    let read = [ReadColumn {
        name: "ID".to_owned(),
        expression: "\"ID\"".to_owned(),
        wire: WireType::Int8,
        output: ColumnType::Int,
    }];
    let columns = [
        column("ID", ColumnType::Int),
        column("_sqlload_load_id", ColumnType::Text),
    ];
    let table = TableName {
        database: None,
        schema: "main".to_owned(),
        table: "loaded".to_owned(),
    };
    let batch = UnloadedBatch {
        files: &files,
        read: &read,
        write_columns: &columns,
        target: WriteTarget {
            table: &table,
            staging: None,
            columns: &columns,
            write_columns: &columns,
            load_id: "L1",
            batch: 1,
            direct: true,
            builds: false,
            constants: &[],
        },
        loaded_at: "2026-11-08T00:00:00Z",
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let outcome = runtime.block_on(async {
        let outcome = writer
            .pull_unloaded(&batch)
            .await
            .map_or_else(|_| "failed".to_owned(), |loaded| format!("{loaded:?}"));
        writer.rollback_batch().await;
        outcome
    });
    let held: i64 = local(&writer.destination)?
        .lock()
        .map_err(|error| error.to_string())?
        .query_row("SELECT count(*) FROM loaded", [], |row| row.get(0))
        .map_err(failed)?;
    Ok((outcome, held.unsigned_abs()))
}

/// The statement loading an unload's file into `main.loaded`: its `ID` column, a `tenant_id` and an `n` column the files lack, and the load id; `constants` fill such columns.
pub(crate) fn unloaded_insert_sql(constants: &[(&str, ColumnType, &str)]) -> String {
    use crate::adapters::contract::models::TableName;
    use crate::adapters::duckdb::_helpers::pull_sql::{UnloadedColumns, unloaded_insert};
    use crate::postgres_source::models::{ReadColumn, WireType};

    let constants: Vec<(String, ColumnType, String)> = constants
        .iter()
        .map(|(name, column_type, value)| {
            ((*name).to_owned(), column_type.clone(), (*value).to_owned())
        })
        .collect();
    let read = [ReadColumn {
        name: "ID".to_owned(),
        expression: "\"ID\"".to_owned(),
        wire: WireType::Int8,
        output: ColumnType::Int,
    }];
    let columns = [
        column("ID", ColumnType::Int),
        column("tenant_id", ColumnType::Text),
        column("n", ColumnType::Int),
        column("_sqlload_load_id", ColumnType::Text),
    ];
    let table = TableName {
        database: None,
        schema: "main".to_owned(),
        table: "loaded".to_owned(),
    };
    unloaded_insert(
        &table,
        UnloadedColumns {
            columns: &columns,
            read: &read,
            constants: &constants,
        },
        &["/unloaded/data_0.parquet".to_owned()],
        ("L1", "2026-11-08T00:00:00Z"),
    )
}
