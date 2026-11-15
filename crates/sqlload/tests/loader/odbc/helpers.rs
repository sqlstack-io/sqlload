//! Helpers for the ODBC loader test.

/// The project: an odbc source over psqlODBC replacing its tables, its items table merged on its reported primary key after a cursor, a query table, and DuckDB as the target.
pub(crate) fn project(port: u16, database: &std::path::Path, max_text_size: usize) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "Driver={{PostgreSQL Unicode}};Server=127.0.0.1;Port={port};Database=app;Uid=sqlload;Pwd=sqlload;BoolsAsChar=0;"

[connections.local]
kind = "duckdb"
database = "{database}"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Tables of a generic ERP."
include = ["erp.*"]
exclude = ["erp.ignored"]
max_text_size = {max_text_size}
write_strategy = "replace"

[sources.erp.tables."erp.items"]
description = "Items, changed rows found by updated_at."
cursor_column = "updated_at"
unique_key = "{{primary_key}}"
write_strategy = "merge"

[sources.erp.tables.recent]
description = "The newest items."
sql = "SELECT id, name FROM erp.items WHERE id > 2990 -- newest"
"#,
        database = database.display(),
    )
}

/// A project reading `erp.notes` and `erp.codes` over psqlODBC, each appended by its text `label` cursor, into DuckDB.
pub(crate) fn text_cursor_project(port: u16, database: &std::path::Path) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "Driver={{PostgreSQL Unicode}};Server=127.0.0.1;Port={port};Database=app;Uid=sqlload;Pwd=sqlload;"

[connections.local]
kind = "duckdb"
database = "{database}"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Labelled rows of a generic ERP."
include = ["erp.*"]

[sources.erp.tables."erp.notes"]
description = "Notes; the first maximum ends in a backslash."
cursor_column = "label"
write_strategy = "append"

[sources.erp.tables."erp.codes"]
description = "Codes; the first maximum holds a backslash before a quote. Pinned as text, which still binds."
cursor_column = "label"
write_strategy = "append"
columns = {{ label = "text" }}
"#,
        database = database.display(),
    )
}

/// Run each of `statements` through ODBC, one at a time.
pub(crate) fn odbc_execute(connection_string: &str, statements: &[&str]) -> Result<(), String> {
    let connection = arrow_odbc::odbc_api::environment()
        .map_err(|error| error.to_string())?
        .connect_with_connection_string(
            connection_string,
            arrow_odbc::odbc_api::ConnectionOptions::default(),
        )
        .map_err(|error| format!("cannot connect: {error}"))?;
    statements.iter().try_for_each(|statement| {
        connection
            .execute(statement, (), None)
            .map(|_| ())
            .map_err(|error| format!("{statement}: {error}"))
    })
}

/// A project reading an events table appended by its millisecond `happened_at` cursor and a days table replaced from its last `opened` day, through `connection_string`, with `source_settings` added to the source, into DuckDB.
pub(crate) fn driver_project(
    (connection_string, source_settings): (&str, &str),
    (events, days): (&str, &str),
    database: &std::path::Path,
) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "{connection_string}"

[connections.local]
kind = "duckdb"
database = '{database}'

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Events and days of a generic ERP."
include = ["{events}", "{days}"]
{source_settings}

[sources.erp.tables."{events}"]
description = "Events, read after the newest happened_at."
cursor_column = "happened_at"
write_strategy = "append"

[sources.erp.tables."{days}"]
description = "Days, the last one read again."
cursor_column = "opened"
write_strategy = "delete_insert"
"#,
        database = database.display(),
    )
}

/// A project reading both tables through `connection_string` into DuckDB, each merged on the primary key the driver reports (`unique_key = "{primary_key}"`) after its `at` cursor.
pub(crate) fn keyless_project(
    connection_string: &str,
    (first, second): (&str, &str),
    database: &std::path::Path,
) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "{connection_string}"

[connections.local]
kind = "duckdb"
database = '{database}'

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Tables of a generic ERP."
include = ["{first}", "{second}"]
write_strategy = "merge"
unique_key = "{{primary_key}}"

[sources.erp.tables."{first}"]
description = "The first table, read after the newest at."
cursor_column = "at"

[sources.erp.tables."{second}"]
description = "The second table, read after the newest at."
cursor_column = "at"
"#,
        database = database.display(),
    )
}

/// A project merging `erp.t` over psqlODBC by its `updated_at` cursor, with `settings` added to its table entry, into DuckDB.
pub(crate) fn null_cursor_project(port: u16, database: &std::path::Path, settings: &str) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "Driver={{PostgreSQL Unicode}};Server=127.0.0.1;Port={port};Database=app;Uid=sqlload;Pwd=sqlload;"

[connections.local]
kind = "duckdb"
database = "{database}"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Rows of a generic ERP, some never edited."
include = ["erp.t"]

[sources.erp.tables."erp.t"]
description = "Rows whose updated_at stays NULL until their first edit."
write_strategy = "merge"
unique_key = "{{primary_key}}"
{settings}
"#,
        database = database.display(),
    )
}

/// A project replacing each of `tables` (as the source's `include` names them) through `connection_string`, with `source_settings` added to the source, into DuckDB.
pub(crate) fn memory_project(
    (connection_string, source_settings): (&str, &str),
    tables: &[&str],
    database: &std::path::Path,
) -> String {
    let entries: String = tables
        .iter()
        .map(|table| {
            format!("\n[sources.erp.tables.\"{table}\"]\ndescription = \"The {table} table.\"\n")
        })
        .collect();
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "{connection_string}"

[connections.local]
kind = "duckdb"
database = '{database}'

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Tables of a generic ERP."
include = [{include}]
write_strategy = "replace"
{source_settings}
{entries}"#,
        include = tables
            .iter()
            .map(|table| format!("\"{table}\""))
            .collect::<Vec<_>>()
            .join(", "),
        database = database.display(),
    )
}

/// A query of DuckDB table `raw.{table}`: its columns in order, without SQLLoad's own, then each row's id, body length and start, and other values, `NULL` where null.
pub(crate) fn typed_rows(table: &str) -> String {
    format!(
        "SELECT (SELECT string_agg(column_name, ',' ORDER BY ordinal_position) FROM information_schema.columns WHERE table_schema = 'raw' AND table_name = '{table}' AND NOT starts_with(column_name, '_sqlload')) || ' | ' || (SELECT string_agg(concat_ws(':', id::VARCHAR, coalesce(length(body)::VARCHAR, 'NULL'), coalesce(left(body, 3), 'NULL'), coalesce(title, 'NULL'), coalesce(price::VARCHAR, 'NULL'), coalesce(strftime(\"at\", '%Y-%m-%d %H:%M:%S.%g'), 'NULL'), coalesce(\"day\"::VARCHAR, 'NULL'), coalesce(n::VARCHAR, 'NULL')), ' ; ' ORDER BY id) FROM raw.{table})"
    )
}

/// Run the `sqlload` executable on `config` with `args` and `RUST_LOG` set to `log`, its output uncoloured; whether it succeeded, what it printed, and its peak resident memory in KiB, read from `/proc` (VmHWM, the most it ever held) every few milliseconds until it exits.
#[cfg(target_os = "linux")]
pub(crate) fn run_measured(
    config: &std::path::Path,
    args: &[&str],
    log: &str,
) -> Result<(bool, String, u64), String> {
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
        .arg("--config")
        .arg(config)
        .arg("--no-color")
        .args(args)
        .env("RUST_LOG", log)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|error| error.to_string())?;
    let stdout = child.stdout.take().ok_or("no stdout")?;
    let stderr = child.stderr.take().ok_or("no stderr")?;
    let readers = [stdout_reader(stdout), stdout_reader(stderr)];
    let status = format!("/proc/{}/status", child.id());
    let mut peak: u64 = 0;
    let exited = std::iter::repeat_with(|| {
        peak = peak.max(peak_kib(&status));
        std::thread::sleep(std::time::Duration::from_millis(5));
        child.try_wait().transpose()
    })
    .find_map(|waited| waited)
    .ok_or("the process was not waited for")?
    .map_err(|error| error.to_string())?;
    let text = readers
        .into_iter()
        .map(|reader| reader.join().unwrap_or_default())
        .collect::<String>();
    Ok((exited.success(), text, peak))
}

/// A thread reading all of `output` as text.
#[cfg(target_os = "linux")]
fn stdout_reader(
    mut output: impl std::io::Read + Send + 'static,
) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut text = String::new();
        output
            .read_to_string(&mut text)
            .map(|_| text)
            .unwrap_or_default()
    })
}

/// The `VmHWM` line of a process's `/proc/<pid>/status`, in KiB; 0 once it is gone.
#[cfg(target_os = "linux")]
fn peak_kib(status: &str) -> u64 {
    std::fs::read_to_string(status)
        .unwrap_or_default()
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|value| value.trim().trim_end_matches("kB").trim().parse().ok())
        .unwrap_or_default()
}

/// A project whose odbc source `erp` reads through `connection_string` with `source` (its `include`, settings and table entries) added, into DuckDB.
pub(crate) fn catalog_project(
    connection_string: &str,
    source: &str,
    database: &std::path::Path,
) -> String {
    format!(
        r#"
[connections.erp]
kind = "odbc"
connection_string = "{connection_string}"

[connections.local]
kind = "duckdb"
database = '{database}'

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Tables of a generic ERP."
{source}
"#,
        database = database.display(),
    )
}

/// Run the `sqlload` executable on `config` with `args`, unixODBC tracing every call into a file beside it (through an `odbcinst.ini` there, named by `ODBCSYSINI`, holding the system's drivers): whether it succeeded, what it printed, and its catalog calls in order, as [`catalog_calls`] reads them.
#[cfg(target_os = "linux")]
pub(crate) fn run_traced(
    config: &std::path::Path,
    args: &[&str],
) -> Result<(bool, String, Vec<String>), String> {
    let folder = config.parent().ok_or("the config has no folder")?;
    let trace = folder.join("trace.log");
    let drivers =
        std::fs::read_to_string("/etc/odbcinst.ini").map_err(|error| error.to_string())?;
    std::fs::write(
        folder.join("odbcinst.ini"),
        format!(
            "[ODBC]\nTrace=Yes\nTraceFile={}\n\n{drivers}",
            trace.display()
        ),
    )
    .map_err(|error| error.to_string())?;
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_sqlload"))
        .arg("--config")
        .arg(config)
        .arg("--no-color")
        .args(args)
        .env("ODBCSYSINI", folder)
        .output()
        .map_err(|error| error.to_string())?;
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let traced = std::fs::read_to_string(&trace).map_err(|error| format!("no trace: {error}"))?;
    Ok((output.status.success(), text, catalog_calls(&traced)))
}

/// The catalog calls in a unixODBC trace, in order: `SQLTables(schema, table)` and `SQLPrimaryKeys(schema, table)` with their arguments (`NULL` for none), followed by ` refused` when the driver returned an error, and `INFORMATION_SCHEMA` for each run of SQLLoad's query of the keys it lists. Each call is a block from an `[ODBC]` line: one on entry, with its arguments, and one on exit.
#[cfg(target_os = "linux")]
fn catalog_calls(trace: &str) -> Vec<String> {
    // unixODBC labels SQLPrimaryKeys' table argument `Table Type`.
    let functions = [
        ("SQLTables", "Table Name"),
        ("SQLPrimaryKeys", "Table Type"),
    ];
    trace
        .split("\n[ODBC]")
        .flat_map(|block| {
            let function = block
                .split(".c]")
                .next()
                .and_then(|head| head.rsplit('[').next())
                .unwrap_or_default();
            functions
                .iter()
                .filter(move |(name, _)| *name == function)
                .flat_map(move |(name, table)| {
                    [
                        block.contains("Entry:").then(|| {
                            format!(
                                "{name}({}, {})",
                                trace_argument(block, "Schema Name"),
                                trace_argument(block, table)
                            )
                        }),
                        block
                            .contains("Exit:[SQL_ERROR]")
                            .then(|| " refused".to_owned()),
                    ]
                })
                .flatten()
                .chain(
                    block
                        .contains("SQL = [SELECT tc.CONSTRAINT_TYPE")
                        .then(|| "INFORMATION_SCHEMA".to_owned()),
                )
        })
        .collect::<Vec<String>>()
        .join("\n")
        .replace("\n refused", " refused")
        .lines()
        .map(str::to_owned)
        .collect()
}

/// The value a unixODBC trace block shows for argument `name` (`Schema Name = [erp][length = 3]`), `NULL` for none.
#[cfg(target_os = "linux")]
fn trace_argument(block: &str, name: &str) -> String {
    block
        .lines()
        .find_map(|line| line.trim().strip_prefix(&format!("{name} = [")))
        .and_then(|value| value.split(']').next())
        .unwrap_or_default()
        .to_owned()
}
