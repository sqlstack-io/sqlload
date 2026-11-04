use crate::column_types::models::Warehouse;
use crate::run_log::constants::RECORD_FORMAT_VERSION;
use crate::run_log::main::current_run::current_run;
use crate::run_log::main::decode_runs::decode_runs;
use crate::run_log::main::finished_record::finished_record;
use crate::run_log::main::insert_run::insert_run;
use crate::run_log::main::logged_statements::logged_statements;
use crate::run_log::main::record_object_name::record_object_name;
use crate::run_log::main::runs_setup::runs_setup;
use crate::run_log::main::runs_table::runs_table;
use crate::run_log::main::select_runs::select_runs;
use crate::run_log::models::{RunEvent, RunOutcome, RunRecord};
use crate::run_log::tests::test_types::{CurrentRunTestCase, FinishedRecordTestCase};
use crate::write_sql::models::TableName;

/// A run of `shop__orders` with a cursor, a token holding a quote and a backslash, and a note.
pub(crate) fn sample_record() -> RunRecord {
    RunRecord {
        format_version: RECORD_FORMAT_VERSION,
        run_id: "load-1".to_owned(),
        event: RunEvent::Run,
        source: "shop".to_owned(),
        source_table: "orders".to_owned(),
        table_name: "shop__orders".to_owned(),
        cursor_column: Some("updated_at".to_owned()),
        cursor_before: None,
        cursor_after: Some("2026-01-02 00:00:00.000000+00".to_owned()),
        state: Some("\"it's\\\\x\"".to_owned()),
        row_count: 3,
        files: Vec::new(),
        note: None,
        recorded_at: "2026-11-04T10:00:00.000000Z".to_owned(),
    }
}

fn runs() -> TableName {
    runs_table(&TableName {
        database: Some("analytics".to_owned()),
        schema: "raw".to_owned(),
        table: "shop__orders".to_owned(),
    })
}

/// The insert of [`sample_record`] without its JSON copy, which repeats the fields.
pub(crate) fn insert_sql(warehouse: Warehouse) -> Result<String, String> {
    let sql = insert_run(&runs(), &sample_record(), warehouse)?;
    Ok(sql
        .rsplit_once(", '{")
        .map_or(sql.clone(), |(head, _)| format!("{head}, <json>)")))
}

/// The run log table's create statement and its fallback.
pub(crate) fn create_sql(warehouse: Warehouse) -> Result<String, String> {
    let table = TableName {
        table: "shop__orders".to_owned(),
        ..runs()
    };
    let setup =
        runs_setup(&table, warehouse, false).ok_or("no setup for a missing run log".to_owned())?;
    Ok([Some(setup.create), setup.fallback]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(";\n"))
}

/// What creating the run log takes when the plan saw none, joined by `;\n`.
pub(crate) fn setup_sql(warehouse: Warehouse) -> Result<String, String> {
    Ok(setup_joined(warehouse, false))
}

/// What creating the run log takes once it exists: nothing.
pub(crate) fn existing_setup_sql(warehouse: Warehouse) -> Result<String, String> {
    Ok(setup_joined(warehouse, true))
}

fn setup_joined(warehouse: Warehouse, exists: bool) -> String {
    let table = TableName {
        database: None,
        schema: "raw".to_owned(),
        table: "shop__orders".to_owned(),
    };
    runs_setup(&table, warehouse, exists)
        .map(|setup| {
            [Some(setup.schema), Some(setup.create), setup.fallback]
                .into_iter()
                .flatten()
                .collect::<Vec<_>>()
                .join(";\n")
        })
        .unwrap_or_default()
}

/// The query reading `shop__orders`'s records.
pub(crate) fn select_sql(warehouse: Warehouse) -> Result<String, String> {
    Ok(select_runs(&runs(), "shop__orders", warehouse))
}

/// An append's statements with the record's insert.
pub(crate) fn append_logged(warehouse: Warehouse) -> Result<String, String> {
    let statements = vec![
        "BEGIN".to_owned(),
        "INSERT INTO t SELECT 1".to_owned(),
        "COMMIT".to_owned(),
    ];
    Ok(logged_statements(statements, "INSERT INTO runs".to_owned(), warehouse).join(";\n"))
}

/// A merge's one statement with the record's insert.
pub(crate) fn merge_logged(warehouse: Warehouse) -> Result<String, String> {
    let statements = vec!["MERGE INTO t USING s".to_owned()];
    Ok(logged_statements(statements, "INSERT INTO runs".to_owned(), warehouse).join(";\n"))
}

/// A rebuild's one statement (DDL) with the record's insert.
pub(crate) fn recreate_logged(warehouse: Warehouse) -> Result<String, String> {
    let statements = vec!["CREATE OR REPLACE TABLE t AS SELECT 1".to_owned()];
    Ok(logged_statements(statements, "INSERT INTO runs".to_owned(), warehouse).join(";\n"))
}

/// Snowflake's swap with the record's insert.
pub(crate) fn swap_logged(warehouse: Warehouse) -> Result<String, String> {
    let statements = vec![
        "CREATE OR REPLACE TABLE swap LIKE t".to_owned(),
        "ALTER TABLE t SWAP WITH swap".to_owned(),
    ];
    Ok(logged_statements(statements, "INSERT INTO runs".to_owned(), warehouse).join(";\n"))
}

/// The current record's run id among the case's records, and the unreadable ones as `<run id>: <reason's start>`.
pub(crate) fn current_run_id(test_case: &CurrentRunTestCase) -> (Option<String>, Vec<String>) {
    let texts: Vec<String> = test_case
        .records
        .iter()
        .map(|(id, event, at)| record_json(id, event, at))
        .chain(test_case.raw.iter().map(|text| (*text).to_owned()))
        .collect();
    let decoded = decode_runs(&texts);
    let unreadable = decoded
        .unreadable
        .iter()
        .map(|record| {
            let reason = record.reason.split([':', ';']).next().unwrap_or_default();
            format!("{}: {reason}", record.run_id.as_deref().unwrap_or("-"))
        })
        .collect();
    (
        current_run(&decoded.records).map(|record| record.run_id.clone()),
        unreadable,
    )
}

/// A record in JSON: run `id` of kind `event` at `at`.
fn record_json(id: &str, event: &str, at: &str) -> String {
    let record = RunRecord {
        run_id: id.to_owned(),
        recorded_at: at.to_owned(),
        ..sample_record()
    };
    let text = serde_json::to_string(&record).unwrap_or_default();
    text.replace("\"event\":\"run\"", &format!("\"event\":\"{event}\""))
}

/// The cursor after and the object name of the finished record.
pub(crate) fn finished(test_case: &FinishedRecordTestCase) -> (Option<String>, String) {
    let record = finished_record(
        &sample_record(),
        test_case.cursor_type.as_ref(),
        test_case.before.map(str::to_owned),
        RunOutcome {
            run_id: "load-2".to_owned(),
            recorded_at: "2026-11-05T01:02:03.000004Z".to_owned(),
            rows: 7,
            loaded_maximum: test_case.loaded.map(str::to_owned),
            files: vec!["part-load-2-0.parquet".to_owned()],
        },
    );
    (record.cursor_after.clone(), record_object_name(&record))
}
