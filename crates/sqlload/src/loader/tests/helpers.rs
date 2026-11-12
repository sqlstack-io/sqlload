use std::collections::{BTreeSet, HashMap};

use arrow_cast::display::{ArrayFormatter, FormatOptions};

use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::column_types::constants::ARROW_TYPE_METADATA_KEY;
use crate::column_types::main::arrow_field::arrow_field;
use crate::file_store::errors::StoreError;
use crate::http::errors::ApiError;
use crate::loader::_helpers::python::bound_array;
use crate::loader::_helpers::python::{
    YieldedData, declared_problems, inferred_table, python_tables, source_spelling,
};
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::errors::LoaderError;
use crate::loader::models::{PlanDetail, PlanItem, TableSpec};
use crate::loader::tests::test_types::{
    BoundArrayTestCase, DeclaredProblemsTestCase, InferredTableTestCase, ItemSpec,
    SourceSpellingTestCase,
};
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};
use crate::project::main::parse_python_project::parse_python_project;
use crate::python_sources::models::PythonSource;
use crate::schema_plan::models::LiveColumn;

/// The columns of the inferred table, as `name:type` joined by commas.
pub(crate) fn inferred_columns(test_case: &InferredTableTestCase) -> String {
    let planned = SourceTable {
        comment: None,
        schema: String::new(),
        name: "t".to_owned(),
        columns: test_case
            .planned
            .iter()
            .map(|name| SourceColumn {
                name: (*name).to_owned(),
                postgres_type: "python".to_owned(),
                pg_type: PgType::Other,
                column_type: Ok(crate::column_types::models::ColumnType::Text),
                nullable: true,
                source_spelling: None,
            })
            .collect(),
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
    };
    let live: Vec<LiveColumn> = test_case
        .live
        .iter()
        .map(|(name, column_type)| LiveColumn {
            name: (*name).to_owned(),
            column_type: column_type.clone(),
            fraction_digits: None,
        })
        .collect();
    let live = Some(live.as_slice()).filter(|live| !live.is_empty());
    let declared: Vec<String> = test_case
        .declared
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let null_columns: BTreeSet<String> = test_case
        .null_columns
        .iter()
        .map(|name| (*name).to_owned())
        .collect();
    let schema = (test_case.schema)();
    let data = YieldedData {
        schema: &schema,
        null_columns: &null_columns,
        rows: 1,
    };
    inferred_table(&planned, &declared, &data, live)
        .columns
        .iter()
        .map(|column| {
            format!(
                "{}:{}",
                column.name,
                column
                    .column_type
                    .as_ref()
                    .map_or_else(Clone::clone, ToString::to_string)
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// A crossing's schema: an int, a float, text, a UTC timestamp, a JSON column, an all-NULL column and a metadata column.
pub(crate) fn crossed_schema() -> Schema {
    let json = HashMap::from([(ARROW_TYPE_METADATA_KEY.to_owned(), "json".to_owned())]);
    Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("code", DataType::Utf8, true),
        Field::new(
            "at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("meta", DataType::Utf8, true).with_metadata(json),
        Field::new("_sqlload_load_id", DataType::Utf8, true),
    ])
}

/// A schema of one integer column, `code`.
pub(crate) fn integer_code() -> Schema {
    Schema::new(vec![Field::new("code", DataType::Int64, true)])
}

/// The yielded columns `seq` and `kind`, where the live table (on Snowflake) has `SEQ`.
pub(crate) fn lower_case_rows() -> Schema {
    Schema::new(vec![
        Field::new("seq", DataType::Int64, true),
        Field::new("kind", DataType::Utf8, true),
    ])
}

/// The spelling chosen for a live column.
pub(crate) fn spelling(test_case: &SourceSpellingTestCase) -> Result<String, String> {
    let mut spec = python_spec("{}")?;
    spec.warehouse = test_case.warehouse;
    Ok(source_spelling(test_case.stored, &spec))
}

/// The spec of Python table `t` with `settings` (the host's JSON) and a description, loading into DuckDB.
pub(crate) fn python_spec(settings: &str) -> Result<TableSpec, String> {
    let mut settings: serde_json::Value =
        serde_json::from_str(settings).map_err(|error| error.to_string())?;
    settings["description"] = serde_json::Value::from("Table t.");
    let sources = format!(
        r#"[{{"name": "s", "module": "s.py", "settings": {{"mask_key": "k", "tables": {{"t": {settings}}}}}}}]"#
    );
    let python: Vec<PythonSource> =
        serde_json::from_str(&sources).map_err(|error| error.to_string())?;
    let project = parse_python_project(
        "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n",
        &|_| None,
        &python,
    )
    .map_err(|error| error.to_string())?;
    let source = &project.sources["s"];
    let resolved = resolve_tables(&project, "s", &python_tables(source));
    resolved
        .specs
        .into_iter()
        .next()
        .ok_or_else(|| format!("no spec: {:?}", resolved.problems))
}

/// The problems of the declared keys against data with `columns` (all text) and `rows`.
pub(crate) fn problems_for(test_case: &DeclaredProblemsTestCase) -> Result<String, String> {
    let spec = python_spec(test_case.settings)?;
    let schema = Schema::new(
        test_case
            .columns
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect::<Vec<Field>>(),
    );
    let data = YieldedData {
        schema: &schema,
        null_columns: &BTreeSet::new(),
        rows: test_case.rows,
    };
    Ok(declared_problems(&spec, &data).join("; "))
}

/// The bound built for a cursor column of the case's type, as Arrow displays it.
pub(crate) fn bound_text(test_case: &BoundArrayTestCase) -> Result<String, String> {
    let field = arrow_field("c", &test_case.cursor_type).ok_or("no Arrow type")?;
    let array = bound_array(test_case.text, &test_case.cursor_type, field.data_type())
        .map_err(|error| error.to_string())?;
    let formatter = ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default())
        .map_err(|error| error.to_string())?;
    Ok(format!("{} {}", array.data_type(), formatter.value(0)))
}

/// `specs` as plan items.
pub(crate) fn plan_items(specs: &[ItemSpec]) -> Vec<PlanItem> {
    specs
        .iter()
        .map(|(name, action, details)| PlanItem {
            name: (*name).to_owned(),
            action: (*action).to_owned(),
            details: details
                .iter()
                .map(|(label, text)| PlanDetail {
                    label: (*label).to_owned(),
                    text: (*text).to_owned(),
                })
                .collect(),
        })
        .collect()
}

/// A warehouse call that failed with `status` and service `code`.
pub(crate) fn status(status: u16, code: &str) -> LoaderError {
    LoaderError::Snowflake(ApiError::Status {
        status,
        code: code.to_owned(),
        message: "m".to_owned(),
    })
}

/// An object store failure.
pub(crate) fn store(source: object_store::Error) -> LoaderError {
    LoaderError::Store(StoreError::Object {
        action: "get",
        key: "k".to_owned(),
        source,
    })
}

/// An object store failure caused by a socket error of `kind`.
pub(crate) fn io_store(kind: std::io::ErrorKind) -> LoaderError {
    store(object_store::Error::Generic {
        store: "S3",
        source: Box::new(std::io::Error::new(kind, "socket")),
    })
}

/// An object store failure whose client reported an HTTP error of `kind`.
pub(crate) fn http_store(kind: object_store::client::HttpErrorKind) -> LoaderError {
    store(object_store::Error::Generic {
        store: "S3",
        source: Box::new(object_store::client::HttpError::new(
            kind,
            std::io::Error::other("http"),
        )),
    })
}

/// A warehouse call whose request could not be built: a header value with a line break, such as a password read with its trailing newline.
pub(crate) fn unbuildable_request() -> LoaderError {
    crate::http::main::install_tls_provider::install_tls_provider();
    let built = reqwest::Client::new()
        .get("http://127.0.0.1:1/")
        .header("X-Key", "secret\n")
        .build();
    built.map_or_else(
        |error| LoaderError::Snowflake(ApiError::from(error)),
        |_| LoaderError::Invalid("the request was built".to_owned()),
    )
}
