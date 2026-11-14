use crate::column_types::models::{ColumnType, Warehouse};
use crate::contract::_helpers::adopt::adoption;
use crate::contract::_helpers::file_drift::file_drift;
use crate::contract::_helpers::read_from::other_warehouse;
use crate::contract::main::render_contract::render_contract;
use crate::contract::main::write_contracts::write_contracts;
use crate::contract::models::{AdoptedColumn, Adoption, ContractReport, ContractResource};
use crate::contract::tests::test_types::{AdoptionTestCase, FileDriftTestCase};
use crate::load_naming::models::Naming;
use crate::loader::_helpers::python::{python_table, python_tables};
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::models::{FileSchema, TableSpec};
use crate::postgres_source::models::{PgType, SourceColumn, SourceTable};
use crate::project::main::parse_python_project::parse_python_project;
use crate::python_sources::models::PythonSource;
use crate::schema_plan::models::LiveColumn;

const TARGET: &str = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[connections.pg]\nkind = \"postgres\"\nhost = \"h\"\nuser = \"u\"\ndatabase = \"d\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n";

fn text_column(name: &str) -> SourceColumn {
    SourceColumn {
        name: name.to_owned(),
        postgres_type: "text".to_owned(),
        pg_type: PgType::Other,
        column_type: Ok(ColumnType::Text),
        nullable: true,
        source_spelling: None,
    }
}

/// The first spec of `source_name` among `tables`, on `warehouse`.
fn first_spec(
    (text, python): (&str, &str),
    source_name: &str,
    tables: fn(&crate::project::models::SourceConfig) -> Vec<SourceTable>,
    warehouse: Warehouse,
) -> Result<TableSpec, String> {
    let python: Vec<PythonSource> =
        serde_json::from_str(python).map_err(|error| error.to_string())?;
    let project =
        parse_python_project(text, &|_| None, &python).map_err(|error| error.to_string())?;
    let resolved = resolve_tables(
        &project,
        source_name,
        &tables(&project.sources[source_name]),
    );
    let mut spec = resolved
        .specs
        .into_iter()
        .next()
        .ok_or_else(|| format!("no spec: {:?}", resolved.problems))?;
    spec.warehouse = warehouse;
    Ok(spec)
}

fn orders(_: &crate::project::models::SourceConfig) -> Vec<SourceTable> {
    vec![SourceTable {
        schema: "public".to_owned(),
        name: "orders".to_owned(),
        columns: ["id", "Status", "amount", "note"].map(text_column).to_vec(),
        primary_key: Vec::new(),
        indexed_columns: Vec::new(),
        estimated_rows: -1,
        estimated_bytes: -1,
        comment: None,
        listed_keys: Vec::new(),
    }]
}

/// Postgres table `public.orders` of source `app`, with sqlload.toml `settings` under its entry.
pub(crate) fn postgres_spec(settings: &str, warehouse: Warehouse) -> Result<TableSpec, String> {
    let text = format!(
        "{TARGET}[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"public.orders\"]\n[sources.app.tables.\"public.orders\"]\ndescription = \"Orders.\"\n{settings}"
    );
    first_spec((&text, "[]"), "app", orders, warehouse)
}

/// Python table `t` of source `s`, declared in `src/s.py` with JSON `settings`.
pub(crate) fn python_spec(settings: &str, warehouse: Warehouse) -> Result<TableSpec, String> {
    let sources = format!(
        r#"[{{"name": "s", "module": "src/s.py", "settings": {{"tables": {{"t": {settings}}}}}}}]"#
    );
    let mut spec = first_spec((TARGET, &sources), "s", python_tables, warehouse)?;
    spec.python = Some(python_table("src/s.py", &spec.source_table));
    Ok(spec)
}

/// What `contract generate` prints for the case's table, or the error building it.
pub(crate) fn adoption_output(test_case: &AdoptionTestCase) -> Vec<String> {
    printed(test_case).unwrap_or_else(|error| vec![format!("error: {error}")])
}

fn printed(test_case: &AdoptionTestCase) -> Result<Vec<String>, String> {
    let spec = (test_case.spec)(test_case.settings, test_case.warehouse)?;
    let live: Vec<LiveColumn> = test_case
        .live
        .iter()
        .map(|(name, column_type)| LiveColumn {
            name: (*name).to_owned(),
            column_type: column_type.clone(),
            fraction_digits: None,
        })
        .collect();
    let report = ContractReport {
        resources: vec![ContractResource {
            findings: Vec::new(),
            relation: "RAW.t".to_owned(),
            resource_name: "t".to_owned(),
            resource_type: "table",
            adoption: adoption(&spec, &live, test_case.overwrite),
        }],
        ..ContractReport::default()
    };
    Ok(render_contract(&report, false).0)
}

/// `text` after writing an adoption that declares `amount` as `float`, retypes `status` to `text` and removes `gone`, for Postgres table `public.orders` of source `app` described as `description` (empty when unchanged), then the problems left; or the error.
pub(crate) fn written(text: &str, description: &str) -> Vec<String> {
    write(text, description).unwrap_or_else(|error| vec![format!("error: {error}")])
}

fn write(text: &str, description: &str) -> Result<Vec<String>, String> {
    let adopted = |column: &str, declared: Option<&str>, physical: &str| AdoptedColumn {
        column: column.to_owned(),
        declared_type: declared.map(str::to_owned),
        physical_type: physical.to_owned(),
    };
    let report = ContractReport {
        resources: vec![ContractResource {
            findings: Vec::new(),
            relation: "RAW.app__orders".to_owned(),
            resource_name: "app__orders".to_owned(),
            resource_type: "table",
            adoption: Some(Adoption {
                location: "[sources.app.tables.\"public.orders\"]".to_owned(),
                columns: vec![
                    adopted("amount", None, "float"),
                    adopted("status", Some("int"), "text"),
                ],
                removed: vec!["gone".to_owned()],
                paste: None,
                toml_key: ("app".to_owned(), "public.orders".to_owned()),
                description: Some(description.to_owned()),
            }),
        }],
        ..ContractReport::default()
    };
    let (text, report) = write_contracts(text, report)?;
    Ok([vec![text.unwrap_or_default()], report.problems].concat())
}

/// The sources of a project with DuckDB targets `dev` and `test` and Snowflake target `prod` (source `orders` into `dev`, `events` into `prod`) whose own warehouse differs from `from`'s.
pub(crate) fn other_warehouse_sources(from: Option<&str>) -> Vec<String> {
    let text = "[connections.wh]\nkind = \"duckdb\"\ndatabase = \":memory:\"\n[connections.sf]\nkind = \"snowflake\"\naccount = \"acct\"\nuser = \"loader\"\nauth = { method = \"pat\", token = \"t\" }\ndatabase = \"DB\"\nload_method = { type = \"internal_user_stage\" }\n[connections.lake]\nkind = \"filesystem\"\nroot = \"/tmp\"\n[targets.dev]\nconnection = \"wh\"\nloader_schema = \"RAW\"\n[targets.test]\nconnection = \"wh\"\nloader_schema = \"TEST\"\n[targets.prod]\nconnection = \"sf\"\nloader_schema = \"RAW\"\n[sources.orders]\nkind = \"files\"\nconnection = \"lake\"\npath = \"o/\"\ndescription = \"O.\"\ndestination = \"dev\"\n[sources.events]\nkind = \"files\"\nconnection = \"lake\"\npath = \"e/\"\ndescription = \"E.\"\ndestination = \"prod\"\n";
    parse_python_project(text, &|_| None, &[])
        .map(|project| other_warehouse(&project, from))
        .unwrap_or_else(|error| vec![format!("error: {error}")])
}

fn live_of(columns: &[(&str, ColumnType)]) -> Vec<LiveColumn> {
    columns
        .iter()
        .map(|(name, column_type)| LiveColumn {
            name: (*name).to_owned(),
            column_type: column_type.clone(),
            fraction_digits: None,
        })
        .collect()
}

/// The case's drift findings as `kind: message`.
pub(crate) fn drift_messages(test_case: &FileDriftTestCase) -> Vec<String> {
    let files: Vec<FileSchema> = test_case
        .files
        .iter()
        .map(|(key, columns)| FileSchema {
            key: (*key).to_owned(),
            columns: live_of(columns),
        })
        .collect();
    file_drift(Naming::Sqlload, &live_of(&test_case.live), &files)
        .into_iter()
        .map(|finding| format!("{}: {}", finding.kind, finding.message))
        .collect()
}
