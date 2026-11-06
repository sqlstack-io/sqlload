use crate::column_types::models::{ColumnType, Warehouse};
use crate::contract::_helpers::adopt::adoption;
use crate::contract::main::adopt_contracts::adopt_contracts;
use crate::contract::models::{AdoptedColumn, Adoption, ContractReport, ContractResource};
use crate::contract::tests::test_types::AdoptionTestCase;
use crate::loader::_helpers::python::{python_table, python_tables};
use crate::loader::_helpers::resolve::resolve_tables;
use crate::loader::models::TableSpec;
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
    }
}

/// Postgres table `public.orders` of source `app` with `settings`, or Python table `t` of source `s` with JSON `settings`.
fn spec(settings: &str, python: bool, warehouse: Warehouse) -> Result<TableSpec, String> {
    let (text, sources, source_name) = match python {
        true => (
            TARGET.to_owned(),
            format!(
                r#"[{{"name": "s", "module": "src/s.py", "settings": {{"tables": {{"t": {settings}}}}}}}]"#
            ),
            "s",
        ),
        false => (
            format!(
                "{TARGET}[sources.app]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"public.orders\"]\n[sources.app.tables.\"public.orders\"]\ndescription = \"Orders.\"\n{settings}"
            ),
            "[]".to_owned(),
            "app",
        ),
    };
    let python_sources: Vec<PythonSource> =
        serde_json::from_str(&sources).map_err(|error| error.to_string())?;
    let project = parse_python_project(&text, &|_| None, &python_sources)
        .map_err(|error| error.to_string())?;
    let source = &project.sources[source_name];
    let tables = match python {
        true => python_tables(source),
        false => vec![SourceTable {
            schema: "public".to_owned(),
            name: "orders".to_owned(),
            columns: ["id", "Status", "amount", "note"].map(text_column).to_vec(),
            primary_key: Vec::new(),
            indexed_columns: Vec::new(),
            estimated_rows: -1,
            estimated_bytes: -1,
            comment: None,
        }],
    };
    let resolved = resolve_tables(&project, source_name, &tables);
    let mut spec = resolved
        .specs
        .into_iter()
        .next()
        .ok_or_else(|| format!("no spec: {:?}", resolved.problems))?;
    spec.warehouse = warehouse;
    spec.python = python.then(|| python_table("src/s.py", &spec.source_table));
    Ok(spec)
}

fn column_line(column: &AdoptedColumn) -> String {
    match &column.declared_type {
        Some(declared) => format!(
            "~ {} {} (declared {declared})",
            column.column, column.physical_type
        ),
        None => format!("+ {} {}", column.column, column.physical_type),
    }
}

/// The case's adoption as lines.
pub(crate) fn adopted_lines(test_case: &AdoptionTestCase) -> Result<Vec<String>, String> {
    let spec = spec(test_case.settings, test_case.python, test_case.warehouse)?;
    let live: Vec<LiveColumn> = test_case
        .live
        .iter()
        .map(|(name, column_type)| LiveColumn {
            name: (*name).to_owned(),
            column_type: column_type.clone(),
            fraction_digits: None,
        })
        .collect();
    let Some(adopted) = adoption(&spec, &live, test_case.overwrite) else {
        return Ok(Vec::new());
    };
    let mut lines: Vec<String> = adopted.columns.iter().map(column_line).collect();
    lines.extend(adopted.removed.iter().map(|name| format!("- {name}")));
    lines.extend(
        adopted
            .paste
            .iter()
            .map(|paste| format!("paste {paste} in {}", adopted.location)),
    );
    Ok(lines)
}

/// `text` after writing an adoption that declares `amount` as `float`, retypes `status` to `text` and removes `gone`, for Postgres table `public.orders` of source `app` described as `description`, and the problems left.
pub(crate) fn written(text: &str, description: &str) -> Result<(String, Vec<String>), String> {
    let mut report = ContractReport {
        resources: vec![ContractResource {
            findings: Vec::new(),
            relation: "RAW.app__orders".to_owned(),
            resource_name: "app__orders".to_owned(),
            resource_type: "table",
            adoption: Some(Adoption {
                location: "[sources.app.tables.\"public.orders\"]".to_owned(),
                columns: vec![
                    AdoptedColumn {
                        column: "amount".to_owned(),
                        declared_type: None,
                        physical_type: "float".to_owned(),
                    },
                    AdoptedColumn {
                        column: "status".to_owned(),
                        declared_type: Some("int".to_owned()),
                        physical_type: "text".to_owned(),
                    },
                ],
                removed: vec!["gone".to_owned()],
                paste: None,
                toml_key: ("app".to_owned(), "public.orders".to_owned()),
                description: Some(description.to_owned()),
            }),
        }],
        ..ContractReport::default()
    };
    let text = adopt_contracts(text, &mut report)?.unwrap_or_default();
    Ok((text, report.problems))
}
