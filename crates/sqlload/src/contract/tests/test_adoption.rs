use crate::column_types::models::{ColumnType, Warehouse};
use crate::contract::tests::helpers::{adoption_output, postgres_spec, python_spec};
use crate::contract::tests::test_types::AdoptionTestCase;

#[test]
fn given_live_columns_when_adopting_then_undeclared_ones_are_declared_and_overwrite_also_retypes_and_removes()
 {
    let decimal = |scale| ColumnType::Decimal {
        precision: 12,
        scale,
    };
    let test_cases = [
        AdoptionTestCase {
            description: "undeclared columns take the source's spelling; ones the source lacks and SQLLoad's own are left out",
            settings: "",
            spec: postgres_spec,
            warehouse: Warehouse::DuckDb,
            live: vec![
                ("id", ColumnType::Int),
                ("status", ColumnType::Text),
                ("extra", ColumnType::Bool),
                ("_sqlload_load_id", ColumnType::Text),
            ],
            overwrite: false,
            expected_lines: &[
                "Contract comparison",
                "  OK    table:t",
                "        adopt: columns of [sources.app.tables.\"public.orders\"]",
                "          + id = \"int\"",
                "          + Status = \"text\"",
                "",
                "Run contract generate --write to update sqlload.toml.",
                "",
                "0 contract difference(s)",
            ],
        },
        AdoptionTestCase {
            description: "without overwrite, declared columns stay as they are even when the live type differs or the column is gone",
            settings: "columns = { amount = \"decimal(12,2)\", note = \"text\" }\n",
            spec: postgres_spec,
            warehouse: Warehouse::DuckDb,
            live: vec![("id", ColumnType::Int), ("amount", decimal(4))],
            overwrite: false,
            expected_lines: &[
                "Contract comparison",
                "  OK    table:t",
                "        adopt: columns of [sources.app.tables.\"public.orders\"]",
                "          + id = \"int\"",
                "",
                "Run contract generate --write to update sqlload.toml.",
                "",
                "0 contract difference(s)",
            ],
        },
        AdoptionTestCase {
            description: "overwrite retypes a declared column to its live type and removes one the table lacks",
            settings: "columns = { amount = \"decimal(12,2)\", note = \"text\" }\n",
            spec: postgres_spec,
            warehouse: Warehouse::DuckDb,
            live: vec![("id", ColumnType::Int), ("amount", decimal(4))],
            overwrite: true,
            expected_lines: &[
                "Contract comparison",
                "  OK    table:t",
                "        adopt: columns of [sources.app.tables.\"public.orders\"]",
                "          + id = \"int\"",
                "          ~ amount = \"decimal(12,4)\" (declared decimal(12,2))",
                "          - note (not in the live table)",
                "",
                "Run contract generate --write to update sqlload.toml.",
                "",
                "0 contract difference(s)",
            ],
        },
        AdoptionTestCase {
            description: "a declared type the warehouse stores the same way agrees, so nothing changes",
            settings: "columns = { note = \"varchar(50)\" }\n",
            spec: postgres_spec,
            warehouse: Warehouse::DuckDb,
            live: vec![("note", ColumnType::Text)],
            overwrite: true,
            expected_lines: &[
                "Contract comparison",
                "  OK    table:t",
                "",
                "0 contract difference(s)",
            ],
        },
        AdoptionTestCase {
            description: "a masked column stays under mask: never declared, retyped or removed",
            settings: "mask = { note = \"hash\" }\n",
            spec: postgres_spec,
            warehouse: Warehouse::DuckDb,
            live: vec![("id", ColumnType::Int)],
            overwrite: true,
            expected_lines: &[
                "Contract comparison",
                "  OK    table:t",
                "        adopt: columns of [sources.app.tables.\"public.orders\"]",
                "          + id = \"int\"",
                "",
                "Run contract generate --write to update sqlload.toml.",
                "",
                "0 contract difference(s)",
            ],
        },
        AdoptionTestCase {
            description: "a Python table on Snowflake: names as the function yields them, flattened columns left out, and the whole columns= to paste",
            settings: r#"{"description": "T.", "columns": {"id": "int"}, "flatten": ["meta"]}"#,
            spec: python_spec,
            warehouse: Warehouse::Snowflake,
            live: vec![
                ("ID", ColumnType::Int),
                ("NAME", ColumnType::Text),
                ("META__A", ColumnType::Text),
                ("Mixed", ColumnType::Bool),
                ("_SQLLOAD_LOAD_ID", ColumnType::Text),
            ],
            overwrite: false,
            expected_lines: &[
                "Contract comparison",
                "  OK    table:t",
                "        adopt: paste into the table's declaration in src/s.py",
                r#"          columns={"id": "int", "name": "text", "Mixed": "bool"}"#,
                "",
                "0 contract difference(s)",
            ],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            adoption_output(&test_case),
            test_case.expected_lines,
            "{}",
            test_case.description
        );
    }
}
