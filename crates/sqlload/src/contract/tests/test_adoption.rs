use crate::column_types::models::{ColumnType, Warehouse};
use crate::contract::tests::helpers::adopted_lines;
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
            python: false,
            warehouse: Warehouse::DuckDb,
            live: vec![
                ("id", ColumnType::Int),
                ("status", ColumnType::Text),
                ("extra", ColumnType::Bool),
                ("_sqlload_load_id", ColumnType::Text),
            ],
            overwrite: false,
            expected: &["+ id int", "+ Status text"],
        },
        AdoptionTestCase {
            description: "without overwrite, declared columns stay as they are even when the live type differs or the column is gone",
            settings: "columns = { amount = \"decimal(12,2)\", note = \"text\" }\n",
            python: false,
            warehouse: Warehouse::DuckDb,
            live: vec![("id", ColumnType::Int), ("amount", decimal(4))],
            overwrite: false,
            expected: &["+ id int"],
        },
        AdoptionTestCase {
            description: "overwrite retypes a declared column to its live type and removes one the table lacks",
            settings: "columns = { amount = \"decimal(12,2)\", note = \"text\" }\n",
            python: false,
            warehouse: Warehouse::DuckDb,
            live: vec![("id", ColumnType::Int), ("amount", decimal(4))],
            overwrite: true,
            expected: &[
                "+ id int",
                "~ amount decimal(12,4) (declared decimal(12,2))",
                "- note",
            ],
        },
        AdoptionTestCase {
            description: "a declared type the warehouse stores the same way agrees, so nothing changes",
            settings: "columns = { note = \"varchar(50)\" }\n",
            python: false,
            warehouse: Warehouse::DuckDb,
            live: vec![("note", ColumnType::Text)],
            overwrite: true,
            expected: &[],
        },
        AdoptionTestCase {
            description: "a masked column stays under mask: never declared, retyped or removed",
            settings: "mask = { note = \"hash\" }\n",
            python: false,
            warehouse: Warehouse::DuckDb,
            live: vec![("id", ColumnType::Int)],
            overwrite: true,
            expected: &["+ id int"],
        },
        AdoptionTestCase {
            description: "a Python table on Snowflake: names as the function yields them, flattened columns left out, and the whole columns= to paste",
            settings: r#"{"description": "T.", "columns": {"id": "int"}, "flatten": ["meta"]}"#,
            python: true,
            warehouse: Warehouse::Snowflake,
            live: vec![
                ("ID", ColumnType::Int),
                ("NAME", ColumnType::Text),
                ("META__A", ColumnType::Text),
                ("Mixed", ColumnType::Bool),
                ("_SQLLOAD_LOAD_ID", ColumnType::Text),
            ],
            overwrite: false,
            expected: &[
                "+ name text",
                "+ Mixed bool",
                r#"paste columns={"id": "int", "name": "text", "Mixed": "bool"} in src/s.py"#,
            ],
        },
    ];
    for test_case in test_cases {
        let expected: Vec<String> = test_case
            .expected
            .iter()
            .map(|line| (*line).to_owned())
            .collect();
        assert_eq!(
            adopted_lines(&test_case),
            Ok(expected),
            "{}",
            test_case.description
        );
    }
}
