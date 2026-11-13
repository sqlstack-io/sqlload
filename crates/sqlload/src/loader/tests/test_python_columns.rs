use crate::column_types::models::ColumnType;
use crate::column_types::models::Warehouse;
use crate::loader::_helpers::python::fitting_type;
use crate::loader::tests::helpers::{
    bound_text, crossed_schema, inferred_columns, integer_code, live_paths, lower_case_rows,
    problems_for, spelling,
};
use crate::loader::tests::test_types::{
    BoundArrayTestCase, ChildPathsTestCase, DeclaredProblemsTestCase, FittingTypeTestCase,
    InferredTableTestCase, SourceSpellingTestCase,
};

#[test]
fn given_an_inferred_and_a_live_type_when_choosing_then_the_live_type_wins_when_the_values_fit() {
    let test_cases = [
        FittingTypeTestCase {
            description: "ints fit a text column",
            inferred: ColumnType::Int,
            live: Some(ColumnType::Text),
            expected_type: ColumnType::Text,
        },
        FittingTypeTestCase {
            description: "ints fit a float column",
            inferred: ColumnType::Int,
            live: Some(ColumnType::Float),
            expected_type: ColumnType::Float,
        },
        FittingTypeTestCase {
            description: "floats do not fit an int column, so planning decides about widening",
            inferred: ColumnType::Float,
            live: Some(ColumnType::Int),
            expected_type: ColumnType::Float,
        },
        FittingTypeTestCase {
            description: "text does not fit a timestamp column",
            inferred: ColumnType::Text,
            live: Some(ColumnType::TimestampTz),
            expected_type: ColumnType::Text,
        },
        FittingTypeTestCase {
            description: "no live column keeps the inferred type",
            inferred: ColumnType::Json,
            live: None,
            expected_type: ColumnType::Json,
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            fitting_type(test_case.inferred, test_case.live),
            test_case.expected_type,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_crossed_batches_when_inferring_the_table_then_columns_follow_the_data_and_the_live_table()
{
    let test_cases = [
        InferredTableTestCase {
            description: "Arrow types map to portable types; metadata columns are not data; declared columns no row had are kept",
            schema: crossed_schema,
            declared: &["total"],
            planned: &["total"],
            null_columns: &[],
            live: &[],
            expected_columns: "id:int,score:float,code:text,at:timestamp_tz,meta:json,total:text",
        },
        InferredTableTestCase {
            description: "a value that fits the live column takes its type",
            schema: integer_code,
            declared: &[],
            planned: &[],
            null_columns: &[],
            live: &[("CODE", ColumnType::Text)],
            expected_columns: "code:text",
        },
        InferredTableTestCase {
            description: "upper-case live columns (Snowflake) known at plan are not added again beside the data's lower-case ones",
            schema: lower_case_rows,
            declared: &[],
            planned: &["SEQ", "KIND"],
            null_columns: &[],
            live: &[("SEQ", ColumnType::Int), ("KIND", ColumnType::Text)],
            expected_columns: "seq:int,kind:text",
        },
        InferredTableTestCase {
            description: "a declared column the data has in another case is not added twice",
            schema: lower_case_rows,
            declared: &["SEQ", "note"],
            planned: &["SEQ", "note"],
            null_columns: &[],
            live: &[],
            expected_columns: "seq:int,kind:text,note:text",
        },
        InferredTableTestCase {
            description: "a column that only held NULL takes the live type instead of text",
            schema: lower_case_rows,
            declared: &[],
            planned: &[],
            null_columns: &["seq"],
            live: &[("SEQ", ColumnType::Int)],
            expected_columns: "seq:int,kind:text",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            inferred_columns(&test_case),
            test_case.expected_columns,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_live_column_when_naming_it_for_a_python_table_then_the_lower_case_spelling_is_used_when_the_warehouse_stores_it_so()
-> Result<(), String> {
    let test_cases = [
        SourceSpellingTestCase {
            description: "Snowflake stores plain lower-case names in upper case",
            stored: "UPDATED_AT",
            warehouse: Warehouse::Snowflake,
            expected_name: "updated_at",
        },
        SourceSpellingTestCase {
            description: "a quoted mixed-case Snowflake name keeps its spelling",
            stored: "Updated",
            warehouse: Warehouse::Snowflake,
            expected_name: "Updated",
        },
        SourceSpellingTestCase {
            description: "DuckDB keeps lower-case names",
            stored: "seq",
            warehouse: Warehouse::DuckDb,
            expected_name: "seq",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            spelling(&test_case)?,
            test_case.expected_name,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_declared_keys_when_the_data_lacks_them_then_close_names_and_missing_masks_fail()
-> Result<(), String> {
    let test_cases = [
        DeclaredProblemsTestCase {
            description: "a mask key in another case than the data's column fails with the right name",
            settings: r#"{"mask": {"Email": "hash"}}"#,
            columns: &["id", "email"],
            rows: 1,
            expected_problems: "mask key Email names no column of the data; did you mean email?",
        },
        DeclaredProblemsTestCase {
            description: "a misspelt columns key fails with the right name",
            settings: r#"{"columns": {"totl": "int"}}"#,
            columns: &["id", "total"],
            rows: 1,
            expected_problems: "columns key totl names no column of the data; did you mean total?",
        },
        DeclaredProblemsTestCase {
            description: "a masked column the data does not have fails while there are rows",
            settings: r#"{"mask": {"ssn": "hash"}}"#,
            columns: &["id"],
            rows: 3,
            expected_problems: "mask ssn names no column of the data",
        },
        DeclaredProblemsTestCase {
            description: "no rows, nothing to mask",
            settings: r#"{"mask": {"ssn": "hash"}}"#,
            columns: &[],
            rows: 0,
            expected_problems: "",
        },
        DeclaredProblemsTestCase {
            description: "a declared column no row had is created as declared",
            settings: r#"{"columns": {"extra": "int"}, "mask": {"id": "hash"}}"#,
            columns: &["id"],
            rows: 1,
            expected_problems: "",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            problems_for(&test_case)?,
            test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_cursor_bound_when_filtering_python_rows_then_it_takes_the_cursor_columns_arrow_type()
-> Result<(), String> {
    let test_cases = [
        BoundArrayTestCase {
            description: "decimal",
            text: "12.5",
            cursor_type: ColumnType::Decimal {
                precision: 18,
                scale: 2,
            },
            expected_value: "Decimal128(18, 2) 12.50",
        },
        BoundArrayTestCase {
            description: "smallint",
            text: "3",
            cursor_type: ColumnType::SmallInt,
            expected_value: "Int16 3",
        },
        BoundArrayTestCase {
            description: "int",
            text: "42",
            cursor_type: ColumnType::Int,
            expected_value: "Int64 42",
        },
        BoundArrayTestCase {
            description: "timestamp with a time zone, as the destination's maximum",
            text: "2026-01-02 09:00:00.000000+00",
            cursor_type: ColumnType::TimestampTz,
            expected_value: "Timestamp(µs, \"UTC\") 2026-01-02T09:00:00Z",
        },
        BoundArrayTestCase {
            description: "date",
            text: "2026-01-02",
            cursor_type: ColumnType::Date,
            expected_value: "Date32 2026-01-02",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            bound_text(&test_case)?,
            test_case.expected_value,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_live_child_tables_when_reading_their_paths_then_each_warehouses_spelling_gives_the_fields_path()
-> Result<(), String> {
    let test_cases = [
        ChildPathsTestCase {
            description: "Snowflake lists plain names upper-cased; their paths are the fields as the rows name them",
            warehouse: Warehouse::Snowflake,
            parent: "s__t",
            tables: &["S__T__ITEMS", "S__T__ITEMS__PARTS"],
            expected_paths: &[
                ("items", "S__T__ITEMS"),
                ("items.parts", "S__T__ITEMS__PARTS"),
            ],
        },
        ChildPathsTestCase {
            description: "DuckDB lists them as written",
            warehouse: Warehouse::DuckDb,
            parent: "s__t",
            tables: &["s__t__items"],
            expected_paths: &[("items", "s__t__items")],
        },
        ChildPathsTestCase {
            description: "Postgres lists an upper-case parent's children folded to lower case",
            warehouse: Warehouse::Postgres,
            parent: "S__T",
            tables: &["s__t__items", "other__items"],
            expected_paths: &[("items", "s__t__items")],
        },
    ];
    for test_case in test_cases {
        let found = live_paths(&test_case)?;
        let found: Vec<(&str, &str)> = found
            .iter()
            .map(|(path, table)| (path.as_str(), table.as_str()))
            .collect();
        assert_eq!(found, test_case.expected_paths, "{}", test_case.description);
    }
    Ok(())
}
