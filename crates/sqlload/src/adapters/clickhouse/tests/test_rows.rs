use crate::adapters::clickhouse::_helpers::session::rows::text_rows;
use crate::adapters::clickhouse::tests::test_types::TextRowsTestCase;

#[test]
fn given_a_clickhouse_answer_when_reading_its_rows_then_null_text_is_null_only_where_the_column_can_hold_null()
-> Result<(), String> {
    let test_cases = [
        TextRowsTestCase {
            description: "an empty table's maximum, a Nullable column, is NULL",
            answer: r#"{"meta":[{"name":"m","type":"Nullable(String)"}],"data":[["ᴺᵁᴸᴸ"]],"rows":1}"#,
            expected_rows: &[&[None]],
        },
        TextRowsTestCase {
            description: "a bare NULL, typed Nullable(Nothing), is NULL",
            answer: r#"{"meta":[{"name":"n","type":"Nullable(Nothing)"}],"data":[["ᴺᵁᴸᴸ"]],"rows":1}"#,
            expected_rows: &[&[None]],
        },
        TextRowsTestCase {
            description: "a NULL in a LowCardinality(Nullable) column is NULL, beside a value",
            answer: r#"{"meta":[{"name":"a","type":"LowCardinality(Nullable(String))"},{"name":"b","type":"String"}],"data":[["ᴺᵁᴸᴸ","x"],["y","z"]],"rows":2}"#,
            expected_rows: &[&[None, Some("x")], &[Some("y"), Some("z")]],
        },
        TextRowsTestCase {
            description: "the same text in a column that cannot hold NULL is the value itself",
            answer: r#"{"meta":[{"name":"s","type":"String"},{"name":"n","type":"Nullable(String)"}],"data":[["ᴺᵁᴸᴸ","ᴺᵁᴸᴸ"]],"rows":1}"#,
            expected_rows: &[&[Some("ᴺᵁᴸᴸ"), None]],
        },
        TextRowsTestCase {
            description: "a NULL in a Dynamic or a Variant column is NULL",
            answer: r#"{"meta":[{"name":"d","type":"Dynamic"},{"name":"v","type":"Variant(Int64, String)"}],"data":[["ᴺᵁᴸᴸ","ᴺᵁᴸᴸ"]],"rows":1}"#,
            expected_rows: &[&[None, None]],
        },
        TextRowsTestCase {
            description: "a JSON null is NULL",
            answer: r#"{"meta":[{"name":"s","type":"String"}],"data":[[null]],"rows":1}"#,
            expected_rows: &[&[None]],
        },
    ];
    for test_case in test_cases {
        let rows = text_rows(test_case.answer)
            .map_err(|error| format!("{}: {error}", test_case.description))?;
        let read: Vec<Vec<Option<&str>>> = rows
            .iter()
            .map(|row| row.iter().map(Option::as_deref).collect())
            .collect();
        assert_eq!(read, test_case.expected_rows, "{}", test_case.description);
    }
    Ok(())
}
