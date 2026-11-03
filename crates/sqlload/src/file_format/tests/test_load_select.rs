use crate::file_format::_helpers::load_select::load_select;
use crate::file_format::tests::helpers::parquet_config;
use crate::file_format::tests::test_types::LoadSelectTestCase;

#[test]
fn given_a_column_when_loading_parquet_then_the_expression_matches_a_json_load()
-> Result<(), String> {
    let test_cases = [
        LoadSelectTestCase {
            description: "exported_at into TIMESTAMP_TZ carries the configured zone's offset",
            time_zone: "America/Los_Angeles",
            column: "EXPORTED_AT",
            field: "exported_at",
            sql_type: "TIMESTAMP_TZ",
            expected_expression: "CONVERT_TIMEZONE('America/Los_Angeles', $1:\"EXPORTED_AT\"::TIMESTAMP_TZ)",
        },
        LoadSelectTestCase {
            description: "exported_at into TIMESTAMP_NTZ is the configured zone's wall clock",
            time_zone: "UTC",
            column: "EXPORTED_AT",
            field: "exported_at",
            sql_type: "TIMESTAMP_NTZ(9)",
            expected_expression: "CONVERT_TIMEZONE('UTC', $1:\"EXPORTED_AT\"::TIMESTAMP_TZ)::TIMESTAMP_NTZ",
        },
        LoadSelectTestCase {
            description: "semi-structured columns parse their JSON text",
            time_zone: "UTC",
            column: "PAYLOAD",
            field: "value",
            sql_type: "VARIANT",
            expected_expression: "PARSE_JSON($1:\"PAYLOAD\"::VARCHAR)",
        },
        LoadSelectTestCase {
            description: "other columns are cast to their type",
            time_zone: "UTC",
            column: "KAFKA_OFFSET",
            field: "offset",
            sql_type: "NUMBER(38,0)",
            expected_expression: "$1:\"KAFKA_OFFSET\"::NUMBER(38,0)",
        },
    ];
    for test_case in &test_cases {
        let config = parquet_config(&format!(
            "[envelope]\ntime_zone = \"{}\"\n[[envelope.columns]]\nname = \"{}\"\nfield = \"{}\"\ntype = \"{}\"\n",
            test_case.time_zone, test_case.column, test_case.field, test_case.sql_type
        ))?;
        let select = load_select(&config);
        assert_eq!(
            select.columns,
            format!("({})", test_case.column),
            "{}",
            test_case.description
        );
        assert_eq!(
            select.expressions, test_case.expected_expression,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
