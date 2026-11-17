use crate::file_format::_helpers::load_select::load_select;
use crate::file_format::tests::helpers::parquet_config;
use crate::config::models::{ColumnConfig, EnvelopeField};
use crate::file_format::main::databricks_copy::databricks_copy;
use crate::file_format::tests::test_types::{DatabricksCopyTestCase, LoadSelectTestCase};

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

#[test]
fn given_databricks_columns_when_copying_then_each_is_cast_and_files_are_listed() {
    let test_cases = [
        DatabricksCopyTestCase {
            description: "plain columns are cast and VARIANT parses its JSON text",
            columns: &[("kafka_offset", "BIGINT"), ("value", "VARIANT")],
            files: &["orders_ab.parquet", "orders_cd.parquet"],
            expected_statement: "COPY INTO `main`.`raw`.`orders` FROM (SELECT CAST(`kafka_offset` AS BIGINT) AS `kafka_offset`, parse_json(`value`) AS `value` FROM '/Volumes/main/raw/stage/2026-11-17/') FILEFORMAT = PARQUET FILES = ('orders_ab.parquet', 'orders_cd.parquet')",
        },
        DatabricksCopyTestCase {
            description: "other semi-structured types read their JSON text with from_json",
            columns: &[("headers", "MAP<STRING, STRING>")],
            files: &["orders_ab.parquet"],
            expected_statement: "COPY INTO `main`.`raw`.`orders` FROM (SELECT from_json(`headers`, 'MAP<STRING, STRING>') AS `headers` FROM '/Volumes/main/raw/stage/2026-11-17/') FILEFORMAT = PARQUET FILES = ('orders_ab.parquet')",
        },
    ];
    for test_case in &test_cases {
        let columns: Vec<ColumnConfig> = test_case
            .columns
            .iter()
            .map(|(name, sql_type)| ColumnConfig {
                name: (*name).to_owned(),
                field: EnvelopeField::Value,
                sql_type: (*sql_type).to_owned(),
            })
            .collect();
        let statement = databricks_copy(
            "`main`.`raw`.`orders`",
            &columns,
            "/Volumes/main/raw/stage/2026-11-17/",
            test_case.files,
        );
        assert_eq!(
            statement, test_case.expected_statement,
            "{}",
            test_case.description
        );
    }
}
