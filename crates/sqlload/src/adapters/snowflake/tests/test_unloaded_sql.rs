use crate::adapters::snowflake::tests::helpers::unloaded_sql;
use crate::adapters::snowflake::tests::test_types::UnloadedSqlTestCase;
use crate::column_types::models::ColumnType;

const TYPED: &[(&str, ColumnType, Option<ColumnType>)] = &[
    ("id", ColumnType::Int, Some(ColumnType::Int)),
    (
        "seen_at",
        ColumnType::TimestampNtz,
        Some(ColumnType::TimestampNtz),
    ),
    (
        "placed_at",
        ColumnType::TimestampTz,
        Some(ColumnType::TimestampTz),
    ),
    ("opens", ColumnType::Time, Some(ColumnType::Time)),
    ("meta", ColumnType::Json, Some(ColumnType::Json)),
    ("region", ColumnType::Text, None),
    ("_sqlload_load_id", ColumnType::Text, None),
    ("_sqlload_loaded_at", ColumnType::TimestampTz, None),
    ("gone", ColumnType::Text, None),
];

const NUMBERED: &[(&str, ColumnType, Option<ColumnType>)] = &[
    ("id", ColumnType::Int, Some(ColumnType::Int)),
    ("_sqlload_row", ColumnType::Int, None),
];

#[test]
fn given_a_snowflake_sources_rows_when_loading_them_inside_snowflake_then_their_microseconds_turn_back_and_the_loads_columns_fill()
 {
    let test_cases = [
        UnloadedSqlTestCase {
            description: "times and timestamps from whole microseconds (a zoned one in UTC), JSON parsed, the constant, load id and load time as literals, a column the files lack NULL",
            columns: TYPED,
            files: Some(2),
            expected_statements: 1,
            expected_first: &[
                "COPY INTO \"RAW\".\"ORDERS\" (\"ID\", \"SEEN_AT\", \"PLACED_AT\", \"OPENS\", \"META\", \"REGION\", \"_SQLLOAD_LOAD_ID\", \"_SQLLOAD_LOADED_AT\", \"GONE\") FROM (SELECT ",
                "CAST(GET($1, 'id') AS NUMBER(38,0)), ",
                "CAST(TO_TIMESTAMP_NTZ(CAST(GET($1, 'seen_at') AS NUMBER(38,0)), 6) AS TIMESTAMP_NTZ(9)), ",
                "CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CAST(GET($1, 'placed_at') AS NUMBER(38,0)), 6)) AS TIMESTAMP_TZ(9)), ",
                "CAST(TIMEADD(MICROSECOND, CAST(GET($1, 'opens') AS NUMBER(38,0)), '00:00:00'::TIME) AS TIME(9)), ",
                "PARSE_JSON(CAST(GET($1, 'meta') AS VARCHAR)), ",
                "CAST('EU' AS VARCHAR), ",
                "'L1', ",
                "CAST('2026-11-18T00:00:00Z' AS TIMESTAMP_TZ(6)), ",
                "NULL ",
                "FROM @\"DB\".\"RAW\".\"LAKE\"/unload/run-1/) FILES = ('data_0.parquet', 'data_1.parquet') FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE) ON_ERROR = ABORT_STATEMENT FORCE = TRUE",
            ],
            expected_last: "FORCE = TRUE",
        },
        UnloadedSqlTestCase {
            description: "a merge numbers its rows by batch, then file, counting on across COPYs of 1,000 files, then row in the file",
            columns: NUMBERED,
            files: Some(1001),
            expected_statements: 2,
            expected_first: &[
                "3 * 281474976710656 + (CASE METADATA$FILENAME WHEN 'lake/unload/run-1/data_0.parquet' THEN 0 WHEN ",
                "WHEN 'lake/unload/run-1/data_999.parquet' THEN 999 ELSE TO_NUMBER('sqlload: unexpected unloaded file ' || METADATA$FILENAME) END) * 4294967296 + METADATA$FILE_ROW_NUMBER",
            ],
            expected_last: "(CASE METADATA$FILENAME WHEN 'lake/unload/run-1/data_1000.parquet' THEN 1000 ELSE TO_NUMBER('sqlload: unexpected unloaded file ' || METADATA$FILENAME) END) * 4294967296 + METADATA$FILE_ROW_NUMBER FROM @\"DB\".\"RAW\".\"LAKE\"/unload/run-1/) FILES = ('data_1000.parquet')",
        },
        UnloadedSqlTestCase {
            description: "selected in the source's session: one INSERT reading the query's columns by name, turned back the same way; a merge numbers rows in the order the query gives them",
            columns: &[
                ("id", ColumnType::Int, Some(ColumnType::Int)),
                (
                    "placed_at",
                    ColumnType::TimestampTz,
                    Some(ColumnType::TimestampTz),
                ),
                ("_sqlload_row", ColumnType::Int, None),
            ],
            files: None,
            expected_statements: 1,
            expected_first: &[
                "INSERT INTO \"RAW\".\"ORDERS\" (\"ID\", \"PLACED_AT\", \"_SQLLOAD_ROW\") SELECT ",
                "CAST(u.\"id\" AS NUMBER(38,0)), ",
                "CAST(CONVERT_TIMEZONE('UTC', TO_TIMESTAMP_TZ(CAST(u.\"placed_at\" AS NUMBER(38,0)), 6)) AS TIMESTAMP_TZ(9)), ",
                "3 * 281474976710656 + SEQ8() FROM (SELECT ...) AS u",
            ],
            expected_last: "FROM (SELECT ...) AS u",
        },
    ];
    for test_case in test_cases {
        let statements = unloaded_sql(test_case.columns, test_case.files);
        assert_eq!(
            statements.len(),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
        let (first, last) = (&statements[0], &statements[statements.len() - 1]);
        let mut rest = first.as_str();
        for expected in test_case.expected_first {
            let at = rest.find(expected);
            assert!(
                at.is_some(),
                "{}: {expected:?} not in order in {first}",
                test_case.description
            );
            rest = at.map_or("", |at| &rest[at + expected.len()..]);
        }
        assert!(
            last.contains(test_case.expected_last),
            "{}: {last}",
            test_case.description
        );
    }
}
