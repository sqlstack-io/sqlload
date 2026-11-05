use crate::column_types::models::ColumnType;
use crate::external_stage::models::CopyValue;
use crate::external_stage::tests::helpers::{column, direct_sql, lake, many_files, order_columns};
use crate::external_stage::tests::test_types::DirectCopiesTestCase;

const TWO_FILES: &str = "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" (\"ID\", \"NAME\", \"DAY\", \"_SQLLOAD_SOURCE_FILE\", \"_SQLLOAD_SOURCE_VERSION\") FROM (SELECT CAST(GET($1, 'id') AS NUMBER(38,0)), CAST(GET($1, 'name') AS VARCHAR), CASE WHEN METADATA$FILENAME IN ('shop/orders/day=2026-11-03/part-0.parquet', 's3://lake/raw/shop/orders/day=2026-11-03/part-0.parquet') THEN CAST('2026-11-03' AS DATE) WHEN METADATA$FILENAME IN ('shop/orders/day=2026-11-04/part-0.parquet', 's3://lake/raw/shop/orders/day=2026-11-04/part-0.parquet') THEN CAST('2026-11-04' AS DATE) ELSE NULL END, CASE WHEN METADATA$FILENAME IN ('shop/orders/day=2026-11-03/part-0.parquet', 's3://lake/raw/shop/orders/day=2026-11-03/part-0.parquet') THEN 's3://lake/raw/shop/orders/day=2026-11-03/part-0.parquet' WHEN METADATA$FILENAME IN ('shop/orders/day=2026-11-04/part-0.parquet', 's3://lake/raw/shop/orders/day=2026-11-04/part-0.parquet') THEN 's3://lake/raw/shop/orders/day=2026-11-04/part-0.parquet' ELSE TO_VARCHAR(TO_NUMBER('sqlload: unexpected staged file ' || METADATA$FILENAME)) END, CASE WHEN METADATA$FILENAME IN ('shop/orders/day=2026-11-03/part-0.parquet', 's3://lake/raw/shop/orders/day=2026-11-03/part-0.parquet') THEN 'etag-2026-11-03' WHEN METADATA$FILENAME IN ('shop/orders/day=2026-11-04/part-0.parquet', 's3://lake/raw/shop/orders/day=2026-11-04/part-0.parquet') THEN 'etag-2026-11-04' ELSE NULL END FROM @\"ANALYTICS\".\"RAW\".\"_SQLLOAD_STAGE\"/shop/orders/) FILES = ('day=2026-11-03/part-0.parquet', 'day=2026-11-04/part-0.parquet') FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE) ON_ERROR = ABORT_STATEMENT FORCE = TRUE";

#[test]
fn given_files_under_the_stage_when_building_direct_copies_then_values_match_the_rust_reader() {
    let test_cases = [
        DirectCopiesTestCase {
            description: "data columns by name, typed partition values, file name and version per file, FORCE and ABORT",
            files: vec![
                ("day=2026-11-03/part-0.parquet".to_owned(), "2026-11-03"),
                ("day=2026-11-04/part-0.parquet".to_owned(), "2026-11-04"),
            ],
            columns: order_columns(),
            source: "raw/shop/orders/",
            expected_copies: Ok((1, TWO_FILES)),
        },
        DirectCopiesTestCase {
            description: "a timestamp partition is printed in full with its offset",
            files: vec![("hour=x/part-0.parquet".to_owned(), "2026-11-04T10:00:00Z")],
            columns: vec![column("day", ColumnType::TimestampTz, CopyValue::Partition)],
            source: "raw/shop/orders/",
            expected_copies: Ok((
                1,
                "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" (\"DAY\") FROM (SELECT CASE WHEN METADATA$FILENAME IN ('shop/orders/hour=x/part-0.parquet', 's3://lake/raw/shop/orders/hour=x/part-0.parquet') THEN CAST('2026-11-04 10:00:00.000000 +00:00' AS TIMESTAMP_TZ(6)) ELSE NULL END FROM",
            )),
        },
        DirectCopiesTestCase {
            description: "an integer partition keeps the reader's conversion (leading zeros dropped)",
            files: vec![("n=007/part-0.parquet".to_owned(), "007")],
            columns: vec![column("day", ColumnType::Int, CopyValue::Partition)],
            source: "raw/shop/orders/",
            expected_copies: Ok((
                1,
                "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" (\"DAY\") FROM (SELECT CASE WHEN METADATA$FILENAME IN ('shop/orders/n=007/part-0.parquet', 's3://lake/raw/shop/orders/n=007/part-0.parquet') THEN CAST('7' AS NUMBER(38,0)) ELSE NULL END FROM",
            )),
        },
        DirectCopiesTestCase {
            description: "a partition value the reader cannot convert fails, naming the file",
            files: vec![("day=soon/part-0.parquet".to_owned(), "soon")],
            columns: order_columns(),
            source: "raw/shop/orders/",
            expected_copies: Err(
                "partition column day of day=soon/part-0.parquet: \"soon\" does not convert to date",
            ),
        },
        DirectCopiesTestCase {
            description: "1,001 files take two COPY statements",
            files: many_files(1001, 0),
            columns: order_columns(),
            source: "raw/shop/orders/",
            expected_copies: Ok((2, "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\"")),
        },
        DirectCopiesTestCase {
            description: "a batch whose statement would pass 900 KiB is split",
            files: many_files(900, 400),
            columns: order_columns(),
            source: "raw/shop/orders/",
            expected_copies: Ok((4, "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\"")),
        },
        DirectCopiesTestCase {
            description: "files outside the stage url are an error",
            files: vec![("part-0.parquet".to_owned(), "2026-11-04")],
            columns: order_columns(),
            source: "other/",
            expected_copies: Err("s3://lake/other/ is not under the stage url s3://lake/raw/"),
        },
    ];
    for test_case in test_cases {
        let copies = direct_sql(
            &test_case.files,
            &test_case.columns,
            &lake(test_case.source),
        );
        let (count, first) = copies.clone().unwrap_or_default();
        let (expected_count, expected_start) = test_case.expected_copies.unwrap_or_default();
        assert_eq!(count, expected_count, "{}", test_case.description);
        assert!(
            first.starts_with(expected_start),
            "{}: {first}",
            test_case.description
        );
        assert!(
            copies
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_copies.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
