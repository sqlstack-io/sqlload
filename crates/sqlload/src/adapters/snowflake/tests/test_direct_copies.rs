use crate::adapters::snowflake::_helpers::dialect::text_values::snowflake_encoding;
use crate::adapters::snowflake::models::CopyValue;
use crate::adapters::snowflake::tests::helpers::{
    EXTERNAL, INTERNAL, column, csv_in, direct_sql, many_files, order_columns,
};
use crate::adapters::snowflake::tests::test_types::{
    DirectCopiesTestCase, SnowflakeEncodingTestCase,
};
use crate::column_types::models::ColumnType;
use crate::files_source::constants::RECORD_COLUMN;
use crate::files_source::main::text_charset::text_charset;
use crate::files_source::models::{CsvOptions, FilesEncoding, SourceFormat, StoredRead};

const WHOLE_FILE: CopyValue = CopyValue::File {
    file_type: None,
    stored: StoredRead::Plain,
    short: false,
};

const TWO_FILES: &str = "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" (\"ID\", \"NAME\", \"DAY\", \"_SQLLOAD_SOURCE_FILE\", \"_SQLLOAD_SOURCE_VERSION\") FROM (SELECT CAST(GET($1, 'id') AS NUMBER(38,0)), CAST(GET($1, 'name') AS VARCHAR), CASE WHEN METADATA$FILENAME = 'raw/shop/orders/day=2026-11-03/part-0.parquet' THEN CAST('2026-11-03' AS DATE) WHEN METADATA$FILENAME = 'raw/shop/orders/day=2026-11-04/part-0.parquet' THEN CAST('2026-11-04' AS DATE) ELSE NULL END, CASE WHEN METADATA$FILENAME = 'raw/shop/orders/day=2026-11-03/part-0.parquet' THEN 's3://lake/raw/shop/orders/day=2026-11-03/part-0.parquet' WHEN METADATA$FILENAME = 'raw/shop/orders/day=2026-11-04/part-0.parquet' THEN 's3://lake/raw/shop/orders/day=2026-11-04/part-0.parquet' ELSE TO_VARCHAR(TO_NUMBER('sqlload: unexpected staged file ' || METADATA$FILENAME)) END, CASE WHEN METADATA$FILENAME = 'raw/shop/orders/day=2026-11-03/part-0.parquet' THEN 'etag-2026-11-03' WHEN METADATA$FILENAME = 'raw/shop/orders/day=2026-11-04/part-0.parquet' THEN 'etag-2026-11-04' ELSE NULL END FROM @\"ANALYTICS\".\"RAW\".\"_SQLLOAD_STAGE\"/shop/orders/) FILES = ('day=2026-11-03/part-0.parquet', 'day=2026-11-04/part-0.parquet') FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE) ON_ERROR = ABORT_STATEMENT FORCE = TRUE";

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
            from: EXTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Ok((1, TWO_FILES)),
        },
        DirectCopiesTestCase {
            description: "on an external stage METADATA$FILENAME is the bucket-relative key: the stage's prefix included, no URL",
            files: vec![("part-0.parquet".to_owned(), "2026-11-04")],
            columns: vec![column(
                "_sqlload_source_file",
                ColumnType::Text,
                CopyValue::SourceFile,
            )],
            from: EXTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Ok((
                1,
                "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" (\"_SQLLOAD_SOURCE_FILE\") FROM (SELECT CASE WHEN METADATA$FILENAME = 'raw/shop/orders/part-0.parquet' THEN 's3://lake/raw/shop/orders/part-0.parquet' ELSE TO_VARCHAR(TO_NUMBER('sqlload: unexpected staged file ' || METADATA$FILENAME)) END FROM @\"ANALYTICS\".\"RAW\".\"_SQLLOAD_STAGE\"/shop/orders/)",
            )),
        },
        DirectCopiesTestCase {
            description: "a timestamp partition is printed in full with its offset",
            files: vec![("hour=x/part-0.parquet".to_owned(), "2026-11-04T10:00:00Z")],
            columns: vec![column("day", ColumnType::TimestampTz, CopyValue::Partition)],
            from: EXTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Ok((
                1,
                "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" (\"DAY\") FROM (SELECT CASE WHEN METADATA$FILENAME = 'raw/shop/orders/hour=x/part-0.parquet' THEN CAST('2026-11-04 10:00:00.000000 +00:00' AS TIMESTAMP_TZ(9)) ELSE NULL END FROM",
            )),
        },
        DirectCopiesTestCase {
            description: "an integer partition keeps the reader's conversion (leading zeros dropped)",
            files: vec![("n=007/part-0.parquet".to_owned(), "007")],
            columns: vec![column("day", ColumnType::Int, CopyValue::Partition)],
            from: EXTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Ok((
                1,
                "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\" (\"DAY\") FROM (SELECT CASE WHEN METADATA$FILENAME = 'raw/shop/orders/n=007/part-0.parquet' THEN CAST('7' AS NUMBER(38,0)) ELSE NULL END FROM",
            )),
        },
        DirectCopiesTestCase {
            description: "a partition value the reader cannot convert fails, naming the file",
            files: vec![("day=soon/part-0.parquet".to_owned(), "soon")],
            columns: order_columns(),
            from: EXTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Err(
                "partition column day of day=soon/part-0.parquet: \"soon\" does not convert to date",
            ),
        },
        DirectCopiesTestCase {
            description: "1,001 files take two COPY statements",
            files: many_files(1001, 0),
            columns: order_columns(),
            from: EXTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Ok((2, "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\"")),
        },
        DirectCopiesTestCase {
            description: "a batch whose statement would pass 900 KiB is split",
            files: many_files(900, 400),
            columns: order_columns(),
            from: EXTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Ok((4, "COPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS\"")),
        },
        DirectCopiesTestCase {
            description: "files copied unchanged to @~ are read under the batch's folder, and matched by the prefix the load gives them",
            files: vec![("day=2026-11-04/part-0.parquet".to_owned(), "2026-11-04")],
            columns: order_columns(),
            from: INTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Ok((
                1,
                "WHEN METADATA$FILENAME = '@~/sqlload/L/00001/day=2026-11-04/part-0.parquet' THEN 's3://lake/other/day=2026-11-04/part-0.parquet' ELSE TO_VARCHAR(TO_NUMBER('sqlload: unexpected staged file ' || METADATA$FILENAME)) END",
            )),
        },
        DirectCopiesTestCase {
            description: "files copied unchanged to @~ are loaded from there",
            files: vec![("day=2026-11-04/part-0.parquet".to_owned(), "2026-11-04")],
            columns: order_columns(),
            from: INTERNAL,
            encoding: FilesEncoding::default(),
            header: &[],
            expected_copies: Ok((
                1,
                "FROM @~/sqlload/L/00001/) FILES = ('day=2026-11-04/part-0.parquet')",
            )),
        },
        DirectCopiesTestCase {
            description: "record shape, ndjson: each record itself, failing on one that is not an object",
            files: vec![("part-0.ndjson".to_owned(), "2026-11-04")],
            columns: vec![column(RECORD_COLUMN, ColumnType::Json, WHOLE_FILE)],
            from: EXTERNAL,
            encoding: FilesEncoding {
                format: SourceFormat::Ndjson,
                record: Some(RECORD_COLUMN.to_owned()),
                ..FilesEncoding::default()
            },
            header: &[],
            expected_copies: Ok((
                1,
                "(\"RECORD\") FROM (SELECT IFF(IS_OBJECT($1), $1, CAST(TO_VARCHAR(TO_NUMBER('sqlload: ' || METADATA$FILENAME || ' row ' || METADATA$FILE_ROW_NUMBER || ': the record is not a JSON object')) AS VARIANT)) FROM",
            )),
        },
        DirectCopiesTestCase {
            description: "record shape, csv: each line an array of its fields as strings, a NULL value as JSON null, a line with another number of fields failing",
            files: vec![("part-0.csv".to_owned(), "2026-11-04")],
            columns: vec![column(RECORD_COLUMN, ColumnType::Json, WHOLE_FILE)],
            from: EXTERNAL,
            encoding: FilesEncoding {
                format: SourceFormat::Csv,
                csv: CsvOptions {
                    null_values: vec!["NA".to_owned()],
                    ..CsvOptions::default()
                },
                record: Some(RECORD_COLUMN.to_owned()),
                ..FilesEncoding::default()
            },
            header: &["column_1", "column_2"],
            expected_copies: Ok((
                1,
                "(SELECT IFF($2 IS NULL OR $3 IS NOT NULL, CAST(TO_VARCHAR(TO_NUMBER('sqlload: a line does not have 2 fields, as the file''s first line does')) AS VARIANT), ARRAY_CONSTRUCT(COALESCE(TO_VARIANT(NULLIF($1, 'NA')), PARSE_JSON('null')), COALESCE(TO_VARIANT(NULLIF($2, 'NA')), PARSE_JSON('null')))) FROM",
            )),
        },
        DirectCopiesTestCase {
            description: "csv in cp1252 with two lines before the header: Snowflake decodes WINDOWS1252 and skips the two lines and the header",
            files: vec![("part-0.csv".to_owned(), "2026-11-04")],
            columns: vec![column(RECORD_COLUMN, ColumnType::Json, WHOLE_FILE)],
            from: EXTERNAL,
            encoding: csv_in("cp1252", true, 2),
            header: &["column_1"],
            expected_copies: Ok((
                1,
                "SKIP_HEADER = 3 NULL_IF = () EMPTY_FIELD_AS_NULL = FALSE COMPRESSION = AUTO ENCODING = 'WINDOWS1252')",
            )),
        },
        DirectCopiesTestCase {
            description: "csv in latin-1 without a header, one line skipped: ISO88591, and only that line skipped",
            files: vec![("part-0.csv".to_owned(), "2026-11-04")],
            columns: vec![column(RECORD_COLUMN, ColumnType::Json, WHOLE_FILE)],
            from: EXTERNAL,
            encoding: csv_in("latin-1", false, 1),
            header: &["column_1"],
            expected_copies: Ok((
                1,
                "SKIP_HEADER = 1 NULL_IF = () EMPTY_FIELD_AS_NULL = FALSE COMPRESSION = AUTO ENCODING = 'ISO88591')",
            )),
        },
        DirectCopiesTestCase {
            description: "csv in utf-8 under another label leaves ENCODING to its UTF8 default",
            files: vec![("part-0.csv".to_owned(), "2026-11-04")],
            columns: vec![column(RECORD_COLUMN, ColumnType::Json, WHOLE_FILE)],
            from: EXTERNAL,
            encoding: csv_in("UTF8", true, 0),
            header: &["column_1"],
            expected_copies: Ok((
                1,
                "SKIP_HEADER = 1 NULL_IF = () EMPTY_FIELD_AS_NULL = FALSE COMPRESSION = AUTO)",
            )),
        },
    ];
    for test_case in test_cases {
        let copies = direct_sql(
            &test_case.files,
            &test_case.columns,
            &test_case.from,
            (&test_case.encoding, test_case.header),
        );
        let (count, first) = copies.clone().unwrap_or_default();
        let (expected_count, expected_start) = test_case.expected_copies.unwrap_or_default();
        assert_eq!(count, expected_count, "{}", test_case.description);
        assert!(
            first.contains(expected_start),
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

#[test]
fn given_a_csv_encoding_when_choosing_snowflakes_then_only_charsets_decoded_alike_map() {
    let test_cases = [
        SnowflakeEncodingTestCase {
            description: "utf-8 is Snowflake's default",
            label: "utf-8",
            expected_encoding: Some("UTF8"),
        },
        SnowflakeEncodingTestCase {
            description: "cp1252 is windows-1252",
            label: "cp1252",
            expected_encoding: Some("WINDOWS1252"),
        },
        SnowflakeEncodingTestCase {
            description: "windows-1250",
            label: "windows-1250",
            expected_encoding: Some("WINDOWS1250"),
        },
        SnowflakeEncodingTestCase {
            description: "latin-1 reads byte for byte, as ISO88591 does",
            label: "latin-1",
            expected_encoding: Some("ISO88591"),
        },
        SnowflakeEncodingTestCase {
            description: "ISO-8859-1 is latin-1",
            label: "ISO-8859-1",
            expected_encoding: Some("ISO88591"),
        },
        SnowflakeEncodingTestCase {
            description: "iso88591, Snowflake's own spelling, is latin-1",
            label: "iso88591",
            expected_encoding: Some("ISO88591"),
        },
        SnowflakeEncodingTestCase {
            description: "iso-8859-9 decodes as windows-1254",
            label: "iso-8859-9",
            expected_encoding: Some("WINDOWS1254"),
        },
        SnowflakeEncodingTestCase {
            description: "iso-8859-15",
            label: "iso-8859-15",
            expected_encoding: Some("ISO885915"),
        },
        SnowflakeEncodingTestCase {
            description: "koi8-r",
            label: "koi8-r",
            expected_encoding: Some("KOI8R"),
        },
        SnowflakeEncodingTestCase {
            description: "utf-16le",
            label: "utf-16le",
            expected_encoding: Some("UTF16LE"),
        },
        SnowflakeEncodingTestCase {
            description: "Snowflake has no ASCII",
            label: "ascii",
            expected_encoding: None,
        },
        SnowflakeEncodingTestCase {
            description: "shift_jis is not mapped",
            label: "shift_jis",
            expected_encoding: None,
        },
        SnowflakeEncodingTestCase {
            description: "gb18030 is not mapped",
            label: "gb18030",
            expected_encoding: None,
        },
        SnowflakeEncodingTestCase {
            description: "an unknown label",
            label: "klingon",
            expected_encoding: None,
        },
    ];
    for test_case in test_cases {
        let encoding = text_charset(test_case.label).map(snowflake_encoding);
        assert_eq!(
            encoding.unwrap_or_default(),
            test_case.expected_encoding,
            "{}",
            test_case.description
        );
    }
}
