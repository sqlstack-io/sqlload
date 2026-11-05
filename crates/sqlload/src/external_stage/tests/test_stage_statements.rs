use crate::external_stage::tests::helpers::{
    existing_create, existing_lookup, managed_create, managed_lookup, managed_reference,
    staged_copy_batches,
};
use crate::external_stage::tests::test_types::StageStatementTestCase;

#[test]
fn given_an_external_stage_when_writing_its_sql_then_each_statement_names_it_exactly() {
    let test_cases = [
        StageStatementTestCase {
            description: "SQLLoad creates _SQLLOAD_STAGE in the table's schema on the stage url with the integration",
            build: managed_create,
            expected_sql: "CREATE STAGE IF NOT EXISTS \"ANALYTICS\".\"RAW\".\"_SQLLOAD_STAGE\" URL = 's3://lake/raw/' STORAGE_INTEGRATION = \"LAKE_INT\"",
        },
        StageStatementTestCase {
            description: "an existing stage is never created",
            build: existing_create,
            expected_sql: "",
        },
        StageStatementTestCase {
            description: "the managed stage is found in the table's schema",
            build: managed_lookup,
            expected_sql: "SHOW STAGES LIKE '_SQLLOAD_STAGE' IN SCHEMA \"ANALYTICS\".\"RAW\";\n_SQLLOAD_STAGE",
        },
        StageStatementTestCase {
            description: "an existing stage is found where its name says",
            build: existing_lookup,
            expected_sql: "SHOW STAGES LIKE 'lake_stage' IN SCHEMA ops.loading;\nlake_stage",
        },
        StageStatementTestCase {
            description: "loads name the managed stage fully qualified",
            build: managed_reference,
            expected_sql: "@\"ANALYTICS\".\"RAW\".\"_SQLLOAD_STAGE\"",
        },
        StageStatementTestCase {
            description: "staged Parquet loads at most 1,000 named files per COPY, by column name, always with FORCE",
            build: staged_copy_batches,
            expected_sql: "2;\nCOPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS__SQLLOAD_STAGING_LOAD1\" FROM @STAGE/_sqlload/load-1/ FILES = ('part-00001.parq;\nCOPY INTO \"ANALYTICS\".\"RAW\".\"SHOP__ORDERS__SQLLOAD_STAGING_LOAD1\" FROM @STAGE/_sqlload/load-1/ FILES = ('part-01001.parquet') FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE BINARY_AS_TEXT = FALSE) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = ABORT_STATEMENT FORCE = TRUE",
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            (test_case.build)(),
            test_case.expected_sql,
            "{}",
            test_case.description
        );
    }
}
