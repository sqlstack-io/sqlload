use crate::planning::tests::helpers::{object_summary, plan_for, plan_with_pipe};
use crate::planning::tests::test_types::{BuildPlanTestCase, PipeFormatTestCase};

const S3: &str = "[staging]\nkind = \"s3\"\nformat = \"FORMAT\"\n[staging.s3]\nbucket = \"b\"\nregion = \"eu-west-2\"\nstage = \"LAKE\"\n";

#[test]
fn given_a_delivery_mode_when_planning_then_each_stream_needs_the_matching_objects()
-> Result<(), String> {
    let test_cases = [
        BuildPlanTestCase {
            description: "streaming with the default pipe needs only the table",
            staging: "[staging]\nkind = \"none\"\n",
            key_pair: false,
            existing_columns: &[],
            expected_objects: &["+table SHOP__ORDERS"],
            expected_problems: 0,
        },
        BuildPlanTestCase {
            description: "an internal stage loaded by COPY needs a stage",
            staging: "[staging]\nkind = \"internal\"\n",
            key_pair: false,
            existing_columns: &[],
            expected_objects: &["+table SHOP__ORDERS", "+stage SHOP__ORDERS_SQLLOAD_STAGE"],
            expected_problems: 0,
        },
        BuildPlanTestCase {
            description: "an internal stage with a key pair also needs a pipe",
            staging: "[staging]\nkind = \"internal\"\n",
            key_pair: true,
            existing_columns: &[],
            expected_objects: &[
                "+table SHOP__ORDERS",
                "+stage SHOP__ORDERS_SQLLOAD_STAGE",
                "+pipe SHOP__ORDERS_SQLLOAD_PIPE",
            ],
            expected_problems: 0,
        },
        BuildPlanTestCase {
            description: "an existing table missing configured columns is incompatible",
            staging: "[staging]\nkind = \"none\"\n",
            key_pair: false,
            existing_columns: &[("TOPIC", "TEXT")],
            expected_objects: &["!table SHOP__ORDERS"],
            expected_problems: 8,
        },
        BuildPlanTestCase {
            description: "an existing table with matching type families exists",
            staging: "[staging]\nkind = \"none\"\n[[envelope.columns]]\nname = \"TOPIC\"\nfield = \"topic\"\ntype = \"VARCHAR(100)\"\n[[envelope.columns]]\nname = \"KAFKA_OFFSET\"\nfield = \"offset\"\ntype = \"BIGINT\"\n",
            key_pair: false,
            existing_columns: &[
                ("TOPIC", "TEXT"),
                ("KAFKA_OFFSET", "NUMBER"),
                ("EXTRA", "DATE"),
            ],
            expected_objects: &["=table SHOP__ORDERS"],
            expected_problems: 0,
        },
    ];
    for test_case in &test_cases {
        let plan = plan_for(
            test_case.staging,
            test_case.existing_columns,
            test_case.key_pair,
        )?;
        assert_eq!(
            object_summary(&plan),
            test_case.expected_objects,
            "{}",
            test_case.description
        );
        assert_eq!(
            plan.problems().len(),
            test_case.expected_problems,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_a_file_format_when_planning_s3_then_the_pipe_loads_that_format() -> Result<(), String> {
    let test_cases = [
        PipeFormatTestCase {
            description: "a new Parquet pipe selects each column",
            format: "parquet",
            existing_definition: None,
            expected_objects: &["+table SHOP__ORDERS", "+pipe SHOP__ORDERS_SQLLOAD_PIPE"],
            expected_sql: "FROM (SELECT $1:\"TOPIC\"::VARCHAR, ",
        },
        PipeFormatTestCase {
            description: "an existing JSON pipe is incompatible with Parquet",
            format: "parquet",
            existing_definition: Some(
                "COPY INTO MY_DB.RAW.SHOP__ORDERS FROM @LAKE/ FILE_FORMAT = (TYPE = JSON COMPRESSION = AUTO) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
            ),
            expected_objects: &["+table SHOP__ORDERS", "!pipe SHOP__ORDERS_SQLLOAD_PIPE"],
            expected_sql: "TYPE = PARQUET USE_LOGICAL_TYPE = TRUE",
        },
        PipeFormatTestCase {
            description: "an existing Parquet pipe fits",
            format: "parquet",
            existing_definition: Some(
                "copy into t from (select $1 from @LAKE/) file_format=(type='parquet')",
            ),
            expected_objects: &["+table SHOP__ORDERS", "=pipe SHOP__ORDERS_SQLLOAD_PIPE"],
            expected_sql: "COPY INTO MY_DB.RAW.SHOP__ORDERS (TOPIC, ",
        },
        PipeFormatTestCase {
            description: "an existing Parquet pipe is incompatible with JSON",
            format: "json",
            existing_definition: Some(
                "COPY INTO T FROM (SELECT $1 FROM @LAKE/) FILE_FORMAT = (TYPE = PARQUET)",
            ),
            expected_objects: &["+table SHOP__ORDERS", "!pipe SHOP__ORDERS_SQLLOAD_PIPE"],
            expected_sql: "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE",
        },
    ];
    for test_case in &test_cases {
        let staging = S3.replace("FORMAT", test_case.format);
        let plan = plan_with_pipe(&staging, &[], true, test_case.existing_definition)?;
        assert_eq!(
            object_summary(&plan),
            test_case.expected_objects,
            "{}",
            test_case.description
        );
        let pipe_sql = plan
            .streams
            .iter()
            .flat_map(|stream| stream.objects.iter())
            .map(|object| object.create_sql.as_str())
            .find(|sql| sql.starts_with("CREATE PIPE"))
            .ok_or(test_case.description)?;
        assert!(
            pipe_sql.contains(test_case.expected_sql),
            "{}: {pipe_sql}",
            test_case.description
        );
    }
    Ok(())
}
