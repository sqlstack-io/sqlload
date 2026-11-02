use crate::planning::tests::helpers::{object_summary, plan_for};
use crate::planning::tests::test_types::BuildPlanTestCase;

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
