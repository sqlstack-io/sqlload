use crate::adapters::clickhouse::_helpers::publish::{PartitionPublish, partition_statements};
use crate::adapters::clickhouse::_helpers::steps::publish_step;
use crate::adapters::clickhouse::models::PublishStep;
use crate::adapters::clickhouse::tests::test_types::PartitionStatementsTestCase;

const EXCHANGE: &str =
    "-- sqlload publish {\"Exchange\":{\"target\":\"`raw`.`events`\",\"build\":\"`raw`.`c`\"}}";

const WINDOW: &str = "`day` >= CAST('2024-01-10' AS Date32)";

#[test]
fn given_the_partitions_a_write_touches_when_publishing_then_they_are_swapped_in_whole_and_together()
 {
    let test_cases = [
        PartitionStatementsTestCase {
            description: "an append of no rows publishes nothing",
            displaced: None,
            new: &[],
            replaced: &[],
            expected_statements: &[],
        },
        PartitionStatementsTestCase {
            description: "an append into one partition attaches it from the delta, copying nothing",
            displaced: None,
            new: &["202401"],
            replaced: &[],
            expected_statements: &[
                "ALTER TABLE `raw`.`events` ATTACH PARTITION ID '202401' FROM `raw`.`d`",
            ],
        },
        PartitionStatementsTestCase {
            description: "an append into two partitions attaches them to a hard-linked copy, exchanged in at once",
            displaced: None,
            new: &["202401", "202402"],
            replaced: &[],
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`c`",
                "CREATE TABLE `raw`.`c` AS `raw`.`events`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ALL FROM `raw`.`events`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202401' FROM `raw`.`d`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202402' FROM `raw`.`d`",
                EXCHANGE,
                "DROP TABLE IF EXISTS `raw`.`c`",
            ],
        },
        PartitionStatementsTestCase {
            description: "a window in one partition rebuilds it beside the table from its kept rows and the delta's, then replaces it",
            displaced: Some(WINDOW),
            new: &["202401"],
            replaced: &["202401"],
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`c`",
                "CREATE TABLE `raw`.`c` AS `raw`.`events`",
                "INSERT INTO `raw`.`c` SELECT * FROM `raw`.`events` WHERE _partition_id IN ('202401') AND NOT ifNull(`day` >= CAST('2024-01-10' AS Date32), 0)",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202401' FROM `raw`.`d`",
                "ALTER TABLE `raw`.`events` REPLACE PARTITION ID '202401' FROM `raw`.`c`",
                "DROP TABLE IF EXISTS `raw`.`c`",
            ],
        },
        PartitionStatementsTestCase {
            description: "a partition whose window rows are all gone upstream is still rebuilt, from its kept rows alone",
            displaced: Some(WINDOW),
            new: &[],
            replaced: &["202402"],
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`c`",
                "CREATE TABLE `raw`.`c` AS `raw`.`events`",
                "INSERT INTO `raw`.`c` SELECT * FROM `raw`.`events` WHERE _partition_id IN ('202402') AND NOT ifNull(`day` >= CAST('2024-01-10' AS Date32), 0)",
                "ALTER TABLE `raw`.`events` REPLACE PARTITION ID '202402' FROM `raw`.`c`",
                "DROP TABLE IF EXISTS `raw`.`c`",
            ],
        },
        PartitionStatementsTestCase {
            description: "a window over two partitions rebuilds both in a hard-linked copy and exchanges it in, so readers see both or neither",
            displaced: Some(WINDOW),
            new: &["202401"],
            replaced: &["202401", "202312"],
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`c`",
                "CREATE TABLE `raw`.`c` AS `raw`.`events`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ALL FROM `raw`.`events`",
                "ALTER TABLE `raw`.`c` DROP PARTITION ID '202312', DROP PARTITION ID '202401'",
                "INSERT INTO `raw`.`c` SELECT * FROM `raw`.`events` WHERE _partition_id IN ('202312', '202401') AND NOT ifNull(`day` >= CAST('2024-01-10' AS Date32), 0)",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202401' FROM `raw`.`d`",
                EXCHANGE,
                "DROP TABLE IF EXISTS `raw`.`c`",
            ],
        },
    ];
    for test_case in test_cases {
        let owned = |partitions: &[&str]| -> Vec<String> {
            partitions
                .iter()
                .map(|partition| (*partition).to_owned())
                .collect()
        };
        let publish = PartitionPublish {
            target: "`raw`.`events`",
            delta: "`raw`.`d`",
            copy: "`raw`.`c`",
            displaced: test_case.displaced,
        };
        assert_eq!(
            partition_statements(
                &publish,
                (&owned(test_case.new), &owned(test_case.replaced))
            ),
            test_case.expected_statements,
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_a_publish_statement_when_running_it_then_the_step_it_names_is_read_back()
-> Result<(), String> {
    assert_eq!(
        publish_step(EXCHANGE).map_err(|error| error.to_string())?,
        Some(PublishStep::Exchange {
            target: "`raw`.`events`".to_owned(),
            build: "`raw`.`c`".to_owned(),
        })
    );
    assert_eq!(
        publish_step("DROP TABLE IF EXISTS `raw`.`c`").map_err(|error| error.to_string())?,
        None
    );
    assert!(publish_step("-- sqlload publish {").is_err());
    Ok(())
}
