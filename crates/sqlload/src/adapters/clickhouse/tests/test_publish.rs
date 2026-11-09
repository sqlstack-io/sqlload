use crate::adapters::clickhouse::_helpers::dialect::publish::{
    PartitionPublish, partition_statements,
};
use crate::adapters::clickhouse::_helpers::session::steps::publish_step;
use crate::adapters::clickhouse::models::PublishStep;
use crate::adapters::clickhouse::tests::test_types::{
    PartitionStatementsTestCase, PublishStepTestCase,
};

const EXCHANGE: &str = "-- sqlload publish {\"Exchange\":{\"target\":\"`raw`.`events`\",\"build\":\"`raw`.`c`\",\"aside\":\"`raw`.`a`\"}}";

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
            description: "a write into two partitions displacing no rows hard-links the table, deletes nothing and exchanges it in",
            displaced: Some(WINDOW),
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
            description: "a window in one partition hard-links it beside the table, deletes the window there, attaches the delta's rows and replaces it",
            displaced: Some(WINDOW),
            new: &["202401"],
            replaced: &["202401"],
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`c`",
                "CREATE TABLE `raw`.`c` AS `raw`.`events`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202401' FROM `raw`.`events`",
                "-- sqlload publish {\"DeleteRows\":{\"table\":\"`raw`.`c`\",\"condition\":\"_partition_id IN ('202401') AND ifNull(`day` >= CAST('2024-01-10' AS Date32), 0)\"}}",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202401' FROM `raw`.`d`",
                "ALTER TABLE `raw`.`events` REPLACE PARTITION ID '202401' FROM `raw`.`c`",
                "DROP TABLE IF EXISTS `raw`.`c`",
            ],
        },
        PartitionStatementsTestCase {
            description: "a write displacing no rows hard-links the partition, deletes nothing and replaces it with the delta's rows added",
            displaced: Some(WINDOW),
            new: &["202401"],
            replaced: &[],
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`c`",
                "CREATE TABLE `raw`.`c` AS `raw`.`events`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202401' FROM `raw`.`events`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202401' FROM `raw`.`d`",
                "ALTER TABLE `raw`.`events` REPLACE PARTITION ID '202401' FROM `raw`.`c`",
                "DROP TABLE IF EXISTS `raw`.`c`",
            ],
        },
        PartitionStatementsTestCase {
            description: "a partition whose window rows are all gone upstream is still rebuilt, from its kept rows alone, and dropped if none are kept",
            displaced: Some(WINDOW),
            new: &[],
            replaced: &["202402"],
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`c`",
                "CREATE TABLE `raw`.`c` AS `raw`.`events`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ID '202402' FROM `raw`.`events`",
                "-- sqlload publish {\"DeleteRows\":{\"table\":\"`raw`.`c`\",\"condition\":\"_partition_id IN ('202402') AND ifNull(`day` >= CAST('2024-01-10' AS Date32), 0)\"}}",
                "-- sqlload publish {\"ReplacePartition\":{\"target\":\"`raw`.`events`\",\"copy\":\"`raw`.`c`\",\"partition\":\"202402\"}}",
                "DROP TABLE IF EXISTS `raw`.`c`",
            ],
        },
        PartitionStatementsTestCase {
            description: "a window over two partitions rebuilds both in a hard-linked copy, deleting only the displaced partitions' window rows, and exchanges it in, so readers see both or neither",
            displaced: Some(WINDOW),
            new: &["202401"],
            replaced: &["202401", "202312"],
            expected_statements: &[
                "DROP TABLE IF EXISTS `raw`.`c`",
                "CREATE TABLE `raw`.`c` AS `raw`.`events`",
                "ALTER TABLE `raw`.`c` ATTACH PARTITION ALL FROM `raw`.`events`",
                "-- sqlload publish {\"DeleteRows\":{\"table\":\"`raw`.`c`\",\"condition\":\"_partition_id IN ('202401', '202312') AND ifNull(`day` >= CAST('2024-01-10' AS Date32), 0)\"}}",
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
            aside: "`raw`.`a`",
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
fn given_a_publish_statement_when_running_it_then_the_step_it_names_is_read_back() {
    let test_cases = [
        PublishStepTestCase {
            description: "an exchange step is read back from its JSON",
            statement: EXCHANGE,
            expected_step: Ok(Some(PublishStep::Exchange {
                target: "`raw`.`events`".to_owned(),
                build: "`raw`.`c`".to_owned(),
                aside: "`raw`.`a`".to_owned(),
            })),
        },
        PublishStepTestCase {
            description: "plain SQL is sent as it is",
            statement: "DROP TABLE IF EXISTS `raw`.`c`",
            expected_step: Ok(None),
        },
        PublishStepTestCase {
            description: "a malformed step fails rather than being sent",
            statement: "-- sqlload publish {",
            expected_step: Err(()),
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            publish_step(test_case.statement).map_err(|_| ()),
            test_case.expected_step,
            "{}",
            test_case.description
        );
    }
}
