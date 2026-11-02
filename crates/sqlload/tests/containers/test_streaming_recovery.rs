use sqlload_testkit::redpanda::main::produce_records::produce_records;

use crate::helpers::{
    PARTITIONS, config, distinct_positions, orders, start_exporter, start_stack, stop,
    wait_for_appends, wait_for_rows,
};
use crate::test_types::{ParallelTestCase, RecoveryTestCase};

const FAST: &str = "[batching]\nmax_age = \"1s\"\nmax_rows = 40\n";

#[tokio::test]
async fn given_a_restart_or_failures_when_exporting_then_rows_resume_without_gaps_or_duplicates()
-> Result<(), String> {
    let test_cases = [
        RecoveryTestCase {
            description: "a graceful restart resumes from the committed offsets",
            topic: "orders.restart",
            first_records: 300,
            second_records: 200,
            injected_failures: 0,
            injected_status: 503,
            expected_table: "ORDERS__RESTART",
            expected_rows: 500,
        },
        RecoveryTestCase {
            description: "server errors on append reopen channels and resend from the committed offsets",
            topic: "orders.flaky",
            first_records: 300,
            second_records: 100,
            injected_failures: 3,
            injected_status: 503,
            expected_table: "ORDERS__FLAKY",
            expected_rows: 400,
        },
        RecoveryTestCase {
            description: "throttling on append is retried the same way",
            topic: "orders.throttled",
            first_records: 200,
            second_records: 50,
            injected_failures: 2,
            injected_status: 429,
            expected_table: "ORDERS__THROTTLED",
            expected_rows: 250,
        },
    ];
    for test_case in &test_cases {
        let stack = start_stack().await?;
        produce_records(
            &stack.redpanda.brokers,
            test_case.topic,
            PARTITIONS,
            &orders(0, test_case.first_records),
        )
        .await?;
        stack
            .fake
            .fail_next_appends(test_case.injected_failures, test_case.injected_status);
        let first = start_exporter(config(&stack, "orders.*", FAST)?);
        wait_for_rows(
            &stack.fake,
            test_case.expected_table,
            test_case.first_records,
        )
        .await?;
        stop(first).await?;
        let more = orders(test_case.first_records, test_case.second_records);
        produce_records(&stack.redpanda.brokers, test_case.topic, PARTITIONS, &more).await?;
        let second = start_exporter(config(&stack, "orders.*", FAST)?);
        wait_for_rows(
            &stack.fake,
            test_case.expected_table,
            test_case.expected_rows,
        )
        .await?;
        stop(second).await?;
        let rows = stack.fake.rows(test_case.expected_table);
        assert_eq!(
            rows.len(),
            test_case.expected_rows,
            "{}",
            test_case.description
        );
        assert_eq!(
            distinct_positions(&rows),
            test_case.expected_rows,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[tokio::test]
async fn given_a_crash_after_appending_when_restarting_then_uncommitted_rows_are_sent_again_once()
-> Result<(), String> {
    let test_cases = [RecoveryTestCase {
        description: "aborting between append and commit loses nothing and duplicates nothing",
        topic: "orders.crash",
        first_records: 400,
        second_records: 0,
        injected_failures: 0,
        injected_status: 503,
        expected_table: "ORDERS__CRASH",
        expected_rows: 400,
    }];
    for test_case in &test_cases {
        let stack = start_stack().await?;
        produce_records(
            &stack.redpanda.brokers,
            test_case.topic,
            PARTITIONS,
            &orders(0, test_case.first_records),
        )
        .await?;
        let crashing = start_exporter(config(&stack, "orders.*", FAST)?);
        wait_for_appends(&stack.fake, 1).await?;
        crashing.task.abort();
        let restarted = start_exporter(config(&stack, "orders.*", FAST)?);
        wait_for_rows(
            &stack.fake,
            test_case.expected_table,
            test_case.expected_rows,
        )
        .await?;
        stop(restarted).await?;
        let rows = stack.fake.rows(test_case.expected_table);
        assert_eq!(
            rows.len(),
            test_case.expected_rows,
            "{}",
            test_case.description
        );
        assert_eq!(
            distinct_positions(&rows),
            test_case.expected_rows,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[tokio::test]
async fn given_two_instances_in_one_group_when_exporting_then_partitions_are_shared_without_duplicates()
-> Result<(), String> {
    let test_cases = [ParallelTestCase {
        description: "two exporters with the same consumer group split the partitions",
        topic: "orders.shared",
        records: 900,
        instances: 2,
        expected_table: "ORDERS__SHARED",
    }];
    for test_case in &test_cases {
        let stack = start_stack().await?;
        produce_records(
            &stack.redpanda.brokers,
            test_case.topic,
            PARTITIONS,
            &orders(0, test_case.records),
        )
        .await?;
        let mut running = Vec::with_capacity(test_case.instances);
        for _ in 0..test_case.instances {
            running.push(start_exporter(config(&stack, "orders.*", FAST)?));
        }
        wait_for_rows(&stack.fake, test_case.expected_table, test_case.records).await?;
        for instance in running {
            stop(instance).await?;
        }
        let rows = stack.fake.rows(test_case.expected_table);
        assert_eq!(rows.len(), test_case.records, "{}", test_case.description);
        assert_eq!(
            distinct_positions(&rows),
            test_case.records,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
