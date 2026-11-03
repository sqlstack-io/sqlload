use std::collections::BTreeSet;

use sqlload_testkit::harness::main::start_exporter::start_exporter;
use sqlload_testkit::harness::main::start_stack::start_stack;
use sqlload_testkit::harness::main::stop_exporter::stop_exporter;
use sqlload_testkit::harness::main::wait_for_rows::wait_for_rows;
use sqlload_testkit::records::main::distinct_positions::distinct_positions;
use sqlload_testkit::records::main::order_values::order_values;
use sqlload_testkit::redpanda::main::produce_records::produce_records;

use crate::helpers::{FAST, PARTITIONS, config, run_async};
use crate::test_types::ShardingTestCase;

#[test]
fn given_shards_when_exporting_then_each_stream_runs_on_exactly_one_instance() -> Result<(), String>
{
    run_async(async {
        let test_cases = [ShardingTestCase {
            description: "two shards split six streams with no stream on both",
            topics: &[
                "orders.a", "orders.b", "orders.c", "orders.d", "orders.e", "orders.f",
            ],
            records: 60,
            shards: 2,
            expected_streams: 6,
        }];
        for test_case in &test_cases {
            let stack = start_stack().await?;
            for topic in test_case.topics {
                produce_records(
                    &stack.redpanda.brokers,
                    topic,
                    PARTITIONS,
                    &order_values(0, test_case.records),
                )
                .await?;
            }
            let mut instances = Vec::new();
            for shard in 0..test_case.shards {
                let mut sharded = config(&stack, "orders.*", FAST)?;
                sharded.source.shards = test_case.shards;
                sharded.source.shard = shard;
                instances.push(start_exporter(sharded));
            }
            for topic in test_case.topics {
                wait_for_rows(
                    &stack.fake,
                    &topic.replace('.', "__").to_ascii_uppercase(),
                    test_case.records,
                )
                .await?;
            }
            let mut owned: Vec<Vec<String>> = Vec::new();
            for instance in instances {
                owned.push(stop_exporter(instance).await?.streams);
            }
            let all: BTreeSet<&String> = owned.iter().flatten().collect();
            let total: usize = owned.iter().map(Vec::len).sum();
            assert_eq!(
                all.len(),
                test_case.expected_streams,
                "{}",
                test_case.description
            );
            assert_eq!(
                total, test_case.expected_streams,
                "{}: {owned:?}",
                test_case.description
            );
            for topic in test_case.topics {
                let rows = stack
                    .fake
                    .rows(&topic.replace('.', "__").to_ascii_uppercase());
                assert_eq!(rows.len(), test_case.records, "{}", test_case.description);
                assert_eq!(
                    distinct_positions(&rows),
                    test_case.records,
                    "{}",
                    test_case.description
                );
            }
        }
        Ok(())
    })
}
