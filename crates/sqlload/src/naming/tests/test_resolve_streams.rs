use crate::naming::main::resolve_streams::resolve_streams;
use crate::naming::tests::helpers::{config_with_naming, topics};
use crate::naming::tests::test_types::ResolveStreamsTestCase;

#[test]
fn given_grouped_topics_when_resolving_streams_then_names_come_from_templates() -> Result<(), String>
{
    let test_cases = [
        ResolveStreamsTestCase {
            description: "default templates name the group and stage after the table",
            naming: "",
            topics: &["shop.orders"],
            expected_streams: &[(
                "SHOP__ORDERS",
                "sqlload-shop__orders",
                "SHOP__ORDERS_SQLLOAD_STAGE",
            )],
        },
        ResolveStreamsTestCase {
            description: "table_flat collapses double underscores for existing stage names",
            naming: "stage = \"LEGACY_{table_flat}_STAGE\"",
            topics: &["shop.orders"],
            expected_streams: &[(
                "SHOP__ORDERS",
                "sqlload-shop__orders",
                "LEGACY_SHOP_ORDERS_STAGE",
            )],
        },
    ];
    for test_case in &test_cases {
        let config = config_with_naming(test_case.naming)?;
        let streams = resolve_streams(&config, &topics(test_case.topics))
            .map_err(|problems| problems.join("; "))?;
        let found: Vec<(&str, &str, &str)> = streams
            .iter()
            .map(|stream| {
                (
                    stream.table.as_str(),
                    stream.consumer_group.as_str(),
                    stream.stage.as_str(),
                )
            })
            .collect();
        assert_eq!(
            found, test_case.expected_streams,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
