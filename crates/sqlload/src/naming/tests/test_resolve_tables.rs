use crate::naming::main::resolve_tables::resolve_tables;
use crate::naming::tests::helpers::{config_with_naming, topics};
use crate::naming::tests::test_types::{ResolveTablesErrorTestCase, ResolveTablesTestCase};

#[test]
fn given_topics_when_resolving_tables_then_map_rules_and_default_apply_in_order()
-> Result<(), String> {
    let test_cases = [
        ResolveTablesTestCase {
            description: "the default rule upper-cases and turns dots into double underscores",
            naming: "",
            topics: &["shop.orders-v2.created"],
            expected_tables: &[("shop.orders-v2.created", "SHOP__ORDERS_V2__CREATED")],
        },
        ResolveTablesTestCase {
            description: "strip_prefix only applies to the default rule",
            naming: "strip_prefix = \"shop.\"",
            topics: &["shop.orders.created"],
            expected_tables: &[("shop.orders.created", "ORDERS__CREATED")],
        },
        ResolveTablesTestCase {
            description: "a rule's captures fill its table template and can group topics",
            naming: "[[naming.rules]]\nmatch = \"shop.{service}_{variant}.{kind}\"\ntable = \"{service}__{kind}\"",
            topics: &["shop.orders_raw.events", "shop.orders_clean.events"],
            expected_tables: &[
                ("shop.orders_raw.events", "ORDERS__EVENTS"),
                ("shop.orders_clean.events", "ORDERS__EVENTS"),
            ],
        },
        ResolveTablesTestCase {
            description: "the explicit map wins over rules",
            naming: "[naming.map]\n\"shop.orders.created\" = \"NEW_ORDERS\"\n[[naming.rules]]\nmatch = \"shop.*\"\ntable = \"OTHER\"",
            topics: &["shop.orders.created"],
            expected_tables: &[("shop.orders.created", "NEW_ORDERS")],
        },
        ResolveTablesTestCase {
            description: "skip rules leave topics out and earlier rules still win",
            naming: "[[naming.rules]]\nmatch = \"shop.keep_mapped.*\"\ntable = \"KEPT\"\n[[naming.rules]]\nmatch = \"*_mapped.*\"\nskip = true",
            topics: &["shop.keep_mapped.prices", "shop.drop_mapped.prices"],
            expected_tables: &[("shop.keep_mapped.prices", "KEPT")],
        },
        ResolveTablesTestCase {
            description: "a starred capture spans dots",
            naming: "[[naming.rules]]\nmatch = \"source.kc.{path*}\"\ntable = \"KC__{path}\"",
            topics: &["source.kc.podgy.bf_placed"],
            expected_tables: &[("source.kc.podgy.bf_placed", "KC__PODGY__BF_PLACED")],
        },
        ResolveTablesTestCase {
            description: "table_prefix applies to map, rule and default tables",
            naming: "table_prefix = \"TEST__\"\n[naming.map]\n\"a.b\" = \"MAPPED\"\n[[naming.rules]]\nmatch = \"r.{x}\"\ntable = \"R__{x}\"",
            topics: &["a.b", "r.s", "d.e"],
            expected_tables: &[
                ("a.b", "TEST__MAPPED"),
                ("r.s", "TEST__R__S"),
                ("d.e", "TEST__D__E"),
            ],
        },
        ResolveTablesTestCase {
            description: "invalid names go to the fallback table when one is set",
            naming: "fallback = \"_UNRECOGNISED__TOPICS\"\n[naming.map]\n\"shop.orders\" = \"SELECT\"",
            topics: &["shop.orders"],
            expected_tables: &[("shop.orders", "_UNRECOGNISED__TOPICS")],
        },
    ];
    for test_case in &test_cases {
        let config = config_with_naming(test_case.naming)?;
        let tables = resolve_tables(&config.naming, &topics(test_case.topics))
            .map_err(|problems| problems.join("; "))?;
        let found: Vec<(&str, &str)> = tables
            .iter()
            .map(|entry| (entry.topic.as_str(), entry.table.as_str()))
            .collect();
        assert_eq!(
            found, test_case.expected_tables,
            "{}",
            test_case.description
        );
    }
    Ok(())
}

#[test]
fn given_bad_mappings_when_resolving_tables_then_each_problem_is_reported() -> Result<(), String> {
    let test_cases = [
        ResolveTablesErrorTestCase {
            description: "two topics colliding through the default rule are rejected",
            naming: "",
            topics: &["shop.orders-v2", "shop.orders_v2"],
            expected_problem: "through the default rule",
        },
        ResolveTablesErrorTestCase {
            description: "unmatched topics fail when the policy says so",
            naming: "unmatched = \"fail\"",
            topics: &["shop.orders"],
            expected_problem: "matches no naming rule",
        },
        ResolveTablesErrorTestCase {
            description: "reserved words are rejected",
            naming: "[naming.map]\n\"shop.orders\" = \"SELECT\"",
            topics: &["shop.orders"],
            expected_problem: "reserved word",
        },
    ];
    for test_case in &test_cases {
        let config = config_with_naming(test_case.naming)?;
        let problems = resolve_tables(&config.naming, &topics(test_case.topics))
            .expect_err(test_case.description);
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains(test_case.expected_problem)),
            "{}: {problems:?}",
            test_case.description
        );
    }
    Ok(())
}
