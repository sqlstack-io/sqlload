use crate::project::tests::helpers::parse_with_python;
use crate::project::tests::test_types::ParsePythonProjectTestCase;

const SHOP: &str = r#"[{"name": "shop", "module": "sources/shop.py", "settings": {
    "connection": "pg", "write_strategy": "merge", "description": "costs ${NOT_INTERPOLATED}",
    "tables": {"shop_orders": {"unique_key": "order_id", "columns": {"total": "decimal(18,2)"}}}}}]"#;

#[test]
fn given_python_sources_when_parsing_the_project_then_they_join_it_under_the_same_rules() {
    let test_cases = [
        ParsePythonProjectTestCase {
            description: "a Python source becomes a python-kind source with its tables and defaults",
            toml: "",
            sources: SHOP,
            read: |project| {
                let shop = &project.sources["shop"];
                format!(
                    "{:?} {:?} {:?} {:?}",
                    shop.kind,
                    shop.defaults.write_strategy,
                    shop.tables["shop_orders"].settings.unique_key,
                    project.python_modules["shop"]
                )
            },
            expected_value: Ok("Python Some(Merge) Some([\"order_id\"]) \"sources/shop.py\""),
        },
        ParsePythonProjectTestCase {
            description: "Python settings are not interpolated; connections are, and reach table functions",
            toml: "",
            sources: SHOP,
            read: |project| {
                format!(
                    "{:?} {}",
                    project.sources["shop"].description, project.connection_settings["pg"]["host"]
                )
            },
            expected_value: Ok("Some(\"costs ${NOT_INTERPOLATED}\") \"h\""),
        },
        ParsePythonProjectTestCase {
            description: "an unknown table setting names the Python file",
            toml: "",
            sources: r#"[{"name": "t", "module": "sources/t.py", "settings": {"tables": {"t": {"flatten": ["a"]}}}}]"#,
            read: |_| String::new(),
            expected_value: Err("unknown key \"flatten\" in Python table t (sources/t.py)"),
        },
        ParsePythonProjectTestCase {
            description: "a Python source may not take a declarative source's keys",
            toml: "",
            sources: r#"[{"name": "s", "module": "sources/s.py", "settings": {"include": ["*"], "tables": {}}}]"#,
            read: |_| String::new(),
            expected_value: Err("unknown key \"include\" in Python source s (sources/s.py)"),
        },
        ParsePythonProjectTestCase {
            description: "a Python source named like a [sources] entry clashes",
            toml: "[sources.shop]\nkind = \"postgres\"\nconnection = \"pg\"\ninclude = [\"*\"]\n",
            sources: SHOP,
            read: |_| String::new(),
            expected_value: Err(
                "Python source shop (sources/shop.py) has the same name as [sources.shop]",
            ),
        },
        ParsePythonProjectTestCase {
            description: "a table name used by two Python sources clashes",
            toml: "",
            sources: r#"[{"name": "a", "module": "sources/a.py", "settings": {"tables": {"t": {}}}},
                         {"name": "b", "module": "sources/b.py", "settings": {"tables": {"t": {}}}}]"#,
            read: |_| String::new(),
            expected_value: Err("Python table t is declared twice (sources/a.py and sources/b.py)"),
        },
        ParsePythonProjectTestCase {
            description: "kind = \"python\" in sqlload.toml is rejected",
            toml: "[sources.shop]\nkind = \"python\"\n",
            sources: "[]",
            read: |_| String::new(),
            expected_value: Err("Python sources are declared in Python with sl.source"),
        },
        ParsePythonProjectTestCase {
            description: "a Python source's connection must exist; it may name none",
            toml: "",
            sources: r#"[{"name": "a", "module": "sources/a.py", "settings": {"connection": "nope", "tables": {}}},
                         {"name": "b", "module": "sources/b.py", "settings": {"tables": {}}}]"#,
            read: |_| String::new(),
            expected_value: Err(
                "Python source a (sources/a.py) connection \"nope\" is not a [connections] entry",
            ),
        },
        ParsePythonProjectTestCase {
            description: "a None setting from Python is refused",
            toml: "",
            sources: r#"[{"name": "a", "module": "sources/a.py", "settings": {"schema": null, "tables": {}}}]"#,
            read: |_| String::new(),
            expected_value: Err("schema is None; leave the setting out instead"),
        },
    ];
    for test_case in test_cases {
        let value = parse_with_python(test_case.toml, test_case.sources, test_case.read);
        assert_eq!(
            value.as_deref().ok(),
            test_case.expected_value.ok(),
            "{}: {value:?}",
            test_case.description
        );
        assert!(
            value
                .as_ref()
                .err()
                .map_or("", String::as_str)
                .contains(test_case.expected_value.err().unwrap_or_default()),
            "{}: {value:?}",
            test_case.description
        );
    }
}
