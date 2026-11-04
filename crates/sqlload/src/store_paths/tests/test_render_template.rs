use crate::store_paths::main::render_template::render_template;
use crate::store_paths::tests::test_types::RenderTemplateTestCase;

#[test]
fn given_a_path_template_when_rendering_then_placeholders_are_filled_safely() {
    let test_cases = [
        RenderTemplateTestCase {
            description: "placeholders are filled",
            template: "shop/{table}/date={date}/",
            values: &[("table", "orders"), ("date", "2026-11-04")],
            expected_text: Ok("shop/orders/date=2026-11-04/"),
        },
        RenderTemplateTestCase {
            description: "an unknown placeholder names the known ones",
            template: "shop/{tabel}/",
            values: &[("table", "orders")],
            expected_text: Err("uses {tabel}, which is not one of: {table}"),
        },
        RenderTemplateTestCase {
            description: "a value cannot add folders",
            template: "shop/{table}/",
            values: &[("table", "a/b")],
            expected_text: Err("contains '/'"),
        },
        RenderTemplateTestCase {
            description: "an unclosed brace is rejected",
            template: "shop/{table/",
            values: &[("table", "orders")],
            expected_text: Err("unclosed"),
        },
    ];
    for test_case in test_cases {
        let result = render_template(test_case.template, test_case.values)
            .map_err(|error| error.to_string());
        assert_eq!(
            result.as_deref().ok(),
            test_case.expected_text.ok(),
            "{}",
            test_case.description
        );
        assert!(
            result
                .err()
                .unwrap_or_default()
                .contains(test_case.expected_text.err().unwrap_or_default()),
            "{}",
            test_case.description
        );
    }
}
