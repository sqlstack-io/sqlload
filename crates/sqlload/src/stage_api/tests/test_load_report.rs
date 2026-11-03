use crate::stage_api::_helpers::load_report::parse_load_report;
use crate::stage_api::models::LoadStatus;
use crate::stage_api::tests::test_types::LoadReportTestCase;

#[test]
fn given_a_snowpipe_report_when_parsing_then_each_file_has_its_status() -> Result<(), String> {
    let test_cases = [
        LoadReportTestCase {
            description: "insertReport lists loaded and failed files and the next begin mark",
            response: r#"{"pipe":"DB.RAW.ORDERS_PIPE","completeResult":true,"nextBeginMark":"1_39","files":[
                {"path":"2024-03-10/orders_a.json.gz","status":"LOADED","rowsInserted":3,"rowsParsed":3,"errorsSeen":0,"complete":true},
                {"path":"2024-03-10/orders_b.json.gz","status":"LOAD_FAILED","rowsInserted":0,"rowsParsed":0,"errorsSeen":1,"firstError":"Numeric value 'x' is not recognized"},
                {"path":"2024-03-10/orders_c.json.gz","status":"PARTIALLY_LOADED","rowsInserted":2,"rowsParsed":3,"errorsSeen":1,"firstError":"bad row"},
                {"path":"2024-03-10/orders_d.json.gz","status":"LOAD_IN_PROGRESS"}]}"#,
            expected_files: &[
                ("2024-03-10/orders_a.json.gz", LoadStatus::Loaded, None),
                (
                    "2024-03-10/orders_b.json.gz",
                    LoadStatus::Failed,
                    Some("Numeric value 'x' is not recognized"),
                ),
                (
                    "2024-03-10/orders_c.json.gz",
                    LoadStatus::PartiallyLoaded,
                    Some("bad row"),
                ),
                ("2024-03-10/orders_d.json.gz", LoadStatus::InProgress, None),
            ],
            expected_begin_mark: Some("1_39"),
            expected_complete: true,
        },
        LoadReportTestCase {
            description: "a history scan page says it is incomplete in text, and a system error stands in for the first error",
            response: r#"{"completeResult":"false","rangeEndTime":"2024-03-10T12:00:00.000Z","files":[
                {"path":"orders_e.json.gz","status":"LOAD_FAILED","systemError":"file not found"}]}"#,
            expected_files: &[(
                "orders_e.json.gz",
                LoadStatus::Failed,
                Some("file not found"),
            )],
            expected_begin_mark: None,
            expected_complete: false,
        },
    ];
    for test_case in &test_cases {
        let body: serde_json::Value =
            serde_json::from_str(test_case.response).map_err(|error| error.to_string())?;
        let report = parse_load_report(&body);
        let files: Vec<(&str, LoadStatus, Option<&str>)> = report
            .files
            .iter()
            .map(|file| (file.path.as_str(), file.status, file.first_error.as_deref()))
            .collect();
        assert_eq!(files, test_case.expected_files, "{}", test_case.description);
        assert_eq!(
            report.next_begin_mark.as_deref(),
            test_case.expected_begin_mark,
            "{}",
            test_case.description
        );
        assert_eq!(
            report.complete, test_case.expected_complete,
            "{}",
            test_case.description
        );
    }
    Ok(())
}
