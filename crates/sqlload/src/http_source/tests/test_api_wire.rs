use crate::http_source::tests::helpers::{
    wire_checkpoint_problems, wire_files, wire_job_problems, wire_json_problems,
};
use crate::http_source::tests::test_types::{ApiWireTestCase, WireFilesTestCase};

#[test]
fn given_each_item_sqlload_api_commits_as_its_wire_when_the_engine_reads_it_then_it_finds_no_problem()
 {
    let test_cases = [
        ApiWireTestCase {
            description: "the fan out batch job reads without a problem",
            file: "fan_out_batch",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the fan out nested job reads without a problem",
            file: "fan_out_nested",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the fan out values job reads without a problem",
            file: "fan_out_values",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the paginate cells job reads without a problem",
            file: "paginate_cells",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the paginate cursor job reads without a problem",
            file: "paginate_cursor",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the paginate header row job reads without a problem",
            file: "paginate_header_row",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the paginate link header job reads without a problem",
            file: "paginate_link_header",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the paginate next link job reads without a problem",
            file: "paginate_next_link",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the paginate offset job reads without a problem",
            file: "paginate_offset",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the paginate page number post job reads without a problem",
            file: "paginate_page_number_post",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the paginate projected job reads without a problem",
            file: "paginate_projected",
            read: wire_job_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the JSON document reads as its spec asks",
            file: "json_document",
            read: wire_json_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the NDJSON lines read as their spec asks",
            file: "ndjson_lines",
            read: wire_json_problems,
            expected_problems: &[],
        },
        ApiWireTestCase {
            description: "the checkpoint holds its state",
            file: "checkpoint",
            read: wire_checkpoint_problems,
            expected_problems: &[],
        },
    ];
    for test_case in test_cases {
        assert_eq!(
            (test_case.read)(test_case.file),
            Ok(test_case
                .expected_problems
                .iter()
                .map(|problem| (*problem).to_owned())
                .collect()),
            "{}",
            test_case.description
        );
    }
}

#[test]
fn given_the_wire_folder_when_listing_it_then_every_file_has_a_case_above() {
    let test_cases = [WireFilesTestCase {
        description: "every committed wire file is read by the test above",
        expected_files: &[
            "checkpoint",
            "fan_out_batch",
            "fan_out_nested",
            "fan_out_values",
            "json_document",
            "ndjson_lines",
            "paginate_cells",
            "paginate_cursor",
            "paginate_header_row",
            "paginate_link_header",
            "paginate_next_link",
            "paginate_offset",
            "paginate_page_number_post",
            "paginate_projected",
        ],
    }];
    for test_case in test_cases {
        assert_eq!(
            wire_files(),
            Ok(test_case
                .expected_files
                .iter()
                .map(|file| (*file).to_owned())
                .collect()),
            "{}",
            test_case.description
        );
    }
}
