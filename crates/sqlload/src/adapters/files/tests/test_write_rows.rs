use crate::adapters::files::constants::OPEN_FILES_MAX;
use crate::adapters::files::main::open_writer::open_writer;
use crate::adapters::files::main::write_rows::write_rows;
use crate::adapters::files::models::{PartitionBy, WriterRequest};
use crate::adapters::files::tests::helpers::{
    block_on, day_columns, days_batch, orders_path, scratch_folder,
};
use crate::adapters::files::tests::test_types::WriteRowsTestCase;
use crate::project::models::OutputEncoding;

#[test]
fn given_one_batch_with_many_partitions_when_writing_then_open_files_stay_under_the_cap()
-> Result<(), String> {
    let test_cases = [
        WriteRowsTestCase {
            description: "a batch within the cap keeps every file open",
            partitions: 10,
            expected_open: 10,
            expected_stored: 0,
        },
        WriteRowsTestCase {
            description: "a batch over the cap stores files as it goes",
            partitions: OPEN_FILES_MAX + 44,
            expected_open: OPEN_FILES_MAX,
            expected_stored: 44,
        },
    ];
    for test_case in test_cases {
        let (folder, root) = scratch_folder()?;
        let path = orders_path(Some(("day=", PartitionBy::Value)));
        let columns = day_columns();
        let mut writer = open_writer(WriterRequest {
            folder,
            path: &path,
            cursor_column: Some("day"),
            columns: &columns,
            load_id: "load",
            file_id: "load",
            loaded_at: "2026-11-04T00:00:00.000000Z",
            encoding: &OutputEncoding::default(),
        })?;
        let batch = days_batch(test_case.partitions)?;
        block_on(async {
            write_rows(&mut writer, batch)
                .await
                .map(|_| ())
                .map_err(|error| error.to_string())
        })?;
        let _ = std::fs::remove_dir_all(&root);
        assert_eq!(
            (writer.open.len(), writer.written.len()),
            (test_case.expected_open, test_case.expected_stored),
            "{}",
            test_case.description
        );
    }
    Ok(())
}
