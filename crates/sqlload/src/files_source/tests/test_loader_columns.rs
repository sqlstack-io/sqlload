use crate::files_source::_helpers::plan::columns::{SampledColumn, drop_loader_columns};
use crate::files_source::tests::test_types::LoaderColumnsTestCase;

#[test]
fn given_files_with_the_loaders_own_columns_when_sampling_then_they_are_dropped_ignoring_case() {
    let test_cases = [
        LoaderColumnsTestCase {
            description: "files a files target wrote carry the load metadata",
            found: &["id", "_sqlload_load_id", "_sqlload_loaded_at"],
            expected_columns: (&["id"], &["_sqlload_load_id", "_sqlload_loaded_at"]),
        },
        LoaderColumnsTestCase {
            description: "upper and mixed case, as a warehouse export spells them, are dropped too",
            found: &["ID", "_SQLLOAD_LOAD_ID", "_SqlLoad_Loaded_At"],
            expected_columns: (&["ID"], &["_SQLLOAD_LOAD_ID", "_SqlLoad_Loaded_At"]),
        },
        LoaderColumnsTestCase {
            description: "source tracking columns from an earlier files load are dropped",
            found: &["_sqlload_source_file", "_SQLLOAD_SOURCE_VERSION", "v"],
            expected_columns: (&["v"], &["_sqlload_source_file", "_SQLLOAD_SOURCE_VERSION"]),
        },
        LoaderColumnsTestCase {
            description: "other names that merely look alike are kept",
            found: &["_sqlload_load_id_old", "sqlload_load_id", "_sqlload_note"],
            expected_columns: (
                &["_sqlload_load_id_old", "sqlload_load_id", "_sqlload_note"],
                &[],
            ),
        },
    ];
    for test_case in test_cases {
        let found: Vec<SampledColumn> = test_case
            .found
            .iter()
            .map(|name| SampledColumn {
                name: (*name).to_owned(),
                column_type: None,
                direct_refusal: None,
                stored_read: crate::files_source::models::StoredRead::Plain,
            })
            .collect();
        let (kept, dropped) = drop_loader_columns(found);
        let kept: Vec<String> = kept.into_iter().map(|column| column.name).collect();
        assert_eq!(
            (kept, dropped),
            (
                test_case
                    .expected_columns
                    .0
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect(),
                test_case
                    .expected_columns
                    .1
                    .iter()
                    .map(|name| (*name).to_owned())
                    .collect()
            ),
            "{}",
            test_case.description
        );
    }
}
