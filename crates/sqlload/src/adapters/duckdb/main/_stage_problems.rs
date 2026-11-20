//! Whether a MotherDuck target's `external_stage` works.

use arrow_array::{Int64Array, RecordBatch};

use crate::adapters::duckdb::main::_check_files::check_files;
use crate::adapters::duckdb::models::{DuckDbDestination, ExternalStage};
use crate::adapters::motherduck_bridge::main::_chunk_parquet::chunk_parquet;
use crate::file_store::main::delete_files::delete_files;
use crate::file_store::main::write_file::write_file;

/// Why `destination`'s `external_stage` cannot load: SQLLoad cannot write a file under its prefix, or MotherDuck's servers cannot read it there. Checked once for the destination's tables, with a one-row file it removes; none without a stage.
pub(in crate::adapters) async fn stage_problems(destination: &DuckDbDestination) -> Vec<String> {
    let Some(stage) = &destination.stage else {
        return Vec::new();
    };
    let checked = stage
        .checked
        .get_or_init(|| check_stage(destination, stage))
        .await;
    checked.clone().err().into_iter().collect()
}

/// Write a one-row file under the stage, have MotherDuck's servers count it, and remove it.
async fn check_stage(destination: &DuckDbDestination, stage: &ExternalStage) -> Result<(), String> {
    let location = &stage.folder.location;
    let key = format!("_sqlload_check/{}.parquet", uuid::Uuid::new_v4());
    let batch = RecordBatch::try_from_iter([(
        "checked",
        std::sync::Arc::new(Int64Array::from(vec![1])) as _,
    )])
    .map_err(|error| error.to_string())?;
    let mut body: Vec<u8> = Vec::new();
    chunk_parquet(&mut body, &batch, &[])?;
    write_file(&stage.folder, &key, body.into())
        .await
        .map_err(|error| {
            format!(
                "load_method external_stage: SQLLoad cannot write under {location}: {error}; its keys for the bucket need to put and delete objects there"
            )
        })?;
    let read = check_files(destination, &location.store, &format!("{location}{key}")).await;
    if let Err(error) = delete_files(&stage.folder, &[key]).await {
        tracing::warn!(stage = %location, %error, "the external_stage check left its file");
    }
    read.map_err(|error| {
        format!(
            "load_method external_stage: MotherDuck's servers cannot read {location}: {error}. They read it from their own network with the keys SQLLoad finds for the bucket: give those keys read access there and let MotherDuck through the bucket's policy, or remove load_method to send the rows from this machine"
        )
    })
}
