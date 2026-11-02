use std::collections::BTreeMap;

use crate::config::models::Config;
use crate::naming::_helpers::patterns::fill_template;
use crate::naming::models::Stream;

/// The stream for `table`, with names from the configured templates.
pub(crate) fn build_stream(
    config: &Config,
    table: String,
    topics: Vec<String>,
) -> Result<Stream, String> {
    let name = table.to_ascii_lowercase();
    let values = BTreeMap::from([
        ("table".to_owned(), table.clone()),
        ("table_flat".to_owned(), table.replace("__", "_")),
        ("stream".to_owned(), name.clone()),
    ]);
    Ok(Stream {
        consumer_group: fill_template(&config.source.consumer_group, &values)?,
        stage: fill_template(&config.naming.stage, &values)?,
        pipe: fill_template(&config.naming.pipe, &values)?,
        streaming_pipe: fill_template(&config.naming.streaming_pipe, &values)?,
        table,
        name,
        topics,
    })
}
