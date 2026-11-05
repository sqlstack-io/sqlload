//! Decide whether Snowflake can load a files source's files itself.

use crate::external_stage::_helpers::values::{copyable_type, plain_key};
use crate::external_stage::models::{CopyValue, DirectCheck};
use crate::files_source::models::SourceFormat;
use crate::project::models::MaskMethod;
use crate::store_paths::models::StoreKind;

/// Why the files must go through SQLLoad instead of a direct `COPY` from the stage, or `None` when Snowflake can load them exactly as SQLLoad would.
pub fn direct_problem(check: &DirectCheck<'_>) -> Option<String> {
    if check.format != SourceFormat::Parquet {
        return Some(format!(
            "{} files are parsed by SQLLoad, whose type inference and checks differ from Snowflake's",
            check.format
        ));
    }
    let (
        StoreKind::S3 {
            bucket, endpoint, ..
        },
        StoreKind::S3 {
            bucket: stage_bucket,
            endpoint: stage_endpoint,
            ..
        },
    ) = (&check.source.store, &check.stage_url.store)
    else {
        return Some("the files are not in S3".to_owned());
    };
    if bucket != stage_bucket
        || endpoint != stage_endpoint
        || !check.source.prefix.starts_with(&check.stage_url.prefix)
    {
        return Some(format!(
            "the files are outside the stage url {}",
            check.stage_url
        ));
    }
    let odd_key = std::iter::once(check.source.prefix.as_str())
        .chain(
            check
                .files
                .iter()
                .map(|data| data.file.relative_key.as_str()),
        )
        .find(|key| !plain_key(key));
    if let Some(key) = odd_key {
        return Some(format!(
            "{key} has characters a stage path cannot name exactly"
        ));
    }
    for (name, method) in check.masks {
        let column = check.columns.iter().find(|column| column.name == *name);
        let text_file = column.is_some_and(|column| match &column.value {
            CopyValue::File { file_type } => file_type.as_ref().is_none_or(|file_type| {
                *file_type == crate::column_types::models::ColumnType::Text
            }),
            _ => true,
        });
        match method {
            MaskMethod::Hmac => {
                return Some(format!(
                    "{name} is masked with hmac, which Snowflake cannot compute without its key in SQL"
                ));
            }
            MaskMethod::Hash
                if !text_file
                    || column.is_some_and(|column| {
                        column.column_type != crate::column_types::models::ColumnType::Text
                    }) =>
            {
                return Some(format!(
                    "{name} is hashed but is not text in the files; SQLLoad hashes its text"
                ));
            }
            MaskMethod::Hash => {}
        }
    }
    for column in check.columns {
        if matches!(
            column.value,
            CopyValue::SourceFile | CopyValue::SourceVersion | CopyValue::Literal(_)
        ) {
            continue;
        }
        if !copyable_type(&column.column_type) {
            return Some(format!(
                "{} is {}, which the direct COPY does not load exactly",
                column.name, column.column_type
            ));
        }
        if let CopyValue::File {
            file_type: Some(file_type),
        } = &column.value
            && *file_type != column.column_type
        {
            return Some(format!(
                "{} is {file_type} in the files but loads as {}; SQLLoad converts it",
                column.name, column.column_type
            ));
        }
    }
    None
}
