use crate::column_types::models::ColumnType;
use crate::postgres_source::constants::COPY_SIGNATURE;
use crate::postgres_source::models::{PgType, ReadColumn, SourceColumn, WireType};

/// A binary COPY stream holding `rows`, each field `None` for NULL.
pub(crate) fn copy_stream(rows: &[Vec<Option<Vec<u8>>>]) -> Vec<u8> {
    let mut bytes = COPY_SIGNATURE.to_vec();
    bytes.extend_from_slice(&0_i32.to_be_bytes());
    bytes.extend_from_slice(&0_i32.to_be_bytes());
    for row in rows {
        bytes.extend_from_slice(&i16::try_from(row.len()).unwrap_or(i16::MAX).to_be_bytes());
        for field in row {
            let length = field
                .as_ref()
                .map_or(-1, |value| i32::try_from(value.len()).unwrap_or(i32::MAX));
            bytes.extend_from_slice(&length.to_be_bytes());
            bytes.extend_from_slice(field.as_deref().unwrap_or_default());
        }
    }
    bytes.extend_from_slice(&(-1_i16).to_be_bytes());
    bytes
}

/// A binary `numeric` value.
pub(crate) fn numeric_bytes(digits: &[i16], weight: i16, sign: u16, display_scale: u16) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &i16::try_from(digits.len())
            .unwrap_or(i16::MAX)
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&weight.to_be_bytes());
    bytes.extend_from_slice(&sign.to_be_bytes());
    bytes.extend_from_slice(&display_scale.to_be_bytes());
    digits
        .iter()
        .for_each(|digit| bytes.extend_from_slice(&digit.to_be_bytes()));
    bytes
}

/// Columns `id int8`, `name text`, `total numeric(10,2)`.
pub(crate) fn sample_columns() -> Vec<ReadColumn> {
    vec![
        ReadColumn {
            name: "id".to_owned(),
            expression: "\"id\"".to_owned(),
            wire: WireType::Int8,
            output: ColumnType::Int,
        },
        ReadColumn {
            name: "name".to_owned(),
            expression: "\"name\"".to_owned(),
            wire: WireType::Text,
            output: ColumnType::Text,
        },
        ReadColumn {
            name: "total".to_owned(),
            expression: "\"total\"".to_owned(),
            wire: WireType::Numeric,
            output: ColumnType::Decimal {
                precision: 10,
                scale: 2,
            },
        },
    ]
}

/// Three rows for [`sample_columns`], the second with NULL name and total.
pub(crate) fn sample_stream() -> Vec<u8> {
    let row = |id: i64, name: Option<&str>, total: Option<Vec<u8>>| {
        vec![
            Some(id.to_be_bytes().to_vec()),
            name.map(|name| name.as_bytes().to_vec()),
            total,
        ]
    };
    copy_stream(&[
        row(1, Some("first"), Some(numeric_bytes(&[12, 3400], 0, 0, 2))),
        row(2, None, None),
        row(
            3,
            Some("third"),
            Some(numeric_bytes(&[5000], -1, 0x4000, 2)),
        ),
    ])
}

/// A source column of `pg_type`.
pub(crate) fn source_column(pg_type: PgType) -> SourceColumn {
    SourceColumn {
        name: "value".to_owned(),
        postgres_type: format!("{pg_type:?}").to_ascii_lowercase(),
        pg_type,
        column_type: Ok(ColumnType::Text),
        nullable: true,
        source_spelling: None,
    }
}

/// A startup message of protocol `version` with `params`, framed as libpq and pgwire-replication frame it.
pub(crate) fn startup_message(version: [u8; 4], params: &[(&str, &str)]) -> Vec<u8> {
    let mut body = version.to_vec();
    for (name, value) in params {
        body.extend_from_slice(name.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let length = u32::try_from(body.len() + 4).unwrap_or(u32::MAX);
    let mut message = length.to_be_bytes().to_vec();
    message.extend_from_slice(&body);
    message
}

/// The parameters of a well-formed protocol 3.0 startup message, in order; an error naming what is wrong otherwise.
pub(crate) fn startup_params(message: &[u8]) -> Result<Vec<(String, String)>, String> {
    let declared = message
        .get(..4)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_be_bytes)
        .ok_or("no length")?;
    if usize::try_from(declared).ok() != Some(message.len()) {
        return Err(format!("length {declared} for {} bytes", message.len()));
    }
    if message.get(4..8) != Some(&[0, 3, 0, 0][..]) {
        return Err("not protocol 3.0".to_owned());
    }
    let text = String::from_utf8(message[8..].to_vec()).map_err(|error| error.to_string())?;
    let mut parts: Vec<&str> = text.split('\0').collect();
    // The pairs' terminators and the final NUL leave two empty parts at the end.
    if parts.len() < 2 || parts.pop() != Some("") || parts.pop() != Some("") {
        return Err("not NUL-terminated".to_owned());
    }
    Ok(parts
        .chunks(2)
        .map(|pair| {
            (
                pair[0].to_owned(),
                pair.get(1).copied().unwrap_or_default().to_owned(),
            )
        })
        .collect())
}
