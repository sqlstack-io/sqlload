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
