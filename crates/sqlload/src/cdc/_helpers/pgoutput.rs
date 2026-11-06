//! Decoding pgoutput messages (protocol version 1, text values).

use crate::cdc::_helpers::reader::Reader;
use crate::cdc::models::{PgOutputMessage, Relation, RelationColumn, TupleValue};

/// The message in `bytes`, as an `XLogData` payload carries it.
pub(crate) fn decode_message(bytes: &[u8]) -> Result<PgOutputMessage, String> {
    let mut reader = Reader::new(bytes);
    let tag = char::from(reader.byte()?);
    match tag {
        'R' => relation(&mut reader).map(PgOutputMessage::Relation),
        'I' => {
            let relation = reader.uint32()?;
            require_part(&mut reader, 'N')?;
            Ok(PgOutputMessage::Insert {
                relation,
                new: tuple(&mut reader)?,
            })
        }
        'U' => {
            let relation = reader.uint32()?;
            let (old, new) = match char::from(reader.byte()?) {
                'K' | 'O' => {
                    let old = tuple(&mut reader)?;
                    require_part(&mut reader, 'N')?;
                    (Some(old), tuple(&mut reader)?)
                }
                'N' => (None, tuple(&mut reader)?),
                other => return Err(format!("an Update holds an unknown part {other:?}")),
            };
            Ok(PgOutputMessage::Update { relation, old, new })
        }
        'D' => {
            let relation = reader.uint32()?;
            match char::from(reader.byte()?) {
                'K' | 'O' => Ok(PgOutputMessage::Delete {
                    relation,
                    old: tuple(&mut reader)?,
                }),
                other => Err(format!("a Delete holds an unknown part {other:?}")),
            }
        }
        'T' => {
            let count = reader.int32()?;
            let options = reader.byte()?;
            let mut relations: Vec<u32> = Vec::new();
            for _ in 0..count.max(0) {
                relations.push(reader.uint32()?);
            }
            Ok(PgOutputMessage::Truncate {
                relations,
                cascade: options & 1 != 0,
                restart_identity: options & 2 != 0,
            })
        }
        other => Ok(PgOutputMessage::Other(other)),
    }
}

fn require_part(reader: &mut Reader<'_>, wanted: char) -> Result<(), String> {
    match char::from(reader.byte()?) {
        found if found == wanted => Ok(()),
        found => Err(format!(
            "expected part {wanted:?} in a change, found {found:?}"
        )),
    }
}

fn relation(reader: &mut Reader<'_>) -> Result<Relation, String> {
    let oid = reader.uint32()?;
    let schema = reader.string()?;
    let table = reader.string()?;
    let replica_identity = char::from(reader.byte()?);
    let count = reader.int16()?;
    let mut columns: Vec<RelationColumn> = Vec::new();
    for _ in 0..count.max(0) {
        let flags = reader.byte()?;
        columns.push(RelationColumn {
            name: reader.string()?,
            key: flags & 1 != 0,
            type_oid: reader.uint32()?,
            type_modifier: reader.int32()?,
        });
    }
    Ok(Relation {
        oid,
        schema,
        table,
        replica_identity,
        columns,
    })
}

fn tuple(reader: &mut Reader<'_>) -> Result<Vec<TupleValue>, String> {
    let count = reader.int16()?;
    let mut values: Vec<TupleValue> = Vec::new();
    for _ in 0..count.max(0) {
        values.push(match char::from(reader.byte()?) {
            'n' => TupleValue::Null,
            'u' => TupleValue::Unchanged,
            't' => {
                let length = usize::try_from(reader.int32()?)
                    .map_err(|_| "a text value has a negative length".to_owned())?;
                TupleValue::Text(reader.text(length)?)
            }
            other => {
                return Err(format!(
                    "a value has kind {other:?}; SQLLoad asks pgoutput for text values only"
                ));
            }
        });
    }
    Ok(values)
}
