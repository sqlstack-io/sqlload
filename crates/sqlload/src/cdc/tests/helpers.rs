use crate::adapters::contract::models::ChangeApply;
use crate::cdc::_helpers::batch::change_parts;
use crate::cdc::_helpers::spill::SpillFile;
use crate::cdc::models::{Change, ChangeOp, Relation, RelationColumn, TableChanges, TupleValue};

/// A NUL-terminated string.
pub(crate) fn cstring(text: &str) -> Vec<u8> {
    [text.as_bytes(), &[0]].concat()
}

/// The encoded part of a NULL value.
pub(crate) const NULL_PART: &[u8] = b"n";
/// The encoded part of an unchanged TOAST value.
pub(crate) const UNCHANGED_PART: &[u8] = b"u";

/// The encoded part of a text value.
pub(crate) fn text_part(value: &str) -> Vec<u8> {
    let length = i32::try_from(value.len()).unwrap_or(i32::MAX);
    [b"t".as_slice(), &length.to_be_bytes(), value.as_bytes()].concat()
}

/// pgoutput TupleData of the encoded `parts`.
pub(crate) fn tuple(parts: &[&[u8]]) -> Vec<u8> {
    let count = i16::try_from(parts.len()).unwrap_or(i16::MAX);
    [count.to_be_bytes().to_vec(), parts.concat()].concat()
}

/// A text value.
pub(crate) fn text(value: &str) -> TupleValue {
    TupleValue::Text(value.to_owned())
}

/// The non-text values of the test table, and how they are written.
const SYMBOLS: [(TupleValue, &str); 2] = [(TupleValue::Unchanged, "~"), (TupleValue::Null, "-")];

/// A value of the test table: `~` for an unchanged TOAST value, `-` for NULL.
fn value(text: &str) -> TupleValue {
    SYMBOLS
        .iter()
        .find(|(_, symbol)| *symbol == text)
        .map_or_else(
            || TupleValue::Text(text.to_owned()),
            |(value, _)| value.clone(),
        )
}

/// How a staged value is written: its text, `~` or `-`.
fn shown(value: Option<&TupleValue>) -> String {
    let symbol = SYMBOLS
        .iter()
        .find(|(known, _)| Some(known) == value)
        .map(|(_, symbol)| (*symbol).to_owned());
    symbol.unwrap_or_else(|| {
        format!("{value:?}")
            .trim_start_matches("Some(Text(\"")
            .trim_end_matches("\"))")
            .to_owned()
    })
}

/// A change to the test table `(id, v)` in transaction `lsn` at `seq`; `old` is the old id of an update that moved the row.
pub(crate) fn change(
    op: ChangeOp,
    (lsn, seq): (i64, i64),
    [id, v]: [&str; 2],
    old: Option<&str>,
) -> Change {
    Change {
        op,
        values: vec![value(id), value(v)],
        old_key: old.map(|old| vec![value(old), TupleValue::Null]),
        lsn,
        seq,
        committed_at: 0,
    }
}

/// The test table: `id`, its primary key, and `v`.
fn relation() -> Relation {
    let column = |name: &str, key: bool| RelationColumn {
        name: name.to_owned(),
        key,
        type_oid: 25,
        type_modifier: -1,
    };
    Relation {
        oid: 1,
        schema: "public".to_owned(),
        table: "t".to_owned(),
        replica_identity: 'd',
        columns: vec![column("id", true), column("v", false)],
    }
}

/// The rows `merge` stages for `changes`, of which the first `spilled` are on disk (`None`: all in memory), as `op:id:value` by id, with the truncate's LSN; `spill_bytes` sets how many key partitions a spilled batch uses.
pub(crate) fn staged_rows(
    changes: &[Change],
    spilled: Option<usize>,
    spill_bytes: usize,
) -> Result<(Vec<String>, Option<i64>), String> {
    let split = spilled.unwrap_or(0);
    let mut file = SpillFile::new()?;
    for change in changes.get(..split).unwrap_or_default() {
        file.push_change(change)?;
    }
    let table = TableChanges {
        relation: relation(),
        changes: changes.get(split..).unwrap_or_default().to_vec(),
    };
    let key = ["id".to_owned()];
    let (mut parts, truncated) = change_parts(
        &table,
        (spilled.map(|_| file), spill_bytes),
        (ChangeApply::SoftDeletes, &key),
    )?;
    let staged: Vec<Vec<Change>> =
        std::iter::from_fn(|| parts.next_changes().transpose()).collect::<Result<_, String>>()?;
    let mut rows: Vec<String> = staged.iter().flatten().map(row_text).collect();
    rows.sort();
    Ok((rows, truncated.map(|truncation| truncation.lsn)))
}

/// `op:id:value` for a staged change.
fn row_text(change: &Change) -> String {
    format!(
        "{}:{}:{}",
        format!("{:?}", change.op).to_lowercase(),
        shown(change.values.first()),
        shown(change.values.get(1))
    )
}

/// A pgoutput Relation message for table 7 `public.t` with text `columns`, the first its key.
pub(crate) fn relation_message(columns: &[&str]) -> Vec<u8> {
    let count = i16::try_from(columns.len()).unwrap_or(i16::MAX);
    let mut bytes = [
        vec![b'R'],
        7_u32.to_be_bytes().to_vec(),
        cstring("public"),
        cstring("t"),
        vec![b'd'],
        count.to_be_bytes().to_vec(),
    ]
    .concat();
    for (index, name) in columns.iter().enumerate() {
        bytes.push(u8::from(index == 0));
        bytes.extend(cstring(name));
        bytes.extend(25_u32.to_be_bytes());
        bytes.extend((-1_i32).to_be_bytes());
    }
    bytes
}

/// A pgoutput Insert into table 7 of text `values`.
pub(crate) fn insert_message(values: &[&str]) -> Vec<u8> {
    let parts: Vec<Vec<u8>> = values.iter().map(|value| text_part(value)).collect();
    let parts: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
    [
        vec![b'I'],
        7_u32.to_be_bytes().to_vec(),
        vec![b'N'],
        tuple(&parts),
    ]
    .concat()
}

/// Buffer one transaction of `messages`, keeping at most `spill_bytes` in memory, and commit it: the `_sqlload_seq` of each change filed, disk before memory, or the error.
pub(crate) fn buffered_seqs(messages: &[Vec<u8>], spill_bytes: usize) -> Result<Vec<i64>, String> {
    let start = crate::cdc::_helpers::buffer::buffer_change(
        crate::cdc::models::ChangeBuffer {
            spill_bytes,
            ..crate::cdc::models::ChangeBuffer::default()
        },
        crate::cdc::models::StreamStep::Begin,
    )?;
    let held = messages.iter().try_fold(start, |buffer, message| {
        crate::cdc::_helpers::buffer::buffer_change(
            buffer,
            crate::cdc::models::StreamStep::Message(message),
        )
    })?;
    let mut buffer = crate::cdc::_helpers::buffer::buffer_change(
        held,
        crate::cdc::models::StreamStep::Commit {
            lsn: 100,
            end_lsn: 101,
            time: 0,
        },
    )?;
    let mut seqs: Vec<i64> = Vec::new();
    for file in buffer.spilled.values_mut() {
        let mut reader = file.read()?;
        let spilled: Vec<Change> = std::iter::from_fn(|| reader.next_change().transpose())
            .collect::<Result<_, String>>()?;
        seqs.extend(spilled.iter().map(|change| change.seq));
    }
    seqs.extend(
        buffer
            .tables
            .values()
            .flat_map(|table| table.changes.iter().map(|change| change.seq)),
    );
    Ok(seqs)
}
