use crate::adapters::contract::models::ChangeApply;
use crate::cdc::_helpers::batch::{change_parts, rows_batch};
use crate::cdc::_helpers::buffer::buffer_change;
use crate::cdc::_helpers::spill::SpillFile;
use crate::cdc::models::{
    Change, ChangeBuffer, ChangeOp, Relation, RelationColumn, StreamStep, TableChanges, TupleValue,
};
use crate::column_types::models::ColumnType;
use crate::postgres_source::models::{ReadColumn, WireType};
use crate::schema_plan::constants::CDC_UNCHANGED_COLUMN;

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

/// A pgoutput Update of table 7 to text `values`, without old values.
pub(crate) fn update_message(values: &[&str]) -> Vec<u8> {
    [&[b'U'][..], &insert_message(values)[1..]].concat()
}

/// Buffer `transactions`, each a list of messages committed at LSN 100, 101, ..., keeping at most `spill_bytes` in memory.
fn committed(transactions: &[Vec<Vec<u8>>], spill_bytes: usize) -> Result<ChangeBuffer, String> {
    let mut buffer = ChangeBuffer {
        spill_bytes,
        ..ChangeBuffer::default()
    };
    for (index, messages) in transactions.iter().enumerate() {
        let lsn = 100 + u64::try_from(index).unwrap_or(u64::MAX);
        buffer = buffer_change(buffer, StreamStep::Begin)?;
        for message in messages {
            buffer = buffer_change(buffer, StreamStep::Message(message))?;
        }
        buffer = buffer_change(
            buffer,
            StreamStep::Commit {
                lsn,
                end_lsn: lsn,
                time: 0,
            },
        )?;
    }
    Ok(buffer)
}

/// Buffer one transaction of `messages`, keeping at most `spill_bytes` in memory, and commit it: the `_sqlload_seq` of each change filed, disk before memory, or the error.
pub(crate) fn buffered_seqs(messages: &[Vec<u8>], spill_bytes: usize) -> Result<Vec<i64>, String> {
    let mut buffer = committed(&[messages.to_vec()], spill_bytes)?;
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

/// The rows table 7 stages after `transactions` (see [`committed`]) for `apply` keyed by `id`, reading text `columns`: each `values|unchanged`, data values comma-separated with `-` for NULL, then the `_sqlload_unchanged` flags (`-` for none); sorted.
pub(crate) fn staged_texts(
    transactions: &[Vec<Vec<u8>>],
    (apply, spill_bytes): (ChangeApply, usize),
    columns: &[&str],
) -> Result<Vec<String>, String> {
    let mut buffer = committed(transactions, spill_bytes)?;
    let table = buffer.tables.remove(&7).ok_or("table 7 has no changes")?;
    let key = ["id".to_owned()];
    let (mut parts, _) = change_parts(
        &table,
        (buffer.spilled.remove(&7), spill_bytes),
        (apply, &key),
    )?;
    let read: Vec<ReadColumn> = columns
        .iter()
        .copied()
        .chain([CDC_UNCHANGED_COLUMN])
        .map(|name| ReadColumn {
            name: name.to_owned(),
            expression: format!("\"{name}\""),
            wire: WireType::Text,
            output: ColumnType::Text,
        })
        .collect();
    let staged: Vec<Vec<Change>> =
        std::iter::from_fn(|| parts.next_changes().transpose()).collect::<Result<_, String>>()?;
    let mut rows: Vec<String> = Vec::new();
    for part in staged {
        let batch = rows_batch(&part, &table.relation, &read)?;
        let texts: Vec<Vec<String>> = batch
            .columns()
            .iter()
            .map(|array| column_texts(array, batch.num_rows()))
            .collect::<Result<_, String>>()?;
        for row in 0..batch.num_rows() {
            let values: Vec<&str> = texts
                .iter()
                .filter_map(|column| column.get(row).map(String::as_str))
                .collect();
            let (data, flags) = values.split_at(columns.len());
            rows.push(format!("{}|{}", data.join(","), flags.concat()));
        }
    }
    rows.sort();
    Ok(rows)
}

/// The first `rows` values of `array` as text, `-` for NULL.
fn column_texts(array: &arrow_array::ArrayRef, rows: usize) -> Result<Vec<String>, String> {
    (0..rows)
        .map(|row| {
            arrow_cast::display::array_value_to_string(array, row)
                .map(|text| {
                    Some(text)
                        .filter(|text| !text.is_empty())
                        .unwrap_or_else(|| "-".to_owned())
                })
                .map_err(|error| error.to_string())
        })
        .collect()
}
