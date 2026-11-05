//! Deterministic benchmark inputs: binary `COPY` streams, text files in memory, and yielded rows.

use std::sync::Arc;

use arrow_array::RecordBatch;
use object_store::ObjectStoreExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use sqlload::column_types::models::ColumnType;
use sqlload::file_store::models::OpenFolder;
use sqlload::postgres_source::main::decode_copy::decode_copy;
use sqlload::postgres_source::models::{ReadColumn, WireType};
use sqlload::row_batches::main::columns_batch::columns_batch;
use sqlload::row_batches::models::{Cell, RowColumns};
use sqlload::store_paths::models::{Location, StoreKind};
use tokio::runtime::Runtime;

const COPY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";
const WORDS: [&str; 10] = [
    "alpha", "bravo", "cobalt", "delta", "ember", "fjord", "garnet", "harbor", "indigo", "juniper",
];
const STATUSES: [&str; 6] = ["new", "paid", "packed", "shipped", "delivered", "returned"];
/// 2026-01-01T00:00:00Z in microseconds since Postgres' 2000-01-01 epoch.
const PG_2026_MICROS: i64 = 820_540_800_000_000;

/// A binary `COPY` stream as Postgres sends it: one chunk per `CopyData` message (header, rows, trailer).
pub(crate) struct CopyStream {
    pub(crate) columns: Vec<ReadColumn>,
    pub(crate) chunks: Vec<Vec<u8>>,
}

/// Yielded rows as the crossing gathers them: field names and cells in each row's key order.
pub(crate) type Rows = Vec<Vec<(&'static str, Cell)>>;

/// A stable 31-bit hash of `(value, salt)`.
pub(crate) fn mix(value: u64, salt: u64) -> u64 {
    let mut x = value
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(salt.wrapping_mul(0xC2B2_AE3D_27D4_EB4F));
    x ^= x >> 31;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 29;
    x & 0x7FFF_FFFF
}

fn read_column(name: &str, wire: WireType, output: ColumnType) -> ReadColumn {
    ReadColumn {
        name: name.to_owned(),
        expression: format!("\"{name}\""),
        wire,
        output,
    }
}

/// A binary `numeric` with `scale` (at most 4) decimal places, from its unscaled value.
fn numeric(unscaled: i64, scale: u32) -> Vec<u8> {
    let magnitude = unscaled.unsigned_abs();
    let fraction = (magnitude % 10_u64.pow(scale)) * 10_u64.pow(4 - scale);
    let mut whole = magnitude / 10_u64.pow(scale);
    let mut digits: Vec<u16> = Vec::new();
    while whole > 0 {
        digits.insert(0, (whole % 10_000) as u16);
        whole /= 10_000;
    }
    let weight = digits.len() as i16 - 1;
    digits.push(fraction as u16);
    while digits.last() == Some(&0) {
        digits.pop();
    }
    let weight = if digits.is_empty() { 0 } else { weight };
    let sign: u16 = if unscaled < 0 { 0x4000 } else { 0 };
    let mut bytes = Vec::with_capacity(8 + 2 * digits.len());
    bytes.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    bytes.extend_from_slice(&weight.to_be_bytes());
    bytes.extend_from_slice(&sign.to_be_bytes());
    bytes.extend_from_slice(&(scale as u16).to_be_bytes());
    digits
        .iter()
        .for_each(|digit| bytes.extend_from_slice(&digit.to_be_bytes()));
    bytes
}

fn copy_row(fields: &[Option<Vec<u8>>]) -> Vec<u8> {
    let mut row = (fields.len() as i16).to_be_bytes().to_vec();
    for field in fields {
        match field {
            Some(value) => {
                row.extend_from_slice(&(value.len() as i32).to_be_bytes());
                row.extend_from_slice(value);
            }
            None => row.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
    row
}

fn copy_stream(columns: Vec<ReadColumn>, rows: impl Iterator<Item = Vec<u8>>) -> CopyStream {
    let mut header = COPY_SIGNATURE.to_vec();
    header.extend_from_slice(&0_i32.to_be_bytes());
    header.extend_from_slice(&0_i32.to_be_bytes());
    let mut chunks = vec![header];
    chunks.extend(rows);
    chunks.push((-1_i16).to_be_bytes().to_vec());
    CopyStream { columns, chunks }
}

/// The narrow table: `id int8, customer_name text, created_at timestamptz, amount numeric(12,2), is_active bool, quantity int4`.
pub(crate) fn narrow_stream(rows: u64) -> CopyStream {
    let columns = vec![
        read_column("id", WireType::Int8, ColumnType::Int),
        read_column("customer_name", WireType::Text, ColumnType::Text),
        read_column("created_at", WireType::Timestamp, ColumnType::TimestampTz),
        read_column(
            "amount",
            WireType::Numeric,
            ColumnType::Decimal {
                precision: 12,
                scale: 2,
            },
        ),
        read_column("is_active", WireType::Bool, ColumnType::Bool),
        read_column("quantity", WireType::Int4, ColumnType::Int),
    ];
    let row = |id: u64| {
        let h: Vec<u64> = (0..6).map(|salt| mix(id, salt)).collect();
        let created = PG_2026_MICROS + ((id * 3 + h[5] % 3) * 1_000_000) as i64;
        copy_row(&[
            Some((id as i64).to_be_bytes().to_vec()),
            Some(
                format!(
                    "customer {} {}",
                    h[0] % 100_000,
                    WORDS[(h[1] % 10) as usize]
                )
                .into_bytes(),
            ),
            Some(created.to_be_bytes().to_vec()),
            (!h[2].is_multiple_of(20)).then(|| numeric((h[2] % 10_000_000) as i64, 2)),
            Some(vec![u8::from(!h[3].is_multiple_of(3))]),
            Some(((h[4] % 1000) as i32).to_be_bytes().to_vec()),
        ])
    };
    copy_stream(columns, (1..=rows).map(row))
}

/// The wide table: 40 columns of integers, text, numeric(18,4), timestamps, dates, booleans, floats and two jsonb (read as text).
pub(crate) fn wide_stream(rows: u64) -> CopyStream {
    let mut columns = vec![read_column("id", WireType::Int8, ColumnType::Int)];
    for n in 1..=8_u32 {
        let wire = if !n.is_multiple_of(2) {
            WireType::Int4
        } else {
            WireType::Int8
        };
        columns.push(read_column(&format!("c_int_{n:02}"), wire, ColumnType::Int));
    }
    for n in 1..=9 {
        columns.push(read_column(
            &format!("c_text_{n:02}"),
            WireType::Text,
            ColumnType::Text,
        ));
    }
    for n in 1..=6 {
        let decimal = ColumnType::Decimal {
            precision: 18,
            scale: 4,
        };
        columns.push(read_column(
            &format!("c_num_{n:02}"),
            WireType::Numeric,
            decimal,
        ));
    }
    for n in 1..=4 {
        columns.push(read_column(
            &format!("c_ts_{n:02}"),
            WireType::Timestamp,
            ColumnType::TimestampTz,
        ));
    }
    for n in 1..=2 {
        columns.push(read_column(
            &format!("c_date_{n:02}"),
            WireType::Date,
            ColumnType::Date,
        ));
    }
    for n in 1..=4 {
        columns.push(read_column(
            &format!("c_bool_{n:02}"),
            WireType::Bool,
            ColumnType::Bool,
        ));
    }
    for n in 1..=4 {
        columns.push(read_column(
            &format!("c_float_{n:02}"),
            WireType::Float8,
            ColumnType::Float,
        ));
    }
    for n in 1..=2 {
        let mut column = read_column(&format!("c_json_{n:02}"), WireType::Text, ColumnType::Json);
        column.expression = format!("\"c_json_{n:02}\"::text");
        columns.push(column);
    }
    let row = |id: u64| {
        let h = |salt: u64| mix(id, salt);
        let mut fields: Vec<Option<Vec<u8>>> = vec![Some((id as i64).to_be_bytes().to_vec())];
        for n in 1..=8_u64 {
            let value = h(10 + n) % 1_000_000;
            fields.push(Some(match n % 2 {
                1 => (value as i32).to_be_bytes().to_vec(),
                _ => (value as i64).to_be_bytes().to_vec(),
            }));
        }
        for n in 1..=9_u64 {
            let text = format!(
                "{}-{}",
                WORDS[(h(90 + n) % 10) as usize],
                h(20 + n) % 100_000
            );
            fields.push(Some(text.into_bytes()));
        }
        for n in 1..=6_u64 {
            fields.push(
                (!h(30 + n).is_multiple_of(25))
                    .then(|| numeric((h(30 + n) % 100_000_000) as i64, 4)),
            );
        }
        for n in 1..=4_u64 {
            let micros =
                PG_2026_MICROS - 31_536_000_000_000 + (h(40 + n) % 31_536_000) as i64 * 1_000_000;
            fields.push(Some(micros.to_be_bytes().to_vec()));
        }
        for n in 1..=2_u64 {
            fields.push(Some(
                (7305 + (h(50 + n) % 3650) as i32).to_be_bytes().to_vec(),
            ));
        }
        for n in 1..=4_u64 {
            fields.push(Some(vec![u8::from(h(60 + n).is_multiple_of(2))]));
        }
        for n in 1..=4_u64 {
            fields.push(Some(
                ((h(70 + n) % 1_000_000) as f64 / 64.0)
                    .to_be_bytes()
                    .to_vec(),
            ));
        }
        let first = format!(
            r#"{{"n": {}, "meta": {{"k": "v{}"}}, "tags": [{}, {}]}}"#,
            h(81) % 1000,
            h(84) % 10,
            h(82) % 7,
            h(83) % 7
        );
        fields.push(Some(first.into_bytes()));
        fields.push(Some(
            format!(
                r#"{{"n": {}, "ok": {}}}"#,
                h(85) % 1000,
                h(86).is_multiple_of(2)
            )
            .into_bytes(),
        ));
        copy_row(&fields)
    };
    copy_stream(columns, (1..=rows).map(row))
}

/// The narrow table's rows as one Arrow batch, decoded from its `COPY` stream.
pub(crate) fn narrow_batch(rows: u64) -> RecordBatch {
    let stream = narrow_stream(rows);
    let chunks = stream.chunks.iter().map(Vec::as_slice);
    let mut batches = decode_copy("public.narrow", stream.columns, chunks, usize::MAX)
        .unwrap_or_else(|error| panic!("the narrow stream decodes: {error}"));
    batches
        .pop()
        .unwrap_or_else(|| panic!("the narrow stream has rows"))
}

fn timestamp_text(seconds: u64) -> String {
    let (days, rest) = (seconds / 86_400, seconds % 86_400);
    let (hour, minute, second) = (rest / 3600, rest / 60 % 60, rest % 60);
    let (month, day) = (1 + days / 28 % 12, 1 + days % 28);
    format!(
        "{}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z",
        2026 + days / 336
    )
}

/// The narrow rows as JSON objects with a nested `meta` object, one per line.
pub(crate) fn narrow_ndjson(first: u64, rows: u64) -> String {
    (first..first + rows)
        .map(|id| {
            let h: Vec<u64> = (0..6).map(|salt| mix(id, salt)).collect();
            let amount = match h[2] % 20 {
                0 => "null".to_owned(),
                _ => format!("{}.{:02}", h[2] % 100_000, h[2] % 100),
            };
            format!(
                "{{\"id\":{id},\"customer_name\":\"customer {} {}\",\"created_at\":\"{}\",\"amount\":{amount},\"is_active\":{},\"quantity\":{},\"meta\":{{\"tags\":[\"{}\"],\"n\":{}}}}}\n",
                h[0] % 100_000,
                WORDS[(h[1] % 10) as usize],
                timestamp_text(id * 3 + h[5] % 3),
                !h[3].is_multiple_of(3),
                h[4] % 1000,
                WORDS[(h[4] % 10) as usize],
                h[0] % 7
            )
        })
        .collect()
}

/// The narrow rows as CSV with a header line.
pub(crate) fn narrow_csv(first: u64, rows: u64) -> String {
    let mut text = "id,customer_name,created_at,amount,is_active,quantity\n".to_owned();
    for id in first..first + rows {
        let h: Vec<u64> = (0..6).map(|salt| mix(id, salt)).collect();
        let amount = match h[2] % 20 {
            0 => String::new(),
            _ => format!("{}.{:02}", h[2] % 100_000, h[2] % 100),
        };
        text.push_str(&format!(
            "{id},\"customer {}, {}\",{},{amount},{},{}\n",
            h[0] % 100_000,
            WORDS[(h[1] % 10) as usize],
            timestamp_text(id * 3 + h[5] % 3),
            !h[3].is_multiple_of(3),
            h[4] % 1000
        ));
    }
    text
}

/// The narrow rows as one JSON array of objects.
pub(crate) fn narrow_json(first: u64, rows: u64) -> String {
    let lines: Vec<String> = narrow_ndjson(first, rows)
        .lines()
        .map(str::to_owned)
        .collect();
    format!("[\n{}\n]\n", lines.join(",\n"))
}

/// A runtime and an in-memory folder holding `files`, keyed by name.
pub(crate) fn memory_folder(files: Vec<(String, String)>) -> (Runtime, OpenFolder) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap_or_else(|error| panic!("a runtime starts: {error}"));
    let store = Arc::new(InMemory::new());
    runtime.block_on(async {
        for (key, text) in files {
            store
                .put(&Path::from(key), text.into_bytes().into())
                .await
                .unwrap_or_else(|error| panic!("the in-memory store takes a file: {error}"));
        }
    });
    let location = Location {
        store: StoreKind::Filesystem {
            root: "/bench".to_owned(),
        },
        prefix: String::new(),
    };
    (runtime, OpenFolder { store, location })
}

/// `rows` flat 20-field order rows as Python dicts cross: ints, floats, booleans, text and ISO timestamps.
pub(crate) fn flat_rows(rows: u64) -> Rows {
    (1..=rows)
        .map(|id| {
            let h: Vec<u64> = (0..8).map(|salt| mix(id, salt)).collect();
            vec![
                ("id", Cell::Int(id as i64)),
                ("customer_id", Cell::Int((h[0] % 100_000) as i64)),
                ("product_id", Cell::Int((h[1] % 5_000) as i64)),
                ("quantity", Cell::Int((1 + h[2] % 20) as i64)),
                ("warehouse_id", Cell::Int((h[3] % 50) as i64)),
                ("priority", Cell::Int((h[4] % 5) as i64)),
                ("sku", Cell::Text(format!("SKU-{:05}", h[1] % 5_000))),
                (
                    "status",
                    Cell::Text(STATUSES[(h[2] % 6) as usize].to_owned()),
                ),
                (
                    "customer_name",
                    Cell::Text(format!("{} {}", WORDS[(h[0] % 10) as usize], h[0] % 997)),
                ),
                ("city", Cell::Text(WORDS[(h[5] % 10) as usize].to_owned())),
                (
                    "channel",
                    Cell::Text(["web", "app", "partner"][(h[6] % 3) as usize].to_owned()),
                ),
                ("unit_price", Cell::Float((h[5] % 100_000) as f64 / 100.0)),
                ("discount", Cell::Float((h[6] % 5_000) as f64 / 100.0)),
                ("tax", Cell::Float((h[7] % 2_000) as f64 / 100.0)),
                ("weight_kg", Cell::Float((h[4] % 10_000) as f64 / 100.0)),
                ("is_gift", Cell::Bool(h[7].is_multiple_of(7))),
                ("is_priority", Cell::Bool(h[4].is_multiple_of(3))),
                ("created_at", Cell::Text(timestamp_text(id * 7))),
                ("updated_at", Cell::Text(timestamp_text(id * 7 + 3_600))),
                (
                    "note",
                    match h[2] % 4 {
                        0 => Cell::Text(format!("note {}", h[3] % 1_000)),
                        _ => Cell::Null,
                    },
                ),
            ]
        })
        .collect()
}

/// Gather `rows` column by column, as the crossing does for yielded dicts.
pub(crate) fn gather(rows: &Rows) -> RowColumns {
    let mut columns = RowColumns::new();
    for row in rows {
        for (name, cell) in row {
            columns.push(name, cell.clone());
        }
        columns.end_row();
    }
    columns
}

/// Pages of flat rows whose columns drift: a field missing from some pages, integers turning into floats, and a column NULL in the first page only.
pub(crate) fn drifting_batches(pages: u64, rows: u64) -> Vec<RecordBatch> {
    (0..pages)
        .map(|page| {
            let mut rows = flat_rows(rows);
            for row in &mut rows {
                if page % 3 == 1 {
                    row.retain(|(name, _)| *name != "discount");
                }
                if page >= pages / 2 {
                    for (name, cell) in row.iter_mut() {
                        if *name == "quantity"
                            && let Cell::Int(value) = cell
                        {
                            *cell = Cell::Float(*value as f64 + 0.5);
                        }
                    }
                }
                if page == 0 {
                    for (name, cell) in row.iter_mut() {
                        if *name == "note" {
                            *cell = Cell::Null;
                        }
                    }
                }
            }
            columns_batch(gather(&rows)).unwrap_or_else(|error| panic!("a page converts: {error}"))
        })
        .collect()
}

/// The flat rows as NDJSON bytes, as `sl.ndjson(...)` hands them over.
pub(crate) fn flat_ndjson(rows: u64) -> Vec<u8> {
    let mut text = String::new();
    for row in flat_rows(rows) {
        let fields: Vec<String> = row
            .iter()
            .map(|(name, cell)| {
                let value = match cell {
                    Cell::Null => "null".to_owned(),
                    Cell::Bool(value) => value.to_string(),
                    Cell::Int(value) => value.to_string(),
                    Cell::Float(value) => format!("{value:?}"),
                    Cell::Number(text) | Cell::Json(text) => text.clone(),
                    Cell::Text(text) => format!("\"{text}\""),
                };
                format!("\"{name}\":{value}")
            })
            .collect();
        text.push('{');
        text.push_str(&fields.join(","));
        text.push_str("}\n");
    }
    text.into_bytes()
}

/// API-shaped nested orders as NDJSON: a customer object, a list of line objects, a map and a list of tags.
pub(crate) fn nested_ndjson(rows: u64) -> Vec<u8> {
    let mut text = String::new();
    for id in 1..=rows {
        let h: Vec<u64> = (0..8).map(|salt| mix(id, salt)).collect();
        let lines: Vec<String> = (0..1 + h[0] % 5)
            .map(|line| {
                let key = id * 8 + line;
                format!(
                    r#"{{"line":{line},"product_id":{},"quantity":{},"price":{}.{:02}}}"#,
                    mix(key, 11) % 5_000,
                    1 + mix(key, 12) % 9,
                    mix(key, 13) % 500,
                    mix(key, 13) % 100
                )
            })
            .collect();
        let attributes: Vec<String> = (0..h[3] % 4)
            .map(|key| format!(r#""k{key}":"{}""#, WORDS[((h[7] + key) % 10) as usize]))
            .collect();
        let tags: Vec<String> = (0..h[1] % 3)
            .map(|tag| format!(r#""{}""#, WORDS[((h[4] + tag) % 10) as usize]))
            .collect();
        text.push_str(&format!(
            r#"{{"id":{id},"status":"{}","created_at":"{}","customer":{{"id":{},"name":"{} {}","address":{{"city":"{}","postcode":"{:05}"}}}},"lines":[{}],"attributes":{{{}}},"tags":[{}]}}"#,
            STATUSES[(h[2] % 6) as usize],
            timestamp_text(id * 7),
            h[0] % 100_000,
            WORDS[(h[0] % 10) as usize],
            h[0] % 997,
            WORDS[(h[5] % 10) as usize],
            h[6] % 100_000,
            lines.join(","),
            attributes.join(","),
            tags.join(",")
        ));
        text.push('\n');
    }
    text.into_bytes()
}

/// The flat rows inside an API response document, at `data.items`.
pub(crate) fn flat_document(rows: u64) -> Vec<u8> {
    let rows = String::from_utf8(flat_ndjson(rows)).unwrap_or_default();
    let items: Vec<&str> = rows.lines().collect();
    format!(
        r#"{{"meta":{{"page":1,"next":null}},"data":{{"items":[{}]}}}}"#,
        items.join(",")
    )
    .into_bytes()
}
