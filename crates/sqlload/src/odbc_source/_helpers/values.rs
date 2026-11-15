//! Rows read value by value with `SQLGetData`: every column of a table with a column too long to bind, each value at its own size.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, FixedSizeBinaryBuilder, Float32Builder,
    Float64Builder, Int8Builder, Int16Builder, Int32Builder, Int64Builder, StringBuilder,
    Time32MillisecondBuilder, Time32SecondBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder, UInt8Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_odbc::odbc_api::buffers::Indicator;
use arrow_odbc::odbc_api::parameter::{Binary, Text, VarCell, VarKind, WideText};
use arrow_odbc::odbc_api::sys::{Date, Time, Timestamp};
use arrow_odbc::odbc_api::{Bit, Cursor, CursorRow, Nullable, Pod};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use chrono::{NaiveDate, NaiveTime, Timelike};

use crate::odbc_source::constants::{KEPT_VALUE_BUFFER, VALUE_OVERHEAD};
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::ValueLimits;

/// One column read value by value: what it appends to, and the buffer its text or binary values are read into.
struct ValueColumn {
    name: String,
    values: Values,
    bytes: Vec<u8>,
    wide: Vec<u16>,
}

/// The Arrow array a column's values are appended to, by its fetched type.
enum Values {
    Boolean(BooleanBuilder),
    Int8(Int8Builder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt8(UInt8Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Date(Date32Builder),
    TimeSeconds(Time32SecondBuilder),
    TimeMilliseconds(Time32MillisecondBuilder),
    TimeMicroseconds(Time64MicrosecondBuilder),
    TimeNanoseconds(Time64NanosecondBuilder),
    TimestampMicroseconds(TimestampMicrosecondBuilder, Option<Arc<str>>),
    TimestampNanoseconds(TimestampNanosecondBuilder, Option<Arc<str>>),
    Text(StringBuilder),
    Binary(BinaryBuilder),
    FixedBinary(FixedSizeBinaryBuilder, i32),
}

/// Fetch `cursor`'s rows one at a time, reading every value of `schema`'s columns (as `fetch_schema` fetches them) with `SQLGetData` in column order, and hand them to `send` in batches that end at `max_rows` rows or once their values hold `budget` bytes; a value longer than `max_text_size` or `max_binary_size` fails the read rather than being cut. A single value bigger than the budget loads, ends its batch, and is logged with `table`'s name. Stops when the rows end or `send` says no more.
pub(crate) fn read_values<C: Cursor>(
    (mut cursor, schema): (C, &SchemaRef),
    (budget, max_rows): (usize, usize),
    (limits, table): (&ValueLimits, &str),
    mut send: impl FnMut(RecordBatch) -> bool,
) -> Result<(), OdbcSourceError> {
    let mut columns: Vec<ValueColumn> = schema
        .fields()
        .iter()
        .map(|field| ValueColumn::new(field))
        .collect::<Result<_, _>>()?;
    loop {
        let (mut rows, mut bytes, mut ended) = (0usize, 0usize, false);
        while rows < max_rows.max(1) && bytes < budget {
            let Some(mut row) = cursor.next_row()? else {
                ended = true;
                break;
            };
            for (column, number) in columns.iter_mut().zip(1u16..) {
                let (next, size) = column.read(row, number, limits)?;
                row = next;
                if size > budget {
                    tracing::warn!(
                        table,
                        column = %column.name,
                        bytes = size,
                        budget,
                        "a value is larger than the table's fetch buffer (max_batch_memory); it loads, and its fetch ends after its row"
                    );
                }
                bytes = bytes.saturating_add(size + VALUE_OVERHEAD);
            }
            rows += 1;
        }
        if rows > 0 {
            tracing::debug!(table, rows, bytes, budget, "fetched rows value by value");
            let arrays: Vec<ArrayRef> = columns.iter_mut().map(ValueColumn::finish).collect();
            let batch = RecordBatch::try_new(schema.clone(), arrays)
                .map_err(|error| OdbcSourceError::Invalid(error.to_string()))?;
            if !send(batch) {
                return Ok(());
            }
        }
        if ended {
            return Ok(());
        }
    }
}

impl ValueColumn {
    /// An empty column of `field`'s fetched type.
    fn new(field: &Field) -> Result<Self, OdbcSourceError> {
        let values = match field.data_type() {
            DataType::Boolean => Values::Boolean(BooleanBuilder::new()),
            DataType::Int8 => Values::Int8(Int8Builder::new()),
            DataType::Int16 => Values::Int16(Int16Builder::new()),
            DataType::Int32 => Values::Int32(Int32Builder::new()),
            DataType::Int64 => Values::Int64(Int64Builder::new()),
            DataType::UInt8 => Values::UInt8(UInt8Builder::new()),
            DataType::Float32 => Values::Float32(Float32Builder::new()),
            DataType::Float64 => Values::Float64(Float64Builder::new()),
            DataType::Date32 => Values::Date(Date32Builder::new()),
            DataType::Time32(TimeUnit::Second) => Values::TimeSeconds(Time32SecondBuilder::new()),
            DataType::Time32(TimeUnit::Millisecond) => {
                Values::TimeMilliseconds(Time32MillisecondBuilder::new())
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                Values::TimeMicroseconds(Time64MicrosecondBuilder::new())
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                Values::TimeNanoseconds(Time64NanosecondBuilder::new())
            }
            DataType::Timestamp(TimeUnit::Microsecond, zone) => {
                Values::TimestampMicroseconds(TimestampMicrosecondBuilder::new(), zone.clone())
            }
            DataType::Timestamp(TimeUnit::Nanosecond, zone) => {
                Values::TimestampNanoseconds(TimestampNanosecondBuilder::new(), zone.clone())
            }
            DataType::Utf8 => Values::Text(StringBuilder::new()),
            DataType::Binary => Values::Binary(BinaryBuilder::new()),
            DataType::FixedSizeBinary(length) => {
                Values::FixedBinary(FixedSizeBinaryBuilder::new(*length), *length)
            }
            other => {
                return Err(OdbcSourceError::Invalid(format!(
                    "column {} is fetched as {other}, which SQLLoad doesn't read value by value",
                    field.name()
                )));
            }
        };
        Ok(Self {
            name: field.name().clone(),
            values,
            bytes: Vec::new(),
            wide: Vec::new(),
        })
    }

    /// Read the current row's value of column `number` from `row` and append it; returns the row, and the value's bytes (a text or binary value's own length).
    fn read<'r>(
        &mut self,
        mut row: CursorRow<'r>,
        number: u16,
        limits: &ValueLimits,
    ) -> Result<(CursorRow<'r>, usize), OdbcSourceError> {
        let row_ref = &mut row;
        let name = self.name.as_str();
        let failed = |error: arrow_odbc::odbc_api::Error| {
            OdbcSourceError::Invalid(format!("column {name}: ODBC: {error}"))
        };
        let size = match &mut self.values {
            Values::Boolean(builder) => {
                builder.append_option(
                    fixed::<Bit>(row_ref, number)
                        .map_err(failed)?
                        .map(Bit::as_bool),
                );
                size_of::<bool>()
            }
            Values::Int8(builder) => {
                builder.append_option(fixed(row_ref, number).map_err(failed)?);
                size_of::<i8>()
            }
            Values::Int16(builder) => {
                builder.append_option(fixed(row_ref, number).map_err(failed)?);
                size_of::<i16>()
            }
            Values::Int32(builder) => {
                builder.append_option(fixed(row_ref, number).map_err(failed)?);
                size_of::<i32>()
            }
            Values::Int64(builder) => {
                builder.append_option(fixed(row_ref, number).map_err(failed)?);
                size_of::<i64>()
            }
            Values::UInt8(builder) => {
                builder.append_option(fixed(row_ref, number).map_err(failed)?);
                size_of::<u8>()
            }
            Values::Float32(builder) => {
                builder.append_option(fixed(row_ref, number).map_err(failed)?);
                size_of::<f32>()
            }
            Values::Float64(builder) => {
                builder.append_option(fixed(row_ref, number).map_err(failed)?);
                size_of::<f64>()
            }
            Values::Date(builder) => {
                let days = match fixed::<Date>(row_ref, number).map_err(failed)? {
                    Some(date) => Some(epoch_days(&date).ok_or_else(|| invalid(name, "date"))?),
                    None => None,
                };
                builder.append_option(days);
                size_of::<i32>()
            }
            Values::TimeSeconds(builder) => {
                let time = fixed::<Time>(row_ref, number).map_err(failed)?;
                builder.append_option(time.map(|time| {
                    (i32::from(time.hour) * 60 + i32::from(time.minute)) * 60
                        + i32::from(time.second)
                }));
                size_of::<i32>()
            }
            Values::TimeMilliseconds(builder) => {
                let time = time_text((&mut self.bytes, &self.name), row_ref, number)?;
                builder.append_option(time.map(|(seconds, nanoseconds)| {
                    i32::try_from(seconds * 1_000 + nanoseconds / 1_000_000).unwrap_or(i32::MAX)
                }));
                size_of::<i32>()
            }
            Values::TimeMicroseconds(builder) => {
                let time = time_text((&mut self.bytes, &self.name), row_ref, number)?;
                builder.append_option(
                    time.map(|(seconds, nanoseconds)| seconds * 1_000_000 + nanoseconds / 1_000),
                );
                size_of::<i64>()
            }
            Values::TimeNanoseconds(builder) => {
                let time = time_text((&mut self.bytes, &self.name), row_ref, number)?;
                builder.append_option(
                    time.map(|(seconds, nanoseconds)| seconds * 1_000_000_000 + nanoseconds),
                );
                size_of::<i64>()
            }
            Values::TimestampMicroseconds(builder, _) => {
                let micros = match fixed::<Timestamp>(row_ref, number).map_err(failed)? {
                    Some(time) => {
                        Some(epoch_micros(&time).ok_or_else(|| invalid(name, "timestamp"))?)
                    }
                    None => None,
                };
                builder.append_option(micros);
                size_of::<i64>()
            }
            Values::TimestampNanoseconds(builder, _) => {
                let nanos = match fixed::<Timestamp>(row_ref, number).map_err(failed)? {
                    Some(time) => Some(
                        epoch_nanos(&time)
                            .ok_or_else(|| invalid(name, "timestamp in nanoseconds"))?,
                    ),
                    None => None,
                };
                builder.append_option(nanos);
                size_of::<i64>()
            }
            Values::Text(_) => self.read_text(row_ref, number, limits.max_text_size)?,
            Values::Binary(_) | Values::FixedBinary(..) => {
                self.read_binary(row_ref, number, limits.max_binary_size)?
            }
        };
        Ok((row, size))
    }

    /// The current row's text value of column `number`, as UTF-16 on Windows and UTF-8 elsewhere (as arrow-odbc reads text), appended as UTF-8; fails when it is longer than `cap` bytes of UTF-8 (on Windows too) or isn't valid text. The read stops as soon as the pieces read pass `cap`: a UTF-16 unit is at least one byte of UTF-8.
    fn read_text(
        &mut self,
        row: &mut CursorRow<'_>,
        number: u16,
        cap: Option<usize>,
    ) -> Result<usize, OdbcSourceError> {
        let Values::Text(builder) = &mut self.values else {
            return Ok(0);
        };
        let name = self.name.as_str();
        let size = match cfg!(target_os = "windows") {
            true => {
                let capped = (cap, "max_text_size");
                if !read_capped::<WideText>(row, number, &mut self.wide, (name, capped))? {
                    builder.append_null();
                    return Ok(0);
                }
                let text = String::from_utf16(&self.wide).map_err(|_| invalid(name, "text"))?;
                over_cap(name, (text.len(), true), cap, "max_text_size")?;
                builder.append_value(&text);
                self.wide = kept(std::mem::take(&mut self.wide));
                text.len()
            }
            false => {
                let capped = (cap, "max_text_size");
                if !read_capped::<Text>(row, number, &mut self.bytes, (name, capped))? {
                    builder.append_null();
                    return Ok(0);
                }
                over_cap(name, (self.bytes.len(), true), cap, "max_text_size")?;
                let text = std::str::from_utf8(&self.bytes).map_err(|_| {
                    OdbcSourceError::Invalid(format!("column {name} holds text that is not UTF-8"))
                })?;
                builder.append_value(text);
                let size = self.bytes.len();
                self.bytes = kept(std::mem::take(&mut self.bytes));
                size
            }
        };
        Ok(size)
    }

    /// The current row's binary value of column `number`, appended; fails when it is longer than `cap` (stopping as soon as the pieces read pass it), or than a fixed-size column's width.
    fn read_binary(
        &mut self,
        row: &mut CursorRow<'_>,
        number: u16,
        cap: Option<usize>,
    ) -> Result<usize, OdbcSourceError> {
        let name = self.name.as_str();
        let capped = (cap, "max_binary_size");
        let present = read_capped::<Binary>(row, number, &mut self.bytes, (name, capped))?;
        over_cap(name, (self.bytes.len(), true), cap, "max_binary_size")?;
        let size = self.bytes.len();
        match (&mut self.values, present) {
            (Values::Binary(builder), true) => builder.append_value(&self.bytes),
            (Values::Binary(builder), false) => builder.append_null(),
            (Values::FixedBinary(builder, width), true) => {
                builder.append_value(&self.bytes).map_err(|_| {
                    OdbcSourceError::Invalid(format!(
                        "column {name} holds a value of {size} bytes, not its {width}"
                    ))
                })?;
            }
            (Values::FixedBinary(builder, _), false) => builder.append_null(),
            _ => {}
        }
        self.bytes = kept(std::mem::take(&mut self.bytes));
        Ok(size)
    }

    /// The column's values read since the last call, as one array; the column starts empty again.
    fn finish(&mut self) -> ArrayRef {
        match &mut self.values {
            Values::Boolean(builder) => Arc::new(builder.finish()),
            Values::Int8(builder) => Arc::new(builder.finish()),
            Values::Int16(builder) => Arc::new(builder.finish()),
            Values::Int32(builder) => Arc::new(builder.finish()),
            Values::Int64(builder) => Arc::new(builder.finish()),
            Values::UInt8(builder) => Arc::new(builder.finish()),
            Values::Float32(builder) => Arc::new(builder.finish()),
            Values::Float64(builder) => Arc::new(builder.finish()),
            Values::Date(builder) => Arc::new(builder.finish()),
            Values::TimeSeconds(builder) => Arc::new(builder.finish()),
            Values::TimeMilliseconds(builder) => Arc::new(builder.finish()),
            Values::TimeMicroseconds(builder) => Arc::new(builder.finish()),
            Values::TimeNanoseconds(builder) => Arc::new(builder.finish()),
            Values::TimestampMicroseconds(builder, zone) => {
                Arc::new(builder.finish().with_timezone_opt(zone.clone()))
            }
            Values::TimestampNanoseconds(builder, zone) => {
                Arc::new(builder.finish().with_timezone_opt(zone.clone()))
            }
            Values::Text(builder) => Arc::new(builder.finish()),
            Values::Binary(builder) => Arc::new(builder.finish()),
            Values::FixedBinary(builder, _) => Arc::new(builder.finish()),
        }
    }
}

/// The current row's time of column `number` (of column `name`), read into `bytes` as `HH:MM:SS.fff…` text, as arrow-odbc reads a time with fractional seconds: its seconds since midnight and the nanoseconds after them.
fn time_text(
    (bytes, name): (&mut Vec<u8>, &str),
    row: &mut CursorRow<'_>,
    number: u16,
) -> Result<Option<(i64, i64)>, OdbcSourceError> {
    let present = row
        .get_text(number, bytes)
        .map_err(|error| OdbcSourceError::Invalid(format!("column {name}: ODBC: {error}")))?;
    if !present {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(bytes);
    let time =
        NaiveTime::parse_from_str(text.trim(), "%H:%M:%S%.f").map_err(|_| invalid(name, "time"))?;
    Ok(Some((
        i64::from(time.num_seconds_from_midnight()),
        i64::from(time.nanosecond()),
    )))
}

/// The current row's fixed-size value of column `number`; `None` when NULL.
fn fixed<T: Pod>(
    row: &mut CursorRow<'_>,
    number: u16,
) -> Result<Option<T>, arrow_odbc::odbc_api::Error> {
    let mut value = Nullable::<T>::null();
    row.get_data(number, &mut value)?;
    Ok(value.into_opt())
}

/// The date of an ODBC date's or timestamp's fields; `None` when there is no such day.
fn day(year: i16, month: u16, day: u16) -> Option<NaiveDate> {
    NaiveDate::from_ymd_opt(i32::from(year), u32::from(month), u32::from(day))
}

/// The wall-clock time of an ODBC timestamp; `None` when its fields make none.
fn moment(time: &Timestamp) -> Option<chrono::NaiveDateTime> {
    day(time.year, time.month, time.day)?.and_hms_nano_opt(
        u32::from(time.hour),
        u32::from(time.minute),
        u32::from(time.second),
        time.fraction,
    )
}

/// An ODBC date as days since 1970-01-01; `None` when there is no such day.
fn epoch_days(date: &Date) -> Option<i32> {
    Some(day(date.year, date.month, date.day)?.to_epoch_days())
}

/// An ODBC timestamp as microseconds since 1970; `None` when its fields make no time.
fn epoch_micros(time: &Timestamp) -> Option<i64> {
    Some(moment(time)?.and_utc().timestamp_micros())
}

/// An ODBC timestamp as nanoseconds since 1970; `None` when its fields make no time or it is outside 1677 to 2262.
fn epoch_nanos(time: &Timestamp) -> Option<i64> {
    moment(time)?.and_utc().timestamp_nanos_opt()
}

/// A value of column `name` that isn't a valid `what`.
fn invalid(name: &str, what: &str) -> OdbcSourceError {
    OdbcSourceError::Invalid(format!(
        "column {name} holds a value that is not a valid {what}"
    ))
}

/// Fails when a value of `length` passes `cap`, naming the setting: SQLLoad never cuts a value.
fn over_cap(
    name: &str,
    (length, exact): (usize, bool),
    cap: Option<usize>,
    setting: &str,
) -> Result<(), OdbcSourceError> {
    let at_least = match exact {
        true => "",
        false => "at least ",
    };
    match cap {
        Some(cap) if length > cap => Err(OdbcSourceError::Invalid(format!(
            "column {name} holds a value of {at_least}{length} bytes, longer than {setting} = {cap}; a value longer than its cap fails the read rather than being cut, so raise {setting} on the source, or remove it"
        ))),
        _ => Ok(()),
    }
}

/// Read the current row's value of column `number` into `buffer` piece by piece with `SQLGetData`, as odbc-api's `get_text` does, but stop with `over_cap`'s error as soon as the value is known to be longer than the cap (in `K`'s elements): once the driver reports the length still to come, or the pieces read pass it. `false` for NULL, with `buffer` empty.
fn read_capped<K: VarKind>(
    row: &mut CursorRow<'_>,
    number: u16,
    buffer: &mut Vec<K::Element>,
    (name, (cap, setting)): (&str, (Option<usize>, &str)),
) -> Result<bool, OdbcSourceError> {
    let failed = |error: arrow_odbc::odbc_api::Error| {
        OdbcSourceError::Invalid(format!("column {name}: ODBC: {error}"))
    };
    let element = size_of::<K::Element>();
    if buffer.capacity() == 0 {
        buffer.reserve(256);
    }
    buffer.resize(buffer.capacity(), K::ZERO);
    let mut start = 0;
    loop {
        let mut piece =
            VarCell::<&mut [K::Element], K>::from_buffer(&mut buffer[start..], Indicator::NoTotal);
        row.get_data(number, &mut piece).map_err(failed)?;
        let (complete, indicator) = (piece.is_complete(), piece.indicator());
        let Some(fetched) = piece.len_in_bytes() else {
            buffer.clear();
            return Ok(false);
        };
        if complete {
            buffer.truncate(start + fetched / element);
            return Ok(true);
        }
        let known = match indicator {
            Indicator::Length(length) => (start + length / element, true),
            _ => (start + fetched / element, false),
        };
        over_cap(name, known, cap, setting)?;
        let old = buffer.len();
        let grown = match indicator {
            Indicator::Length(length) => old + (length - fetched) / element,
            _ => old * 2,
        };
        buffer.resize(grown, K::ZERO);
        start = old - K::TERMINATING_ZEROES;
    }
}

/// `buffer`, or an empty one once it has grown past the size kept between values, since each read fills the whole buffer.
fn kept<T>(buffer: Vec<T>) -> Vec<T> {
    match buffer.capacity() > KEPT_VALUE_BUFFER {
        true => Vec::new(),
        false => buffer,
    }
}
