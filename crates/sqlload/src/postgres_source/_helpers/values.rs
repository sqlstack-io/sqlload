use crate::postgres_source::constants::{
    NUMERIC_DIGIT_WIDTH, NUMERIC_NEGATIVE, NUMERIC_POSITIVE, POSTGRES_EPOCH_DAYS,
    POSTGRES_EPOCH_MICROS,
};

fn array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], String> {
    bytes
        .try_into()
        .map_err(|_| format!("expected {N} bytes, got {}", bytes.len()))
}

pub(crate) fn read_i16(bytes: &[u8]) -> Result<i16, String> {
    Ok(i16::from_be_bytes(array(bytes)?))
}

pub(crate) fn read_i32(bytes: &[u8]) -> Result<i32, String> {
    Ok(i32::from_be_bytes(array(bytes)?))
}

pub(crate) fn read_i64(bytes: &[u8]) -> Result<i64, String> {
    Ok(i64::from_be_bytes(array(bytes)?))
}

pub(crate) fn read_f32(bytes: &[u8]) -> Result<f32, String> {
    Ok(f32::from_be_bytes(array(bytes)?))
}

pub(crate) fn read_f64(bytes: &[u8]) -> Result<f64, String> {
    Ok(f64::from_be_bytes(array(bytes)?))
}

/// A Postgres `date` as days since 1970-01-01.
pub(crate) fn read_date(bytes: &[u8]) -> Result<i32, String> {
    let days = read_i32(bytes)?;
    if days == i32::MAX || days == i32::MIN {
        return Err("infinite dates cannot be loaded".to_owned());
    }
    days.checked_add(POSTGRES_EPOCH_DAYS)
        .ok_or_else(|| format!("date {days} days after 2000-01-01 is out of range"))
}

/// A Postgres `timestamp`/`timestamptz` as microseconds since 1970-01-01 UTC.
pub(crate) fn read_timestamp(bytes: &[u8]) -> Result<i64, String> {
    let micros = read_i64(bytes)?;
    if micros == i64::MAX || micros == i64::MIN {
        return Err("infinite timestamps cannot be loaded".to_owned());
    }
    micros
        .checked_add(POSTGRES_EPOCH_MICROS)
        .ok_or_else(|| "timestamp is out of range".to_owned())
}

/// A Postgres `uuid` in its usual hyphenated form.
pub(crate) fn read_uuid(bytes: &[u8]) -> Result<String, String> {
    let bytes: [u8; 16] = array(bytes)?;
    Ok(uuid::Uuid::from_bytes(bytes).hyphenated().to_string())
}

/// A Postgres binary `numeric` as an integer scaled by `10^scale`, failing rather than rounding.
pub(crate) fn read_numeric(bytes: &[u8], precision: u8, scale: u8) -> Result<i128, String> {
    let word = |index: usize| -> Result<i16, String> {
        bytes
            .get(index * 2..index * 2 + 2)
            .ok_or_else(|| "truncated numeric".to_owned())
            .and_then(read_i16)
    };
    let digit_count = usize::try_from(word(0)?).map_err(|_| "negative numeric length")?;
    let weight = i32::from(word(1)?);
    let sign = u16::from_be_bytes(word(2)?.to_be_bytes());
    if sign != NUMERIC_POSITIVE && sign != NUMERIC_NEGATIVE {
        return Err("NaN and infinite numerics cannot be loaded".to_owned());
    }
    let mut value: i128 = 0;
    for index in 0..digit_count {
        let digit = i128::from(word(4 + index)?);
        let position = i32::try_from(index).map_err(|_| "numeric too long")?;
        let exponent = (weight - position) * NUMERIC_DIGIT_WIDTH + i32::from(scale);
        value = add_scaled_digit(value, digit, exponent).ok_or_else(|| {
            format!("does not fit decimal({precision},{scale}) without losing digits")
        })?;
    }
    let limit = 10_i128.pow(u32::from(precision));
    if value >= limit {
        return Err(format!("is too large for decimal({precision},{scale})"));
    }
    Ok(if sign == NUMERIC_NEGATIVE {
        -value
    } else {
        value
    })
}

/// `value + digit * 10^exponent`, or `None` on overflow or when a negative exponent would drop non-zero digits.
fn add_scaled_digit(value: i128, digit: i128, exponent: i32) -> Option<i128> {
    if exponent >= 0 {
        let factor = 10_i128.checked_pow(exponent.unsigned_abs())?;
        return value.checked_add(digit.checked_mul(factor)?);
    }
    if -exponent >= NUMERIC_DIGIT_WIDTH {
        return (digit == 0).then_some(value);
    }
    let divisor = 10_i128.pow(exponent.unsigned_abs());
    if digit % divisor != 0 {
        return None;
    }
    value.checked_add(digit / divisor)
}
