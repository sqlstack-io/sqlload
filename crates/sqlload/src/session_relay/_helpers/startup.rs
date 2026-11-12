use crate::session_relay::constants::{APPLICATION_NAME, PROTOCOL_VERSION};

/// `message`, a whole protocol 3.0 startup message (its length, the version, then pairs of NUL-terminated names and values and one more NUL), with its one `application_name` replaced by [`APPLICATION_NAME`]; refused unless that name is `expected`. The forwarded message is rebuilt from the parsed fields, so it is well formed whatever was sent.
pub(crate) fn admitted_startup(message: &[u8], expected: &str) -> Result<Vec<u8>, &'static str> {
    let declared = u32::from_be_bytes(*message.first_chunk::<4>().ok_or("no length")?);
    let length = u32::try_from(message.len()).map_err(|_| "too long")?;
    if declared != length || message.get(4..8) != Some(&PROTOCOL_VERSION[..]) {
        return Err("not one protocol 3.0 startup message");
    }
    let fields: Vec<&[u8]> = message[8..]
        .strip_suffix(b"\0\0")
        .ok_or("not NUL-terminated")?
        .split(|byte| *byte == 0)
        .collect();
    if !fields.len().is_multiple_of(2) || fields.iter().step_by(2).any(|name| name.is_empty()) {
        return Err("not pairs of names and values");
    }
    let named: Vec<usize> = (0..fields.len())
        .step_by(2)
        .filter(|index| fields[*index] == b"application_name")
        .collect();
    let [name] = named.as_slice() else {
        return Err("not one application name");
    };
    if !same_secret(fields[name + 1], expected.as_bytes()) {
        return Err("not this relay's application name");
    }
    let mut forwarded = vec![0; 4];
    forwarded.extend_from_slice(&PROTOCOL_VERSION);
    for (index, field) in fields.iter().enumerate() {
        match index == name + 1 {
            true => forwarded.extend_from_slice(APPLICATION_NAME.as_bytes()),
            false => forwarded.extend_from_slice(field),
        }
        forwarded.push(0);
    }
    forwarded.push(0);
    let length = u32::try_from(forwarded.len()).map_err(|_| "too long")?;
    forwarded[..4].copy_from_slice(&length.to_be_bytes());
    Ok(forwarded)
}

/// Whether `given` is `expected`, comparing every byte whatever the first difference, so the time taken tells nothing of the secret.
fn same_secret(given: &[u8], expected: &[u8]) -> bool {
    given.len() == expected.len()
        && given
            .iter()
            .zip(expected)
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0
}
