//! Connections through the process's one ODBC environment.

use arrow_odbc::odbc_api::{Connection, ConnectionOptions, Cursor, environment};

use crate::odbc_source::constants::{ANSI_QUOTES_PROBE, IDENTIFIER_QUOTE, NO_QUOTE};
use crate::odbc_source::errors::OdbcSourceError;
use crate::odbc_source::models::OdbcReader;

/// A connection with the reader's connection string, refused when its database doesn't read `"name"` as a name, since SQLLoad writes ANSI double quotes. A driver that reports another quote is asked once whether the database reads them anyway: MySQL Connector/ODBC reports a backtick even when the session's SQL mode includes `ANSI_QUOTES`.
pub(crate) fn connect(reader: &OdbcReader) -> Result<Connection<'static>, OdbcSourceError> {
    let connection = environment()?
        .connect_with_connection_string(
            reader.connection_string.expose(),
            ConnectionOptions::default(),
        )
        .map_err(with_loader_reason)?;
    match connection.identifier_quote_char()? {
        None | Some(IDENTIFIER_QUOTE | NO_QUOTE) => Ok(connection),
        Some(_) if reads_ansi_quotes(&connection) => Ok(connection),
        Some(quote) => Err(OdbcSourceError::Invalid(format!(
            "the driver quotes identifiers with {quote:?}; SQLLoad's ODBC source writes ANSI double quotes, so switch the database to them (for MySQL, add INITSTMT=SET SESSION sql_mode=CONCAT(@@SESSION.sql_mode,',ANSI_QUOTES') to the connection string)"
        ))),
    }
}

/// The connect error, with the dynamic loader's own reason when unixODBC couldn't load the driver library: it reports every failed load as "file not found", even when the file exists and the loader refused it for another reason.
fn with_loader_reason(error: arrow_odbc::odbc_api::Error) -> OdbcSourceError {
    let message = error.to_string();
    match unloadable_library(&message).and_then(loader_reason) {
        Some(reason) => OdbcSourceError::Invalid(format!("ODBC: {message}\n{reason}")),
        None => error.into(),
    }
}

/// The absolute path in unixODBC's "Can't open lib '/path' : file not found".
fn unloadable_library(message: &str) -> Option<&str> {
    let (_, rest) = message.split_once("Can't open lib '")?;
    let (path, _) = rest.split_once("' :")?;
    path.starts_with('/').then_some(path)
}

/// What to check when unixODBC couldn't load `path`: the registration when no such file exists (with the fix when it's in a multiarch folder instead), and the error when it can't be read; otherwise the static thread-local storage the library is built for (its dynamic section's STATIC_TLS flag), which glibc can't give a library loaded late when it reaches thread-locals already in use, and the libraries it needs.
fn loader_reason(path: &str) -> Option<String> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Some(match multiarch_library(path) {
                Some((installed, prefix, arch)) => format!(
                    "The driver library {path} doesn't exist, but {installed} does: correct the Driver= line that registers it. MySQL Connector/ODBC's Debian 13 package registers its drivers under {prefix}//odbc/ in /etc/odbcinst.ini; that registration is corrected with: sudo sed -i 's#{prefix}//odbc/#{prefix}/{arch}/odbc/#' /etc/odbcinst.ini"
                ),
                None => format!(
                    "The driver library {path} doesn't exist: correct the Driver= line that registers the driver in odbcinst.ini."
                ),
            });
        }
        Err(error) => return Some(format!("The driver library {path} can't be read: {error}.")),
    };
    Some(if static_tls(&bytes) {
        format!(
            "The driver library {path} exists, and is built for static thread-local storage (STATIC_TLS), which the dynamic loader refuses once it reaches thread-locals already in use, as libstdc++'s are in a Python process that loaded libstdc++ after it started. Start the process with LD_PRELOAD=libstdc++.so.6, or use the sqlload-cli package's sqlload. If it still fails, `ldd {path}` lists the libraries it needs."
        )
    } else {
        format!(
            "The driver library {path} exists, so a library it needs is missing or doesn't load: `ldd {path}` lists them."
        )
    })
}

/// For a library registered as `<prefix>//odbc/<file>`, as MySQL's Debian 13 package registers its drivers, the same file in a multiarch folder `<prefix>/<arch>-linux-gnu/odbc/`, this machine's own architecture first: its path, the prefix and the multiarch folder's name. `None` when `<prefix>` can't be listed: this is only a hint under unixODBC's own error, which still names the registration to correct.
fn multiarch_library(path: &str) -> Option<(String, &str, String)> {
    let (prefix, file) = path.split_once("//odbc/")?;
    let own = format!("{}-linux-gnu", std::env::consts::ARCH);
    let entries = match std::fs::read_dir(prefix) {
        Ok(entries) => entries,
        Err(_) => return None,
    };
    entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str().map(str::to_owned))
        .filter(|arch| arch.ends_with("-linux-gnu"))
        .map(|arch| (format!("{prefix}/{arch}/odbc/{file}"), arch))
        .filter(|(installed, _)| std::path::Path::new(installed).is_file())
        .min_by_key(|(_, arch)| *arch != own)
        .map(|(installed, arch)| (installed, prefix, arch))
}

/// Whether a 64-bit little-endian ELF file's dynamic section sets DF_STATIC_TLS in DT_FLAGS.
fn static_tls(elf: &[u8]) -> bool {
    const DF_STATIC_TLS: u64 = 0x10;
    dynamic_flags(elf).is_some_and(|flags| flags & DF_STATIC_TLS != 0)
}

/// DT_FLAGS of a 64-bit little-endian ELF file: in the dynamic section its PT_DYNAMIC program header points at, read up to its DT_NULL entry or the end of the file. None for any other file, or one without the entry.
fn dynamic_flags(elf: &[u8]) -> Option<u64> {
    const PT_DYNAMIC: u64 = 2;
    const DT_NULL: u64 = 0;
    const DT_FLAGS: u64 = 30;
    if elf.get(..6)? != b"\x7fELF\x02\x01" {
        return None;
    }
    let headers = field(elf, 0x20, 8)? as usize;
    let (size, count) = (field(elf, 0x36, 2)? as usize, field(elf, 0x38, 2)? as usize);
    let header = (0..count)
        .map(|index| headers.saturating_add(index.saturating_mul(size)))
        .find(|&at| field(elf, at, 4) == Some(PT_DYNAMIC))?;
    let start = field(elf, header.saturating_add(0x08), 8)? as usize;
    let length = field(elf, header.saturating_add(0x20), 8)? as usize;
    let entry = (start..start.saturating_add(length).min(elf.len()))
        .step_by(16)
        .map(|at| (at, field(elf, at, 8)))
        .take_while(|&(_, tag)| tag.is_some_and(|tag| tag != DT_NULL))
        .find(|&(_, tag)| tag == Some(DT_FLAGS))
        .map(|(at, _)| at)?;
    field(elf, entry.saturating_add(8), 8)
}

/// The little-endian unsigned number of `width` bytes at `at`, if the file holds them.
fn field(elf: &[u8], at: usize, width: usize) -> Option<u64> {
    let bytes = elf.get(at..at.checked_add(width)?)?;
    Some(
        bytes
            .iter()
            .rev()
            .fold(0, |value, &byte| (value << 8) | u64::from(byte)),
    )
}

/// Whether the database reads `"name"` as a column name: the probe returns its row only then (a string literal never equals 1). A failed probe counts as no.
fn reads_ansi_quotes(connection: &Connection<'_>) -> bool {
    match connection.execute(ANSI_QUOTES_PROBE, (), None) {
        Ok(Some(mut cursor)) => matches!(cursor.next_row(), Ok(Some(_))),
        Ok(None) | Err(_) => false,
    }
}
