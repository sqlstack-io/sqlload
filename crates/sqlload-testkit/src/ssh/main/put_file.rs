//! Put a file on the test bastion, for its SFTP server to serve.

use std::io::Write;
use std::process::{Command, Stdio};

use crate::ssh::models::Bastion;

/// Write `bytes` to `path` (absolute) on `bastion`, creating its folders, readable by everyone and last modified at `mtime` (Unix seconds).
pub fn put_file(bastion: &Bastion, path: &str, bytes: &[u8], mtime: u32) -> Result<(), String> {
    let script = format!(
        "set -e; mkdir -p \"$(dirname '{path}')\"; cat > '{path}'; chmod 644 '{path}'; touch -d @{mtime} '{path}'"
    );
    let mut child = Command::new("docker")
        .args(["exec", "-i", bastion.container.id(), "sh", "-c", &script])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("cannot run docker: {error}"))?;
    child
        .stdin
        .take()
        .ok_or("docker exec has no stdin")?
        .write_all(bytes)
        .map_err(|error| format!("cannot send {path}: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("cannot put {path}: {error}"))?;
    match output.status.success() {
        true => Ok(()),
        false => Err(format!(
            "cannot put {path}: {}",
            String::from_utf8_lossy(&output.stderr)
        )),
    }
}
