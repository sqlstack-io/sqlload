//! The files a connection names to reach the test bastion.

use testcontainers::core::ExecCommand;
use testcontainers::{ContainerAsync, GenericImage};

use crate::ssh::constants::KEYS_SCRIPT;

/// A folder holding the key files [`crate::ssh::models::Bastion`] describes, made by running [`KEYS_SCRIPT`] in `container`, the bastion on host port `port`.
pub(crate) async fn key_files(
    container: &ContainerAsync<GenericImage>,
    port: u16,
) -> Result<tempfile::TempDir, String> {
    let mut keys = container
        .exec(ExecCommand::new(["sh", "-c", KEYS_SCRIPT]))
        .await
        .map_err(|error| format!("cannot make the bastion's keys: {error}"))?;
    let output = String::from_utf8(
        keys.stdout_to_vec()
            .await
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let [client, other, client_public, host_keys] = output
        .split("@@\n")
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|parts: Vec<&str>| {
            format!("the bastion printed {} key blocks, not 4", parts.len())
        })?;
    let listed = format!("[127.0.0.1]:{port}");
    let known = host_keys
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some(format!("{listed} {} {}\n", fields.next()?, fields.next()?))
        })
        .collect::<String>();
    let impostor = client_public.split_whitespace().nth(1).unwrap_or_default();
    let changed = format!("{listed} ssh-ed25519 {impostor}\n");
    let files = tempfile::tempdir().map_err(|error| error.to_string())?;
    for (name, text, mode) in [
        ("client.key", client, 0o600),
        ("other.key", other, 0o600),
        ("known_hosts", known.as_str(), 0o644),
        ("changed_hosts", changed.as_str(), 0o644),
        ("empty_hosts", "", 0o644),
    ] {
        let path = files.path().join(name);
        std::fs::write(&path, text).map_err(|error| error.to_string())?;
        std::fs::set_permissions(&path, std::os::unix::fs::PermissionsExt::from_mode(mode))
            .map_err(|error| error.to_string())?;
    }
    Ok(files)
}
