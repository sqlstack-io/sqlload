//! Start an SSH bastion.

use std::time::Duration;

use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::ssh::constants::{FORWARDING, IMAGE, KEYS_SCRIPT, SSH_PORT, SSH_USER, TAG};
use crate::ssh::models::Bastion;

/// A bastion that lets [`SSH_USER`] in with a fresh key and forwards connections to other containers, on a free host port.
pub async fn start_bastion() -> Result<Bastion, String> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(SSH_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server listening on"))
        .with_env_var("USER_NAME", SSH_USER)
        .with_copy_to(
            "/config/sshd/sshd_config.d/forwarding.conf",
            FORWARDING.as_bytes().to_vec(),
        )
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .map_err(|error| format!("cannot start the SSH bastion: {error}"))?;
    let port = container
        .get_host_port_ipv4(SSH_PORT.tcp())
        .await
        .map_err(|error| error.to_string())?;
    let mut keys = container
        .exec(ExecCommand::new(["sh", "-c", KEYS_SCRIPT]))
        .await
        .map_err(|error| format!("cannot make the bastion's keys: {error}"))?;
    let output = String::from_utf8(keys.stdout_to_vec().await.map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())?;
    let [client, other, client_public, host_keys] = output
        .split("---\n")
        .collect::<Vec<_>>()
        .try_into()
        .map_err(|parts: Vec<&str>| format!("the bastion printed {} key blocks, not 4", parts.len()))?;
    let listed = format!("[127.0.0.1]:{port}");
    let entries = |keys: &str| {
        keys.lines()
            .filter_map(|line| {
                let mut fields = line.split_whitespace();
                Some(format!("{listed} {} {}\n", fields.next()?, fields.next()?))
            })
            .collect::<String>()
    };
    let known = entries(host_keys);
    let impostor = client_public.split_whitespace().nth(1).unwrap_or_default();
    let changed = entries(host_keys)
        .lines()
        .map(|line| match line.split(' ').nth(1) {
            Some("ssh-ed25519") => format!("{listed} ssh-ed25519 {impostor}\n"),
            _ => format!("{line}\n"),
        })
        .collect::<String>();
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
    Ok(Bastion {
        port,
        files,
        container,
    })
}
