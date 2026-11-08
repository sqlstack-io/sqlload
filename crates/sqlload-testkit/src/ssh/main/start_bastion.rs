//! Start an SSH bastion.

use std::time::Duration;

use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::ssh::_helpers::key_files::key_files;
use crate::ssh::constants::{FORWARDING, IMAGE, KEYS_SCRIPT, SSH_PORT, SSH_USER, TAG};
use crate::ssh::models::Bastion;

/// A bastion that lets [`SSH_USER`] in with a fresh key and forwards connections to other containers, on a free host port.
pub async fn start_bastion() -> Result<Bastion, String> {
    let container = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(SSH_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("[ls.io-init] done."))
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
    let output = String::from_utf8(
        keys.stdout_to_vec()
            .await
            .map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let files = key_files(&output, port)?;
    Ok(Bastion {
        port,
        files,
        container,
    })
}
