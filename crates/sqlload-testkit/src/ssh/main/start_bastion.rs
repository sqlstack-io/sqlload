//! Start an SSH bastion.

use std::time::Duration;

use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

use crate::ssh::_helpers::key_files::key_files;
use crate::ssh::constants::{
    IMAGE, SSH_PASSWORD, SSH_PORT, SSH_USER, SSHD_SETTINGS, START_LIMIT, TAG,
};
use crate::ssh::models::Bastion;

/// A bastion (which also serves SFTP) that lets [`SSH_USER`] in with a fresh key or [`SSH_PASSWORD`] and forwards connections to containers on its own network (see [`crate::ssh::main::behind_bastion::behind_bastion`]), on a free host port.
pub async fn start_bastion() -> Result<Bastion, String> {
    let network = format!("sqlload-bastion-{}", uuid::Uuid::new_v4().simple());
    let starting = GenericImage::new(IMAGE, TAG)
        .with_exposed_port(SSH_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout("[ls.io-init] done."))
        .with_env_var("USER_NAME", SSH_USER)
        .with_env_var("PASSWORD_ACCESS", "true")
        .with_env_var("USER_PASSWORD", SSH_PASSWORD)
        .with_copy_to(
            "/config/sshd/sshd_config.d/forwarding.conf",
            SSHD_SETTINGS.as_bytes().to_vec(),
        )
        .with_network(network.clone())
        .with_startup_timeout(Duration::from_secs(120))
        .start();
    let container = tokio::time::timeout(START_LIMIT, starting)
        .await
        .map_err(|_| {
            format!(
                "pulling and starting the SSH bastion ({IMAGE}:{TAG}) took over {} s",
                START_LIMIT.as_secs()
            )
        })?
        .map_err(|error| format!("cannot start the SSH bastion: {error}"))?;
    let port = container
        .get_host_port_ipv4(SSH_PORT.tcp())
        .await
        .map_err(|error| error.to_string())?;
    let files = key_files(&container, port).await?;
    Ok(Bastion {
        port,
        files,
        network,
        container,
    })
}
