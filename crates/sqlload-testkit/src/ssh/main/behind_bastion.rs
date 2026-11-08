//! Put a container behind the test bastion.

use crate::ssh::models::Bastion;

/// Join container `id` to `bastion`'s network as `alias`, a name only the bastion resolves.
pub fn behind_bastion(bastion: &Bastion, id: &str, alias: &str) -> Result<(), String> {
    let output = std::process::Command::new("docker")
        .args(["network", "connect", "--alias", alias, &bastion.network, id])
        .output()
        .map_err(|error| format!("cannot run docker: {error}"))?;
    match output.status.success() {
        true => Ok(()),
        false => Err(format!(
            "cannot join {id} to {}: {}",
            bastion.network,
            String::from_utf8_lossy(&output.stderr)
        )),
    }
}
