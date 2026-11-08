//! Signing in to the bastion and checking who it is.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use russh::client::{Config, Handle};
use russh::keys::agent::client::AgentClient;
use russh::keys::known_hosts::check_known_hosts_path;
use russh::keys::{HashAlg, PrivateKeyWithHashAlg, PublicKeyOrCertificate, load_secret_key};

use crate::ssh_tunnel::constants::{CONNECT_TIMEOUT, KEEPALIVE_INTERVAL, KEEPALIVE_MAX};
use crate::ssh_tunnel::models::{HostCheck, SshTunnel};

/// `path` with a leading `~/` read as the home folder.
pub(crate) fn expanded(path: &Path) -> PathBuf {
    match (path.strip_prefix("~"), std::env::var_os("HOME")) {
        (Ok(rest), Some(home)) => PathBuf::from(home).join(rest),
        _ => path.to_path_buf(),
    }
}

/// Why the bastion's host key is not trusted, with its fingerprint and the fix; `None` when `known_hosts` lists it.
pub(crate) fn host_key_refusal(check: &HostCheck, key: &PublicKeyOrCertificate) -> Option<String> {
    let PublicKeyOrCertificate::PublicKey { key, .. } = key else {
        return Some("the bastion presented a host certificate; SQLLoad checks plain host keys against known_hosts only".to_owned());
    };
    let shown = check.known_hosts.display();
    let fingerprint = key.fingerprint(HashAlg::Sha256);
    let listed = match check.port {
        22 => check.host.clone(),
        port => format!("[{}]:{port}", check.host),
    };
    match check_known_hosts_path(&check.host, check.port, key, &check.known_hosts) {
        Ok(true) => None,
        Ok(false) => Some(format!(
            "the bastion's host key {fingerprint} ({}) is not in {shown}; once its administrator confirms that fingerprint, add it with `ssh-keyscan -p {} {} >> {shown}` (entries for {listed})",
            key.algorithm(),
            check.port,
            check.host,
        )),
        Err(russh::keys::Error::KeyChanged { line }) => Some(format!(
            "the bastion's host key changed: it now presents {fingerprint} ({}), unlike line {line} of {shown}; if the change is expected, replace that line (`ssh-keygen -R '{listed}' -f {shown}`, then `ssh-keyscan`)",
            key.algorithm(),
        )),
        Err(error) => Some(format!("cannot read {shown}: {error}")),
    }
}

/// A signed-in session with the bastion of `tunnel`, sending keepalives.
pub(crate) async fn open_session(tunnel: &SshTunnel) -> Result<Handle<HostCheck>, String> {
    let config = Config {
        keepalive_interval: Some(KEEPALIVE_INTERVAL),
        keepalive_max: KEEPALIVE_MAX,
        nodelay: true,
        ..Config::default()
    };
    let refusal = Arc::new(Mutex::new(None));
    let check = HostCheck {
        host: tunnel.host.clone(),
        port: tunnel.port(),
        known_hosts: expanded(
            tunnel
                .known_hosts
                .as_deref()
                .unwrap_or(Path::new("~/.ssh/known_hosts")),
        ),
        refusal: refusal.clone(),
    };
    let connected = tokio::time::timeout(
        CONNECT_TIMEOUT,
        russh::client::connect(
            Arc::new(config),
            (tunnel.host.as_str(), tunnel.port()),
            check,
        ),
    )
    .await;
    let mut session = match connected {
        Err(_) => return Err(format!("no answer within {}s", CONNECT_TIMEOUT.as_secs())),
        Ok(Ok(session)) => session,
        Ok(Err(error)) => {
            let refused = refusal.lock().ok().and_then(|mut kept| kept.take());
            return Err(refused.unwrap_or_else(|| error.to_string()));
        }
    };
    tokio::time::timeout(CONNECT_TIMEOUT, sign_in(&mut session, tunnel))
        .await
        .map_err(|_| format!("signing in took longer than {}s", CONNECT_TIMEOUT.as_secs()))??;
    Ok(session)
}

/// Sign in as `tunnel.user` with its private key, or with ssh-agent's keys when it names none.
async fn sign_in(session: &mut Handle<HostCheck>, tunnel: &SshTunnel) -> Result<(), String> {
    let rsa_hash = session
        .best_supported_rsa_hash()
        .await
        .map_err(|error| error.to_string())?
        .flatten();
    let user = tunnel.user.as_str();
    let Some(file) = &tunnel.private_key_file else {
        return agent_sign_in(session, user, rsa_hash).await;
    };
    let file = expanded(file);
    let passphrase = tunnel.private_key_passphrase.as_ref().map(|secret| secret.expose());
    let key = load_secret_key(&file, passphrase)
        .map_err(|error| format!("cannot read private key {}: {error}", file.display()))?;
    let result = session
        .authenticate_publickey(user, PrivateKeyWithHashAlg::new(Arc::new(key), rsa_hash))
        .await
        .map_err(|error| error.to_string())?;
    match result.success() {
        true => Ok(()),
        false => Err(format!(
            "the bastion refused {user}'s key {}",
            file.display()
        )),
    }
}

/// Sign in as `user` with each key ssh-agent holds, until one is accepted.
async fn agent_sign_in(
    session: &mut Handle<HostCheck>,
    user: &str,
    rsa_hash: Option<HashAlg>,
) -> Result<(), String> {
    let mut agent = AgentClient::connect_env().await.map_err(|error| {
        format!("no private_key_file is set and ssh-agent is not reachable ({error}); set private_key_file in the ssh table")
    })?;
    let identities = agent
        .request_identities()
        .await
        .map_err(|error| format!("cannot list ssh-agent's keys: {error}"))?;
    for identity in &identities {
        let accepted = session
            .authenticate_publickey_with(
                user,
                identity.public_key().into_owned(),
                rsa_hash,
                &mut agent,
            )
            .await
            .map_err(|error| format!("ssh-agent could not sign: {error}"))?;
        if accepted.success() {
            return Ok(());
        }
    }
    Err(format!(
        "the bastion refused {user} with each of ssh-agent's {} keys; set private_key_file in the ssh table",
        identities.len()
    ))
}
