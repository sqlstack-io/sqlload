/// An OpenSSH server image whose init takes the user, keys and extra settings from its environment and `/config`, pulled from GHCR (which lscr.io redirects to, with lower anonymous limits).
pub const IMAGE: &str = "ghcr.io/linuxserver/openssh-server";
/// The image's tag, pinned to its digest so every run pulls the same image.
pub const TAG: &str =
    "10.3_p1-r2-ls242@sha256:aa4e1b3916e8555f24e574e6108b0b89a1b20362b0a11800a71280b45999649e";

/// How long pulling and starting the bastion may take before the test fails, naming it.
pub const START_LIMIT: std::time::Duration = std::time::Duration::from_secs(240);

/// The port sshd listens on inside the container.
pub const SSH_PORT: u16 = 2222;

/// The bastion's user.
pub const SSH_USER: &str = "loader";

/// The password [`SSH_USER`] may also sign in with.
pub const SSH_PASSWORD: &str = "loader-test-password";

/// Lets the bastion open connections onward, which the image turns off, and turns off OpenSSH's per-source penalties, so tests that fail to sign in on purpose don't lock out the next ones; read before the main config, so it wins.
pub const SSHD_SETTINGS: &str = "AllowTcpForwarding yes\nPerSourcePenalties no\n";

/// Run in the started container once sshd answers (it logs to a file, not the container output): a client key for [`SSH_USER`] and one the bastion does not know, then the host's public keys, each block after an `@@` line.
pub const KEYS_SCRIPT: &str = "set -e; until nc -z localhost 2222; do sleep 0.2; done; ssh-keygen -q -t ed25519 -N '' -f /tmp/client; ssh-keygen -q -t ed25519 -N '' -f /tmp/other; cat /tmp/client.pub >> /config/.ssh/authorized_keys; cat /tmp/client; echo @@; cat /tmp/other; echo @@; cat /tmp/client.pub; echo @@; cat /config/ssh_host_keys/ssh_host_*_key.pub";
