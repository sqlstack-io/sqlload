/// An OpenSSH server image whose init takes the user, keys and extra settings from its environment and `/config`.
pub const IMAGE: &str = "lscr.io/linuxserver/openssh-server";
pub const TAG: &str = "10.3_p1-r2-ls242";

/// The port sshd listens on inside the container.
pub const SSH_PORT: u16 = 2222;

/// The bastion's user.
pub const SSH_USER: &str = "loader";

/// Lets the bastion open connections onward, which the image turns off; read before the main config, so it wins.
pub const FORWARDING: &str = "AllowTcpForwarding yes\n";

/// Run in the started container once sshd answers (it logs to a file, not the container output): a client key for [`SSH_USER`] and one the bastion does not know, then the host's public keys, each block after an `@@` line.
pub const KEYS_SCRIPT: &str = "set -e; until nc -z localhost 2222; do sleep 0.2; done; ssh-keygen -q -t ed25519 -N '' -f /tmp/client; ssh-keygen -q -t ed25519 -N '' -f /tmp/other; cat /tmp/client.pub >> /config/.ssh/authorized_keys; cat /tmp/client; echo @@; cat /tmp/other; echo @@; cat /tmp/client.pub; echo @@; cat /config/ssh_host_keys/ssh_host_*_key.pub";
