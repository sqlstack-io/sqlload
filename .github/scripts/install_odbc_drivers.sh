#!/usr/bin/env bash
# Installs unixODBC and the ODBC drivers the container tests read through, on Debian or Ubuntu x86_64,
# and registers each driver under the name the tests connect with:
#   PostgreSQL Unicode            psqlODBC, from the distribution
#   ODBC Driver 18 for SQL Server Microsoft's msodbcsql18, the same release as on Windows (installing it accepts Microsoft's licence)
#   MySQL ODBC Unicode Driver     MySQL Connector/ODBC, MySQL's package for this distribution release
# Usage: install_odbc_drivers.sh [--with-dev]   (--with-dev adds the unixODBC headers to build against)
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

MYSQL_ODBC_VERSION=26.10.0
MSODBCSQL_VERSION=18.7.1.1-1
MYSQL_ODBC_NAME="MySQL ODBC Unicode Driver"

sudo_() { if [ "$(id -u)" = 0 ]; then "$@"; else sudo "$@"; fi; }

packages=(unixodbc odbc-postgresql curl ca-certificates)
if [ "${1:-}" = "--with-dev" ]; then
	packages+=(unixodbc-dev)
fi
sudo_ apt-get update -qq
sudo_ apt-get install -y -qq "${packages[@]}"

# Microsoft's repository for this distribution release, then msodbcsql18.
. /etc/os-release
if ! dpkg -s msodbcsql18 > /dev/null 2>&1; then
	curl -fsSL -o /tmp/packages-microsoft-prod.deb "https://packages.microsoft.com/config/${ID}/${VERSION_ID}/packages-microsoft-prod.deb"
	sudo_ dpkg -i /tmp/packages-microsoft-prod.deb > /dev/null
	rm -f /tmp/packages-microsoft-prod.deb
	sudo_ apt-get update -qq
	sudo_ env ACCEPT_EULA=Y apt-get install -y -qq "msodbcsql18=${MSODBCSQL_VERSION}"
fi

# MySQL Connector/ODBC: MySQL's package built for this distribution release, which links the system
# OpenSSL. Its generic tarball carries a newer private libssl.so.3, which can't load in a process
# where another driver already loaded the system one (OPENSSL_3.2.0 not found on Ubuntu 24.04).
# The SHA-256s are of the packages as cdn.mysql.com served them over TLS. The Unicode driver is
# registered under a name that doesn't change with the version.
case "${ID}${VERSION_ID}" in
	ubuntu24.04) mysql_sha256=b99d26f320ca640d24d8a82bdaea33e41f1ce9bb16cbea5b44ee3b7547bc5673 ;;
	ubuntu22.04) mysql_sha256=2e89b5ca8149ba77c24e0828597c85d160ff66fe57aa81873c742a152a747c65 ;;
	debian13) mysql_sha256=20b506e3b44271fb3b0276333ae6595b9cecf6c2bdf888a8d21ff421c54bbdc9 ;;
	debian12) mysql_sha256=119d0a5413a97e155a886df03cc01921a3294ff0cb02569c1d8e11642f4c0b7d ;;
	*) echo "no pinned MySQL Connector/ODBC package for ${ID} ${VERSION_ID}" >&2; exit 1 ;;
esac
if ! dpkg -s mysql-connector-odbc > /dev/null 2>&1; then
	package="mysql-connector-odbc_${MYSQL_ODBC_VERSION}-1${ID}${VERSION_ID}_amd64.deb"
	work="$(mktemp -d)"
	curl -fsSL -o "$work/$package" "https://cdn.mysql.com/Downloads/Connector-ODBC/${MYSQL_ODBC_VERSION%.*}/$package" ||
		curl -fsSL -o "$work/$package" "https://downloads.mysql.com/archives/get/p/10/file/$package"
	echo "$mysql_sha256  $work/$package" | sha256sum -c --quiet -
	sudo_ apt-get install -y -qq "$work/$package"
	rm -rf "$work"
fi
driver="/usr/lib/x86_64-linux-gnu/odbc/libmyodbc${MYSQL_ODBC_VERSION%%.*}w.so"
test -f "$driver"
printf '[%s]\nDescription=MySQL Connector/ODBC %s\nDriver=%s\nUsageCount=1\n' "$MYSQL_ODBC_NAME" "$MYSQL_ODBC_VERSION" "$driver" > /tmp/mysql-odbc.ini
sudo_ odbcinst -i -d -r -f /tmp/mysql-odbc.ini > /dev/null
rm -f /tmp/mysql-odbc.ini

odbcinst -q -d
