#!/usr/bin/env bash
# Installs unixODBC and the ODBC drivers the container tests read through, on Debian or Ubuntu x86_64,
# and registers each driver under the name the tests connect with:
#   PostgreSQL Unicode            psqlODBC, from the distribution
#   ODBC Driver 18 for SQL Server Microsoft's msodbcsql18 (installing it accepts Microsoft's licence)
#   MySQL ODBC Unicode Driver     MySQL Connector/ODBC, from MySQL's generic Linux tarball
# Usage: install_odbc_drivers.sh [--with-dev]   (--with-dev adds the unixODBC headers to build against)
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

MYSQL_ODBC_VERSION=26.10.0
MYSQL_ODBC_MD5=09f9e895d60ce69bd1e6a576df9dd4de
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
	sudo_ env ACCEPT_EULA=Y apt-get install -y -qq msodbcsql18
fi

# MySQL Connector/ODBC: the pinned tarball (the CDN keeps the current release, the archive older
# ones), checked against MySQL's published MD5, then its Unicode driver registered under a name
# that doesn't change with the version.
tarball="mysql-connector-odbc-${MYSQL_ODBC_VERSION}-linux-glibc2.28-x86-64bit.tar.gz"
work="$(mktemp -d)"
curl -fsSL -o "$work/$tarball" "https://cdn.mysql.com/Downloads/Connector-ODBC/${MYSQL_ODBC_VERSION%.*}/$tarball" ||
	curl -fsSL -o "$work/$tarball" "https://downloads.mysql.com/archives/get/p/10/file/$tarball"
echo "$MYSQL_ODBC_MD5  $work/$tarball" | md5sum -c --quiet -
sudo_ mkdir -p /opt/mysql-connector-odbc
sudo_ tar -xzf "$work/$tarball" -C /opt/mysql-connector-odbc --strip-components=1
rm -rf "$work"
driver="$(ls /opt/mysql-connector-odbc/lib/libmyodbc*w.so)"
printf '[%s]\nDescription=MySQL Connector/ODBC %s\nDriver=%s\nUsageCount=1\n' "$MYSQL_ODBC_NAME" "$MYSQL_ODBC_VERSION" "$driver" > /tmp/mysql-odbc.ini
sudo_ odbcinst -i -d -r -f /tmp/mysql-odbc.ini > /dev/null
rm -f /tmp/mysql-odbc.ini

odbcinst -q -d
