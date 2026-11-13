#!/usr/bin/env bash
# Load a MySQL 8.4 table with each Linux x86_64 wheel through MySQL Connector/ODBC from MySQL's
# Debian 13 package, in python:3.12-slim (Debian 13), through the wheel's bundled unixODBC.
#
# The driver is built for static thread-local storage into libstdc++ (its dynamic section sets
# STATIC_TLS), which glibc can't give it once a process that loaded libstdc++ after it started has
# used libstdc++'s thread-locals, as the sqlload wheel's Python process has by the time it connects.
# So the sqlload-cli wheel must load as is, the sqlload wheel with LD_PRELOAD=libstdc++.so.6, and the
# sqlload wheel without it must fail with SQLLoad's error naming STATIC_TLS and the preload, as the
# ODBC source's docs say.
#
# Usage: smoke_mysql_odbc_wheel.sh DIST   (DIST holds the sqlload and sqlload-cli wheels)
set -euo pipefail

MYSQL_ODBC_VERSION=9.7.0
MYSQL_ODBC_SHA256=7c697dfba67f04271858380c847d3694190646c36edeb2c5fe4175878f7e9ec2
# mysql:8.4 and python:3.12-slim (Debian 13) as of the pin.
MYSQL_IMAGE=mysql@sha256:a81d043930e51745522bd5c430961cf8286f0a23f0a9a2a6e0108f7ea71b0902
PYTHON_IMAGE=python@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b
NETWORK=sqlload-mysql-smoke
SERVER=sqlload-mysql-smoke

if [[ "${1:-}" != --inside ]]; then
	dist="$(cd "$1" && pwd)"
	scripts="$(cd "$(dirname "$0")" && pwd)"
	for image in "$MYSQL_IMAGE" "$PYTHON_IMAGE"; do
		for attempt in 1 2 3; do
			docker pull --quiet "$image" > /dev/null && break
			[[ $attempt == 3 ]] && exit 1
			sleep 30
		done
	done
	docker network create "$NETWORK" > /dev/null
	trap 'docker rm -f "$SERVER" > /dev/null 2>&1; docker network rm "$NETWORK" > /dev/null 2>&1' EXIT
	docker run -d --name "$SERVER" --network "$NETWORK" -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=shop "$MYSQL_IMAGE" > /dev/null
	# Over TCP, which the image's first, initializing server doesn't listen on.
	for _ in $(seq 90); do
		if docker exec "$SERVER" mysql -h127.0.0.1 -uroot -e 'SELECT 1' > /dev/null 2>&1; then break; fi
		sleep 2
	done
	docker exec "$SERVER" mysql -h127.0.0.1 -uroot shop -e "CREATE TABLE items (id int PRIMARY KEY, name varchar(20)); INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c');"
	docker run --rm --network "$NETWORK" -v "$dist:/dist:ro" -v "$scripts:/scripts:ro" "$PYTHON_IMAGE" \
		bash /scripts/smoke_mysql_odbc_wheel.sh --inside
	exit
fi

export DEBIAN_FRONTEND=noninteractive PIP_DISABLE_PIP_VERSION_CHECK=1
apt-get -o Acquire::Retries=3 update -qq > /dev/null
apt-get -o Acquire::Retries=3 install -y -qq curl ca-certificates > /dev/null
package="mysql-connector-odbc_${MYSQL_ODBC_VERSION}-1debian13_amd64.deb"
curl -fsSL --retry 3 --retry-all-errors -o "/tmp/$package" "https://cdn.mysql.com/Downloads/Connector-ODBC/${MYSQL_ODBC_VERSION%.*}/$package" ||
	curl -fsSL --retry 3 --retry-all-errors -o "/tmp/$package" "https://downloads.mysql.com/archives/get/p/10/file/$package"
echo "$MYSQL_ODBC_SHA256  /tmp/$package" | sha256sum -c --quiet -
apt-get -o Acquire::Retries=3 install -y -qq "/tmp/$package" > /dev/null
# The package registers the driver under /usr/lib//odbc/, where it isn't; register it where it is.
sed -i 's#/usr/lib//odbc/#/usr/lib/x86_64-linux-gnu/odbc/#' /etc/odbcinst.ini
python -m venv /v && /v/bin/pip install --quiet /dist/sqlload-*.whl
python -m venv /c && /c/bin/pip install --quiet /dist/sqlload_cli-*.whl

# Load into a fresh project with SQLLOAD (and the environment before it); print what it said.
load() {
	local project
	project="$(mktemp -d)"
	cat > "$project/sqlload.toml" <<-EOF
		[connections.shop]
		kind = "odbc"
		connection_string = "Driver={MySQL ODBC ${MYSQL_ODBC_VERSION%.*} Unicode Driver};Server=$SERVER;Port=3306;Database=shop;Uid=root;Pwd=;INITSTMT=SET SESSION sql_mode=CONCAT(@@SESSION.sql_mode,',ANSI_QUOTES');"

		[connections.local]
		kind = "duckdb"
		database = "out.duckdb"

		[targets.dev]
		connection = "local"
		loader_schema = "raw"

		[sources.shop]
		kind = "odbc"
		connection = "shop"
		description = "Smoke test."
		include = ["items"]

		[sources.shop.tables.items]
		description = "Items."
	EOF
	(cd "$project" && env "$@" load --json 2>&1)
}
loaded() { grep -q '"loaded": *1' <<< "$1" && grep -q '"rows": *3' <<< "$1"; }

out="$(load /c/bin/sqlload)" || true
loaded "$out" || { echo "$out"; echo "FAIL: the sqlload-cli wheel didn't load the table through the driver" >&2; exit 1; }
echo "ok: the sqlload-cli wheel loads through MySQL Connector/ODBC ${MYSQL_ODBC_VERSION}"
out="$(load LD_PRELOAD=libstdc++.so.6 /v/bin/sqlload)" || true
loaded "$out" || { echo "$out"; echo "FAIL: the sqlload wheel, libstdc++ preloaded, didn't load the table" >&2; exit 1; }
echo "ok: the sqlload wheel loads through it with libstdc++ preloaded"
out="$(load /v/bin/sqlload)" || true
driver=/usr/lib/x86_64-linux-gnu/odbc/libmyodbc${MYSQL_ODBC_VERSION%%.*}w.so
expected="The driver library $driver exists, and is built for static thread-local storage (STATIC_TLS), which the dynamic loader refuses once it reaches thread-locals already in use, as libstdc++'s are in a Python process that loaded libstdc++ after it started. Start the process with LD_PRELOAD=libstdc++.so.6, or use the sqlload-cli package's sqlload."
if loaded "$out" || ! grep -qF "$expected" <<< "$out"; then
	echo "$out"
	echo "FAIL: without the preload, the sqlload wheel's load didn't fail with: $expected" >&2
	exit 1
fi
echo "ok: without the preload, the sqlload wheel fails naming STATIC_TLS and the preload"
