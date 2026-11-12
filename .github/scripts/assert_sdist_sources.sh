#!/usr/bin/env bash
# Asserts every sdist in DIR carries unixODBC's pinned source release, unchanged: the corresponding
# source of the driver manager the Linux and macOS wheels bundle (version and SHA-256 from
# unixodbc_source.sh).
# Usage: assert_sdist_sources.sh DIR
set -euo pipefail

pins="$(dirname "$0")/unixodbc_source.sh"
version="$(sed -n 's/^UNIXODBC_VERSION=//p' "$pins")"
sha256="$(sed -n 's/^UNIXODBC_SHA256=//p' "$pins")"
name="unixODBC-${version}.tar.gz"

shopt -s nullglob
sdists=("$1"/*.tar.gz)
if [ "${#sdists[@]}" -eq 0 ]; then
	echo "no sdists in $1" >&2
	exit 1
fi
for sdist in "${sdists[@]}"; do
	member="$(tar -tzf "$sdist" | grep -E "^[^/]+/${name//./\\.}\$" || true)"
	if [ -z "$member" ]; then
		echo "$sdist: no ${name} at its root" >&2
		exit 1
	fi
	found="$(tar -xzOf "$sdist" "$member" | sha256sum | cut -d' ' -f1)"
	if [ "$found" != "$sha256" ]; then
		echo "$sdist: ${name} has SHA-256 ${found}, expected ${sha256}" >&2
		exit 1
	fi
	echo "ok: $(basename "$sdist") carries ${name} (SHA-256 ${sha256})"
done
