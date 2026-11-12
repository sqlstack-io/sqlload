#!/usr/bin/env bash
# Downloads unixODBC's pinned source release, checks its SHA-256 and copies it into each FOLDER.
# The Linux and macOS wheels bundle the driver manager built from it (build_unixodbc.sh), and both
# sdists carry it as the library's corresponding source (LGPL-2.1 section 4).
# Usage: unixodbc_source.sh FOLDER...      Prints the tarball's name.
# Keep the version in step with the NOTICE files under packages/*/licenses/.
set -euo pipefail

UNIXODBC_VERSION=2.3.14
UNIXODBC_SHA256=4e2814de3e01fc30b0b9f75e83bb5aba91ab0384ee951286504bb70205524771
name="unixODBC-${UNIXODBC_VERSION}.tar.gz"

work="$(mktemp -d)"
curl -fsSL -o "$work/$name" "https://www.unixodbc.org/$name" ||
	curl -fsSL -o "$work/$name" "https://github.com/lurcher/unixODBC/releases/download/v${UNIXODBC_VERSION}/$name"
if command -v sha256sum > /dev/null; then
	echo "$UNIXODBC_SHA256  $work/$name" | sha256sum -c --quiet - >&2
else
	echo "$UNIXODBC_SHA256  $work/$name" | shasum -a 256 -c --quiet - >&2
fi
for folder in "$@"; do
	cp "$work/$name" "$folder/$name"
done
rm -rf "$work"
echo "$name"
