#!/usr/bin/env bash
# Builds the unixODBC driver manager the Linux and macOS release wheels bundle, from its pinned source
# release, so the wheels link it dynamically and carry it as a separate, replaceable shared library.
# Usage: build_unixodbc.sh PREFIX SYSCONFDIR [LIBDIR]
#   SYSCONFDIR is where the bundled driver manager reads odbcinst.ini and odbc.ini when ODBCSYSINI
#   is unset: /etc on Linux, as distribution driver packages register there; Homebrew's etc on macOS.
# Keep UNIXODBC_VERSION in step with the NOTICE files under packages/*/licenses/.
set -euo pipefail

UNIXODBC_VERSION=2.3.14
UNIXODBC_SHA256=4e2814de3e01fc30b0b9f75e83bb5aba91ab0384ee951286504bb70205524771

prefix="$1"
sysconfdir="$2"
libdir="${3:-$prefix/lib}"

work="$(mktemp -d)"
tarball="$work/unixODBC-${UNIXODBC_VERSION}.tar.gz"
curl -fsSL -o "$tarball" "https://www.unixodbc.org/unixODBC-${UNIXODBC_VERSION}.tar.gz" ||
	curl -fsSL -o "$tarball" "https://github.com/lurcher/unixODBC/releases/download/v${UNIXODBC_VERSION}/unixODBC-${UNIXODBC_VERSION}.tar.gz"
if command -v sha256sum > /dev/null; then
	echo "$UNIXODBC_SHA256  $tarball" | sha256sum -c --quiet -
else
	echo "$UNIXODBC_SHA256  $tarball" | shasum -a 256 -c --quiet -
fi
tar -xzf "$tarball" -C "$work"
cd "$work/unixODBC-${UNIXODBC_VERSION}"
# The included libltdl is built into the libraries, so they need no system libltdl; no readline, as
# only the libraries ship.
./configure --prefix="$prefix" --libdir="$libdir" --sysconfdir="$sysconfdir" \
	--with-included-ltdl --enable-readline=no --enable-editline=no --enable-static=no > "$work/configure.log" ||
	{ tail -50 "$work/configure.log"; exit 1; }
make -j"$(getconf _NPROCESSORS_ONLN)" > "$work/make.log" 2>&1 || { tail -50 "$work/make.log"; exit 1; }
make install > /dev/null
cd /
rm -rf "$work"
echo "unixODBC ${UNIXODBC_VERSION} installed in ${libdir}, reading ${sysconfdir}"
