#!/usr/bin/env bash
# Builds the unixODBC driver manager the Linux and macOS release wheels bundle, from its pinned source
# release, so the wheels link it dynamically and carry it as a separate, replaceable shared library.
# Usage: build_unixodbc.sh PREFIX SYSCONFDIR [LIBDIR] [DRIVER_PATH]
#   SYSCONFDIR is where the bundled driver manager reads odbcinst.ini and odbc.ini when ODBCSYSINI
#   is unset: /etc on Linux, as distribution driver packages register there; Homebrew's etc on macOS.
#   DRIVER_PATH is a colon-separated list of folders searched for a driver registered by file name
#   alone, as Debian and Ubuntu packages register theirs (Driver=psqlodbcw.so).
# Cross-compiles for UNIXODBC_HOST when it is set (a target triple such as x86_64-apple-darwin), with
# the compiler in CC.
# The source release and its SHA-256 are pinned in unixodbc_source.sh.
set -euo pipefail

prefix="$1"
sysconfdir="$2"
libdir="${3:-$prefix/lib}"
driver_path="${4:-}"

work="$(mktemp -d)"
tarball="$(bash "$(dirname "$0")/unixodbc_source.sh" "$work")"
tar -xzf "$work/$tarball" -C "$work"
cd "$work/${tarball%.tar.gz}"
# The included libltdl is built into the libraries, so they need no system libltdl; no readline, as
# only the libraries ship.
./configure --prefix="$prefix" --libdir="$libdir" --sysconfdir="$sysconfdir" \
	${UNIXODBC_HOST:+--host="$UNIXODBC_HOST"} \
	${driver_path:+--with-odbc-driver-path="$driver_path"} \
	--with-included-ltdl --enable-readline=no --enable-editline=no --enable-static=no > "$work/configure.log" ||
	{ tail -50 "$work/configure.log"; exit 1; }
make -j"$(getconf _NPROCESSORS_ONLN)" > "$work/make.log" 2>&1 || { tail -50 "$work/make.log"; exit 1; }
make install > /dev/null
cd /
rm -rf "$work"
echo "${tarball%.tar.gz} installed in ${libdir}, reading ${sysconfdir}, finding drivers by name in ${driver_path:-the library search path}"
