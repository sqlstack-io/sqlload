#!/usr/bin/env bash
# Asserts every wheel in DIR carries unixODBC's notice and licence text, and that each Linux and
# macOS wheel bundles the unixODBC driver manager as one separate shared library (Windows wheels
# use odbc32.dll and bundle none).
# Usage: assert_wheel_notices.sh DIR
set -euo pipefail

python="$(command -v python3 || command -v python)"
shopt -s nullglob
wheels=("$1"/*.whl)
if [ "${#wheels[@]}" -eq 0 ]; then
	echo "no wheels in $1" >&2
	exit 1
fi
for wheel in "${wheels[@]}"; do
	names="$("$python" -c 'import sys, zipfile; print("\n".join(zipfile.ZipFile(sys.argv[1]).namelist()))' "$wheel")"
	for notice in unixODBC-NOTICE.md unixODBC-COPYING.txt; do
		if ! grep -Eq "\.dist-info/licenses/licenses/${notice}\$" <<< "$names"; then
			echo "$wheel: no ${notice} in its .dist-info/licenses" >&2
			exit 1
		fi
	done
	bundled="$(grep -Ec '\.(libs|dylibs)/libodbc[^/]*\.(so[.0-9]*|dylib)$' <<< "$names" || true)"
	case "$wheel" in
		*win_amd64.whl | *win32.whl | *win_arm64.whl) expected=0 ;;
		*) expected=1 ;;
	esac
	if [ "$bundled" -ne "$expected" ]; then
		echo "$wheel: bundles ${bundled} unixODBC libraries, expected ${expected}" >&2
		exit 1
	fi
	echo "ok: $(basename "$wheel") (bundled libodbc: ${bundled})"
done
