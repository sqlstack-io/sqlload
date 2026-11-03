#!/usr/bin/env bash
# Guard the sqlload-cli wheel tag. The package ships one standalone Rust
# executable that links no libpython, so an interpreter-specific tag makes the
# wheel uninstallable on every other Python and silently forces a source build.
set -euo pipefail
shopt -s nullglob

directory="${1:?usage: assert_cli_wheel_tags.sh <directory>}"
wheels=("${directory}"/sqlload_cli-*.whl)

if [ ${#wheels[@]} -eq 0 ]; then
  echo "No sqlload-cli wheel found in ${directory}."
  exit 1
fi

status=0
for wheel in "${wheels[@]}"; do
  name="$(basename "${wheel}")"
  case "${name}" in
    sqlload_cli-*-py3-none-*.whl)
      echo "ok: ${name}"
      ;;
    *)
      echo "error: sqlload-cli wheel must be tagged py3-none-<platform>: ${name}"
      status=1
      ;;
  esac
done

exit "${status}"
