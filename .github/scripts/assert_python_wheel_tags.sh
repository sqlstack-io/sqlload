#!/usr/bin/env bash
# Guard the sqlload wheel tag. The package is a PyO3 extension built against the
# stable ABI (abi3, CPython 3.10+); a version-specific tag would leave every other
# Python without a wheel and silently force a source build.
set -euo pipefail
shopt -s nullglob

directory="${1:?usage: assert_python_wheel_tags.sh <directory>}"
wheels=("${directory}"/sqlload-*.whl)

if [ ${#wheels[@]} -eq 0 ]; then
  echo "No sqlload wheel found in ${directory}."
  exit 1
fi

status=0
for wheel in "${wheels[@]}"; do
  name="$(basename "${wheel}")"
  case "${name}" in
    sqlload-*-cp310-abi3-*.whl)
      echo "ok: ${name}"
      ;;
    *)
      echo "error: sqlload wheel must be tagged cp310-abi3-<platform>: ${name}"
      status=1
      ;;
  esac
done

exit "${status}"
