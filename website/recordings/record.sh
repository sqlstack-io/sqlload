#!/usr/bin/env bash
# Record one demo to out/<demo>.gif, out/<demo>.mp4 and out/<demo>.png. Usage: record.sh <demo>
# Uses the sqlload on PATH; set SQLLOAD_BIN to a directory containing another sqlload to use that instead.
set -euo pipefail
demo="$1"
here="$(cd "$(dirname "$0")" && pwd)"
if [[ -n "${SQLLOAD_BIN:-}" ]]; then
  export PATH="$SQLLOAD_BIN:$PATH"
fi
"$here/prepare.sh" "$demo"
mkdir -p "$here/out"
cd "$here"
# Headless Chromium's sandbox is unavailable in many containers and CI runners.
VHS_NO_SANDBOX="${VHS_NO_SANDBOX:-true}" vhs "tapes/$demo.tape"
