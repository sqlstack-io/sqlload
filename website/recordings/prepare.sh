#!/usr/bin/env bash
# Build a clean scratch project for one demo. Usage: prepare.sh <demo>
set -euo pipefail
demo="$1"
here="$(cd "$(dirname "$0")" && pwd)"
dir="$here/scratch/$demo"
rm -rf "$dir"
mkdir -p "$dir"
cd "$dir"
case "$demo" in
  plan-load)
    # The playground project, and a copy of its settings with cursor_column misspelled, which the
    # tape swaps in after the first load.
    sqlload playground shop >/dev/null
    awk '{print} /^unique_key = \["order_id"\]$/ {print "cursor_column = \"updated\""}' shop/sqlload.toml > typo.toml
    grep -q '^cursor_column = "updated"' typo.toml || { echo "prepare.sh: no unique_key line to add the typo after" >&2; exit 1; } ;;
  *)
    echo "unknown demo: $demo" >&2
    exit 2 ;;
esac
