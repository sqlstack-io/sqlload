"""Smoke-test sqlload as installed from PyPI: each `sqlload` reports VERSION and loads a CSV into DuckDB.

Usage: python smoke_published_wheel.py VERSION SQLLOAD...

Each SQLLOAD is an installed `sqlload` command (the sqlload wheel's and the sqlload-cli wheel's). Each
must print VERSION for `--version`, then `load --json` a two-row CSV into a new DuckDB database: the
report must say it succeeded and loaded both rows, and a second load must add none.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT = """\
[connections.lake]
kind = "filesystem"
root = "lake"

[connections.local]
kind = "duckdb"
database = "out.duckdb"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.orders]
kind = "files"
connection = "lake"
path = "drop/"
format = "csv"
description = "Smoke test."
write_strategy = "append"
"""


def run(command, folder=None):
    return subprocess.run(command, cwd=folder, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)


def check(condition, what, output=""):
    if not condition:
        sys.exit(f"{what}\n{output}")
    print(f"ok: {what}", flush=True)


def loaded(sqlload, folder):
    """The rows a `load --json` in `folder` loaded, and its output."""
    done = run([sqlload, "--no-color", "load", "--json"], folder)
    output = done.stdout + done.stderr
    try:
        report = json.loads(done.stdout)
    except json.JSONDecodeError:
        sys.exit(f"{sqlload} load --json printed no JSON report (exit {done.returncode})\n{output}")
    check(done.returncode == 0 and report["ok"], f"{sqlload} load --json succeeds", output)
    return report["summary"]["rows"], output


def main():
    version, commands = sys.argv[1], sys.argv[2:]
    for sqlload in commands:
        # Absolute, as each load runs in its own folder and POSIX resolves a relative path from there.
        sqlload = os.path.abspath(shutil.which(sqlload) or sqlload)
        shown = run([sqlload, "--version"])
        check(shown.returncode == 0 and shown.stdout.split()[-1:] == [version], f"{sqlload} --version prints {version}", shown.stdout + shown.stderr)
        with tempfile.TemporaryDirectory() as name:
            folder = Path(name)
            (folder / "lake" / "drop").mkdir(parents=True)
            (folder / "lake" / "drop" / "orders.csv").write_text("id,v\n1,a\n2,b\n", encoding="utf-8")
            (folder / "sqlload.toml").write_text(PROJECT, encoding="utf-8")
            rows, output = loaded(sqlload, folder)
            check(rows == 2, "the first load loads the CSV's 2 rows", output)
            rows, output = loaded(sqlload, folder)
            check(rows == 0, "a second load finds the file loaded and adds no rows", output)


if __name__ == "__main__":
    main()
