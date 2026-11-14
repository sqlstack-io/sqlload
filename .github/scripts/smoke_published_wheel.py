"""Smoke-test sqlload as installed from PyPI: each `sqlload` reports VERSION and loads a CSV into DuckDB.

Usage: python smoke_published_wheel.py VERSION SQLLOAD... [--lone-shim SQLL]

Each SQLLOAD is an installed `sqlload` command (the sqlload wheel's and the sqlload-cli wheel's). Each
must print VERSION for `--version`, then `load --json` a two-row CSV into a new DuckDB database: the
report must say it succeeded and loaded both rows, and a second load must add none. The `sqll` installed
beside each must be the same CLI: the same output and exit code for `--version`, a usage error and the
load. With --lone-shim, a copy of the sqlload-cli wheel's `sqll` launcher alone in a folder must say
`sqlload` isn't installed next to it.
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


def installed(command):
    """`command` as an absolute path, as each load runs in its own folder and POSIX resolves a relative path from there."""
    return os.path.abspath(shutil.which(command) or command)


def same_cli(sqlload, sqll, args, folder=None):
    """Fails unless `sqll` gives `sqlload`'s exit code and output for `args`."""
    expected, shown = run([sqlload, *args], folder), run([sqll, *args], folder)
    check(
        (shown.returncode, shown.stdout, shown.stderr) == (expected.returncode, expected.stdout, expected.stderr),
        f"{sqll} {' '.join(args)} matches sqlload (exit {expected.returncode})",
        f"sqlload: {expected.returncode} {expected.stdout}{expected.stderr}\nsqll: {shown.returncode} {shown.stdout}{shown.stderr}",
    )


def main():
    args = sys.argv[1:]
    lone = None
    if "--lone-shim" in args:
        at = args.index("--lone-shim")
        lone, args = installed(args[at + 1]), args[:at] + args[at + 2 :]
    version, commands = args[0], args[1:]
    for sqlload in map(installed, commands):
        shown = run([sqlload, "--version"])
        check(shown.returncode == 0 and shown.stdout.split()[-1:] == [version], f"{sqlload} --version prints {version}", shown.stdout + shown.stderr)
        sqll = installed(str(Path(sqlload).with_name(Path(sqlload).name.replace("sqlload", "sqll"))))
        same_cli(sqlload, sqll, ["--version"])
        same_cli(sqlload, sqll, ["--no-such-flag"])
        with tempfile.TemporaryDirectory() as name:
            folder = Path(name)
            (folder / "lake" / "drop").mkdir(parents=True)
            (folder / "lake" / "drop" / "orders.csv").write_text("id,v\n1,a\n2,b\n", encoding="utf-8")
            (folder / "sqlload.toml").write_text(PROJECT, encoding="utf-8")
            rows, output = loaded(sqlload, folder)
            check(rows == 2, "the first load loads the CSV's 2 rows", output)
            rows, output = loaded(sqll, folder)
            check(rows == 0, "a second load, through sqll, finds the file loaded and adds no rows", output)
    if lone is not None:
        with tempfile.TemporaryDirectory() as name:
            copy = Path(name) / Path(lone).name
            shutil.copy2(lone, copy)
            shown = run([str(copy), "--version"])
            check(
                shown.returncode == 1 and "sqll is a short name for sqlload, which isn't installed next to it" in shown.stderr,
                "sqll alone in a folder says sqlload isn't installed next to it",
                shown.stdout + shown.stderr,
            )


if __name__ == "__main__":
    main()
