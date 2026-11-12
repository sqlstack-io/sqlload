"""Smoke-test the driver manager an installed wheel's `sqlload` reaches ODBC drivers through.

Usage: python smoke_odbc_wheel.py SQLLOAD... [--default-ini PATH]

Each SQLLOAD is an installed `sqlload` command (the sqlload wheel's and the sqlload-cli wheel's).
On Linux and macOS it must run where no unixODBC is installed, so the wheel's bundled driver
manager is the only one; it must find a driver registered in an odbcinst.ini named by ODBCSYSINI
and, with --default-ini, one registered in the default odbcinst.ini (/etc on Linux, Homebrew's etc
on macOS). The drivers don't exist, so the driver manager reports the library it tried to load,
which shows the registration it read. On Windows, the driver manager built in (odbc32.dll) reports
an unregistered driver.
"""

import argparse
import glob
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

PROJECT = """\
[connections.erp]
kind = "odbc"
connection_string = "Driver={{{driver}}};"

[connections.local]
kind = "duckdb"
database = "out.duckdb"

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Smoke test."
include = ["*"]
"""

#: Where a unixODBC library would be if one were installed.
SYSTEM_LIBODBC = ["/usr/lib*/libodbc.so*", "/usr/lib/*/libodbc.so*", "/usr/local/lib/libodbc*", "/opt/homebrew/lib/libodbc*", "/opt/homebrew/opt/unixodbc"]


def plan(sqlload, folder, driver, environment):
    (folder / "sqlload.toml").write_text(PROJECT.format(driver=driver), encoding="utf-8")
    result = subprocess.run([sqlload, "--no-color", "plan"], cwd=folder, capture_output=True, text=True, encoding="utf-8", errors="replace", env=environment, check=False)
    return result.returncode, result.stdout + result.stderr


def check(condition, what, output=""):
    if not condition:
        sys.exit(f"{what}\n{output}")
    print(f"ok: {what}", flush=True)


def registered(ini, name, library):
    previous = ini.read_text(encoding="utf-8") if ini.exists() else ""
    ini.parent.mkdir(parents=True, exist_ok=True)
    ini.write_text(f"{previous}\n[{name}]\nDriver={library}\n", encoding="utf-8")
    return previous


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("sqlload", nargs="+")
    parser.add_argument("--default-ini", type=Path)
    arguments = parser.parse_args()
    if os.name != "nt":
        found = [path for pattern in SYSTEM_LIBODBC for path in glob.glob(pattern)]
        check(not found, "no unixODBC is installed, so only the wheel's driver manager can load", str(found))
    for sqlload in arguments.sqlload:
        sqlload = shutil.which(sqlload) or sqlload
        with tempfile.TemporaryDirectory() as name:
            folder = Path(name)
            if os.name == "nt":
                code, output = plan(sqlload, folder, "SQLLoad Smoke Missing", dict(os.environ))
                check(code == 1 and "Data source name not found" in output, f"{sqlload}: Windows's driver manager reports an unregistered driver", output)
                continue
            ini = folder / "ini" / "odbcinst.ini"
            registered(ini, "SQLLoad Smoke", "/nonexistent/libsmoke-odbcsysini.so")
            code, output = plan(sqlload, folder, "SQLLoad Smoke", {**os.environ, "ODBCSYSINI": str(ini.parent)})
            check(code == 1 and "Can't open lib '/nonexistent/libsmoke-odbcsysini.so'" in output, f"{sqlload}: the bundled driver manager finds a driver registered under ODBCSYSINI", output)
            if arguments.default_ini:
                previous = registered(arguments.default_ini, "SQLLoad Smoke Default", "/nonexistent/libsmoke-default.so")
                try:
                    environment = {name: value for name, value in os.environ.items() if name not in ("ODBCSYSINI", "ODBCINSTINI")}
                    code, output = plan(sqlload, folder, "SQLLoad Smoke Default", environment)
                finally:
                    arguments.default_ini.write_text(previous, encoding="utf-8")
                check(code == 1 and "Can't open lib '/nonexistent/libsmoke-default.so'" in output, f"{sqlload}: the bundled driver manager finds a driver registered in {arguments.default_ini}", output)


if __name__ == "__main__":
    main()
