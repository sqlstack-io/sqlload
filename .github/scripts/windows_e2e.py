"""End-to-end checks of a Windows sqlload executable: drive paths, a UNC share and an ODBC driver.

Usage: python windows_e2e.py SQLLOAD_EXE WORK_DIR SHARE [--odbc-driver NAME] [--cdc]

WORK_DIR is a local folder (C:\\...) whose `share` folder is shared as SHARE (\\\\localhost\\name),
so what the share holds can also be read locally. With --odbc-driver, a table in the local Postgres (the runner's
preinstalled server, user postgres) is loaded through that ODBC driver, then loaded again
incrementally. With --cdc, a postgres_cdc source snapshots a table and then streams its changes, which
on Windows go through the loopback TLS relay (that server needs wal_level = logical). Needs the
duckdb Python package to read the results back.
"""

import argparse
import os
import pathlib
import subprocess
import sys

import duckdb

ROWS = [(1, "first", "2026-01-01 10:00:00"), (2, "second", "2026-01-02 11:30:00"), (3, "third", "2026-01-03 12:45:30")]


def sqlload(exe, *args, config=None):
    command = [exe, "--no-color"]
    if config is not None:
        command += ["--config", str(config)]
    command += list(args)
    print("$", subprocess.list2cmdline(command), flush=True)
    result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace")
    print(result.stdout + result.stderr, flush=True)
    if result.returncode != 0:
        sys.exit(f"sqlload exited {result.returncode}")
    return result.stdout + result.stderr


def query(database, sql):
    with duckdb.connect(str(database), read_only=True) as connection:
        return connection.execute(sql).fetchall()


def expect(found, expected, what):
    if found != expected:
        sys.exit(f"{what}: found {found!r}, expected {expected!r}")
    print(f"ok: {what}", flush=True)


def files_checks(exe, work, share):
    source = work / "in" / "orders"
    source.mkdir(parents=True, exist_ok=True)
    lines = ["id,name,updated_at"] + [f"{i},{name},{at}" for i, name, at in ROWS]
    (source / "orders.csv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    database = work / "shop.duckdb"

    # A drive path with backslashes to a DuckDB file on a drive path.
    sqlload(exe, "copy", str(source), f"duckdb://{database}#raw.orders", "--format", "csv")
    expect(query(database, "SELECT count(*), max(name) FROM raw.orders"), [(3, "third")], "drive folder to DuckDB")

    # The same folder into Parquet files on the UNC share.
    lake = f"{share}\\lake\\orders"
    sqlload(exe, "copy", str(source), lake, "--format", "csv")
    written = sorted(path.name for path in (work / "share" / "lake" / "orders").rglob("*.parquet"))
    if not written:
        sys.exit(f"no Parquet files under {lake}")
    print(f"ok: UNC share holds {written}", flush=True)

    # Read back through a file:// URL naming the share, and through a \\?\UNC\ path.
    server_share = share.lstrip("\\").replace("\\", "/")
    sqlload(exe, "copy", f"file://{server_share}/lake/orders", f"duckdb://{database}#raw.from_share", "--format", "parquet")
    expect(query(database, "SELECT count(*), sum(id) FROM raw.from_share"), [(3, 6)], "file:// share URL to DuckDB")
    verbatim = "\\\\?\\UNC\\" + share.lstrip("\\") + "\\lake\\orders"
    sqlload(exe, "copy", verbatim, f"duckdb://{database}#raw.from_verbatim", "--format", "parquet")
    expect(query(database, "SELECT count(*) FROM raw.from_verbatim"), [(3,)], "\\\\?\\UNC\\ path to DuckDB")


def psql(sql):
    psql_exe = pathlib.Path(os.environ["PGBIN"]) / "psql.exe"
    subprocess.run(
        [str(psql_exe), "-v", "ON_ERROR_STOP=1", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-c", sql],
        check=True,
        env={**os.environ, "PGPASSWORD": "root"},
    )


def odbc_checks(exe, work, driver):
    psql(
        "DROP SCHEMA IF EXISTS erp CASCADE; CREATE SCHEMA erp;"
        " CREATE TABLE erp.items (id bigint PRIMARY KEY, name text, price numeric(10,2), updated_at timestamp NOT NULL);"
        " INSERT INTO erp.items SELECT g, 'item ' || g, g * 1.25, TIMESTAMP '2026-01-01' + g * INTERVAL '1 minute' FROM generate_series(1, 500) g;"
    )
    database = work / "odbc.duckdb"
    project = work / "odbc" / "sqlload.toml"
    project.parent.mkdir(parents=True, exist_ok=True)
    # TOML literal strings, so the Windows path keeps its backslashes.
    project.write_text(
        f"""
[connections.erp]
kind = "odbc"
connection_string = "Driver={{{driver}}};Server=127.0.0.1;Port=5432;Database=postgres;Uid=postgres;Pwd=root;"

[connections.local]
kind = "duckdb"
database = '{database}'

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.erp]
kind = "odbc"
connection = "erp"
description = "Items of a generic ERP."
include = ["erp.items"]

[sources.erp.tables."erp.items"]
description = "Items, changed rows found by updated_at."
cursor_column = "updated_at"
unique_key = ["id"]
write_strategy = "merge"
""",
        encoding="utf-8",
    )
    sqlload(exe, "load", config=project)
    expect(query(database, "SELECT count(*), sum(price) FROM raw.erp__items"), [(500, 156562.50)], "ODBC first load")
    psql(
        "UPDATE erp.items SET name = 'changed', updated_at = TIMESTAMP '2027-01-01 00:00:00.25' WHERE id = 7;"
        " INSERT INTO erp.items VALUES (501, 'new', 1, TIMESTAMP '2027-01-01 00:00:00.5');"
    )
    sqlload(exe, "load", config=project)
    expect(
        query(database, "SELECT count(*), (SELECT name FROM raw.erp__items WHERE id = 7), max(updated_at)::VARCHAR FROM raw.erp__items"),
        [(501, "changed", "2027-01-01 00:00:00.5")],
        "ODBC incremental merge on a fractional-second cursor",
    )


def cdc_checks(exe, work):
    """A postgres_cdc load streams through the loopback relay: the server offers no TLS, so under the default sslmode (prefer) the replication session is relayed in plain text."""
    psql(
        "DROP SCHEMA IF EXISTS shop CASCADE; CREATE SCHEMA shop;"
        " CREATE TABLE shop.orders (id bigint PRIMARY KEY, status text, total numeric(10,2));"
        " INSERT INTO shop.orders SELECT g, 'new', g FROM generate_series(1, 100) g;"
    )
    database = work / "cdc.duckdb"
    project = work / "cdc" / "sqlload.toml"
    project.parent.mkdir(parents=True, exist_ok=True)
    project.write_text(
        f"""
[connections.pg]
kind = "postgres"
host = "127.0.0.1"
port = 5432
user = "postgres"
password = "${{ENV:PGPASSWORD}}"
database = "postgres"

[connections.local]
kind = "duckdb"
database = '{database}'

[targets.dev]
connection = "local"
loader_schema = "raw"

[sources.shop]
kind = "postgres_cdc"
connection = "pg"
include = ["shop.*"]
slot = "sqlload_windows_e2e"
publication = "sqlload_windows_e2e"

[sources.shop.tables."shop.orders"]
description = "Orders, captured."
""",
        encoding="utf-8",
    )
    os.environ["PGPASSWORD"] = "root"
    sqlload(exe, "load", config=project)
    expect(query(database, "SELECT count(*) FROM raw.shop__orders"), [(100,)], "CDC snapshot")
    psql("UPDATE shop.orders SET status = 'paid' WHERE id <= 10; DELETE FROM shop.orders WHERE id = 100; INSERT INTO shop.orders VALUES (101, 'new', 5);")
    sqlload(exe, "load", config=project)
    expect(
        query(
            database,
            "SELECT count(*) FILTER (WHERE status = 'paid'), count(*) FILTER (WHERE id = 101) FROM raw.shop__orders",
        ),
        [(10, 1)],
        "CDC changes streamed through the loopback relay",
    )


def main():
    # sqlload prints UTF-8 (box drawing, check marks); a Windows console or pipe defaults to cp1252.
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser()
    parser.add_argument("exe")
    parser.add_argument("work", type=pathlib.Path)
    parser.add_argument("share")
    parser.add_argument("--odbc-driver")
    parser.add_argument("--cdc", action="store_true")
    arguments = parser.parse_args()
    files_checks(arguments.exe, arguments.work, arguments.share)
    if arguments.odbc_driver:
        odbc_checks(arguments.exe, arguments.work, arguments.odbc_driver)
    if arguments.cdc:
        cdc_checks(arguments.exe, arguments.work)


if __name__ == "__main__":
    main()
