"""`replace` and `--reload`: batches land once in a build table that is swapped in, so no row is written twice."""

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

PROJECT = """\
python_paths = ["sources"]

[connections.wh]
kind = "duckdb"
database = "wh.duckdb"

[targets.dev]
connection = "wh"
loader_schema = "raw"
"""

HEADER = """
import os
import sqlload as sl

api = sl.source("api")
RUN = int(os.environ.get("RUN", "1"))
"""


def project(tmp_path: Path, tables: str) -> Path:
    (tmp_path / "sqlload.toml").write_text(PROJECT)
    (tmp_path / "sources").mkdir()
    (tmp_path / "sources" / "api.py").write_text(HEADER + tables)
    return tmp_path


def sqlload(root: Path, *args: str, run: int = 1) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "RUN": str(run), "HOME": str(root)}
    command = [sys.executable, "-c", "from sqlload._cli import main; main()", *args]
    return subprocess.run(command, cwd=root, env=env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=120, check=False)


def query(root: Path, sql: str) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(root / "wh.duckdb"), read_only=True) as connection:
        return connection.execute(sql).fetchall()


def tables(root: Path) -> list[str]:
    rows = query(root, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw' ORDER BY 1")
    return [row[0] for row in rows]


REPLACED = """
@api.table(description="Test table.", write_strategy="replace", columns={"meta": "json", "ref": "uuid"})
def things(ctx):
    for page in range(3):
        yield [
            {
                "id": RUN * 100 + page * 10 + row,
                "meta": '{"run": %d}' % RUN,
                "ref": "00000000-0000-0000-0000-00000000000%d" % RUN,
            }
            for row in range(3)
        ]
"""


def test_given_two_replaces_when_loading_then_the_second_replaces_every_row_and_leaves_no_build_table(tmp_path: Path) -> None:
    root = project(tmp_path, REPLACED)
    first = sqlload(root, "load", "--batch-bytes", "1")
    assert first.returncode == 0, first.stderr
    second = sqlload(root, "load", "--batch-bytes", "1", run=2)
    assert second.returncode == 0, second.stderr
    assert tables(root) == ["api__things"]
    assert query(root, "SELECT count(*), min(id), max(id) FROM raw.api__things") == [(9, 200, 222)]
    assert query(root, "SELECT DISTINCT typeof(meta), typeof(ref) FROM raw.api__things") == [("JSON", "UUID")]
    assert query(root, "SELECT DISTINCT meta->>'run' FROM raw.api__things") == [("2",)]


def test_given_a_reload_when_loading_then_it_swaps_in_the_whole_table(tmp_path: Path) -> None:
    root = project(tmp_path, REPLACED)
    assert sqlload(root, "load").returncode == 0
    reloaded = sqlload(root, "load", "--reload", "--yes", run=2)
    assert reloaded.returncode == 0, reloaded.stderr
    assert tables(root) == ["api__things"]
    assert query(root, "SELECT count(*), min(id) FROM raw.api__things") == [(9, 200)]


def test_given_a_reload_without_yes_off_a_terminal_when_loading_then_it_is_refused_and_nothing_changes(tmp_path: Path) -> None:
    root = project(tmp_path, REPLACED)
    assert sqlload(root, "load").returncode == 0
    before = query(root, "SELECT count(*), min(id) FROM raw.api__things")
    refused = sqlload(root, "load", "--reload", run=2)
    assert refused.returncode == 1, refused.stdout + refused.stderr
    assert "--reload rebuilds api__things from the whole source, replacing what they hold; confirm it in a terminal, or add --yes" in refused.stderr
    assert query(root, "SELECT count(*), min(id) FROM raw.api__things") == before


FAILING = """
@api.table(description="Test table.", write_strategy="replace")
def things(ctx):
    yield [{"id": RUN * 100 + row} for row in range(3)]
    if RUN == 2:
        raise RuntimeError("the source broke")
    yield [{"id": RUN * 100 + 10 + row} for row in range(3)]
"""


def test_given_a_failure_mid_load_when_replacing_then_the_table_is_untouched_and_no_build_table_remains(tmp_path: Path) -> None:
    root = project(tmp_path, FAILING)
    assert sqlload(root, "load").returncode == 0
    failed = sqlload(root, "load", "--batch-bytes", "1", run=2)
    assert failed.returncode == 1
    assert "the source broke" in failed.stdout + failed.stderr
    assert tables(root) == ["api__things"]
    assert query(root, "SELECT count(*), min(id) FROM raw.api__things") == [(6, 100)]


GROWING = """
@api.table(description="Test table.", write_strategy="replace")
def things(ctx):
    yield [{"id": 1}]
    yield [{"id": 2, "added": "x"}]
"""


@pytest.mark.parametrize("batching", [("--batch-bytes", "1"), ()])
def test_given_a_column_appearing_mid_load_when_replacing_then_the_build_table_takes_it(
    tmp_path: Path, batching: tuple[str, ...]
) -> None:
    root = project(tmp_path, GROWING)
    assert sqlload(root, "load").returncode == 0
    second = sqlload(root, "load", *batching, run=2)
    assert second.returncode == 0, second.stderr
    assert query(root, "SELECT id, added FROM raw.api__things ORDER BY id") == [(1, None), (2, "x")]
    assert tables(root) == ["api__things"]


def test_given_constraints_and_an_index_added_in_the_destination_when_replacing_then_they_stay(tmp_path: Path) -> None:
    root = project(tmp_path, FAILING)
    assert sqlload(root, "load").returncode == 0
    with duckdb.connect(str(root / "wh.duckdb")) as connection:
        connection.execute("ALTER TABLE raw.api__things ADD COLUMN note VARCHAR DEFAULT 'kept'")
        connection.execute("ALTER TABLE raw.api__things ALTER COLUMN id SET NOT NULL")
        connection.execute("CREATE INDEX things_id ON raw.api__things (id)")
    second = sqlload(root, "load", run=3)
    assert second.returncode == 0, second.stderr
    assert query(root, "SELECT count(*), min(id), count(note) FROM raw.api__things") == [(6, 300, 0)]
    assert "DEFAULT('kept')" in query(root, "SELECT sql FROM duckdb_tables() WHERE table_name = 'api__things'")[0][0]
    assert query(root, "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'api__things'") == [("things_id",)]
    assert query(root, "SELECT is_nullable FROM duckdb_columns() WHERE table_name = 'api__things' AND column_name = 'id'") == [(False,)]


STARTING_TO_CARRY = """
@api.table(description="Test table.", write_strategy="replace")
def things(ctx):
    yield [{"id": 1}]
    if RUN == 2:
        yield [{"id": 2, "note": "now"}]
"""


@pytest.mark.parametrize("added", ["note", "Note"])
def test_given_a_destination_column_when_the_source_starts_carrying_it_mid_load_then_it_fills(
    tmp_path: Path, added: str
) -> None:
    root = project(tmp_path, STARTING_TO_CARRY)
    assert sqlload(root, "load").returncode == 0
    with duckdb.connect(str(root / "wh.duckdb")) as connection:
        connection.execute(f"ALTER TABLE raw.api__things ADD COLUMN {added} VARCHAR")
    second = sqlload(root, "load", run=2)
    assert second.returncode == 0, second.stderr
    assert query(root, "SELECT id, note FROM raw.api__things ORDER BY id") == [(1, None), (2, "now")]


def test_given_a_macro_named_arrow_when_loading_then_the_batches_still_load(tmp_path: Path) -> None:
    root = project(tmp_path, FAILING)
    assert sqlload(root, "load").returncode == 0
    with duckdb.connect(str(root / "wh.duckdb")) as connection:
        connection.execute("CREATE MACRO arrow(x) AS TABLE SELECT 'shadow' AS id")
    second = sqlload(root, "load", run=3)
    assert second.returncode == 0, second.stderr
    assert query(root, "SELECT count(*), min(id) FROM raw.api__things") == [(6, 300)]


@pytest.mark.parametrize(
    ("column_type", "value"),
    [("json", "{bad"), ("uuid", "not-a-uuid")],
)
def test_given_a_merge_with_a_value_its_declared_type_rejects_when_loading_then_it_never_reaches_the_table(
    tmp_path: Path, column_type: str, value: str
) -> None:
    root = project(
        tmp_path,
        f"""
@api.table(description="Test table.", write_strategy="merge", unique_key="id", columns={{"v": "{column_type}"}})
def things(ctx):
    yield [{{"id": 1, "v": "{value}"}}]
""",
    )
    result = sqlload(root, "load")
    assert result.returncode == 1, result.stdout
    if "api__things" in tables(root):
        assert query(root, "SELECT count(*) FROM raw.api__things") == [(0,)]


@pytest.mark.parametrize(
    ("strategy", "column_type", "value"),
    [
        ("replace", "json", "{bad"),
        ("append", "json", "{bad"),
        ("replace", "uuid", "not-a-uuid"),
        ("append", "uuid", "not-a-uuid"),
    ],
)
def test_given_a_value_its_declared_type_rejects_when_loading_then_the_load_fails(
    tmp_path: Path, strategy: str, column_type: str, value: str
) -> None:
    root = project(
        tmp_path,
        f"""
@api.table(description="Test table.", write_strategy="{strategy}", columns={{"v": "{column_type}"}})
def things(ctx):
    yield [{{"id": 1, "v": "{value}"}}]
""",
    )
    result = sqlload(root, "load")
    assert result.returncode == 1, result.stdout
    assert tables(root) in ([], ["api__things"])
    if tables(root):
        assert query(root, "SELECT count(*) FROM raw.api__things") == [(0,)]
