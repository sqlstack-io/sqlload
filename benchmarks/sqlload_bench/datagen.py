"""Benchmark datasets, generated deterministically from a seed with DuckDB.

The narrow and wide tables are written once as Parquet (the reference every checksum is computed
from), as headerless CSV for Postgres `COPY`, and as Parquet, CSV and NDJSON part files for the
files scenarios. Python data is written as NDJSON and Parquet for the bytes and Arrow scenarios.
"""

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import orjson

from sqlload_bench import pydata

FILE_PARTS = 10

# A 31-bit integer hash of (v, salt) with only 64-bit arithmetic, so every value derives from the seed.
MIX_MACRO = """
CREATE OR REPLACE MACRO shift_xor(x, bits) AS xor(x, x >> bits);
CREATE OR REPLACE MACRO mix(v, salt) AS shift_xor(
  (shift_xor((v * 2654435761 + salt * 3266489917 + {seed} * 2246822519) % 4294967296, 15) * 739982445) % 4294967296, 12
) % 2147483648;
"""

WORDS = "['alpha','bravo','cobalt','delta','ember','fjord','garnet','harbor','indigo','juniper']"

NARROW_SELECT = f"""
SELECT
  i::BIGINT AS id,
  'customer ' || (mix(i, 1) % 100000) || ' ' || {WORDS}[1 + mix(i, 2) % 10] AS customer_name,
  TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_seconds(i * 3 + mix(i, 6) % 3) AS created_at,
  CASE WHEN mix(i, 3) % 20 = 0 THEN NULL ELSE ((mix(i, 3) % 10000000) / 100)::DECIMAL(12, 2) END AS amount,
  mix(i, 4) % 3 <> 0 AS is_active,
  (mix(i, 5) % 1000)::INTEGER AS quantity
FROM range({{start}}, {{stop}}) AS t(i)
"""


def _wide_columns() -> list[str]:
	columns = ["i::BIGINT AS id"]
	for n in range(1, 9):
		kind = "INTEGER" if n % 2 else "BIGINT"
		columns.append(f"(mix(i, {10 + n}) % 1000000)::{kind} AS c_int_{n:02d}")
	for n in range(1, 10):
		columns.append(f"{WORDS}[1 + mix(i, {90 + n}) % 10] || '-' || (mix(i, {20 + n}) % 100000) AS c_text_{n:02d}")
	for n in range(1, 7):
		columns.append(f"CASE WHEN mix(i, {30 + n}) % 25 = 0 THEN NULL ELSE ((mix(i, {30 + n}) % 100000000) / 10000)::DECIMAL(18, 4) END AS c_num_{n:02d}")
	for n in range(1, 5):
		columns.append(f"TIMESTAMPTZ '2025-01-01 00:00:00+00' + to_seconds(mix(i, {40 + n}) % 31536000) AS c_ts_{n:02d}")
	for n in range(1, 3):
		columns.append(f"DATE '2020-01-01' + (mix(i, {50 + n}) % 3650)::INTEGER AS c_date_{n:02d}")
	for n in range(1, 5):
		columns.append(f"mix(i, {60 + n}) % 2 = 0 AS c_bool_{n:02d}")
	for n in range(1, 5):
		columns.append(f"(mix(i, {70 + n}) % 1000000) / 64.0 AS c_float_{n:02d}")
	columns.append(
		"json_object('n', mix(i, 81) % 1000, 'tags', [mix(i, 82) % 7, mix(i, 83) % 7], 'meta', json_object('k', 'v' || mix(i, 84) % 10)) AS c_json_01"
	)
	columns.append("json_object('n', mix(i, 85) % 1000, 'ok', mix(i, 86) % 2 = 0) AS c_json_02")
	return columns


WIDE_SELECT = "SELECT\n  " + ",\n  ".join(_wide_columns()) + "\nFROM range({start}, {stop}) AS t(i)"

# Order-independent checksums over key columns; text is hashed through md5 so values must match exactly.
NARROW_CHECKSUM = """
SELECT count(*), sum(id::BIGINT), sum(('0x' || substr(md5(customer_name::VARCHAR), 1, 12))::BIGINT),
  sum(round(amount::DECIMAL(38, 4), 2))::VARCHAR, count(amount), sum(is_active::BOOLEAN::INTEGER),
  sum(epoch_ms(created_at::TIMESTAMPTZ)), sum(quantity::BIGINT)
FROM {table}
"""

WIDE_CHECKSUM = """
SELECT count(*), sum(id::BIGINT), sum(c_int_01::BIGINT), sum(('0x' || substr(md5(c_text_01::VARCHAR), 1, 12))::BIGINT),
  sum(round(c_num_01::DECIMAL(38, 6), 4))::VARCHAR, sum(epoch_ms(c_ts_02::TIMESTAMPTZ)),
  sum(c_bool_01::BOOLEAN::INTEGER), sum(json_extract(c_json_01::JSON, '$.n')::BIGINT)
FROM {table}
"""

FLAT_CHECKSUM = """
SELECT count(*), sum(id::BIGINT), sum(customer_id::BIGINT), sum(('0x' || substr(md5(customer_name::VARCHAR), 1, 12))::BIGINT),
  round(sum(unit_price::DOUBLE), 2)::VARCHAR, sum(is_gift::BOOLEAN::INTEGER), sum(epoch_ms(created_at::TIMESTAMPTZ)), count(note)
FROM {table}
"""

NESTED_CHECKSUM = "SELECT count(*), sum(id::BIGINT) FROM {table}"

CHECKSUMS = {"narrow": NARROW_CHECKSUM, "wide": WIDE_CHECKSUM, "flat": FLAT_CHECKSUM, "nested": NESTED_CHECKSUM}


@dataclass(frozen=True)
class Sizes:
	"""Row counts of each dataset."""

	narrow: int
	wide: int
	flat: int
	nested: int
	files: int

	@property
	def append(self) -> int:
		"""Rows the incremental scenario appends: 1% of the narrow table."""
		return max(1, self.narrow // 100)


SIZES = {
	"full": Sizes(narrow=10_000_000, wide=1_000_000, flat=1_000_000, nested=1_000_000, files=10_000_000),
	"reduced": Sizes(narrow=1_000_000, wide=100_000, flat=200_000, nested=50_000, files=1_000_000),
	# Only the narrow table to speak of: the timing CI reports.
	"e2e": Sizes(narrow=1_000_000, wide=1, flat=1, nested=1, files=1),
	# The wide tables at 1M rows, the reference size for them, with little else.
	"wide": Sizes(narrow=1_000, wide=1_000_000, flat=1_000, nested=1_000, files=1_000),
	"tiny": Sizes(narrow=20_000, wide=5_000, flat=5_000, nested=2_000, files=20_000),
}


@dataclass(frozen=True)
class Dataset:
	"""Where one generated dataset lives and the checksum it must load with."""

	root: Path
	seed: int
	sizes: Sizes

	def path(self, *parts: str) -> Path:
		return self.root.joinpath(*parts)

	def first_appended_at(self) -> str:
		"""The incremental scenario's lowest new `created_at`: every base row's is below it."""
		return (datetime(2026, 1, 1, tzinfo=UTC) + timedelta(seconds=(self.sizes.narrow + 1) * 3)).strftime("%Y-%m-%dT%H:%M:%SZ")

	def expected(self, name: str) -> list[str]:
		return json.loads(self.path("expected.json").read_text())[name]


def checksum(connection: duckdb.DuckDBPyConnection, kind: str, table: str) -> list[str]:
	"""The checksum row of `table` as text, so DECIMAL, HUGEINT and int results compare equal."""
	row = connection.execute(CHECKSUMS[kind].format(table=table)).fetchone()
	return ["" if value is None else str(value) for value in (row or ())]


#: The parts of a dataset, each generated only when a selected scenario reads it: the Postgres tables
#: (with their Parquet copies), the Parquet part files, the CSV and NDJSON files, and the Python rows.
PARTS = ("pg", "files_parquet", "files_text", "python")


def generate(root: Path, seed: int, sizes: Sizes, parts: tuple[str, ...] = PARTS) -> Dataset:
	"""Generate `parts` of the dataset under `root/<seed>-<sizes>`; parts already there are reused."""
	folder = root / f"seed{seed}-n{sizes.narrow}-w{sizes.wide}-f{sizes.flat}-j{sizes.nested}-x{sizes.files}"
	dataset = Dataset(folder, seed, sizes)
	expected: dict[str, list[str]] = json.loads((folder / "expected.json").read_text()) if (folder / "expected.json").exists() else {}
	# A dataset written before parts existed has every part.
	done: list[str] = json.loads((folder / "parts.json").read_text()) if (folder / "parts.json").exists() else (list(PARTS) if expected else [])
	missing = [part for part in parts if part not in done]
	if not missing:
		return dataset
	for sub in ("pg", "files/parquet", "files/csv", "files/ndjson", "files/single", "python"):
		(folder / sub).mkdir(parents=True, exist_ok=True)
	connection = duckdb.connect()
	connection.execute("SET TimeZone = 'UTC'; SET preserve_insertion_order = true;")
	connection.execute(MIX_MACRO.format(seed=seed))

	def write(select: str, target: Path, options: str) -> None:
		connection.execute(f"COPY ({select}) TO '{target}' ({options})")

	for part in missing:
		{"pg": _write_tables, "files_parquet": _write_parquet_parts, "files_text": _write_text_files, "python": _write_python}[part](
			folder, seed, sizes, write, connection, expected
		)
		done.append(part)
		(folder / "expected.json").write_text(json.dumps(expected, indent=1))
		(folder / "parts.json").write_text(json.dumps(done))
	connection.close()
	(folder / "sizes.json").write_text(json.dumps(asdict(sizes)))
	return dataset


_Write = Callable[[str, Path, str], None]


def _write_tables(folder: Path, seed: int, sizes: Sizes, write: _Write, connection: duckdb.DuckDBPyConnection, expected: dict[str, list[str]]) -> None:
	"""The narrow, appended and wide rows as Parquet, and as headerless CSV for Postgres `COPY`."""
	n, a = sizes.narrow, sizes.append
	write(NARROW_SELECT.format(start=1, stop=n + 1), folder / "narrow.parquet", "FORMAT parquet")
	write(NARROW_SELECT.format(start=n + 1, stop=n + a + 1), folder / "narrow_append.parquet", "FORMAT parquet")
	write(WIDE_SELECT.format(start=1, stop=sizes.wide + 1), folder / "wide.parquet", "FORMAT parquet")
	for name in ("narrow", "narrow_append", "wide"):
		write(f"SELECT * FROM '{folder / name}.parquet'", folder / "pg" / f"{name}.csv", "FORMAT csv, HEADER false")
	expected["narrow"] = checksum(connection, "narrow", f"'{folder / 'narrow.parquet'}'")
	expected["narrow_incremental"] = checksum(
		connection, "narrow", f"(SELECT * FROM '{folder / 'narrow.parquet'}' UNION ALL SELECT * FROM '{folder / 'narrow_append.parquet'}')"
	)
	expected["wide"] = checksum(connection, "wide", f"'{folder / 'wide.parquet'}'")


def _write_parquet_parts(folder: Path, seed: int, sizes: Sizes, write: _Write, connection: duckdb.DuckDBPyConnection, expected: dict[str, list[str]]) -> None:
	"""The files scenarios' rows as Parquet parts."""
	for start, stop, part in _file_parts(sizes):
		write(NARROW_SELECT.format(start=start, stop=stop), folder / "files/parquet" / f"part-{part:03d}.parquet", "FORMAT parquet")
	expected["files"] = checksum(connection, "narrow", f"'{folder / 'files/parquet'}/*.parquet'")


def _file_parts(sizes: Sizes) -> list[tuple[int, int, int]]:
	"""Each part file's first id, the id after its last, and its number."""
	part_rows = -(-sizes.files // FILE_PARTS)
	bounds = [(1 + part * part_rows, min(1 + (part + 1) * part_rows, sizes.files + 1), part) for part in range(FILE_PARTS)]
	return [bound for bound in bounds if bound[0] < bound[1]]


def _write_text_files(folder: Path, seed: int, sizes: Sizes, write: _Write, connection: duckdb.DuckDBPyConnection, expected: dict[str, list[str]]) -> None:
	"""The files scenarios' rows as CSV and NDJSON parts, and as one file of each."""
	text_select = (
		"SELECT id, customer_name, strftime(created_at, '%Y-%m-%dT%H:%M:%SZ') AS created_at, amount, is_active, quantity "
		"FROM ({select})"
	)
	for start, stop, part in _file_parts(sizes):
		select = NARROW_SELECT.format(start=start, stop=stop)
		write(text_select.format(select=select), folder / "files/csv" / f"part-{part:03d}.csv", "FORMAT csv, HEADER true")
		write(text_select.format(select=select), folder / "files/ndjson" / f"part-{part:03d}.ndjson", "FORMAT json")
	whole = text_select.format(select=NARROW_SELECT.format(start=1, stop=sizes.files + 1))
	write(whole, folder / "files/single/narrow.csv", "FORMAT csv, HEADER true")
	write(whole, folder / "files/single/narrow.ndjson", "FORMAT json")


def _write_python(folder: Path, seed: int, sizes: Sizes, write: _Write, connection: duckdb.DuckDBPyConnection, expected: dict[str, list[str]]) -> None:
	"""The Python scenarios' rows, and their checksums."""
	_write_python_data(folder, seed, sizes)
	expected["flat"] = checksum(connection, "flat", f"read_json('{folder / 'python/flat.ndjson'}', format='newline_delimited')")
	expected["nested"] = [str(sizes.nested), str(sizes.nested * (sizes.nested + 1) // 2)]
	expected["nested_lines"] = [str(pydata.nested_lines(sizes.nested, seed))]


def _write_python_data(folder: Path, seed: int, sizes: Sizes) -> None:
	"""The flat rows as NDJSON (the bytes scenario) and Parquet (read into the Arrow scenario's table)."""
	import pyarrow as pa
	import pyarrow.parquet as pq

	writer: pq.ParquetWriter | None = None
	with open(folder / "python/flat.ndjson", "wb") as ndjson:
		for page in pydata.pages(sizes.flat, seed, "flat"):
			ndjson.write(b"".join(orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE) for row in page))
			table = pa.Table.from_pylist(page)
			if writer is None:
				writer = pq.ParquetWriter(folder / "python/flat.parquet", table.schema)
			writer.write_table(table.cast(writer.schema))
	if writer is not None:
		writer.close()
