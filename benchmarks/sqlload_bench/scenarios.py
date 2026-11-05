"""The benchmark scenarios: what is loaded, how many rows, and which checksum it must match."""

from dataclasses import dataclass

from sqlload_bench.datagen import Sizes


@dataclass(frozen=True)
class Scenario:
	"""One source-to-DuckDB load."""

	name: str
	group: str
	#: The dataset the checksum is computed on (a key of `datagen.CHECKSUMS`).
	kind: str
	#: The key of `expected.json` the destination must match.
	expected: str
	description: str
	#: The logical table name every loader is asked to write.
	table: str
	#: Skipped until the feature exists in sqlload; the reason is reported.
	pending: str = ""

	def rows(self, sizes: Sizes) -> int:
		return {
			"pg_narrow": sizes.narrow,
			"pg_wide": sizes.wide,
			"pg_incremental": sizes.append,
			"py_flat": sizes.flat,
			"py_nested": sizes.nested,
			"py_bytes": sizes.flat,
			"py_arrow": sizes.flat,
		}.get(self.name, sizes.files)


SCENARIOS: list[Scenario] = [
	Scenario("pg_narrow", "postgres", "narrow", "narrow", "Postgres narrow table (6 columns), full load", "narrow"),
	Scenario("pg_wide", "postgres", "wide", "wide", "Postgres wide table (40 columns, 2 jsonb), full load", "wide"),
	Scenario(
		"pg_incremental",
		"postgres",
		"narrow",
		"narrow_incremental",
		"Postgres narrow table: load, append 1% new rows, load again (the second load is timed)",
		"narrow",
	),
	Scenario("files_parquet", "files", "narrow", "files", "Parquet files (10 parts) in a local folder", "narrow"),
	Scenario("files_csv", "files", "narrow", "files", "CSV files (10 parts, header) in a local folder", "narrow"),
	Scenario("files_ndjson", "files", "narrow", "files", "NDJSON files (10 parts) in a local folder", "narrow"),
	Scenario("py_flat", "python", "flat", "flat", "Python generator: pages of flat 20-field dicts", "flat"),
	Scenario("py_nested", "python", "nested", "nested", "Python generator: pages of nested API-shaped dicts", "nested"),
	Scenario(
		"py_nested_flatten",
		"python",
		"nested",
		"nested",
		"Python generator: nested dicts with sqlload flattening",
		"nested",
		pending="sqlload has no flatten option on main yet",
	),
	Scenario("py_bytes", "python", "flat", "flat", "Python generator: the flat rows as NDJSON bytes (4 MiB chunks)", "flat"),
	Scenario("py_arrow", "python", "flat", "flat", "Python: the flat rows as one pyarrow table", "flat"),
	Scenario(
		"http_json",
		"http",
		"flat",
		"flat",
		"HTTP JSON API: paginated responses from a local stub server",
		"flat",
		pending="sqlload's HTTP helpers are not on main yet",
	),
]

BY_NAME = {scenario.name: scenario for scenario in SCENARIOS}
