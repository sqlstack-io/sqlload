"""The benchmark scenarios: what is loaded, how many rows, and which checksum it must match."""

from dataclasses import dataclass

from sqlload_bench.datagen import Sizes

#: Tables in the multi-table scenario, which splits the narrow rows evenly among them.
MULTI_TABLES = 8


@dataclass(frozen=True)
class Scenario:
	"""One source-to-destination load."""

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
	#: The Postgres table a Snowflake scenario reads.
	source: str = ""

	def rows(self, sizes: Sizes) -> int:
		return {
			"pg_narrow": sizes.narrow,
			"pg_wide": sizes.wide,
			"pg_wide_nokey": sizes.wide,
			"pg_incremental": sizes.append,
			"pg_multi": sizes.narrow,
			"pg_snowflake": sizes.narrow,
			"pg_wide_snowflake": sizes.wide,
			"pg_wide_nokey_snowflake": sizes.wide,
			"py_flat": sizes.flat,
			"py_nested": sizes.nested,
			"py_nested_flatten": sizes.nested,
			"py_bytes": sizes.flat,
			"py_arrow": sizes.flat,
			"http_json": sizes.flat,
		}.get(self.name, sizes.files)


SCENARIOS: list[Scenario] = [
	Scenario("pg_narrow", "postgres", "narrow", "narrow", "Postgres narrow table (6 columns), full load", "narrow"),
	Scenario("pg_wide", "postgres", "wide", "wide", "Postgres wide table (40 columns, 2 jsonb), full load", "wide"),
	Scenario("pg_wide_nokey", "postgres", "wide", "wide", "The wide table without a primary key, full load", "wide_nokey"),
	Scenario(
		"pg_incremental",
		"postgres",
		"narrow",
		"narrow_incremental",
		"Postgres narrow table: load, append 1% new rows, load again (the second load is timed)",
		"narrow",
	),
	Scenario("pg_multi", "postgres", "narrow", "narrow", f"The narrow rows split over {MULTI_TABLES} Postgres tables: sqlload's --concurrency 1 and its default", "multi"),
	Scenario("files_parquet", "files", "narrow", "files", "Parquet files (10 parts) in a local folder", "narrow"),
	Scenario("files_csv", "files", "narrow", "files", "CSV files (10 parts, header) in a local folder", "narrow"),
	Scenario("files_ndjson", "files", "narrow", "files", "NDJSON files (10 parts) in a local folder", "narrow"),
	Scenario("files_s3_parquet", "s3", "narrow", "files", "Parquet files (10 parts) in S3 into DuckDB", "narrow"),
	Scenario("py_flat", "python", "flat", "flat", "Python generator: pages of flat 20-field dicts", "flat"),
	Scenario("py_nested", "python", "nested", "nested", "Python generator: pages of nested API-shaped dicts", "nested"),
	Scenario(
		"py_nested_flatten",
		"python",
		"nested",
		"nested",
		"Python generator: the nested dicts with customer flattened one level (sqlload only)",
		"nested",
	),
	Scenario("py_bytes", "python", "flat", "flat", "Python generator: the flat rows as NDJSON bytes (4 MiB chunks)", "flat"),
	Scenario("py_arrow", "python", "flat", "flat", "Python: the flat rows as one pyarrow table", "flat"),
	Scenario(
		"http_json",
		"http",
		"flat",
		"flat",
		"HTTP JSON API: paginated, fanned-out responses from a local stub (rows/s, memory, requests, CPU)",
		"flat",
		pending="sqlload's HTTP helpers are not on main yet",
	),
	Scenario("pg_snowflake", "snowflake", "narrow", "narrow", "Postgres narrow table into Snowflake, full load", "BENCH_NARROW", source="narrow"),
	Scenario("pg_wide_snowflake", "snowflake", "wide", "wide", "Postgres wide table into Snowflake, full load", "BENCH_WIDE", source="wide"),
	Scenario(
		"pg_wide_nokey_snowflake",
		"snowflake",
		"wide",
		"wide",
		"Postgres wide table without a primary key into Snowflake, full load",
		"BENCH_WIDE_NOKEY",
		source="wide_nokey",
	),
	Scenario("files_snowflake", "snowflake", "narrow", "files", "Parquet files in S3 into Snowflake: the direct route", "BENCH_PARQUET"),
]

BY_NAME = {scenario.name: scenario for scenario in SCENARIOS}
