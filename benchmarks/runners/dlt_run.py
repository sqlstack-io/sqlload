"""Runs one benchmark scenario with dlt into DuckDB; started by the harness in the dlt environment."""

import json
import sys
from pathlib import Path

import dlt
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from dlt.sources.sql_database import sql_table

from sqlload_bench import pydata


def source(spec: dict) -> object:
	scenario, table = spec["scenario"], spec["table"]
	if scenario.startswith("pg_"):
		incremental = dlt.sources.incremental("id") if scenario == "pg_incremental" else None
		return sql_table(credentials=spec["dsn"], schema="public", table=table, backend=spec["backend"], incremental=incremental)
	if scenario.startswith("files_"):
		file_format = scenario.removeprefix("files_")
		reader = {"parquet": read_parquet, "csv": read_csv, "ndjson": read_jsonl}[file_format]()
		files = filesystem(bucket_url=Path(spec["data"], "files", file_format).as_uri(), file_glob=f"*.{file_format}")
		return (files | reader).with_name(table)
	rows, seed = spec["rows"], spec["seed"]
	if scenario == "py_arrow":
		import pyarrow.parquet as pq

		def arrow():
			yield pq.read_table(Path(spec["data"], "python", "flat.parquet"))

		return dlt.resource(arrow, name=table)
	shape = scenario.removeprefix("py_")

	def pages():
		yield from pydata.pages(rows, seed, shape)

	return dlt.resource(pages, name=table)


def main() -> None:
	spec = json.loads(Path(sys.argv[1]).read_text())
	pipeline = dlt.pipeline(
		pipeline_name="bench",
		destination=dlt.destinations.duckdb(spec["destination"]),
		dataset_name=spec["schema"],
		pipelines_dir=str(Path(spec["run_dir"], "pipelines")),
	)
	info = pipeline.run(source(spec))
	print(info)


if __name__ == "__main__":
	main()
