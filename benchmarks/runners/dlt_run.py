"""Runs one benchmark scenario with dlt; started by the harness in the dlt environment."""

import json
import os
import sys
from pathlib import Path

import dlt
import orjson
from dlt.sources.filesystem import filesystem, read_csv, read_jsonl, read_parquet
from dlt.sources.sql_database import sql_table

from sqlload_bench import pydata


def source(spec: dict) -> object:
	scenario, table = spec["scenario"], spec["table"]
	if scenario.startswith("pg_"):
		incremental = dlt.sources.incremental("id") if scenario == "pg_incremental" else None
		return sql_table(credentials=spec["dsn"], schema="public", table=spec["source"], backend=spec["backend"], incremental=incremental).with_name(table)
	if scenario == "files_s3_parquet":
		from dlt.common.configuration.specs import AwsCredentials

		credentials = AwsCredentials(
			aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"], aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"], region_name=spec["region"]
		)
		files = filesystem(bucket_url=spec["s3_url"], credentials=credentials, file_glob="*.parquet")
		return (files | read_parquet()).with_name(table)
	if scenario.startswith("files_"):
		file_format = scenario.removeprefix("files_")
		reader = {"parquet": read_parquet, "csv": read_csv, "ndjson": read_jsonl}[file_format]()
		files = filesystem(bucket_url=Path(spec["data"], "files", file_format).as_uri(), file_glob=f"*.{file_format}")
		return (files | reader).with_name(table)
	rows, seed, data = spec["rows"], spec["seed"], Path(spec["data"], "python")
	if scenario == "py_arrow":
		import pyarrow.parquet as pq

		def arrow():
			yield pq.read_table(data / "flat.parquet")

		return dlt.resource(arrow, name=table)
	if scenario == "py_bytes":
		# dlt takes objects, not bytes, so the bytes are parsed in the resource as a dlt user would.
		def parsed():
			for chunk in pydata.ndjson_chunks(str(data / "flat.ndjson")):
				yield [orjson.loads(line) for line in chunk.splitlines()]

		return dlt.resource(parsed, name=table)
	shape = scenario.removeprefix("py_")

	def pages():
		yield from pydata.pages(rows, seed, shape)

	return dlt.resource(pages, name=table)


def destination(spec: dict) -> object:
	settings = spec.get("snowflake")
	if not settings:
		return dlt.destinations.duckdb(spec["destination"])
	credentials = {key: settings[key] for key in ("username", "host", "warehouse", "database", "role") if settings[key]}
	if settings["private_key_path"]:
		credentials["private_key_path"] = settings["private_key_path"]
	else:
		credentials["password"] = os.environ["SQLLOAD_BENCH_SF_TOKEN"]
	return dlt.destinations.snowflake(credentials=credentials)


def main() -> None:
	spec = json.loads(Path(sys.argv[1]).read_text())
	settings = spec.get("snowflake")
	pipeline = dlt.pipeline(
		pipeline_name="bench",
		destination=destination(spec),
		dataset_name=settings["schema"] if settings else spec["schema"],
		pipelines_dir=str(Path(spec["run_dir"], "pipelines")),
	)
	# A Snowflake table is shared between runs, so dlt forgets it each time, as the others start fresh.
	print(pipeline.run(source(spec), refresh="drop_resources" if settings else None))


if __name__ == "__main__":
	main()
