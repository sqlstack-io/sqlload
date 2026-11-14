"""S3 Parquet into DuckDB: SQLLoad reading the files itself against DuckDB reading them with httpfs.

Decides how `S3 -> DuckDB` should route. Both sides load the same narrow Parquet parts into a new
DuckDB file and must match the same checksum; runs alternate so network drift hits both alike.

	uv run python -m sqlload_bench.s3_duckdb --sqlload /path/to/sqlload --bucket B --region R --rows 1000000

Needs AWS credentials in the environment. Test data goes under `s3://<bucket>/sqlload/s3bench/` only.
"""

import argparse
import json
import os
import sys
from pathlib import Path

import duckdb
import pyarrow.fs as fs

from sqlload_bench.datagen import FILE_PARTS, MIX_MACRO, NARROW_SELECT, checksum
from sqlload_bench.measure import run

PREFIX = "sqlload/s3bench/"

DUCKDB_LOAD = """
import duckdb, os, sys
connection = duckdb.connect(sys.argv[1])
connection.execute("INSTALL httpfs; LOAD httpfs;")
connection.execute(
	"CREATE SECRET (TYPE s3, KEY_ID ?, SECRET ?, REGION ?)",
	[os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], sys.argv[3]],
)
connection.execute("CREATE SCHEMA RAW")
connection.execute(f"CREATE TABLE RAW.narrow AS SELECT * FROM read_parquet('{sys.argv[2]}*.parquet')")
"""


def generate(folder: Path, rows: int, seed: int) -> list[str]:
	"""Write the narrow rows as `FILE_PARTS` Parquet parts under `folder`; returns their checksum."""
	folder.mkdir(parents=True, exist_ok=True)
	connection = duckdb.connect()
	connection.execute("SET TimeZone = 'UTC'; SET preserve_insertion_order = true;")
	connection.execute(MIX_MACRO.format(seed=seed))
	part_rows = -(-rows // FILE_PARTS)
	for part in range(FILE_PARTS):
		start, stop = 1 + part * part_rows, min(1 + (part + 1) * part_rows, rows + 1)
		target = folder / f"part-{part:03d}.parquet"
		if start < stop and not target.exists():
			connection.execute(f"COPY ({NARROW_SELECT.format(start=start, stop=stop)}) TO '{target}' (FORMAT parquet)")
	return checksum(connection, "narrow", f"'{folder}/*.parquet'")


def upload(folder: Path, bucket: str, region: str, key: str) -> None:
	"""Copy the parts to `s3://bucket/key` unless each is already there at the same size."""
	s3 = fs.S3FileSystem(region=region)
	present = {info.base_name: info.size for info in s3.get_file_info(fs.FileSelector(f"{bucket}/{key}", allow_not_found=True))}
	for part in sorted(folder.glob("*.parquet")):
		if present.get(part.name) != part.stat().st_size:
			fs.copy_files(str(part), f"{bucket}/{key}{part.name}", source_filesystem=fs.LocalFileSystem(), destination_filesystem=s3)


def sqlload_project(work: Path, bucket: str, region: str, key: str) -> Path:
	config = work / "sqlload.toml"
	config.write_text(
		f'[connections.lake]\nkind = "s3"\nbucket = "{bucket}"\nregion = "{region}"\n\n'
		f'[connections.wh]\nkind = "duckdb"\ndatabase = "{work / "wh.duckdb"}"\n\n'
		'[targets.dev]\nconnection = "wh"\nloader_schema = "RAW"\n\n'
		f'[sources.narrow]\nkind = "files"\nconnection = "lake"\npath = "{key}"\nformat = "parquet"\ndescription = "Benchmark rows."\nwrite_strategy = "replace"\n'
	)
	return config


def loaded(database: Path) -> list[str]:
	connection = duckdb.connect(str(database), read_only=True)
	table = connection.execute(
		"SELECT table_schema || '.' || table_name FROM information_schema.tables WHERE lower(table_schema) = 'raw' LIMIT 1"
	).fetchone()[0]
	return checksum(connection, "narrow", table)


def main() -> None:
	parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
	parser.add_argument("--sqlload", required=True, help="the sqlload executable to time")
	parser.add_argument("--bucket", required=True)
	parser.add_argument("--region", required=True)
	parser.add_argument("--rows", type=int, default=1_000_000)
	parser.add_argument("--repeats", type=int, default=3)
	parser.add_argument("--seed", type=int, default=1)
	parser.add_argument("--out", type=Path, default=Path("s3bench-out"))
	args = parser.parse_args()
	key = f"{PREFIX}seed{args.seed}-n{args.rows}/"
	expected = generate(args.out / "data" / f"n{args.rows}", args.rows, args.seed)
	upload(args.out / "data" / f"n{args.rows}", args.bucket, args.region, key)
	results: list[dict[str, object]] = []
	for repeat in range(args.repeats):
		for side in ("sqlload", "duckdb_httpfs"):
			work = args.out / f"{side}-{repeat}"
			work.mkdir(parents=True, exist_ok=True)
			(work / "wh.duckdb").unlink(missing_ok=True)
			argv = {
				"sqlload": [args.sqlload, "load", "-c", str(sqlload_project(work, args.bucket, args.region, key))],
				"duckdb_httpfs": [sys.executable, "-c", DUCKDB_LOAD, str(work / "wh.duckdb"), f"s3://{args.bucket}/{key}", args.region],
			}[side]
			measured = run(argv, cwd=work, env=dict(os.environ), log=work / "output.log")
			correct = measured.returncode == 0 and loaded(work / "wh.duckdb") == expected
			results.append({"side": side, "repeat": repeat, "wall_s": round(measured.wall_s, 2), "cpu_s": round(measured.cpu_s, 2), "peak_rss_mb": round(measured.peak_rss_mb), "correct": correct})
			print(json.dumps(results[-1]), flush=True)
	(args.out / f"results-n{args.rows}.json").write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
	main()
