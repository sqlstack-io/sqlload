"""The benchmark's Postgres: a dedicated `postgres:17` container on a free port."""

import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import psycopg

from sqlload_bench.datagen import Dataset
from sqlload_bench.scenarios import MULTI_TABLES

IMAGE = "postgres:17"
USER = "bench"
PASSWORD = "bench"
DATABASE = "shop"

NARROW_DDL = """
CREATE TABLE public.{name} (
  id BIGINT PRIMARY KEY, customer_name TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL,
  amount NUMERIC(12, 2), is_active BOOLEAN NOT NULL, quantity INTEGER NOT NULL
)
"""


def _wide_ddl() -> str:
	columns = ["id BIGINT PRIMARY KEY"]
	columns += [f"c_int_{n:02d} {'INTEGER' if n % 2 else 'BIGINT'}" for n in range(1, 9)]
	columns += [f"c_text_{n:02d} TEXT" for n in range(1, 10)]
	columns += [f"c_num_{n:02d} NUMERIC(18, 4)" for n in range(1, 7)]
	columns += [f"c_ts_{n:02d} TIMESTAMPTZ" for n in range(1, 5)]
	columns += [f"c_date_{n:02d} DATE" for n in range(1, 3)]
	columns += [f"c_bool_{n:02d} BOOLEAN" for n in range(1, 5)]
	columns += [f"c_float_{n:02d} DOUBLE PRECISION" for n in range(1, 5)]
	columns += ["c_json_01 JSONB", "c_json_02 JSONB"]
	return "CREATE TABLE public.wide (\n  " + ",\n  ".join(columns) + "\n)"


@dataclass
class Postgres:
	"""A running container and how to reach it."""

	container: str
	port: int

	@property
	def dsn(self) -> str:
		return f"postgresql://{USER}:{PASSWORD}@127.0.0.1:{self.port}/{DATABASE}"

	def connect(self) -> psycopg.Connection:
		return psycopg.connect(self.dsn, autocommit=True)

	def stop(self) -> None:
		subprocess.run(["docker", "rm", "-f", "-v", self.container], capture_output=True, check=False)


def multi_table(index: int) -> str:
	"""The name of one of the multi-table scenario's tables."""
	return f"multi_{index + 1:02d}"


def free_port() -> int:
	with socket.socket() as probe:
		probe.bind(("127.0.0.1", 0))
		return int(probe.getsockname()[1])


def start() -> Postgres:
	"""Start a fresh container; durability is off since only reads are measured."""
	port = free_port()
	name = f"sqlload-bench-pg-{uuid.uuid4().hex[:8]}"
	subprocess.run(
		[
			"docker", "run", "-d", "--rm", "--name", name, "-p", f"127.0.0.1:{port}:5432", "--shm-size=1g",
			"-e", f"POSTGRES_USER={USER}", "-e", f"POSTGRES_PASSWORD={PASSWORD}", "-e", f"POSTGRES_DB={DATABASE}",
			IMAGE, "-c", "fsync=off", "-c", "synchronous_commit=off", "-c", "full_page_writes=off",
			"-c", "shared_buffers=1GB", "-c", "max_wal_size=8GB", "-c", "max_connections=200",
		],
		check=True,
		capture_output=True,
	)
	postgres = Postgres(name, port)
	deadline = time.monotonic() + 60
	while True:
		try:
			with postgres.connect() as connection:
				connection.execute("SELECT 1")
			return postgres
		except psycopg.OperationalError:
			if time.monotonic() > deadline:
				postgres.stop()
				raise
			time.sleep(0.5)


def _copy(connection: psycopg.Connection, table: str, path: Path) -> None:
	with connection.cursor() as cursor, cursor.copy(f"COPY public.{table} FROM STDIN (FORMAT csv)") as copy, open(path, "rb") as handle:
		while block := handle.read(1 << 20):
			copy.write(block)


def load(postgres: Postgres, dataset: Dataset) -> None:
	"""Create and fill `narrow`, `wide` and `wide_nokey` (no primary key), then VACUUM ANALYZE so every reader sees settled pages."""
	with postgres.connect() as connection:
		connection.execute(NARROW_DDL.format(name="narrow"))
		connection.execute(_wide_ddl())
		_copy(connection, "narrow", dataset.path("pg", "narrow.csv"))
		_copy(connection, "wide", dataset.path("pg", "wide.csv"))
		connection.execute("VACUUM (FREEZE, ANALYZE) public.narrow")
		connection.execute("VACUUM (FREEZE, ANALYZE) public.wide")
		connection.execute("CREATE TABLE public.wide_nokey AS SELECT * FROM public.wide")
		connection.execute("VACUUM (FREEZE, ANALYZE) public.wide_nokey")
		share = -(-dataset.sizes.narrow // MULTI_TABLES)
		for index in range(MULTI_TABLES):
			table = multi_table(index)
			connection.execute(f"CREATE TABLE public.{table} AS SELECT * FROM public.narrow WHERE id > {index * share} AND id <= {(index + 1) * share}")
			connection.execute(f"VACUUM (FREEZE, ANALYZE) public.{table}")


def append_rows(postgres: Postgres, dataset: Dataset) -> None:
	"""The incremental scenario's new rows: the next 1% of ids."""
	with postgres.connect() as connection:
		_copy(connection, "narrow", dataset.path("pg", "narrow_append.csv"))
		connection.execute("ANALYZE public.narrow")


def remove_appended(postgres: Postgres, dataset: Dataset) -> None:
	with postgres.connect() as connection:
		connection.execute("DELETE FROM public.narrow WHERE id > %s", (dataset.sizes.narrow,))
		connection.execute("VACUUM ANALYZE public.narrow")
