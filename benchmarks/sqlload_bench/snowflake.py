"""The Snowflake scenarios: settings from the environment, test data in S3, and the checks.

Nothing here runs unless every required SQLLOAD_BENCH_* variable is set and a Snowflake scenario is
selected. The harness only ever writes under `s3://<bucket>/sqlload/`, and in Snowflake only drops
the tables it names itself (`BENCH_*`) in the schema it is given.
"""

import os
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode

from sqlload_bench.datagen import Dataset

REQUIRED = ("ACCOUNT", "USER", "WAREHOUSE", "DATABASE", "SCHEMA")
PREFIX = "SQLLOAD_BENCH_SF_"
#: The only folder of the bucket test data and stages use.
S3_ROOT = "sqlload/"
TOKEN_ENV = f"{PREFIX}TOKEN"
PG_TABLE = "BENCH_NARROW"
FILES_TABLE = "BENCH_PARQUET"

# The same quantities as `datagen.NARROW_CHECKSUM`, in Snowflake's SQL.
NARROW_CHECKSUM = """
SELECT count(*), sum({id}), sum(to_number(substr(md5({customer_name}), 1, 12), 'XXXXXXXXXXXX')),
  sum(round({amount}, 2)), count({amount}), sum(iff({is_active}, 1, 0)),
  sum(date_part(epoch_millisecond, {created_at})), sum({quantity})
FROM {table}
"""
NARROW_COLUMNS = ("id", "customer_name", "created_at", "amount", "is_active", "quantity")

# The same quantities as `datagen.WIDE_CHECKSUM`; JSON is read whether a loader stored it as VARIANT or text.
WIDE_CHECKSUM = """
SELECT count(*), sum({id}), sum({c_int_01}), sum(to_number(substr(md5({c_text_01}), 1, 12), 'XXXXXXXXXXXX')),
  sum(round({c_num_01}, 4)), sum(date_part(epoch_millisecond, {c_ts_02})), sum(iff({c_bool_01}, 1, 0)),
  sum(parse_json(to_varchar({c_json_01})):n::number)
FROM {table}
"""
WIDE_COLUMNS = ("id", "c_int_01", "c_text_01", "c_num_01", "c_ts_02", "c_bool_01", "c_json_01")
#: Each dataset's checksum and the columns it reads.
CHECKSUMS = {"narrow": (NARROW_CHECKSUM, NARROW_COLUMNS), "wide": (WIDE_CHECKSUM, WIDE_COLUMNS)}


@dataclass(frozen=True)
class Settings:
	"""One Snowflake account and schema, and the bucket for the files scenario."""

	account: str
	user: str
	warehouse: str
	database: str
	schema: str
	role: str = ""
	token: str = ""
	private_key_file: str = ""
	bucket: str = ""
	region: str = ""
	storage_integration: str = ""
	stage_name: str = ""

	def auth_toml(self) -> str:
		if self.private_key_file:
			return f'{{ method = "key_pair", private_key_file = "{self.private_key_file}" }}'
		return f'{{ method = "pat", token = "${{{TOKEN_ENV}}}" }}'

	def connection_toml(self, stage: str = "") -> str:
		role = f'role = "{self.role}"\n' if self.role else ""
		stage_line = f"stage = {stage}\n" if stage else ""
		return (
			f'[connections.wh]\nkind = "snowflake"\naccount = "{self.account}"\nuser = "{self.user}"\n'
			f'auth = {self.auth_toml()}\n{role}warehouse = "{self.warehouse}"\ndatabase = "{self.database}"\n{stage_line}\n'
			f'[targets.dev]\nconnection = "wh"\nloader_schema = "{self.schema}"\n'
		)

	def stage_url(self) -> str:
		return f"s3://{self.bucket}/{S3_ROOT}"

	def stage_toml(self) -> str:
		"""The external stage the direct route loads through: an existing one, or one SQLLoad creates."""
		owned = f'name = "{self.stage_name}"' if self.stage_name else f'storage_integration = "{self.storage_integration}"'
		return f'{{ kind = "s3", url = "{self.stage_url()}", {owned}, region = "{self.region}" }}'

	def files_key(self, dataset: Dataset) -> str:
		"""The folder of the files scenario's Parquet parts, relative to the bucket."""
		return files_key(dataset)

	def ingestr_uri(self) -> str:
		"""ingestr's destination URI, passed in its environment so the secret stays off the command line."""
		query = {"warehouse": self.warehouse}
		if self.role:
			query["role"] = self.role
		if self.private_key_file:
			query["private_key"] = Path(self.private_key_file).read_text()
			return f"snowflake://{quote(self.user)}@{self.account}/{self.database}?{urlencode(query)}"
		return f"snowflake://{quote(self.user)}:{quote(self.token, safe='')}@{self.account}/{self.database}?{urlencode(query)}"


def files_key(dataset: Dataset) -> str:
	return f"{S3_ROOT}bench/{dataset.root.name}/parquet/"


def missing_settings(scenario_name: str, env: dict[str, str] | None = None) -> list[str]:
	"""The variables a Snowflake scenario still needs."""
	env = os.environ if env is None else env
	missing = [f"{PREFIX}{name}" for name in REQUIRED if not env.get(f"{PREFIX}{name}")]
	if not env.get(TOKEN_ENV) and not env.get(f"{PREFIX}PRIVATE_KEY_FILE"):
		missing.append(f"{TOKEN_ENV} or {PREFIX}PRIVATE_KEY_FILE")
	if scenario_name == "files_snowflake":
		missing += [name for name in ("SQLLOAD_BENCH_S3_BUCKET", "SQLLOAD_BENCH_S3_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY") if not env.get(name)]
		if not env.get(f"{PREFIX}STORAGE_INTEGRATION") and not env.get(f"{PREFIX}STAGE_NAME"):
			missing.append(f"{PREFIX}STORAGE_INTEGRATION or {PREFIX}STAGE_NAME")
	return missing


def settings_from_env(env: dict[str, str] | None = None) -> Settings:
	env = os.environ if env is None else env
	value = lambda name: env.get(f"{PREFIX}{name}", "")  # noqa: E731
	return Settings(
		account=value("ACCOUNT"),
		user=value("USER"),
		warehouse=value("WAREHOUSE"),
		database=value("DATABASE"),
		schema=value("SCHEMA"),
		role=value("ROLE"),
		token=value("TOKEN"),
		private_key_file=value("PRIVATE_KEY_FILE"),
		bucket=env.get("SQLLOAD_BENCH_S3_BUCKET", ""),
		region=env.get("SQLLOAD_BENCH_S3_REGION", ""),
		storage_integration=value("STORAGE_INTEGRATION"),
		stage_name=value("STAGE_NAME"),
	)


def upload_parquet(settings: Settings, dataset: Dataset) -> str:
	"""Put the Parquet parts under `s3://<bucket>/sqlload/bench/...` unless they are there; returns that folder."""
	import pyarrow.fs as fs

	key = files_key(dataset)
	if not key.startswith(S3_ROOT):
		raise ValueError(f"test data must stay under {S3_ROOT}, not {key}")
	s3 = fs.S3FileSystem(region=settings.region)
	folder = f"{settings.bucket}/{key}"
	present = {info.base_name: info.size for info in s3.get_file_info(fs.FileSelector(folder, allow_not_found=True))}
	for part in sorted(dataset.path("files", "parquet").glob("*.parquet")):
		if present.get(part.name) != part.stat().st_size:
			fs.copy_files(str(part), f"{folder}{part.name}", source_filesystem=fs.LocalFileSystem(), destination_filesystem=s3)
	return key


def connect(settings: Settings) -> Any:
	import snowflake.connector

	options: dict[str, Any] = {"account": settings.account, "user": settings.user, "warehouse": settings.warehouse, "database": settings.database}
	if settings.role:
		options["role"] = settings.role
	if settings.private_key_file:
		options["private_key_file"] = settings.private_key_file
	else:
		options["password"] = settings.token
	return snowflake.connector.connect(**options)


def drop_table(settings: Settings, table: str) -> None:
	"""A fresh destination: drop this harness's table (always a `BENCH_` one), whatever its case."""
	if not table.startswith("BENCH_"):
		raise ValueError(f"the harness only drops its own BENCH_ tables, not {table}")
	with connect(settings) as connection:
		cursor = connection.cursor()
		cursor.execute(f'DROP TABLE IF EXISTS "{settings.database}"."{settings.schema}"."{table}"')
		cursor.execute(f'DROP TABLE IF EXISTS "{settings.database}"."{settings.schema}"."{table.lower()}"')


def same_numbers(got: list[Any], expected: list[str]) -> bool:
	"""Compare as numbers: Snowflake's and DuckDB's text forms of one decimal differ."""
	if len(got) != len(expected):
		return False
	try:
		return all(Decimal(str(left)) == Decimal(right) for left, right in zip(got, expected))
	except InvalidOperation:
		return False


def check(settings: Settings, table: str, kind: str, expected: list[str]) -> tuple[bool, str]:
	"""Whether `table` holds the rows: its columns are found whatever case the loader gave them."""
	with connect(settings) as connection:
		cursor = connection.cursor()
		cursor.execute(
			f'SELECT table_name, column_name FROM "{settings.database}".information_schema.columns '
			"WHERE upper(table_schema) = upper(%s) AND upper(table_name) = upper(%s)",
			(settings.schema, table),
		)
		found = cursor.fetchall()
		if not found:
			return False, f"no table {table} in {settings.schema}"
		names = {column.lower(): column for _, column in found}
		checksum, columns = CHECKSUMS[kind]
		quoted = {column: f'"{names[column]}"' for column in columns if column in names}
		absent = [column for column in columns if column not in names]
		if absent:
			return False, f"{table} has no columns {absent}"
		relation = f'"{settings.database}"."{settings.schema}"."{found[0][0]}"'
		cursor.execute(checksum.format(table=relation, **quoted))
		got = list(cursor.fetchone() or ())
	if same_numbers(got, expected):
		return True, ""
	return False, f"checksum {got} != expected {expected}"
