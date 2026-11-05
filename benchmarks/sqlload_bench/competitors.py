"""The loaders compared, and how each one runs a scenario as a process into a fresh destination."""

import json
import os
from urllib.parse import urlencode
from dataclasses import dataclass, field
from pathlib import Path

from sqlload_bench import postgres as pg
from sqlload_bench import snowflake
from sqlload_bench.datagen import Dataset
from sqlload_bench.measure import clean_env
from sqlload_bench.scenarios import MULTI_TABLES, Scenario

BENCH_ROOT = Path(__file__).resolve().parent.parent
RUNNERS = BENCH_ROOT / "runners"
DLT_PYTHON = BENCH_ROOT / "envs/dlt/.venv/bin/python"
INGESTR = BENCH_ROOT / "envs/ingestr/.venv/bin/ingestr"
SQLLOAD = BENCH_ROOT / ".venv/bin/sqlload"
SCHEMA = "raw"
#: `unsupported` returns this for combinations not worth a table row (a dlt backend outside sql_database).
HIDDEN = "hidden"
#: Environment variables a Snowflake scenario's loader needs from the caller.
SNOWFLAKE_ENV = (snowflake.TOKEN_ENV, "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_REGION", "AWS_DEFAULT_REGION")


@dataclass
class Context:
	"""What every run needs: the data, the database and shared caches."""

	dataset: Dataset
	postgres: pg.Postgres | None
	cache: Path
	#: Set only when a Snowflake scenario was selected and its variables are present.
	warehouse: snowflake.Settings | None = None


@dataclass
class Invocation:
	"""One process to run (and time) for a load."""

	argv: list[str]
	cwd: Path
	env: dict[str, str] = field(default_factory=dict)


def _caller_env(names: tuple[str, ...]) -> dict[str, str]:
	return {name: os.environ[name] for name in names if name in os.environ}


class Competitor:
	"""A loader; subclasses say which scenarios they run and how."""

	name: str = ""

	def unsupported(self, scenario: Scenario) -> str | None:
		"""Why this loader cannot run `scenario` (`HIDDEN` for no table row), or None when it can."""
		raise NotImplementedError

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		"""The load to run; `second` is the incremental scenario's load after the append."""
		raise NotImplementedError

	def plan_invocation(self, scenario: Scenario, context: Context, run_dir: Path) -> tuple[Invocation, str] | None:
		"""A command whose output must contain the returned text before the load counts, when a route matters."""
		return None

	def relation(self, scenario: Scenario) -> str:
		"""What the destination's checksum reads."""
		return f"{SCHEMA}.{scenario.table}"

	@staticmethod
	def destination(run_dir: Path) -> Path:
		return run_dir / "dest.duckdb"

	def env(self, run_dir: Path, extra: dict[str, str] | None = None) -> dict[str, str]:
		return clean_env(run_dir / "home", {"PYTHONPATH": str(BENCH_ROOT), **(extra or {})})


def _spec(run_dir: Path, scenario: Scenario, context: Context, **settings: object) -> str:
	"""The JSON a runner script reads: the scenario, the data and the destination."""
	spec = {
		"scenario": scenario.name,
		"table": scenario.table,
		"source": scenario.source or scenario.table,
		"schema": SCHEMA,
		"destination": str(Competitor.destination(run_dir)),
		"data": str(context.dataset.root),
		"seed": context.dataset.seed,
		"rows": scenario.rows(context.dataset.sizes),
		"dsn": context.postgres.dsn if context.postgres else "",
		"run_dir": str(run_dir),
		**settings,
	}
	path = run_dir / "spec.json"
	path.write_text(json.dumps(spec, indent=1))
	return str(path)


class SqlLoad(Competitor):
	"""The sqlload extension wheel built from this repository; its `sqlload load` runs in-process."""

	def __init__(self, *, threads: int | None = None, shaping: str | None = None, external: bool = False, label: str = "") -> None:
		self.threads = threads
		self.shaping = shaping
		#: Postgres into Snowflake through the external stage instead of the user stage.
		self.external = external
		self.name = f"sqlload ({label})" if label else "sqlload"

	def unsupported(self, scenario: Scenario) -> str | None:
		if self.threads is not None:
			return None if scenario.name == "pg_multi" else HIDDEN
		if self.external:
			return None if scenario.source else HIDDEN
		if scenario.name == "files_snowflake":
			return None if self.shaping else HIDDEN
		if self.shaping:
			return HIDDEN
		return scenario.pending or None

	def relation(self, scenario: Scenario) -> str:
		if scenario.name == "pg_multi":
			tables = [f"SELECT * FROM {SCHEMA}.app__{pg.multi_table(index)}" for index in range(MULTI_TABLES)]
			return "(" + " UNION ALL ".join(tables) + ")"
		if scenario.group == "postgres":
			return f"{SCHEMA}.app__{scenario.table}"
		return f"{SCHEMA}.{scenario.table}"

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		argv = [str(SQLLOAD), "load", *(["--threads", str(self.threads)] if self.threads else [])]
		return Invocation(argv, self._project_dir(scenario, context, run_dir), self._env(scenario, run_dir))

	def plan_invocation(self, scenario: Scenario, context: Context, run_dir: Path) -> tuple[Invocation, str] | None:
		if scenario.name != "files_snowflake":
			return None
		expected = 'shaping = "loader"' if self.shaping == "loader" else "destination loads parquet files from"
		return Invocation([str(SQLLOAD), "plan"], self._project_dir(scenario, context, run_dir), self._env(scenario, run_dir)), expected

	def _env(self, scenario: Scenario, run_dir: Path) -> dict[str, str]:
		extra = {"PGPASSWORD": pg.PASSWORD}
		if scenario.group == "snowflake":
			extra.update(_caller_env(SNOWFLAKE_ENV))
		return self.env(run_dir, extra)

	def _project_dir(self, scenario: Scenario, context: Context, run_dir: Path) -> Path:
		project = run_dir / "project"
		if not project.exists():
			project.mkdir(parents=True)
			(project / "sqlload.toml").write_text(self.project(scenario, context, run_dir))
			if scenario.group == "python":
				(project / "sources").mkdir()
				(project / "sources" / "bench.py").write_text(self._module(scenario, context))
		return project

	def project(self, scenario: Scenario, context: Context, run_dir: Path) -> str:
		"""The `sqlload.toml` of this load."""
		if scenario.name == "files_snowflake":
			return self._files_snowflake(scenario, context)
		if scenario.source:
			assert context.warehouse is not None
			stage = context.warehouse.stage_toml() if self.external else ""
			return (
				context.warehouse.connection_toml(stage) + "\n" + self._postgres(scenario, context)
				+ f'\n[sources.app.tables."public.{scenario.source}"]\nname = "{scenario.table}"\n'
			)
		text = (
			f'[connections.wh]\nkind = "duckdb"\npath = "{self.destination(run_dir)}"\n\n'
			f'[targets.dev]\nconnection = "wh"\nloader_schema = "{SCHEMA}"\n'
		)
		if scenario.group == "postgres":
			return text + "\n" + self._postgres(scenario, context)
		if scenario.group == "files":
			file_format = scenario.name.removeprefix("files_")
			return text + (
				f'\n[connections.files]\nkind = "filesystem"\nroot = "{context.dataset.path("files")}"\n\n'
				f'[sources.{scenario.table}]\nkind = "files"\nconnection = "files"\npath = "{file_format}/"\nformat = "{file_format}"\n'
			)
		return 'python_paths = ["sources"]\n\n' + text

	@staticmethod
	def _postgres(scenario: Scenario, context: Context) -> str:
		assert context.postgres is not None
		include = "public.multi_*" if scenario.name == "pg_multi" else f"public.{scenario.source or scenario.table}"
		text = (
			f'[connections.app_pg]\nkind = "postgres"\nhost = "127.0.0.1"\nport = {context.postgres.port}\n'
			f'user = "{pg.USER}"\npassword = "${{PGPASSWORD}}"\ndatabase = "{pg.DATABASE}"\n\n'
			f'[sources.app]\nkind = "postgres"\nconnection = "app_pg"\ninclude = ["{include}"]\n'
		)
		if scenario.name == "pg_incremental":
			# `id` only grows, so the next load starts after the loaded maximum rather than at it.
			text += 'write_strategy = "append"\ncursor_column = "id"\nappend_cursor_inclusive = false\n'
		return text

	def _files_snowflake(self, scenario: Scenario, context: Context) -> str:
		settings = context.warehouse
		assert settings is not None
		shaping = 'shaping = "loader"\n' if self.shaping == "loader" else ""
		key = snowflake.files_key(context.dataset)
		return settings.connection_toml(settings.stage_toml()) + (
			f'\n[connections.lake]\nkind = "s3"\nbucket = "{settings.bucket}"\nregion = "{settings.region}"\n\n'
			f'[sources.files]\nkind = "files"\nconnection = "lake"\npath = "{key}"\nformat = "parquet"\n'
			f'table_name = "{scenario.table}"\nwrite_strategy = "append"\n{shaping}'
		)

	@staticmethod
	def _module(scenario: Scenario, context: Context) -> str:
		rows, seed, data = scenario.rows(context.dataset.sizes), context.dataset.seed, context.dataset.root
		body = {
			"py_flat": f"yield from pydata.pages({rows}, {seed}, 'flat')",
			"py_nested": f"yield from pydata.pages({rows}, {seed}, 'nested')",
			"py_bytes": f"for chunk in pydata.ndjson_chunks('{data}/python/flat.ndjson'):\n        yield sl.ndjson(chunk)",
			"py_arrow": f"import pyarrow.parquet as pq\n    yield pq.read_table('{data}/python/flat.parquet')",
		}[scenario.name]
		return (
			"import sqlload as sl\n\nfrom sqlload_bench import pydata\n\n\n"
			f"@sl.table(name='{scenario.table}', write_strategy='append')\n"
			f"def rows(ctx):\n    {body}\n"
		)


class Dlt(Competitor):
	"""dlt; `backend` picks the sql_database backend (pyarrow or connectorx)."""

	def __init__(self, backend: str | None) -> None:
		self.backend = backend
		self.name = f"dlt ({backend})" if backend else "dlt"

	def unsupported(self, scenario: Scenario) -> str | None:
		if scenario.name == "pg_multi":
			return HIDDEN
		if scenario.group == "postgres" or scenario.source:
			return None if self.backend else HIDDEN
		if self.backend or scenario.name == "files_snowflake":
			return HIDDEN
		if scenario.name == "py_nested_flatten":
			return "dlt flattens by default (py_nested)"
		if scenario.group == "http":
			return "pending with sqlload's HTTP helpers"
		return None

	def relation(self, scenario: Scenario) -> str:
		return f"{SCHEMA}.{scenario.table}"

	def child_table(self, scenario: Scenario) -> str | None:
		"""The child table dlt makes of the nested rows' `lines`."""
		return f"{SCHEMA}.nested__lines" if scenario.name == "py_nested" else None

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		settings: dict[str, object] = {"backend": self.backend or ""}
		env = {"RUNTIME__DLTHUB_TELEMETRY": "false"}
		if context.warehouse is not None and scenario.group == "snowflake":
			sf = context.warehouse
			settings["snowflake"] = {
				"username": sf.user, "host": sf.account, "warehouse": sf.warehouse, "database": sf.database, "role": sf.role, "schema": sf.schema,
				"private_key_path": sf.private_key_file,
			}
			env.update(_caller_env((snowflake.TOKEN_ENV,)))
		spec = _spec(run_dir, scenario, context, **settings)
		return Invocation([str(DLT_PYTHON), str(RUNNERS / "dlt_run.py"), spec], run_dir, self.env(run_dir, env))


class Ingestr(Competitor):
	"""ingestr's release binary, through its pinned PyPI wrapper."""

	name = "ingestr"

	def unsupported(self, scenario: Scenario) -> str | None:
		if scenario.name == "pg_multi" or scenario.group == "python":
			return HIDDEN
		if scenario.group == "http":
			return "pending with sqlload's HTTP helpers"
		return None

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		env = {"INGESTR_BINARY_CACHE_DIR": str(context.cache / "ingestr"), "INGESTR_DISABLE_TELEMETRY": "true"}
		argv = [str(INGESTR), "ingest", "--yes", "--progress", "log", "--pipelines-dir", str(run_dir / "pipelines")]
		if scenario.group == "snowflake":
			assert context.warehouse is not None
			# The URI holds a secret, so it goes through ingestr's environment variable.
			env["INGESTR_DESTINATION_URI"] = context.warehouse.ingestr_uri()
			argv += ["--dest-table", f"{context.warehouse.schema}.{scenario.table}"]
		else:
			argv += ["--dest-uri", f"duckdb:///{self.destination(run_dir)}", "--dest-table", f"{SCHEMA}.{scenario.table}"]
		if scenario.name == "files_snowflake":
			assert context.warehouse is not None
			# Its S3 source takes the keys in the URI, so it goes through the environment too.
			keys = {
				"access_key_id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
				"secret_access_key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
				"region": context.warehouse.region,
			}
			env["INGESTR_SOURCE_URI"] = "s3://?" + urlencode(keys)
			argv += ["--source-table", f"{context.warehouse.bucket}/{snowflake.files_key(context.dataset)}*.parquet"]
		elif scenario.group == "snowflake" or scenario.group == "postgres":
			assert context.postgres is not None
			argv += ["--source-uri", context.postgres.dsn, "--source-table", f"public.{scenario.source or scenario.table}"]
			if scenario.name == "pg_incremental":
				# ingestr keeps no high-water mark: the caller passes the interval. The harness gives
				# the first appended row's created_at, which every base row is below.
				argv += ["--incremental-strategy", "append", "--incremental-key", "created_at"]
				if second:
					argv += ["--interval-start", context.dataset.first_appended_at()]
		elif scenario.name == "files_parquet":
			argv += ["--source-uri", f"parquet://{context.dataset.path('files', 'parquet')}/*.parquet", "--source-table", scenario.table]
		else:
			# Its csv:// and jsonl:// sources read one file (no globs or folders), so ingestr reads the
			# same rows concatenated into one file.
			file_format = scenario.name.removeprefix("files_")
			scheme = {"ndjson": "jsonl"}.get(file_format, file_format)
			argv += ["--source-uri", f"{scheme}://{context.dataset.path('files', 'single', 'narrow.' + file_format)}", "--source-table", scenario.table]
		return Invocation(argv, run_dir, self.env(run_dir, env))


def all_competitors() -> list[Competitor]:
	return [
		SqlLoad(),
		SqlLoad(threads=1, label="--threads 1"),
		SqlLoad(shaping="direct", label="direct"),
		SqlLoad(shaping="loader", label='shaping = "loader"'),
		SqlLoad(external=True, label="external stage"),
		Dlt("pyarrow"),
		Dlt("connectorx"),
		Dlt(None),
		Ingestr(),
	]
