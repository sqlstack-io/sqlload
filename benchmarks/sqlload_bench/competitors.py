"""The loaders compared, and how each one runs a scenario as a process into a fresh DuckDB file."""

import json
import textwrap
from dataclasses import dataclass, field
from pathlib import Path

from sqlload_bench import postgres as pg
from sqlload_bench.datagen import Dataset
from sqlload_bench.measure import clean_env
from sqlload_bench.scenarios import Scenario

BENCH_ROOT = Path(__file__).resolve().parent.parent
RUNNERS = BENCH_ROOT / "runners"
DLT_PYTHON = BENCH_ROOT / "envs/dlt/.venv/bin/python"
INGESTR_PYTHON = BENCH_ROOT / "envs/ingestr/.venv/bin/python"
INGESTR = BENCH_ROOT / "envs/ingestr/.venv/bin/ingestr"
HARNESS_PYTHON = BENCH_ROOT / ".venv/bin/python"
SQLLOAD = BENCH_ROOT / ".venv/bin/sqlload"
SCHEMA = "raw"
#: `unsupported` returns this for combinations not worth a table row (a dlt backend outside sql_database).
HIDDEN = "hidden"


@dataclass
class Context:
	"""What every run needs: the data, the database and shared caches."""

	dataset: Dataset
	postgres: pg.Postgres | None
	cache: Path


@dataclass
class Invocation:
	"""One process to run (and time) for a load."""

	argv: list[str]
	cwd: Path
	env: dict[str, str] = field(default_factory=dict)


class Competitor:
	"""A loader; subclasses say which scenarios they run and how."""

	name: str = ""
	version: str = ""

	def unsupported(self, scenario: Scenario) -> str | None:
		"""Why this loader cannot run `scenario`, or None when it can."""
		raise NotImplementedError

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		"""The load to run; `second` is the incremental scenario's load after the append."""
		raise NotImplementedError

	def tables(self, scenario: Scenario) -> list[str]:
		"""The destination table the checksum reads, then any child tables."""
		return [f"{SCHEMA}.{scenario.table}"]

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
	"""The sqlload extension wheel built from this repository; `sqlload load` runs in-process."""

	name = "sqlload"

	def unsupported(self, scenario: Scenario) -> str | None:
		return scenario.pending or None

	def tables(self, scenario: Scenario) -> list[str]:
		if scenario.group == "postgres":
			return [f"{SCHEMA}.app__{scenario.table}"]
		return [f"{SCHEMA}.{scenario.table}"]

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		project = run_dir / "project"
		if not project.exists():
			project.mkdir(parents=True)
			(project / "sqlload.toml").write_text(self._project(scenario, context, run_dir))
			if scenario.group == "python":
				(project / "sources").mkdir()
				(project / "sources" / "bench.py").write_text(self._module(scenario, context))
		return Invocation([str(SQLLOAD), "load"], project, self.env(run_dir, {"PGPASSWORD": pg.PASSWORD}))

	def _project(self, scenario: Scenario, context: Context, run_dir: Path) -> str:
		text = textwrap.dedent(
			f"""\
			[connections.wh]
			kind = "duckdb"
			path = "{self.destination(run_dir)}"

			[targets.dev]
			connection = "wh"
			loader_schema = "{SCHEMA}"
			"""
		)
		if scenario.group == "postgres":
			assert context.postgres is not None
			# `id` only grows, so the next load starts after the loaded maximum rather than at it.
			incremental = (
				'write_strategy = "append"\ncursor_column = "id"\nappend_cursor_inclusive = false\n' if scenario.name == "pg_incremental" else ""
			)
			text += textwrap.dedent(
				f"""
				[connections.app_pg]
				kind = "postgres"
				host = "127.0.0.1"
				port = {context.postgres.port}
				user = "{pg.USER}"
				password = "${{PGPASSWORD}}"
				database = "{pg.DATABASE}"

				[sources.app]
				kind = "postgres"
				connection = "app_pg"
				include = ["public.{scenario.table}"]
				"""
			)
			text += incremental
		elif scenario.group == "files":
			file_format = scenario.name.removeprefix("files_")
			text += textwrap.dedent(
				f"""
				[connections.files]
				kind = "filesystem"
				root = "{context.dataset.path("files")}"

				[sources.{scenario.table}]
				kind = "files"
				connection = "files"
				path = "{file_format}/"
				format = "{file_format}"
				"""
			)
		else:
			text = 'python_paths = ["sources"]\n\n' + text
		return text

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
	"""dlt with its DuckDB destination; `backend` picks the sql_database backend."""

	def __init__(self, backend: str | None) -> None:
		self.backend = backend
		self.name = f"dlt ({backend})" if backend else "dlt"

	def unsupported(self, scenario: Scenario) -> str | None:
		if scenario.group == "postgres":
			return None if self.backend else HIDDEN
		if self.backend:
			return HIDDEN
		if scenario.name == "py_bytes":
			return "dlt has no NDJSON bytes input; it takes Python objects or Arrow"
		if scenario.name == "py_nested_flatten":
			return "dlt flattens by default (py_nested)"
		if scenario.group == "http":
			return "pending with sqlload's HTTP helpers"
		return None

	def tables(self, scenario: Scenario) -> list[str]:
		if scenario.name == "py_nested":
			return [f"{SCHEMA}.nested", f"{SCHEMA}.nested__lines"]
		return [f"{SCHEMA}.{scenario.table}"]

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		spec = _spec(run_dir, scenario, context, backend=self.backend or "")
		env = self.env(run_dir, {"RUNTIME__DLTHUB_TELEMETRY": "false"})
		return Invocation([str(DLT_PYTHON), str(RUNNERS / "dlt_run.py"), spec], run_dir, env)


class Ingestr(Competitor):
	"""ingestr's release binary (through its pinned PyPI wrapper); Python data via its Arrow SDK."""

	name = "ingestr"

	def unsupported(self, scenario: Scenario) -> str | None:
		if scenario.name == "py_bytes":
			return "ingestr's Python SDK takes rows and Arrow, not NDJSON bytes"
		if scenario.name == "py_nested_flatten":
			return "no flattening option"
		if scenario.group == "http":
			return "pending with sqlload's HTTP helpers"
		return None

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		env = self.env(run_dir, {"INGESTR_BINARY_CACHE_DIR": str(context.cache / "ingestr"), "INGESTR_DISABLE_TELEMETRY": "true"})
		if scenario.group == "python":
			spec = _spec(run_dir, scenario, context)
			return Invocation([str(INGESTR_PYTHON), str(RUNNERS / "ingestr_python.py"), spec], run_dir, env)
		argv = [
			str(INGESTR), "ingest", "--yes", "--progress", "log", "--pipelines-dir", str(run_dir / "pipelines"),
			"--dest-uri", f"duckdb:///{self.destination(run_dir)}", "--dest-table", f"{SCHEMA}.{scenario.table}",
		]
		if scenario.group == "postgres":
			assert context.postgres is not None
			argv += ["--source-uri", context.postgres.dsn, "--source-table", f"public.{scenario.table}"]
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
		return Invocation(argv, run_dir, env)


class GenerateOnly(Competitor):
	"""Not a loader: exhausts the same Python generator, the floor every Python scenario includes."""

	name = "python (generate only)"

	def unsupported(self, scenario: Scenario) -> str | None:
		return None if scenario.name in {"py_flat", "py_nested", "py_bytes", "py_arrow"} else HIDDEN

	def tables(self, scenario: Scenario) -> list[str]:
		return []

	def invocation(self, scenario: Scenario, context: Context, run_dir: Path, *, second: bool = False) -> Invocation:
		spec = _spec(run_dir, scenario, context)
		return Invocation([str(HARNESS_PYTHON), str(RUNNERS / "generate_only.py"), spec], run_dir, self.env(run_dir))


def all_competitors() -> list[Competitor]:
	return [SqlLoad(), Dlt("pyarrow"), Dlt("connectorx"), Dlt(None), Ingestr(), GenerateOnly()]
