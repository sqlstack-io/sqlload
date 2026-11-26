"""A host of its own on SQLLoad's engine: tables declared its way, its data reaching each table's context, and SQLLoad's
`sqlload_api.http` jobs and page transforms, through `sqlload-python-host`."""

import json
import os
import subprocess
import sys
import textwrap
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

#: The orders the stub API serves, two a page.
ORDERS = [{"order_id": n, "status": "open"} for n in range(1, 5)]

#: Run the example host's `sqlload` with this script's arguments.
RUN = "import sys; from example_host._native import run; sys.exit(run(['sqlload', *sys.argv[1:]]))"


class Api(BaseHTTPRequestHandler):
	"""`/v1/pages?p=N`: the orders, two a page, then an empty page."""

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		page = int(parse_qs(urlparse(self.path).query).get("p", ["1"])[0])
		body = json.dumps({"items": ORDERS[(page - 1) * 2 : page * 2]}).encode()
		self.send_response(200)
		self.send_header("Content-Type", "application/json")
		self.end_headers()
		self.wfile.write(body)


@pytest.fixture
def api() -> Iterator[ThreadingHTTPServer]:
	server = ThreadingHTTPServer(("127.0.0.1", 0), Api)
	threading.Thread(target=server.serve_forever, daemon=True).start()
	yield server
	server.shutdown()


def make_project(tmp_path: Path, api: ThreadingHTTPServer) -> Path:
	host, port = api.server_address[:2]
	(tmp_path / "models").mkdir()
	(tmp_path / "sqlload.toml").write_text(
		textwrap.dedent(f"""\
			python_paths = ["models"]

			[connections.wh]
			kind = "duckdb"
			database = "wh.duckdb"

			[connections.shop]
			kind = "http"
			base_url = "http://{host}:{port}/v1"

			[targets.dev]
			connection = "wh"
			loader_schema = "RAW"
		""")
	)
	(tmp_path / "models" / "shop.py").write_text(
		textwrap.dedent("""\
			import sqlload_api as sl
			from example_host import model

			api = sl.http(connection="shop")


			@sl.transform
			def tag_page(rows, ctx):
			    return [{**row, "page": ctx.page} for row in rows.to_pylist()]


			@model("shop")
			def orders(ctx):
			    return api.paginate("/pages", paginator=sl.page_number("p"), select="$.items", transform=tag_page)


			@model("shop")
			def models(ctx):
			    yield {"model": ctx.model, "source": ctx.source, "kind": type(ctx).__name__}
		""")
	)
	return tmp_path


def sqlload(project: Path, *args: str, before: str = "") -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config")}
	return subprocess.run([sys.executable, "-c", before + RUN, *args], cwd=project, env=env, capture_output=True, text=True, timeout=120)


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(sql).fetchall()


def test_given_a_host_of_its_own_when_loading_then_its_tables_jobs_and_transforms_load(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'select order_id, status, page from "RAW".shop__orders order by order_id') == [(1, "open", 1), (2, "open", 1), (3, "open", 2), (4, "open", 2)]
	assert query(project, 'select model, source, kind from "RAW".shop__models') == [("shop.models", "shop", "ExampleContext")]


def test_given_another_sqlload_api_when_loading_then_the_host_says_which_to_install(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api)
	done = sqlload(project, "load", before="import sqlload_api; sqlload_api.__version__ = '0.0.1'; ")
	expected = f'sqlload-api 0.0.1 is installed, but this engine reads sqlload-api {_engine_version()}: pip install "sqlload-api=={_engine_version()}"'
	assert (done.returncode != 0, expected in done.stdout + done.stderr) == (True, True), done.stdout + done.stderr


def _engine_version() -> str:
	from importlib.metadata import version

	return version("sqlload-api")


if __name__ == "__main__":
	pytest.main([__file__, "-vv"])
