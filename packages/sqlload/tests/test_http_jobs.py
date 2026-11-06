"""sl.http jobs end to end: a table function returns a job, Rust runs it against a local stub API, and the rows load into DuckDB."""

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

ORDERS = [{"order_id": n, "status": "open" if n % 2 else "closed"} for n in range(1, 8)]
ITEMS = {n: [{"order_id": n, "line": line} for line in range(n % 3)] for n in range(1, 8)}
TOKEN = "s3cret-token"


class Api(BaseHTTPRequestHandler):
	"""A stub shop API; `self.server.log` records every request's path and auth header."""

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		url = urlparse(self.path)
		query = {key: values[0] for key, values in parse_qs(url.query).items()}
		self.server.log.append((url.path, query, self.headers.get("Authorization")))  # type: ignore[attr-defined]
		if self.headers.get("Authorization") != f"Bearer {TOKEN}" and query.get("key") != TOKEN:
			return self.reply(401, {"message": f"bad token {self.headers.get('Authorization')}"})
		route = {
			"/v1/next": self.next_link,
			"/v1/offset": self.offset,
			"/v1/pages": self.page_number,
			"/v1/cursor": self.cursor,
			"/v1/link": self.link,
			"/v1/flaky": self.flaky,
			"/v1/broken": lambda query: self.reply(500, {"message": f"down, token {TOKEN}"}),
		}.get(url.path)
		if route is not None:
			return route(query)
		parts = url.path.split("/")
		if len(parts) == 5 and parts[2] == "orders" and parts[4] == "items":
			order = int(parts[3])
			return self.reply(404, {}) if order == 4 else self.reply(200, {"items": ITEMS[order]})
		return self.reply(404, {"message": "no route"})

	def reply(self, status: int, body: Any, headers: dict[str, str] | None = None) -> None:
		data = json.dumps(body).encode()
		self.send_response(status)
		self.send_header("Content-Type", "application/json")
		self.send_header("Content-Length", str(len(data)))
		for name, value in (headers or {}).items():
			self.send_header(name, value)
		self.end_headers()
		self.wfile.write(data)

	def next_link(self, query: dict[str, str]) -> None:
		page = int(query.get("page", "0"))
		rows = ORDERS[page * 3 : page * 3 + 3]
		more = (page + 1) * 3 < len(ORDERS)
		self.reply(200, {"data": {"items": rows}, "next": f"/v1/next?page={page + 1}" if more else None})

	def offset(self, query: dict[str, str]) -> None:
		start, limit = int(query["offset"]), int(query["limit"])
		self.reply(200, ORDERS[start : start + limit])

	def page_number(self, query: dict[str, str]) -> None:
		page = int(query["p"])
		self.reply(200, {"items": ORDERS[(page - 1) * 3 : page * 3]})

	def cursor(self, query: dict[str, str]) -> None:
		start = int(query.get("after", "0"))
		rows = ORDERS[start : start + 4]
		self.reply(200, {"items": rows, "meta": {"next": str(start + 4) if start + 4 < len(ORDERS) else ""}})

	def link(self, query: dict[str, str]) -> None:
		page = int(query.get("page", "0"))
		more = page == 0
		headers = {"Link": f'<{self.server_url()}/v1/link?page=1>; rel="next"'} if more else {}
		self.reply(200, ORDERS[page * 4 : page * 4 + 4], headers)

	def flaky(self, query: dict[str, str]) -> None:
		self.server.flaky += 1  # type: ignore[attr-defined]
		if self.server.flaky <= 2:  # type: ignore[attr-defined]
			return self.reply(429 if self.server.flaky == 1 else 503, {}, {"Retry-After": "0"})  # type: ignore[attr-defined]
		self.reply(200, ORDERS[:2])

	def server_url(self) -> str:
		host, port = self.server.server_address[:2]  # type: ignore[misc]
		return f"http://{host}:{port}"


@pytest.fixture
def api() -> Iterator[ThreadingHTTPServer]:
	server = ThreadingHTTPServer(("127.0.0.1", 0), Api)
	server.log = []  # type: ignore[attr-defined]
	server.flaky = 0  # type: ignore[attr-defined]
	thread = threading.Thread(target=server.serve_forever, daemon=True)
	thread.start()
	yield server
	server.shutdown()


def make_project(tmp_path: Path, server: ThreadingHTTPServer, tables: str) -> Path:
	host, port = server.server_address[:2]
	(tmp_path / "sources").mkdir()
	(tmp_path / "sqlload.toml").write_text(
		textwrap.dedent(f"""\
			python_paths = ["sources"]

			[connections.wh]
			kind = "duckdb"
			database = "wh.duckdb"

			[connections.shop]
			kind = "http"
			base_url = "http://{host}:{port}/v1"
			token = "${{ENV:SHOP_TOKEN}}"

			[targets.dev]
			connection = "wh"
			loader_schema = "RAW"
		""")
	)
	module = "import sqlload as sl\n\nshop = sl.source('shop', connection='shop')\napi = sl.http(shop, auth=sl.bearer(), retries=3)\n\n" + textwrap.dedent(tables)
	(tmp_path / "sources" / "shop.py").write_text(module)
	return tmp_path


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "SHOP_TOKEN": TOKEN}
	return subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", *args], cwd=project, env=env, capture_output=True, text=True, timeout=120
	)


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(sql).fetchall()


ALL_ORDERS = [(row["order_id"], row["status"]) for row in ORDERS]


@pytest.mark.parametrize(
	("job", "expected_rows"),
	[
		("api.paginate('/next', paginator=sl.next_link('$.next'), select='$.data.items')", ALL_ORDERS),
		("api.paginate('/offset', paginator=sl.offset(limit=3))", ALL_ORDERS),
		("api.paginate('/pages', paginator=sl.page_number('p'), select='$.items')", ALL_ORDERS),
		("api.paginate('/cursor', paginator=sl.cursor('$.meta.next', 'after'), select='$.items')", ALL_ORDERS),
		("api.paginate('/link', paginator=sl.link_header())", ALL_ORDERS),
		("api.paginate('/flaky', paginator=sl.single())", ALL_ORDERS[:2]),
	],
	ids=["next-link", "offset", "page-number", "cursor", "link-header", "retries-429-and-503"],
)
def test_given_a_paginated_endpoint_when_a_table_returns_its_job_then_every_page_loads(
	tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_rows: list[tuple[Any, ...]]
) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='Orders.')\ndef orders(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__orders ORDER BY order_id') == expected_rows
	assert all(auth == f"Bearer {TOKEN}" for _, _, auth in api.log)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
	"over",
	["api.paginate('/next', paginator=sl.next_link('$.next'), select='$.data.items[*].order_id')", "range(1, 8)"],
	ids=["over-a-paginate", "over-values"],
)
def test_given_a_fan_out_when_loading_then_each_key_is_requested_and_a_skipped_status_gives_no_rows(
	tmp_path: Path, api: ThreadingHTTPServer, over: str
) -> None:
	tables = f"@shop.table(description='Items.')\ndef items(ctx):\n    yield from api.fan_out('/orders/{{order_id}}/items', over={over}, concurrency=3, select='$.items', on_status={{404: 'skip'}})\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	expected = sorted((n, line) for n, lines in ITEMS.items() if n != 4 for line in range(len(lines)))
	assert query(project, 'SELECT order_id, line FROM "RAW".shop__items ORDER BY 1, 2') == expected
	requested = sorted(int(path.split("/")[3]) for path, _, _ in api.log if path.endswith("/items"))  # type: ignore[attr-defined]
	assert requested == list(range(1, 8))


@pytest.mark.parametrize(
	("tables", "expected_text"),
	[
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/broken', paginator=sl.single())\n",
			"/v1/broken: HTTP 500: {\"message\": \"down, token ***\"} (after 4 attempts)",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/missing', paginator=sl.single())\n",
			"HTTP 404: {\"message\": \"no route\"}; list the status in on_status to skip it",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/orders/{id}', paginator=sl.single(), select='items')\n",
			"select \"items\" is not a path such as",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    for order in api.paginate('/next', paginator=sl.next_link('$.next')):\n        if order['status'] == 'open':\n            yield order\n",
			"rows of an sl.http job stay in Rust and are never handed to Python one at a time (rule python_row_loop)",
		),
	],
	ids=["server-error-retried-and-redacted", "unlisted-status-fails", "bad-definition-fails-before-requests", "python-row-loop"],
)
def test_given_a_failing_job_when_loading_then_the_error_names_the_request_and_hides_secrets(
	tmp_path: Path, api: ThreadingHTTPServer, tables: str, expected_text: str
) -> None:
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	assert TOKEN not in done.stdout + done.stderr


def test_given_a_bad_definition_when_loading_then_no_request_is_sent(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api, "@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/next', paginator=sl.single(), select='$.a[*]')\n")
	assert sqlload(project, "load").returncode == 1
	assert api.log == []  # type: ignore[attr-defined]


def test_given_an_api_key_in_the_query_when_loading_then_it_is_sent_only_to_the_connection(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	tables = "keyed = sl.http(shop, auth=sl.api_key(query='key'))\n\n@shop.table(description='T.')\ndef t(ctx):\n    return keyed.paginate('/offset', paginator=sl.offset(limit=10))\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert [(path, query.get("key"), auth) for path, query, auth in api.log] == [("/v1/offset", TOKEN, None)]  # type: ignore[attr-defined]
