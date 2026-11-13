"""sl.http jobs end to end: a table function returns a job, Rust runs it against a local stub API, and the rows load into DuckDB."""

import json
import os
import subprocess
import sys
import textwrap
import threading
import time
from base64 import b64decode
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote_plus, urlparse

import duckdb
import pytest

ORDERS = [{"order_id": n, "status": "open" if n % 2 else "closed"} for n in range(1, 8)]
ITEMS = {n: [{"order_id": n, "line": line} for line in range(n % 3)] for n in range(1, 8)}
TOKEN = "s3cret-token"
CLIENT_SECRET = "cl1ent+secret/x~y"
REFRESH_TOKEN = "r3fresh-first"
PASSWORD = "pa55+word/x"


#: A later page of Google grid data holding only a blank row, then the end.
GRID_BLANK_PAGE = {"sheets": [{"data": [{"rowData": [{}]}]}]}
GRID_END = {"sheets": [{"data": [{}]}]}
SHEET_ROWS = [
	{"id": 1, "rowNumber": 1, "cells": [{"columnId": 7001, "value": "Bolt"}, {"columnId": 7002, "value": 3}, {"columnId": 7003, "value": 0.25}, {"columnId": 7004, "value": True}]},
	{"id": 2, "rowNumber": 2, "cells": [{"columnId": 7003, "value": 1.5}, {"columnId": 7001, "value": "Nut"}]},
	{"id": 3, "rowNumber": 3, "cells": [{"columnId": 7001, "value": "Washer"}, {"columnId": 7002, "value": 12}, {"columnId": 7004, "value": False}]},
	{"id": 4, "rowNumber": 4, "cells": []},
	{"id": 5, "rowNumber": 5, "cells": [{"columnId": 7001, "value": "Gear"}, {"columnId": 7002, "displayValue": 4}, {"columnId": 7003, "value": 9.0}]},
]
GRID = {
	"spreadsheetId": "synthetic",
	"sheets": [
		{
			"data": [
				{
					"rowData": [
						{"values": [{"effectiveValue": {"stringValue": "Name"}}, {"effectiveValue": {"stringValue": "Qty"}}, {"effectiveValue": {"stringValue": "Done"}}, {"userEnteredFormat": {}}]},
						{"values": [{"effectiveValue": {"stringValue": "Bolt"}}, {"effectiveValue": {"numberValue": 3}}, {"effectiveValue": {"boolValue": True}}, {"userEnteredFormat": {}}]},
						{"values": [{"effectiveValue": {"stringValue": "Nut"}}, {}, {"effectiveValue": {"boolValue": False}}]},
						{"values": [{"effectiveValue": {"stringValue": "Washer"}}]},
					]
				}
			]
		}
	],
}


class Api(BaseHTTPRequestHandler):
	"""A stub shop API; `self.server.log` records every request's path and auth header."""

	def lookup(self, path: str, body: dict[str, Any]) -> None:
		"""The items of the orders a body names, by `$.ids` (a list) or `$.order.id`; `/v1/lookup-flaky` answers 503 the first time a batch holds order 4."""
		ids = body["ids"] if "ids" in body else [body["order"]["id"]]
		if path.endswith("-flaky") and 4 in ids and not self.server.lookup_failed:  # type: ignore[attr-defined]
			self.server.lookup_failed = True  # type: ignore[attr-defined]
			return self.reply(503, {"message": "busy"})
		self.reply(200, {"items": [item for order in ids for item in ITEMS[order]]})

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		url = urlparse(self.path)
		query = {key: values[0] for key, values in parse_qs(url.query).items()}
		self.server.log.append((url.path, query, self.headers.get("Authorization")))  # type: ignore[attr-defined]
		bearer = self.headers.get("Authorization", "").removeprefix("Bearer ")
		if TOKEN not in (bearer, query.get("key"), self.headers.get("X-Api-Key")) and not self.access_allowed(bearer):
			self.server.refused += 1  # type: ignore[attr-defined]
			return self.reply(401, {"message": f"bad token {self.headers.get('Authorization')}"})
		route = {
			"/v1/next": self.next_link,
			"/v1/offset": self.offset,
			"/v1/pages": self.page_number,
			"/v1/cursor": self.cursor,
			"/v1/link": self.link,
			"/v1/flaky": self.flaky,
			"/v1/broken": lambda query: self.reply(500, {"message": f"down, token {TOKEN}"}),
			"/v1/long": lambda query: self.reply(400, {"message": "x" * 280 + f" token {TOKEN} and {TOKEN}"}),
			"/v1/redir": lambda query: self.reply(302, {}, {"Location": f"{self.server.other}/steal"}),  # type: ignore[attr-defined]
			"/v1/away": lambda query: self.reply(200, {"items": ORDERS[:1], "next": f"{self.server.other}/steal"}),  # type: ignore[attr-defined]
			"/v1/cycle": lambda query: self.reply(200, {"items": [], "next": "B" if query.get("c") == "A" else "A"}),
			"/v1/wrapped": lambda query: self.reply(200, {"items": ORDERS[:2]}),
			"/v1/gap": lambda query: self.reply(200, {"items": ORDERS[:3]} if query["p"] == "1" else {}),
			"/v1/comma": self.comma,
			"/v1/sheet": self.sheet,
			"/v1/grid": lambda query: self.reply(200, [GRID, GRID_BLANK_PAGE, GRID_END][int(query["page"]) - 1]),
			"/v1/moved": lambda query: self.reply(302, {}, {"Location": "/v1/landed/"}),
			"/v1/landed/": lambda query: self.reply(200, {"items": ORDERS[:3], "next": "more"}),
			"/v1/landed/more": lambda query: self.reply(200, {"items": ORDERS[3:], "next": None}),
			"/v1/sparse": self.sparse,
			"/v1/slow": self.slow,
			"/v1/away401": lambda query: self.reply(200, {"items": ORDERS[:1], "next": f"{self.server.other}/denied"}),  # type: ignore[attr-defined]
			"/v1/leak": lambda query: self.reply(200, {"items": ORDERS[:1], "next": f"/v1/notjson?access_token={bearer}"}),
			"/v1/notjson": lambda query: self.reply_text("<html>maintenance</html>"),
			"/v1/denied": lambda query: self.reply(401, {"message": "not your shop"}),
			"/v1/whoami": lambda query: self.reply(400, {"message": f"you are {self.headers.get('Authorization')}"}),
			"/v1/echo": lambda query: self.reply(400, {"message": f"version {self.headers.get('X-Version')} session {self.headers.get('X-Session-Id')} credential {self.headers.get('X-Credential')}"}),
		}.get(url.path)
		if route is not None:
			return route(query)
		if url.path.startswith("/v1/tree/"):
			return self.tree(url.path, query)
		parts = url.path.split("/")
		if len(parts) == 5 and parts[2] == "orders" and parts[4] == "items":
			order = int(parts[3])
			return self.reply(404, {}) if order == 4 else self.reply(200, {"items": ITEMS[order]})
		return self.reply(404, {"message": "no route"})

	def do_POST(self) -> None:
		"""JSON search endpoints under `/v1/`, recording each body in `self.server.bodies`; any other path is the OAuth2 token endpoint."""
		if self.path.startswith("/v1/"):
			return self.search()
		self.token()

	def search(self) -> None:
		"""Orders after `$.filter.after`, paged by `$.page.offset`/`$.page.limit` or by `$.page.cursor` (four at a time)."""
		body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
		url = urlparse(self.path)
		self.server.bodies.append((url.path, ", ".join(self.headers.get_all("Content-Type") or []), body))  # type: ignore[attr-defined]
		if self.headers.get("Authorization") != f"Bearer {TOKEN}":
			return self.reply(401, {"message": "bad token"})
		moved = {
			"/v1/search-302": (302, "/v1/wrapped"),
			"/v1/search-307": (307, "/v1/search-cursor"),
			"/v1/search-away": (200, None),
		}.get(url.path)
		if moved == (200, None):
			return self.reply(200, {"items": ORDERS[:1], "next": f"{self.server.other}/steal"})  # type: ignore[attr-defined]
		if moved is not None:
			return self.reply(moved[0], {}, {"Location": moved[1]})
		if url.path in ("/v1/lookup", "/v1/lookup-flaky"):
			return self.lookup(url.path, body)
		after = body.get("filter", {}).get("after") or 0
		rows = [row for row in ORDERS if row["order_id"] > after]
		page = body.get("page", {})
		if url.path == "/v1/search":
			start, limit = page["offset"], page["limit"]
			return self.reply(200, {"items": rows[start : start + limit]})
		start = int(page.get("cursor") or 0)
		token: Any = start + 4 if url.path == "/v1/search-number" else str(start + 4)
		self.reply(200, {"items": rows[start : start + 4], "meta": {"next": token if start + 4 < len(rows) else None}})

	def token(self) -> None:
		"""The OAuth2 token endpoint: `self.server.oauth` sets the lifetime, uses before revocation and rotation of tokens it issues."""
		form = {key: values[0] for key, values in parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode()).items()}
		basic = self.headers.get("Authorization", "")
		pair = b64decode(basic.removeprefix("Basic ")).decode().split(":", 1) if basic.startswith("Basic ") else None
		client = [unquote_plus(part) for part in pair] if pair else [form.get("client_id"), form.get("client_secret")]
		server = self.server
		server.token_requests.append({**form, "client": client})  # type: ignore[attr-defined]
		oauth = server.oauth  # type: ignore[attr-defined]
		if client != ["app", CLIENT_SECRET] or form.get("grant_type") == "refresh_token" and form.get("refresh_token") != server.refresh_token:  # type: ignore[attr-defined]
			return self.reply(401, {"error": "invalid_client", "error_description": f"unknown client {client} or refresh token"})
		if form.get("grant_type") == "password" and (form.get("username"), form.get("password")) != ("loader", PASSWORD):
			return self.reply(400, {"message": f"wrong password {form.get('password')} for {form.get('username')}"})
		if form.get("grant_type") == "refresh_token" and oauth.get("refresh_503_once"):
			oauth["refresh_503_once"] = False
			return self.reply(503, {"error": "temporarily_unavailable"}, {"Retry-After": "0"})
		access = f"access-{len(server.token_requests)}"  # type: ignore[attr-defined]
		server.tokens[access] = {"issued": time.monotonic(), "uses": 0}  # type: ignore[attr-defined]
		body: dict[str, Any] = {"access_token": access, "token_type": "Bearer"}
		if oauth.get("lifetime"):
			body["expires_in"] = oauth["lifetime"]
		if oauth.get("rotate"):
			server.refresh_token = body["refresh_token"] = f"r3fresh-{len(server.token_requests) + 1}"  # type: ignore[attr-defined]
		if oauth.get("stale_refresh"):
			body["refresh_token"] = "stale"
		self.reply(200, body)

	def access_allowed(self, bearer: str) -> bool:
		"""Whether `bearer` is an access token the token endpoint issued that has neither expired nor been used up."""
		issued = self.server.tokens.get(bearer)  # type: ignore[attr-defined]
		oauth = self.server.oauth  # type: ignore[attr-defined]
		if issued is None:
			return False
		issued["uses"] += 1
		expired = oauth.get("lifetime") and time.monotonic() - issued["issued"] > oauth["lifetime"]
		used_up = oauth.get("uses") and issued["uses"] > oauth["uses"]
		return not (expired or used_up)

	def slow(self, query: dict[str, str]) -> None:
		time.sleep(0.5)
		page = int(query["p"])
		self.reply(200, {"items": ORDERS[page - 1 : page]})

	def reply_text(self, text: str) -> None:
		data = text.encode()
		self.send_response(200)
		self.send_header("Content-Length", str(len(data)))
		self.end_headers()
		self.wfile.write(data)

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

	def tree(self, path: str, query: dict[str, str]) -> None:
		"""Three levels: orders after `after`, three a page; order o's items `o*10+k` for k < o % 3 (none for 3 and 6, 404 for 4); an item's notes,
		two a page for pages 1 and 2, a 404 when the item is not the order's, and a 500 on item 51's page 2 with `boom`."""
		parts = path.split("/")[3:]
		if len(parts) == 1:
			page, after = int(query["p"]), int(query.get("after") or 0)
			return self.reply(200, {"items": [{"order_id": n} for n in range(after + 1, 8)][(page - 1) * 3 : page * 3]})
		order = int(parts[1])
		if len(parts) == 3:
			return self.reply(404, {"message": "no order"}) if order == 4 else self.reply(200, {"items": [{"item_id": order * 10 + k} for k in range(order % 3)]})
		item, page = int(parts[3]), int(query["page"])
		if item // 10 != order:
			return self.reply(404, {"message": f"item {item} is not order {order}'s"})
		if query.get("boom") and item == 51 and page == 2:
			return self.reply(500, {"message": "notes are down"})
		notes = [{"order_id": order, "item_id": item, "note": f"{item}-{page}-{n}"} for n in range(2)] if page <= 2 else []
		self.reply(200, {"items": notes})

	def sheet(self, query: dict[str, str]) -> None:
		"""A Smartsheet-shaped sheet, two rows a page: its columns listed on page 1 only, the Qty column titled `self.server.sheet_title`,
		and with `unknown` a cell of a column it does not list."""
		page = int(query["page"])
		columns = [{"id": 7001, "title": "Name"}, {"id": 7002, "title": self.server.sheet_title}, {"id": 7003, "title": "Price"}, {"id": 7004, "title": "Done"}]  # type: ignore[attr-defined]
		rows = [dict(row, cells=list(row["cells"])) for row in SHEET_ROWS[(page - 1) * 2 : page * 2]]
		if query.get("unknown") and rows and page == 2:
			rows[0]["cells"].append({"columnId": 7999, "value": "stray"})
		if query.get("twice") and rows and page == 1:
			rows[0]["cells"].append({"columnId": 7002, "value": 4})
		self.reply(200, {"columns": columns, "rows": rows} if page == 1 else {"rows": rows})

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

	def comma(self, query: dict[str, str]) -> None:
		page = int(query.get("page", "0"))
		headers = {"Link": f'<{self.server_url()}/v1/comma?fields=a,b&page=1>; rel=next'} if page == 0 else {}
		self.reply(200, ORDERS[page * 4 : page * 4 + 4], headers)

	def sparse(self, query: dict[str, str]) -> None:
		page = int(query.get("page", "0"))
		headers = {"Link": f'<{self.server_url()}/v1/sparse?page={page + 1}>; rel="next"'} if page < 2 else {}
		self.reply(200, ORDERS[:2] if page == 2 else [], headers)

	def flaky(self, query: dict[str, str]) -> None:
		self.server.flaky += 1  # type: ignore[attr-defined]
		if self.server.flaky <= 2:  # type: ignore[attr-defined]
			return self.reply(429 if self.server.flaky == 1 else 503, {}, {"Retry-After": "0"})  # type: ignore[attr-defined]
		self.reply(200, ORDERS[:2])

	def server_url(self) -> str:
		host, port = self.server.server_address[:2]  # type: ignore[misc]
		return f"http://{host}:{port}"


class Elsewhere(BaseHTTPRequestHandler):
	"""Another host; records the headers every request brought."""

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		self.server.seen.append(dict(self.headers))  # type: ignore[attr-defined]
		data = json.dumps([{"order_id": 99, "status": "stolen"}]).encode()
		self.send_response(401 if self.path == "/denied" else 200)
		self.send_header("Content-Length", str(len(data)))
		self.end_headers()
		self.wfile.write(data)

	do_POST = do_GET


def serve(handler: type[BaseHTTPRequestHandler]) -> ThreadingHTTPServer:
	server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
	threading.Thread(target=server.serve_forever, daemon=True).start()
	return server


@pytest.fixture
def elsewhere() -> Iterator[ThreadingHTTPServer]:
	server = serve(Elsewhere)
	server.seen = []  # type: ignore[attr-defined]
	yield server
	server.shutdown()


@pytest.fixture
def api(elsewhere: ThreadingHTTPServer) -> Iterator[ThreadingHTTPServer]:
	server = serve(Api)
	server.log = []  # type: ignore[attr-defined]
	server.bodies = []  # type: ignore[attr-defined]
	server.flaky = 0  # type: ignore[attr-defined]
	server.lookup_failed = False  # type: ignore[attr-defined]
	server.refused = 0  # type: ignore[attr-defined]
	server.oauth = {}  # type: ignore[attr-defined]
	server.tokens = {}  # type: ignore[attr-defined]
	server.token_requests = []  # type: ignore[attr-defined]
	server.refresh_token = REFRESH_TOKEN  # type: ignore[attr-defined]
	server.sheet_title = "Qty"  # type: ignore[attr-defined]
	host, port = elsewhere.server_address[:2]
	server.other = f"http://localhost:{port}"  # type: ignore[attr-defined]
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
			client_id = "app"
			client_secret = "${{ENV:SHOP_CLIENT_SECRET}}"
			refresh_token = "{REFRESH_TOKEN}"
			username = "loader"
			password = "${{ENV:SHOP_PASSWORD}}"

			[targets.dev]
			connection = "wh"
			loader_schema = "RAW"
		""")
	)
	module = f"import sqlload as sl\n\nTOKEN_URL = 'http://{host}:{port}/oauth/token'\nshop = sl.source('shop', connection='shop')\napi = sl.http(shop, auth=sl.bearer(), retries=3)\n\n" + textwrap.dedent(tables)
	(tmp_path / "sources" / "shop.py").write_text(module)
	return tmp_path


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "SHOP_TOKEN": TOKEN, "SHOP_CLIENT_SECRET": CLIENT_SECRET, "SHOP_PASSWORD": PASSWORD}
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
		("api.paginate('/comma', paginator=sl.link_header())", ALL_ORDERS),
		("api.paginate('/gap', paginator=sl.page_number('p'), select='$.items')", ALL_ORDERS[:3]),
		("api.paginate('/moved', paginator=sl.next_link('$.next'), select='$.items')", ALL_ORDERS),
		("api.paginate('/sparse', paginator=sl.link_header())", ALL_ORDERS[:2]),
	],
	ids=[
		"next-link",
		"offset",
		"page-number",
		"cursor",
		"link-header",
		"retries-429-and-503",
		"link-with-comma-unquoted-rel",
		"page-without-the-selected-key-ends",
		"same-origin-redirect-then-relative-next-link",
		"link-header-through-empty-pages",
	],
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
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/cycle', paginator=sl.cursor('$.next', 'c'), select='$.items')\n",
			"the paginator came back to a page it already read",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/wrapped', paginator=sl.page_number('p'))\n",
			"offset and page-number paging count the rows in a list, but select \"$\" is not one",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/long', paginator=sl.single())\n",
			"HTTP 400",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/wrapped', paginator=sl.page_number('p'), select='$.items')\n",
			"the response repeats the previous page",
		),
		(
			"versioned = sl.http(shop, auth=sl.bearer(), headers={'X-Version': '1', 'X-Session-Id': 'abcdef12', 'X-Credential': 'abc123secret'})\n\n@shop.table(description='T.')\ndef t(ctx):\n    return versioned.paginate('/echo', paginator=sl.single())\n",
			"version 1 session *** credential ***",
		),
	],
	ids=[
		"server-error-retried-and-redacted",
		"unlisted-status-fails",
		"bad-definition-fails-before-requests",
		"python-row-loop",
		"pagination-cycle",
		"page-number-over-an-object",
		"token-at-the-excerpt-cut",
		"page-number-api-ignoring-the-page",
		"only-secret-looking-headers-redacted",
	],
)
def test_given_a_failing_job_when_loading_then_the_error_names_the_request_and_hides_secrets(
	tmp_path: Path, api: ThreadingHTTPServer, tables: str, expected_text: str
) -> None:
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == (75 if "(after 4 attempts)" in expected_text else 1)
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	assert TOKEN[:4] not in done.stdout + done.stderr


@pytest.mark.parametrize(
	("path", "expected_exit", "expected_retryable"),
	[("/broken", 75, True), ("/missing", 1, False)],
	ids=["server-errors-outlast-the-retries", "a-status-that-fails-for-good"],
)
def test_given_a_failing_job_when_loading_with_json_then_the_exit_code_and_report_say_whether_to_retry(
	tmp_path: Path, api: ThreadingHTTPServer, path: str, expected_exit: int, expected_retryable: bool
) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('{path}', paginator=sl.single())\n")
	done = sqlload(project, "load", "--json")
	assert done.returncode == expected_exit, done.stdout + done.stderr
	result = json.loads(done.stdout)
	assert (result["ok"], result["retryable"], result["tables"][0]["retryable"]) == (False, expected_retryable, expected_retryable)


ALL_ITEMS = sorted((n, line) for n, lines in ITEMS.items() for line in range(len(lines)))


@pytest.mark.parametrize(
	("job", "expected_bodies"),
	[
		(
			"api.fan_out('/lookup', method='POST', json_body={'order': {'id': '{order_id}'}, 'note': 'order {order_id}'}, over=range(1, 8), select='$.items')",
			[{"order": {"id": n}, "note": f"order {n}"} for n in range(1, 8)],
		),
		(
			"api.fan_out('/lookup-flaky', method='POST', json_body={'ids': '{order_id}'}, batch=3, concurrency=2, select='$.items', "
			"over=api.paginate('/next', paginator=sl.next_link('$.next'), select='$.data.items[*].order_id'))",
			[{"ids": [1, 2, 3]}, {"ids": [4, 5, 6]}, {"ids": [4, 5, 6]}, {"ids": [7]}],
		),
	],
	ids=["one-key-per-post", "batches-of-keys-each-retried-whole"],
)
def test_given_fan_out_keys_in_a_post_body_when_loading_then_each_request_carries_its_keys(
	tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_bodies: list[dict[str, Any]]
) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='Items.')\ndef items(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, line FROM "RAW".shop__items ORDER BY 1, 2') == ALL_ITEMS
	sent = sorted(json.dumps(body, sort_keys=True) for _, _, body in api.bodies)  # type: ignore[attr-defined]
	assert sent == sorted(json.dumps(body, sort_keys=True) for body in expected_bodies)


def test_given_a_batch_that_fails_when_loading_then_the_error_names_its_keys(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	job = "quick.fan_out('/lookup-flaky', method='POST', json_body={'ids': '{order_id}'}, batch=3, concurrency=1, over=range(1, 8), select='$.items')"
	project = make_project(tmp_path, api, f"quick = sl.http(shop, auth=sl.bearer(), retries=0)\n\n@shop.table(description='Items.')\ndef items(ctx):\n    return {job}\n")
	done = sqlload(project, "load", "--json")
	assert done.returncode == 75, done.stdout + done.stderr
	(table,) = json.loads(done.stdout)["tables"]
	assert table["error"].startswith("order_id=4, 5, 6 → POST http://"), table["error"]


def test_given_a_body_placeholder_no_key_gives_when_loading_then_no_request_is_sent(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	job = "api.fan_out('/lookup', method='POST', json_body={'order': {'id': '{order}'}}, over=api.paginate('/next', paginator=sl.next_link('$.next'), select='$.data.items[*].order_id'), select='$.items')"
	project = make_project(tmp_path, api, f"@shop.table(description='Items.')\ndef items(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "fan-out json_body needs {order}, but over gives only order_id" in done.stdout + done.stderr, done.stdout + done.stderr
	assert api.log == [] and api.bodies == []  # type: ignore[attr-defined]


FAIL_FAST_TABLES = """\
quick = sl.http(shop, auth=sl.bearer(), retries=0)

@shop.table(description='Fails at once.')
def a_fails(ctx):
    return quick.paginate('{path}', paginator=sl.single())

@shop.table(description='Still loading when a_fails fails.')
def b_slow(ctx):
    return api.paginate('/slow', paginator=sl.page_number('p'), select='$.items')

@shop.table(description='Not started.')
def c_later(ctx):
    return api.paginate('/next', paginator=sl.next_link('$.next'), select='$.data.items')

@shop.table(description='Not started.')
def d_later(ctx):
    return api.paginate('/pages', paginator=sl.page_number('p'), select='$.items')
"""


@pytest.mark.parametrize(
	("path", "fail_fast", "expected_exit", "expected_statuses"),
	[
		("/broken", True, 75, ["failed", "loaded", "skipped", "skipped"]),
		("/missing", True, 1, ["failed", "loaded", "skipped", "skipped"]),
		("/missing", False, 1, ["failed", "loaded", "loaded", "loaded"]),
	],
	ids=["retryable-failure-exits-75", "lasting-failure-exits-1", "without-the-flag-the-rest-still-load"],
)
def test_given_fail_fast_when_a_table_fails_while_another_loads_then_that_one_finishes_and_no_other_starts(
	tmp_path: Path, api: ThreadingHTTPServer, path: str, fail_fast: bool, expected_exit: int, expected_statuses: list[str]
) -> None:
	project = make_project(tmp_path, api, FAIL_FAST_TABLES.format(path=path))
	flags = ["--fail-fast"] if fail_fast else []
	done = sqlload(project, "load", "--json", "--concurrency", "2", *flags)
	assert done.returncode == expected_exit, done.stdout + done.stderr
	result = json.loads(done.stdout)
	tables = sorted(result["tables"], key=lambda table: table["table"])
	assert [table["status"] for table in tables] == expected_statuses
	assert (result["summary"]["failed"], result["summary"]["skipped"]) == (1, expected_statuses.count("skipped"))
	assert result["retryable"] == (expected_exit == 75)
	for table in tables:
		if table["status"] == "skipped":
			assert table["error"] == f"not started: --fail-fast stopped the load after {tables[0]['table']} failed"
			assert table["retryable"] is True and table["rows"] is None
	assert query(project, 'SELECT count(*) FROM "RAW".shop__b_slow') == [(len(ORDERS),)]
	loaded = {name for (name,) in query(project, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'RAW'")}
	assert ("shop__c_later" in loaded) == (not fail_fast)
	requested = {path for path, _, _ in api.log}  # type: ignore[attr-defined]
	assert ("/v1/next" in requested) == (not fail_fast)


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


@pytest.mark.parametrize(
	("path", "paginator", "expected_text"),
	[
		("/redir", "sl.single()", "HTTP 302: the redirect was not followed"),
		("/away", "sl.next_link('$.next')", ""),
	],
	ids=["redirect", "next-link-to-another-host"],
)
def test_given_a_response_pointing_at_another_host_when_loading_then_no_credential_or_header_goes_there(
	tmp_path: Path, api: ThreadingHTTPServer, elsewhere: ThreadingHTTPServer, path: str, paginator: str, expected_text: str
) -> None:
	tables = f"keyed = sl.http(shop, auth=sl.api_key(header='X-Api-Key'), headers={{'X-Team': 'blue'}})\n\n@shop.table(description='T.')\ndef t(ctx):\n    return keyed.paginate('{path}', paginator={paginator}, select='$.items')\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	sent = {name.lower() for headers in elsewhere.seen for name in headers}  # type: ignore[attr-defined]
	assert not sent & {"x-api-key", "x-team", "authorization"}


def test_given_a_datetime_param_when_loading_then_it_is_sent_as_iso_text(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	tables = "import datetime\n\n@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/offset', paginator=sl.offset(limit=10), params={'since': datetime.datetime(2024, 1, 2, 3, 4)})\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert [query.get("since") for _, query, _ in api.log] == ["2024-01-02T03:04:00"]  # type: ignore[attr-defined]


def test_given_a_redirect_listed_in_on_status_when_loading_then_it_is_skipped_and_not_followed(
	tmp_path: Path, api: ThreadingHTTPServer, elsewhere: ThreadingHTTPServer
) -> None:
	project = make_project(tmp_path, api, "@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/redir', paginator=sl.single(), on_status={302: 'skip'})\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert elsewhere.seen == []  # type: ignore[attr-defined]


def test_given_fields_and_keep_record_when_a_paginate_and_a_fan_out_load_then_rows_are_projected(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	tables = textwrap.dedent("""\
		@shop.table(description='Orders.')
		def orders(ctx):
		    return api.paginate('/next', paginator=sl.next_link('$.next'), select='$.data.items',
		                        fields={'id': '$.order_id', 'state': sl.field('$.status', required=True)}, keep_record='raw')

		@shop.table(description='Items.')
		def items(ctx):
		    return api.fan_out('/orders/{order_id}/items', over=range(1, 8), select='$.items', on_status={404: 'skip'},
		                       fields={'order': '$.order_id', 'line_no': '$.line'})
		""")
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT id, state FROM "RAW".shop__orders ORDER BY id') == ALL_ORDERS
	raw = query(project, 'SELECT raw FROM "RAW".shop__orders ORDER BY id')
	assert [json.loads(text) for (text,) in raw] == ORDERS
	columns = [name for (name,) in query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'shop__items' AND column_name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY ordinal_position")]
	assert columns == ["order", "line_no"]
	expected = sorted((n, line) for n, lines in ITEMS.items() if n != 4 for line in range(len(lines)))
	assert query(project, 'SELECT "order", line_no FROM "RAW".shop__items ORDER BY 1, 2') == expected


@pytest.mark.parametrize(
	("data", "cursor", "expected_text"),
	[
		("api.paginate('/offset', paginator=sl.offset(limit=10), fields={'id': '$.order_id', 'status': '$.status'})", "id", None),
		("api.paginate('/offset', paginator=sl.offset(limit=10), fields={'id': '$.order_id', 'status': '$.status'})", "ID", None),
		("api.paginate('/offset', paginator=sl.offset(limit=10), fields={'id': '$.order_id'})", "order_id", "the table's cursor_column order_id is not among the fields"),
		("api.fan_out('/orders/{order_id}/items', over=range(1, 8), select='$.items', on_status={404: 'skip'}, fields={'line_no': '$.line'})", "order_id", "the table's cursor_column order_id is not among the fields"),
		("[sl.ndjson(b'{\"order_id\": 1}', fields={'id': '$.order_id'})]", "order_id", "the table's cursor_column order_id is not among the fields"),
	],
	ids=["cursor-on-a-renamed-field", "cursor-in-another-case", "http-cursor-not-projected", "fan-out-cursor-not-projected", "ndjson-cursor-not-projected"],
)
def test_given_a_cursor_column_when_rows_are_projected_then_the_cursor_must_be_among_the_fields(
	tmp_path: Path, api: ThreadingHTTPServer, data: str, cursor: str, expected_text: str | None
) -> None:
	tables = f"@shop.table(description='T.', write_strategy='append', cursor_column='{cursor}')\ndef t(ctx):\n    return {data}\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	output = done.stdout + done.stderr
	if expected_text is None:
		assert done.returncode == 0, output
		assert query(project, 'SELECT max(id) FROM "RAW".shop__t') == [(7,)]
		return
	assert done.returncode == 1, output
	assert expected_text in output, output
	assert api.log == []  # type: ignore[attr-defined]



@pytest.mark.parametrize(
	"data",
	[
		"api.paginate('/offset', paginator=sl.offset(limit=10), fields={'id': '$.order_id', 'status': '$.status'}, keep_record='raw')",
		"[sl.ndjson(b'{\"order_id\": 1, \"status\": \"paid\"}', fields={'id': '$.order_id', 'status': '$.status'}, keep_record='raw')]",
	],
	ids=["paginate", "ndjson"],
)
def test_given_a_masked_table_when_rows_keep_their_record_then_the_load_is_refused(tmp_path: Path, api: ThreadingHTTPServer, data: str) -> None:
	tables = f"@shop.table(description='T.', mask={{'status': 'hash'}})\ndef t(ctx):\n    return {data}\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	output = done.stdout + done.stderr
	assert done.returncode == 1, output
	assert "keep_record would hold the masked columns (status) unmasked" in output, output
	assert api.log == []  # type: ignore[attr-defined]

def oauth_project(tmp_path: Path, server: ThreadingHTTPServer, auth: str, job: str) -> Path:
	tables = f"oauth = sl.http(shop, auth={auth})\n\n@shop.table(description='T.')\ndef t(ctx):\n    return oauth.{job}\n"
	return make_project(tmp_path, server, tables)


@pytest.mark.parametrize(
	("auth", "expected_grant"),
	[("sl.oauth2_client_credentials(TOKEN_URL, scope='orders:read')", "client_credentials"), ("sl.oauth2_password(TOKEN_URL, scope='orders:read')", "password")],
	ids=["client-credentials", "password"],
)
def test_given_short_lived_tokens_when_pages_outlast_one_then_it_is_renewed_before_it_expires(
	tmp_path: Path, api: ThreadingHTTPServer, auth: str, expected_grant: str
) -> None:
	api.oauth = {"lifetime": 2}  # type: ignore[attr-defined]
	project = oauth_project(tmp_path, api, auth, "paginate('/slow', paginator=sl.page_number('p'), select='$.items')")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__t ORDER BY order_id') == ALL_ORDERS
	assert api.refused == 0  # type: ignore[attr-defined]
	assert len(api.token_requests) >= 2  # type: ignore[attr-defined]
	assert {(request["grant_type"], request.get("scope")) for request in api.token_requests} == {(expected_grant, "orders:read")}  # type: ignore[attr-defined]


@pytest.mark.parametrize(
	("auth", "expected_grant"),
	[
		("sl.oauth2_client_credentials(TOKEN_URL)", [("client_credentials", None)] * 3),
		("sl.oauth2_refresh_token(TOKEN_URL, client_auth='post')", [("refresh_token", REFRESH_TOKEN), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-3")]),
		("sl.oauth2_password(TOKEN_URL)", [("password", None), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-3")]),
	],
	ids=["client-credentials", "refresh-token-rotated-client-in-form", "password-then-its-refresh-token"],
)
def test_given_a_token_revoked_mid_pagination_when_the_api_answers_401_then_a_new_one_is_fetched_and_the_page_retried(
	tmp_path: Path, api: ThreadingHTTPServer, auth: str, expected_grant: list[tuple[str, str | None]]
) -> None:
	api.oauth = {"uses": 3, "rotate": True}  # type: ignore[attr-defined]
	project = oauth_project(tmp_path, api, auth, "paginate('/offset', paginator=sl.offset(limit=1))")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__t ORDER BY order_id') == ALL_ORDERS
	assert api.refused == 2  # type: ignore[attr-defined]
	assert [(request["grant_type"], request.get("refresh_token")) for request in api.token_requests] == expected_grant  # type: ignore[attr-defined]
	assert all(("username" in request, "password" in request) == ((request["grant_type"] == "password",) * 2) for request in api.token_requests)  # type: ignore[attr-defined]
	assert all(request["client"] == ["app", CLIENT_SECRET] for request in api.token_requests)  # type: ignore[attr-defined]
	assert {auth for path, _, auth in api.log if path == "/v1/offset"} == {"Bearer access-1", "Bearer access-2", "Bearer access-3"}  # type: ignore[attr-defined]


@pytest.mark.parametrize(
	("auth", "job", "expected_text"),
	[
		(
			"sl.oauth2_client_credentials(TOKEN_URL, client_auth='post')",
			"paginate('/offset', paginator=sl.offset(limit=10))",
			"/oauth/token (OAuth2 token): HTTP 401: invalid_client: unknown client ['app', '***'] or refresh token; check the connection's OAuth2 credentials",
		),
		(
			"sl.oauth2_client_credentials('http://auth.example.com/token')",
			"paginate('/offset', paginator=sl.offset(limit=10))",
			"token_url http://auth.example.com/token must be https",
		),
		(
			"sl.oauth2_client_credentials(TOKEN_URL)",
			"paginate('/whoami', paginator=sl.single())",
			'HTTP 400: {"message": "you are Bearer ***"}',
		),
		(
			"sl.oauth2_client_credentials(TOKEN_URL)",
			"paginate('/leak', paginator=sl.next_link('$.next'), select='$.items')",
			"/v1/notjson?access_token=***: the response is not JSON",
		),
	],
	ids=["bad-client-secret", "http-token-url-elsewhere", "access-token-redacted", "access-token-in-a-next-link-redacted"],
)
def test_given_an_oauth2_failure_when_loading_then_the_error_says_why_and_hides_every_secret(
	tmp_path: Path, api: ThreadingHTTPServer, auth: str, job: str, expected_text: str
) -> None:
	project = oauth_project(tmp_path, api, auth, job)
	if "invalid_client" in expected_text:
		(project / "sqlload.toml").write_text((project / "sqlload.toml").read_text().replace("${ENV:SHOP_CLIENT_SECRET}", "wrong-secret"))
	done = sqlload(project, "load")
	output = done.stdout + done.stderr
	assert done.returncode == 1
	assert expected_text in output, output
	assert not any(secret in output for secret in (CLIENT_SECRET, "wrong-secret", "access-1"))
	assert all(path in ("/v1/whoami", "/v1/leak", "/v1/notjson") for path, _, _ in api.log)  # type: ignore[attr-defined]


def test_given_a_password_grant_without_a_refresh_token_or_with_a_refused_one_when_the_api_answers_401_then_it_asks_with_the_password_again(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	api.oauth = {"uses": 3, "stale_refresh": True}  # type: ignore[attr-defined]
	project = oauth_project(tmp_path, api, "sl.oauth2_password(TOKEN_URL, client_auth='post')", "paginate('/offset', paginator=sl.offset(limit=1))")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__t ORDER BY order_id') == ALL_ORDERS
	expected = [("password", "loader", PASSWORD, None), ("refresh_token", None, None, "stale")] * 2 + [("password", "loader", PASSWORD, None)]
	sent = [(request["grant_type"], request.get("username"), request.get("password"), request.get("refresh_token")) for request in api.token_requests]  # type: ignore[attr-defined]
	assert sent == expected
	assert all(request["client"] == ["app", CLIENT_SECRET] for request in api.token_requests)  # type: ignore[attr-defined]


def test_given_a_token_endpoint_down_once_when_refreshing_after_a_password_grant_then_the_refresh_is_retried_without_the_password(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	api.oauth = {"uses": 3, "rotate": True, "refresh_503_once": True}  # type: ignore[attr-defined]
	project = oauth_project(tmp_path, api, "sl.oauth2_password(TOKEN_URL)", "paginate('/offset', paginator=sl.offset(limit=1))")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__t ORDER BY order_id') == ALL_ORDERS
	expected = [("password", None), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-4")]
	assert [(request["grant_type"], request.get("refresh_token")) for request in api.token_requests] == expected  # type: ignore[attr-defined]


def test_given_a_token_endpoint_down_when_refreshing_without_retries_then_the_run_is_retryable_and_the_password_is_not_resent(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	api.oauth = {"uses": 3, "rotate": True, "refresh_503_once": True}  # type: ignore[attr-defined]
	project = oauth_project(tmp_path, api, "sl.oauth2_password(TOKEN_URL), retries=0", "paginate('/offset', paginator=sl.offset(limit=1))")
	done = sqlload(project, "load")
	assert done.returncode == 75, done.stdout + done.stderr
	assert [(request["grant_type"], request.get("refresh_token")) for request in api.token_requests] == [("password", None), ("refresh_token", "r3fresh-2")]  # type: ignore[attr-defined]


def test_given_a_wrong_password_when_loading_with_json_then_it_fails_for_good_and_hides_the_password(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = oauth_project(tmp_path, api, "sl.oauth2_password(TOKEN_URL)", "paginate('/offset', paginator=sl.offset(limit=10))")
	(project / "sqlload.toml").write_text((project / "sqlload.toml").read_text().replace("${ENV:SHOP_PASSWORD}", "wr0ng+p\u00e4ss\\\\word"))
	done = sqlload(project, "load", "--json")
	assert done.returncode == 1, done.stdout + done.stderr
	result = json.loads(done.stdout)
	assert (result["ok"], result["retryable"], result["tables"][0]["retryable"]) == (False, False, False)
	assert 'HTTP 400: {"message":"wrong password *** for loader"}' in result["tables"][0]["error"]
	assert "wr0ng" not in done.stdout + done.stderr
	assert len(api.token_requests) == 1 and api.log == []  # type: ignore[attr-defined]


def test_given_401_listed_as_skip_when_the_api_answers_it_then_the_request_is_skipped_without_a_new_token(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = oauth_project(tmp_path, api, "sl.oauth2_client_credentials(TOKEN_URL)", "fan_out('/denied?k={k}', over=range(5), concurrency=1, paginator=sl.single(), on_status={401: 'skip'})")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert len([path for path, _, _ in api.log if path == "/v1/denied"]) == 5  # type: ignore[attr-defined]
	assert len(api.token_requests) == 1  # type: ignore[attr-defined]


def test_given_a_401_from_another_origin_when_loading_then_no_token_is_sent_there_or_renewed_for_it(
	tmp_path: Path, api: ThreadingHTTPServer, elsewhere: ThreadingHTTPServer
) -> None:
	project = oauth_project(tmp_path, api, "sl.oauth2_client_credentials(TOKEN_URL)", "paginate('/away401', paginator=sl.next_link('$.next'), select='$.items')")
	done = sqlload(project, "load")
	assert done.returncode == 1
	assert "/denied: HTTP 401" in done.stdout + done.stderr, done.stdout + done.stderr
	assert len(api.token_requests) == 1  # type: ignore[attr-defined]
	assert not any("authorization" in {name.lower() for name in headers} for headers in elsewhere.seen)  # type: ignore[attr-defined]


SEARCH_FILTER = {"filter": {"status": "any"}}


@pytest.mark.parametrize(
	("job", "expected_bodies"),
	[
		(
			"api.paginate('/search', method='POST', json_body=FILTER, paginator=sl.offset('$.page.offset', limit=3, limit_param='$.page.limit', location='body'), select='$.items')",
			[{**SEARCH_FILTER, "page": {"offset": offset, "limit": 3}} for offset in (0, 3, 6)],
		),
		(
			"api.paginate('/search-cursor', method='post', json_body=FILTER, paginator=sl.cursor('$.meta.next', '$.page.cursor', location='body'), select='$.items')",
			[SEARCH_FILTER, {**SEARCH_FILTER, "page": {"cursor": "4"}}],
		),
		(
			"api.paginate('/search-number', method='POST', json_body=FILTER, paginator=sl.cursor('$.meta.next', '$.page.cursor', location='body'), select='$.items')",
			[SEARCH_FILTER, {**SEARCH_FILTER, "page": {"cursor": 4}}],
		),
	],
	ids=["offset-in-body", "cursor-token-in-body", "numeric-cursor-token-in-body"],
)
def test_given_a_post_job_paging_in_its_body_when_loading_then_each_page_sends_the_template_with_its_values(
	tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_bodies: list[dict[str, Any]]
) -> None:
	tables = f"FILTER = {SEARCH_FILTER!r}\n\n@shop.table(description='Orders.')\ndef orders(ctx):\n    job = {job}\n    assert FILTER == {SEARCH_FILTER!r}\n    return job\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__orders ORDER BY order_id') == ALL_ORDERS
	assert [body for _, _, body in api.bodies] == expected_bodies  # type: ignore[attr-defined]
	assert {content_type for _, content_type, _ in api.bodies} == {"application/json"}  # type: ignore[attr-defined]
	assert api.log == []  # type: ignore[attr-defined]


@pytest.mark.parametrize(
	("path", "paginator", "expected_code", "expected_text", "expected_paths"),
	[
		("/search-307", "sl.single()", 0, "", ["/v1/search-307", "/v1/search-cursor"]),
		("/search-302", "sl.single()", 1, "or is a 301, 302 or 303 after a POST", ["/v1/search-302"]),
		("/search-away", "sl.next_link('$.next')", 1, "json_body never leaves the connection's origin", ["/v1/search-away"]),
	],
	ids=["307-resends-the-body", "302-is-not-followed-as-a-get", "next-link-to-another-host"],
)
def test_given_a_post_answered_with_a_redirect_or_link_when_loading_then_its_body_is_resent_or_the_load_fails(
	tmp_path: Path,
	api: ThreadingHTTPServer,
	elsewhere: ThreadingHTTPServer,
	path: str,
	paginator: str,
	expected_code: int,
	expected_text: str,
	expected_paths: list[str],
) -> None:
	tables = f"@shop.table(description='Orders.')\ndef orders(ctx):\n    return api.paginate('{path}', method='POST', json_body={SEARCH_FILTER!r}, paginator={paginator}, select='$.items')\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == expected_code, done.stdout + done.stderr
	assert expected_text in done.stdout + done.stderr
	assert [(path, body) for path, _, body in api.bodies] == [(path, SEARCH_FILTER) for path in expected_paths]  # type: ignore[attr-defined]
	assert api.log == [] and elsewhere.seen == []  # type: ignore[attr-defined]


def test_given_a_connection_content_type_when_posting_then_it_is_the_only_one_sent(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	tables = (
		"typed = sl.http(shop, auth=sl.bearer(), headers={'Content-Type': 'application/json; charset=utf-8'})\n\n"
		"@shop.table(description='Orders.')\ndef orders(ctx):\n"
		"    return typed.paginate('/search-cursor', method='POST', paginator=sl.single(), select='$.items')\n"
	)
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert [content_type for _, content_type, _ in api.bodies] == ["application/json; charset=utf-8"]  # type: ignore[attr-defined]


def test_given_an_incremental_cursor_in_the_body_when_loading_twice_then_the_second_run_sends_the_stored_cursor(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	tables = (
		"@shop.table(description='Orders.', write_strategy='append', cursor_column='order_id')\n"
		"def orders(ctx):\n"
		"    return api.paginate('/search', method='POST', json_body={'filter': {'after': ctx.cursor_value}}, "
		"paginator=sl.offset('$.page.offset', limit=10, limit_param='$.page.limit', location='body'), select='$.items')\n"
	)
	project = make_project(tmp_path, api, tables)
	for _ in range(2):
		done = sqlload(project, "load")
		assert done.returncode == 0, done.stdout + done.stderr
	assert [body["filter"]["after"] for _, _, body in api.bodies] == [None, 7]  # type: ignore[attr-defined]
	assert query(project, 'SELECT count(*), count(DISTINCT order_id) FROM "RAW".shop__orders') == [(7, 7)]


def test_given_a_get_job_when_loading_then_its_requests_are_unchanged_by_body_paging(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api, "@shop.table(description='Orders.')\ndef orders(ctx):\n    return api.paginate('/offset', paginator=sl.offset(limit=3), params={'q': 'x'})\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	expected = [("/v1/offset", {"q": "x", "offset": str(offset), "limit": "3"}, f"Bearer {TOKEN}") for offset in (0, 3, 6)]
	assert api.log == expected  # type: ignore[attr-defined]
	assert api.bodies == []  # type: ignore[attr-defined]


@pytest.mark.parametrize(
	("job", "expected_text"),
	[
		("api.paginate('/search', json_body={'a': 1}, paginator=sl.single())", 'json_body is sent only with method="POST"'),
		("api.paginate('/search', paginator=sl.cursor('$.next', '$.page.cursor', location='body'))", 'a paginator with location="body" needs method="POST"'),
		("api.paginate('/search', method='POST', paginator=sl.cursor('$.next', 'page[cursor]', location='body'))", 'body path "page[cursor]" is not a path such as "$.page.cursor"'),
		(
			"api.paginate('/search-cursor', method='POST', json_body={'page': 1}, paginator=sl.cursor('$.meta.next', '$.page.cursor', location='body'))",
			"json_body has no object to set $.page.cursor in",
		),
		("api.paginate('/search', method='POST', paginator=sl.cursor('$.next', '$.page[0]', location='body'))", 'body path "$.page[0]" is not a path such as "$.page.cursor"'),
	],
	ids=["body-on-get", "body-paging-on-get", "bad-body-path", "body-path-through-a-number", "body-path-with-an-index"],
)
def test_given_a_bad_body_setting_when_loading_then_no_request_is_sent(tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_text: str) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='Orders.')\ndef orders(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode != 0
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	assert api.log == [] and api.bodies == []  # type: ignore[attr-defined]


TREE = """
orders = api.paginate('/tree/orders', paginator=sl.page_number('p'), params={'after': AFTER}, select='$.items[*].order_id')
items = api.fan_out('/tree/orders/{order_id}/items', over=orders, concurrency=3, select='$.items[*].item_id', on_status=ON_STATUS)
"""
TREE_ITEMS = [10, 20, 21, 50, 51, 70]


def tree_project(tmp_path: Path, api: ThreadingHTTPServer, notes: str, *, after: str = "None", on_status: str = "{404: 'skip'}", table: str = "") -> Path:
	body = TREE.replace("AFTER", after).replace("ON_STATUS", on_status)
	tables = f"@shop.table(description='Notes.'{table})\ndef notes(ctx):\n" + textwrap.indent(body, "    ") + f"    return {notes}\n"
	return make_project(tmp_path, api, tables)


NOTES = "api.fan_out('/tree/orders/{order_id}/items/{item_id}/notes', over=items, concurrency=3, paginator=sl.page_number('page'), select='$.items')"


def test_given_three_fan_out_levels_when_loading_then_each_note_is_read_under_its_own_order_and_item(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = tree_project(tmp_path, api, NOTES)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	expected = sorted((item // 10, item, f"{item}-{page}-{n}") for item in TREE_ITEMS for page in (1, 2) for n in range(2))
	assert query(project, 'SELECT order_id, item_id, note FROM "RAW".shop__notes ORDER BY item_id, note') == expected
	notes = {(path, query["page"]) for path, query, _ in api.log if path.endswith("/notes")}  # type: ignore[attr-defined]
	assert notes == {(f"/v1/tree/orders/{item // 10}/items/{item}/notes", str(page)) for item in TREE_ITEMS for page in (1, 2, 3)}
	assert sorted(path for path, _, _ in api.log if path.endswith("/items")) == [f"/v1/tree/orders/{order}/items" for order in range(1, 8)]  # type: ignore[attr-defined]
	assert [query["p"] for path, query, _ in api.log if path == "/v1/tree/orders"] == ["1", "2", "3", "4"]  # type: ignore[attr-defined]


def test_given_levels_renaming_their_keys_when_loading_then_each_renamed_key_fills_its_placeholder(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = tree_project(tmp_path, api, NOTES.replace("{order_id}/items/{item_id}", "{o}/items/{i}"))
	shop_py = project / "sources" / "shop.py"
	renamed = shop_py.read_text().replace("select='$.items[*].order_id')", "select='$.items[*].order_id', key='o')")
	renamed = renamed.replace("/tree/orders/{order_id}/items', over=orders", "/tree/orders/{o}/items', over=orders").replace("select='$.items[*].item_id',", "select='$.items[*].item_id', key='i',")
	shop_py.write_text(renamed)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT note FROM "RAW".shop__notes ORDER BY note') == sorted((f"{item}-{page}-{n}",) for item in TREE_ITEMS for page in (1, 2) for n in range(2))
	notes = {path for path, _, _ in api.log if path.endswith("/notes")}  # type: ignore[attr-defined]
	assert notes == {f"/v1/tree/orders/{item // 10}/items/{item}/notes" for item in TREE_ITEMS}


@pytest.mark.parametrize(
	("on_status", "notes", "expected_code", "expected_text"),
	[
		(
			"{}",
			NOTES,
			1,
			"/tree/orders order_id=4 → GET http://127.0.0.1:PORT/v1/tree/orders/4/items: HTTP 404",
		),
		(
			"{404: 'skip'}",
			NOTES.replace("api.fan_out", "once.fan_out").replace("sl.page_number('page')", "sl.page_number('page'), params={'boom': 1}"),
			75,
			"/tree/orders order_id=5 → /tree/orders/5/items item_id=51 → GET http://127.0.0.1:PORT/v1/tree/orders/5/items/51/notes?boom=1&page=2: HTTP 500",
		),
	],
	ids=["a-404-at-level-2-names-its-order", "a-500-at-level-3-names-its-order-and-item-and-retries-per-level"],
)
def test_given_a_failing_level_when_loading_then_the_error_names_the_keys_that_led_to_it(
	tmp_path: Path, api: ThreadingHTTPServer, on_status: str, notes: str, expected_code: int, expected_text: str
) -> None:
	project = tree_project(tmp_path, api, notes, on_status=on_status)
	shop_py = project / "sources" / "shop.py"
	shop_py.write_text(shop_py.read_text().replace("\n\n@shop.table", "\nonce = sl.http(shop, auth=sl.bearer(), retries=0)\n\n@shop.table", 1))
	done = sqlload(project, "load")
	output = done.stdout + done.stderr
	assert done.returncode == expected_code, output
	assert expected_text.replace("PORT", str(api.server_address[1])) in output, output
	boomed = [path for path, query, _ in api.log if path.endswith("/51/notes") and query["page"] == "2"]  # type: ignore[attr-defined]
	assert len(boomed) == (1 if expected_code == 75 else 0)


def test_given_a_cursor_on_the_top_level_when_loading_twice_then_the_rerun_requests_no_lower_level(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = tree_project(tmp_path, api, NOTES, after="ctx.cursor_value", table=", write_strategy='append', cursor_column='order_id'")
	assert sqlload(project, "load").returncode == 0
	first = len(api.log)  # type: ignore[attr-defined]
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	rerun = api.log[first:]  # type: ignore[attr-defined]
	assert [(path, query.get("after")) for path, query, _ in rerun] == [("/v1/tree/orders", "7")]
	assert query(project, 'SELECT count(*), count(DISTINCT note) FROM "RAW".shop__notes') == [(24, 24)]


ONE_LEVEL_JOB = (
	'{"kind": "fan_out", "client": {"connection": "shop", "auth": {"kind": "bearer"}, "rate_limit": null, "retries": 3, "headers": {}}, '
	'"path": "/orders/{order_id}/items", "params": {}, "method": "GET", "json_body": null, "paginator": {"kind": "single"}, "select": "$.items", '
	'"on_status": {"404": "skip"}, "fields": null, "keep_record": null, "over": {"kind": "paginate", "client": {"connection": "shop", '
	'"auth": {"kind": "bearer"}, "rate_limit": null, "retries": 3, "headers": {}}, "path": "/orders", "params": {}, "method": "GET", '
	'"json_body": null, "paginator": {"kind": "page_number", "param": "p", "start": 1, "location": "query"}, "select": "$.items[*].order_id", '
	'"on_status": {}, "fields": null, "keep_record": null}, "concurrency": 3}'
)


def test_given_a_one_level_fan_out_when_defined_then_its_job_is_unchanged_by_multi_level_support() -> None:
	code = (
		"import sqlload as sl\nshop = sl.source('shop', connection='shop')\napi = sl.http(shop, auth=sl.bearer(), retries=3)\n"
		"orders = api.paginate('/orders', paginator=sl.page_number('p'), select='$.items[*].order_id')\n"
		"print(api.fan_out('/orders/{order_id}/items', over=orders, concurrency=3, select='$.items', on_status={404: 'skip'}).__sqlload_http_job__())"
	)
	done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
	assert done.stdout.strip() == ONE_LEVEL_JOB, done.stderr


SHEET = "api.paginate('/sheet', paginator=sl.page_number('page'), select='$.rows', cells=sl.cells(value=['$.value', '$.displayValue']){extra})"
GRID_JOB = (
	"api.paginate('/grid', paginator=sl.page_number('page'), select='$.sheets[0].data[0].rowData', cells=sl.cells('$.values', key=None, "
	"value=['$.effectiveValue.stringValue', '$.effectiveValue.numberValue', '$.effectiveValue.boolValue'], header_row=True))"
)


def table_project(tmp_path: Path, api: ThreadingHTTPServer, job: str, settings: str = "") -> Path:
	return make_project(tmp_path, api, f"@shop.table(description='Rows.'{settings})\ndef rows(ctx):\n    return {job}\n")


@pytest.mark.parametrize(
	("job", "expected_columns", "expected_rows"),
	[
		(
			SHEET.format(extra=""),
			[("id", "BIGINT"), ("rowNumber", "BIGINT"), ("Name", "VARCHAR"), ("Qty", "BIGINT"), ("Price", "DOUBLE"), ("Done", "BOOLEAN")],
			[(1, 1, "Bolt", 3, 0.25, True), (2, 2, "Nut", None, 1.5, None), (3, 3, "Washer", 12, None, False), (4, 4, None, None, None, None), (5, 5, "Gear", 4, 9.0, None)],
		),
		(
			SHEET.format(extra="").replace("['$.value', '$.displayValue']", "'$.value'"),
			[("id", "BIGINT"), ("rowNumber", "BIGINT"), ("Name", "VARCHAR"), ("Qty", "BIGINT"), ("Price", "DOUBLE"), ("Done", "BOOLEAN")],
			[(1, 1, "Bolt", 3, 0.25, True), (2, 2, "Nut", None, 1.5, None), (3, 3, "Washer", 12, None, False), (4, 4, None, None, None, None), (5, 5, "Gear", None, 9.0, None)],
		),
		(
			GRID_JOB,
			[("Name", "VARCHAR"), ("Qty", "BIGINT"), ("Done", "BOOLEAN")],
			[("Bolt", 3, True), ("Nut", None, False), ("Washer", None, None), (None, None, None)],
		),
	],
	ids=["smartsheet-value-then-display-value", "smartsheet-value-only", "google-grid-header-row"],
)
def test_given_rows_of_cells_when_loading_with_cells_then_each_named_column_loads_typed_and_a_missing_cell_is_null(
	tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_columns: list[tuple[str, str]], expected_rows: list[tuple[Any, ...]]
) -> None:
	project = table_project(tmp_path, api, job)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	described = query(project, 'SELECT column_name, column_type FROM (DESCRIBE "RAW".shop__rows) WHERE column_name NOT LIKE \'_sqlload%\'')
	assert described == expected_columns
	names = ", ".join(f'"{name}"' for name, _ in expected_columns)
	assert query(project, f'SELECT {names} FROM "RAW".shop__rows ORDER BY rowid') == expected_rows


def test_given_a_sheet_listing_its_columns_on_its_first_page_when_paging_then_later_pages_keep_the_names(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = table_project(tmp_path, api, SHEET.format(extra=""))
	assert sqlload(project, "load").returncode == 0
	assert [query["page"] for path, query, _ in api.log if path == "/v1/sheet"] == ["1", "2", "3", "4"]  # type: ignore[attr-defined]
	assert query(project, 'SELECT "Name" FROM "RAW".shop__rows WHERE id = 3') == [("Washer",)]


@pytest.mark.parametrize(
	("settings", "expected_code", "expected_text", "expected_columns"),
	[
		("", 0, "", ["id", "rowNumber", "Name", "Qty", "Price", "Done", "Quantity"]),
		(", on_schema_change='fail'", 1, "Quantity", ["id", "rowNumber", "Name", "Qty", "Price", "Done"]),
	],
	ids=["append-new-columns", "fail"],
)
def test_given_a_column_renamed_between_runs_when_loading_again_then_on_schema_change_decides(
	tmp_path: Path, api: ThreadingHTTPServer, settings: str, expected_code: int, expected_text: str, expected_columns: list[str]
) -> None:
	project = table_project(tmp_path, api, SHEET.format(extra=""), settings)
	assert sqlload(project, "load").returncode == 0
	api.sheet_title = "Quantity"  # type: ignore[attr-defined]
	done = sqlload(project, "load")
	assert done.returncode == expected_code, done.stdout + done.stderr
	assert expected_text in done.stdout + done.stderr
	described = query(project, 'SELECT column_name FROM (DESCRIBE "RAW".shop__rows) WHERE column_name NOT LIKE \'_sqlload%\'')
	assert [name for (name,) in described] == expected_columns


@pytest.mark.parametrize(
	("job", "expected_code", "expected_text", "expected_requests"),
	[
		(
			SHEET.format(extra="").replace("paginator=sl.page_number('page')", "paginator=sl.page_number('page'), params={'unknown': 1}"),
			1,
			"page=2: cells: row 1: a cell's columnId 7999 is not the id of a column $.columns names",
			2,
		),
		(
			"api.paginate('/sheet', paginator=sl.single(), select='$.rows', cells=sl.cells(names_from='$.columns', header_row=True))",
			1,
			"cells takes names_from or header_row, not both",
			0,
		),
		(
			"api.paginate('/sheet', paginator=sl.single(), select='$.rows', cells=sl.cells('$.rows[*].cells'))",
			1,
			"cells path \"$.rows[*].cells\" cannot pick from a list with [*]",
			0,
		),
		(
			"api.fan_out('/orders/{order_id}/items', over=api.paginate('/sheet', paginator=sl.single(), select='$.rows[*].id', cells=sl.cells()), select='$.items')",
			1,
			"fan_out's over picks keys with its select; cells are for the fan-out's own rows",
			0,
		),
		(
			SHEET.format(extra="").replace("paginator=sl.page_number('page')", "paginator=sl.page_number('page'), params={'twice': 1}"),
			1,
			"page=1: cells: row 1: it has two cells for the column Qty",
			1,
		),
		(
			"api.paginate('/sheet', paginator=sl.page_number('page'), select='$.rows', cells=sl.cells('$.cell'))",
			1,
			'cells: no row has a cell field, so cells pointer "$.cell" finds no cells; check it',
			1,
		),
	],
	ids=["unknown-column-id", "names-from-and-header-row", "pointer-into-a-list", "cells-on-an-over", "two-cells-for-one-column", "pointer-naming-no-field"],
)
def test_given_cells_that_cannot_pivot_when_loading_then_it_fails_naming_why(
	tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_code: int, expected_text: str, expected_requests: int
) -> None:
	project = table_project(tmp_path, api, job)
	done = sqlload(project, "load")
	assert done.returncode == expected_code, done.stdout + done.stderr
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	assert len(api.log) == expected_requests  # type: ignore[attr-defined]


def test_given_a_job_without_cells_when_built_then_its_definition_has_no_cells_field() -> None:
	import sqlload as sl

	api = sl.http(sl.source("shop", connection="shop"))
	plain = json.loads(api.paginate("/orders", paginator=sl.single(), select="$.items").__sqlload_http_job__())
	pivoted = json.loads(api.paginate("/orders", paginator=sl.single(), select="$.items", cells=sl.cells()).__sqlload_http_job__())
	assert "cells" not in plain
	assert pivoted["cells"] == {"pointer": "$.cells", "key": "columnId", "value": ["$.value"], "names_from": "$.columns", "name_key": "title", "id_key": "id", "header_row": False}
	assert {key: value for key, value in pivoted.items() if key != "cells"} == plain
