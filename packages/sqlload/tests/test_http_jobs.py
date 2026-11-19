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


class Api(BaseHTTPRequestHandler):
	"""A stub shop API; `self.server.log` records every request's path and auth header."""

	def changelog(self, path: str, body: dict[str, Any]) -> None:
		"""Jira-shaped change histories of the issues `$.ids` names, one issue a page, paged by `$.next`: issue n has histories
		n*10 and n*10+1, created at epoch milliseconds, each changing the status then the assignee; the page envelope counts the request's ids. `/v1/changelog-wrong` answers
		the second page for issue 99; `/v1/changelog-noid` leaves out issue 11's `issueId`."""
		ids, page = body["ids"], int(body.get("next") or 0)
		issue = 99 if path.endswith("-wrong") and page == 1 else ids[page]
		histories = [{"id": issue * 10 + k, "created": 1_700_000_000_000 + k * 1_500, "items": [{"field": "status", "to": str(k)}, {"field": "assignee", "to": f"u{k}"}]} for k in range(2)]
		more = page + 1 < len(ids)
		log: dict[str, Any] = {"changeHistories": histories} if path.endswith("-noid") and issue == 11 else {"issueId": str(issue), "changeHistories": histories}
		self.reply(200, {"total": len(ids), "issueChangeLogs": [log], "next": str(page + 1) if more else None})

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
			"/v1/ahead": self.ahead,
			"/v1/ahead_short": self.ahead,
			"/v1/ahead_end": self.ahead_end,
			"/v1/ahead_wide": self.ahead,
			"/v1/refuse_once": self.refuse_once,
			"/v1/pages": self.page_number,
			"/v1/cursor": self.cursor,
			"/v1/link": self.link,
			"/v1/flaky": self.flaky,
			"/v1/last": lambda query: self.reply(200, {"sheets": [{"rows": [{"order_id": 0, "status": "stale"}]}, {"rows": ORDERS[int(query.get("page", "0")) * 3 :][:3]}], "links": [{"href": "/v1/broken"}, {"href": f"/v1/last?page={int(query.get('page', '0')) + 1}" if int(query.get("page", "0")) * 3 + 3 < len(ORDERS) else None}]}),
			"/v1/grouped": lambda query: self.reply(200, {"groups": [{"name": "g1", "rows": [{"status": "paid", "order_id": 1, "customer": {"name": "Ann", "city": "Leeds"}}]}]}),
			"/v1/nested": lambda query: self.reply(200, {"items": [{"order_id": 1, "customer": {"name": "Ann", "address": {"city": "Leeds", "geo": {"lat": 1}}}}]}),
			"/v1/broken": lambda query: self.reply(500, {"message": f"down, token {TOKEN}"}),
			"/v1/long": lambda query: self.reply(400, {"message": "x" * 280 + f" token {TOKEN} and {TOKEN}"}),
			"/v1/redir": lambda query: self.reply(302, {}, {"Location": f"{self.server.other}/steal"}),  # type: ignore[attr-defined]
			"/v1/away": lambda query: self.reply(200, {"items": ORDERS[:1], "next": f"{self.server.other}/steal"}),  # type: ignore[attr-defined]
			"/v1/cycle": lambda query: self.reply(200, {"items": [], "next": "B" if query.get("c") == "A" else "A"}),
			"/v1/wrapped": lambda query: self.reply(200, {"items": ORDERS[:2]}),
			"/v1/gap": lambda query: self.reply(200, {"items": ORDERS[:3]} if query["p"] == "1" else {}),
			"/v1/quota": lambda query: self.reply(200, {"items": ORDERS[:3]} if query["p"] == "1" else {"error": {"message": "daily quota used"}}),
			"/v1/capped": lambda query: self.reply(200, ORDERS[int(query["offset"]) : int(query["offset"]) + 3]),
			"/v1/capped_small": lambda query: self.reply(200, ORDERS[:2][int(query["offset"]) : int(query["offset"]) + 3]),
			"/v1/rounded": lambda query: self.reply(200, ORDERS[int(query["offset"]) // int(query["limit"]) * int(query["limit"]) :][: int(query["limit"])]),
			"/v1/comma": self.comma,
			"/v1/loop-next": lambda query: self.reply(200, {"items": ORDERS[:1] if "page" not in query else [], "next": "/v1/loop-next?page=1"}),
			"/v1/loop-link": lambda query: self.reply(200, ORDERS[:1] if "page" not in query else [], {"Link": f'<{self.server_url()}/v1/loop-link?page=1>; rel="next"'}),
			"/v1/alert": self.alert,
			"/v1/alert-long": lambda query: self.reply(200, {"items": ORDERS, "errors": {"code": 429, "detail": "x" * 300}}),
			"/v1/alert-only": lambda query: self.reply(200, {"errors": [{"message": f"token {TOKEN} expired"}]}),
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
		if len(parts) == 5 and parts[2] == "regions" and parts[4] == "orders":
			page = int(query["p"])
			return self.reply(200, {"items": [dict(row, region=parts[3]) for row in ORDERS[(page - 1) * 3 : page * 3]]})
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
		"""Orders after `$.filter.after`, paged by `$.page.offset`/`$.page.limit` or by `$.page.cursor` (four at a time); `/v1/search-stuck` answers cursor 4 every time."""
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
		if url.path in ("/v1/changelog", "/v1/changelog-wrong", "/v1/changelog-noid"):
			return self.changelog(url.path, body)
		after = body.get("filter", {}).get("after") or 0
		rows = [row for row in ORDERS if row["order_id"] > after]
		page = body.get("page", {})
		if url.path == "/v1/search":
			start, limit = page["offset"], page["limit"]
			return self.reply(200, {"items": rows[start : start + limit]})
		start = int(page.get("cursor") or 0)
		if url.path == "/v1/search-stuck":
			return self.reply(200, {"items": rows[:4] if start == 0 else [], "meta": {"next": "4"}})
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
		if form.get("grant_type") == "refresh_token" and oauth.get("slow_refresh"):
			time.sleep(oauth["slow_refresh"])
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

	def ahead_end(self, query: dict[str, str]) -> None:
		"""One order per offset: the empty page after the last order answers after 0.3 s, and every page past it answers 401 at once, so a page read
		ahead asks for a new token just before the run ends."""
		start = int(query["offset"])
		if start > len(ORDERS):
			self.server.refused += 1  # type: ignore[attr-defined]
			return self.reply(401, {"message": "token refused"})
		if start == len(ORDERS):
			time.sleep(0.3)
		self.reply(200, ORDERS[start : start + 1])

	def refuse_once(self, query: dict[str, str]) -> None:
		"""Answers 401 to its first request and the orders after it, so the run asks for a new token."""
		if not self.server.refused_once:  # type: ignore[attr-defined]
			self.server.refused_once = True  # type: ignore[attr-defined]
			self.server.refused += 1  # type: ignore[attr-defined]
			return self.reply(401, {"message": "token refused"})
		self.reply(200, ORDERS)

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

	def ahead(self, query: dict[str, str]) -> None:
		"""Offset pages of the orders for reading ahead, logging each request's offset and arrival in `self.server.ahead` and each answer's offset
		in `ahead_answered`, with the most requests open at once in `most_open`. Offset 2 answers slowly, so the pages after it answer first;
		offset 4 answers 503 the first time; on `/v1/ahead_short` offset 4 holds one row, so the paging ends there although offset 6 has more;
		on `/v1/ahead_wide` each order carries a 20 MiB note."""
		server = self.server
		start, limit = int(query["offset"]), int(query["limit"])
		with server.ahead_lock:  # type: ignore[attr-defined]
			server.ahead.append((start, time.monotonic()))  # type: ignore[attr-defined]
			server.open += 1  # type: ignore[attr-defined]
			server.most_open = max(server.most_open, server.open)  # type: ignore[attr-defined]
		try:
			if start == 2:
				time.sleep(0.4)
			if start == 4 and not server.ahead_failed:  # type: ignore[attr-defined]
				server.ahead_failed = True  # type: ignore[attr-defined]
				return self.reply(503, {}, {"Retry-After": "0"})
			short = self.path.startswith("/v1/ahead_short") and start == 4
			rows = ORDERS[start : start + (1 if short else limit)]
			if self.path.startswith("/v1/ahead_wide"):
				rows = [{**row, "note": "x" * (20 << 20)} for row in rows]
			self.reply(200, rows)
		finally:
			with server.ahead_lock:  # type: ignore[attr-defined]
				server.open -= 1  # type: ignore[attr-defined]
				server.ahead_answered.append(start)  # type: ignore[attr-defined]

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

	def alert(self, query: dict[str, str]) -> None:
		"""Three orders a page by `p`, with `$.errors` null on page 1 and, on page 2, the JSON value of `e` beside the orders."""
		page = int(query["p"])
		errors = json.loads(query["e"]) if page == 2 else None
		self.reply(200, {"items": ORDERS[(page - 1) * 3 : page * 3], "errors": errors})

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
	server.ahead, server.ahead_answered, server.ahead_lock = [], [], threading.Lock()  # type: ignore[attr-defined]
	server.open, server.most_open, server.ahead_failed = 0, 0, False  # type: ignore[attr-defined]
	server.lookup_failed = False  # type: ignore[attr-defined]
	server.refused = 0  # type: ignore[attr-defined]
	server.refused_once = False  # type: ignore[attr-defined]
	server.oauth = {}  # type: ignore[attr-defined]
	server.tokens = {}  # type: ignore[attr-defined]
	server.token_requests = []  # type: ignore[attr-defined]
	server.refresh_token = REFRESH_TOKEN  # type: ignore[attr-defined]
	host, port = elsewhere.server_address[:2]
	server.other = f"http://localhost:{port}"  # type: ignore[attr-defined]
	yield server
	server.shutdown()


BEARER = 'auth = { kind = "bearer" }\nretries = 3'
QUICK = 'auth = { kind = "bearer" }\nretries = 0'


def make_project(tmp_path: Path, server: ThreadingHTTPServer, tables: str, connection: str = BEARER) -> Path:
	"""A project whose `shop` connection also sets `connection` (where `TOKEN_URL` is the stub's token endpoint), with `api = sl.http(shop)`."""
	host, port = server.server_address[:2]
	connection = connection.replace("TOKEN_URL", f"http://{host}:{port}/oauth/token")
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
			CONNECTION

			[targets.dev]
			connection = "wh"
			loader_schema = "RAW"
		""").replace("CONNECTION", connection)
	)
	module = f"import sqlload as sl\n\nTOKEN_URL = 'http://{host}:{port}/oauth/token'\nshop = sl.source('shop', write_strategy='replace', connection='shop')\napi = sl.http(shop)\n\n" + textwrap.dedent(tables)
	(tmp_path / "sources" / "shop.py").write_text(module)
	return tmp_path


def sqlload(project: Path, *args: str, script: str = "from sqlload._cli import main; main()") -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "SHOP_TOKEN": TOKEN, "SHOP_CLIENT_SECRET": CLIENT_SECRET, "SHOP_PASSWORD": PASSWORD}
	return subprocess.run([sys.executable, "-c", script, *args], cwd=project, env=env, capture_output=True, text=True, timeout=120)


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
		("api.paginate('/gap', paginator=sl.page_number('p'), select='$.items', select_missing='empty')", ALL_ORDERS[:3]),
		("api.paginate('/moved', paginator=sl.next_link('$.next'), select='$.items')", ALL_ORDERS),
		("api.paginate('/sparse', paginator=sl.link_header())", ALL_ORDERS[:2]),
		("api.paginate('/last', paginator=sl.next_link('$.links[-1].href'), select='$.sheets[-1].rows')", ALL_ORDERS),
	],
	ids=[
		"next-link",
		"offset",
		"page-number",
		"cursor",
		"link-header",
		"retries-429-and-503",
		"link-with-comma-unquoted-rel",
		"page-without-the-selected-key-ends-with-select-missing-empty",
		"same-origin-redirect-then-relative-next-link",
		"link-header-through-empty-pages",
		"select-and-next-link-counting-from-the-end",
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


def test_given_a_dotted_flatten_path_two_levels_deep_when_a_job_loads_then_the_inner_object_flattens_too(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(
		tmp_path, api, "@shop.table(description='Orders.', flatten=['customer', 'customer.address'])\ndef orders(ctx):\n    return api.paginate('/nested', paginator=sl.single(), select='$.items')\n"
	)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, customer__name, customer__address__city, customer__address__geo::VARCHAR FROM "RAW".shop__orders') == [(1, "Ann", "Leeds", '{"lat":1}')]


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


#: A page transform logging each call to `calls.log`, then yielding the page's rows one at a time with where they came from.
PAGE_TRANSFORM = """
@sl.transform
def tag_page(rows, ctx):
    with open("calls.log", "a") as log:
        log.write(f"{ctx.key['region']}:{ctx.page}\\n")
    for position, row in enumerate(rows.to_pylist()):
        yield {**row, "page": ctx.page, "position": position, "unit": ctx.unit, "key_region": ctx.key["region"]}

"""


@pytest.mark.parametrize("transform", ["tag_page", "'tag_page'"], ids=["the-function", "its-registered-name"])
def test_given_a_fan_out_with_a_page_transform_when_loading_then_each_page_goes_through_it_once_in_page_order(
	tmp_path: Path, api: ThreadingHTTPServer, transform: str
) -> None:
	job = f"api.fan_out('/regions/{{region}}/orders', over=['north', 'south'], concurrency=2, paginator=sl.page_number('p'), select='$.items', transform={transform})"
	project = make_project(tmp_path, api, PAGE_TRANSFORM + f"@shop.table(description='Orders by region.')\ndef orders(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	calls = (project / "calls.log").read_text().split()
	# Page 4 is the empty page that ends page-number paging: a page all the same, called with no rows.
	assert sorted(calls) == [f"{region}:{page}" for region in ("north", "south") for page in range(1, 5)], calls
	rows = query(project, 'SELECT region, key_region, unit, page, position, order_id FROM "RAW".shop__orders ORDER BY rowid')
	assert {(region, key_region, unit) for region, key_region, unit, *_ in rows} == {("north", "north", "page"), ("south", "south", "page")}
	for region in ("north", "south"):
		loaded = [(page, position, order_id) for row_region, _, _, page, position, order_id in rows if row_region == region]
		assert loaded == sorted(loaded), loaded
		assert [order_id for *_, order_id in loaded] == [row["order_id"] for row in ORDERS], loaded


def test_given_a_module_level_job_with_a_page_transform_when_loading_twice_in_one_process_then_both_loads_call_it(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	module = """
@sl.transform
def clean_page(rows, ctx):
    with open("calls.log", "a") as log:
        log.write(f"{ctx.page}\\n")
    return rows

orders_job = api.paginate('/next', paginator=sl.next_link('$.next'), select='$.data.items', transform=clean_page)

@shop.table(description='Orders.')
def orders(ctx):
    return orders_job
"""
	project = make_project(tmp_path, api, module)
	# An orchestrator runs commands in its own process, where the module (and its job) stays imported between them.
	twice = "from sqlload import _native; print('exit codes', [_native.run_cli(['sqlload', 'load']) for _ in range(2)])"
	done = sqlload(project, script=twice)
	assert "exit codes [0, 0]" in done.stdout, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__orders ORDER BY order_id') == ALL_ORDERS
	pages = (project / "calls.log").read_text().split()
	assert len(pages) % 2 == 0 and pages[: len(pages) // 2] == pages[len(pages) // 2 :], pages


@pytest.mark.parametrize(
	("failure", "expected_text"),
	[
		("raise ValueError('page 2 is malformed')", "ValueError: page 2 is malformed"),
		("return 42", "it returned int; return or yield"),
	],
	ids=["raises", "returns-what-cannot-load"],
)
def test_given_a_page_transform_that_fails_on_one_page_when_loading_then_the_table_fails_naming_the_page(
	tmp_path: Path, api: ThreadingHTTPServer, failure: str, expected_text: str
) -> None:
	module = f"""
@sl.transform
def strict_page(rows, ctx):
    if ctx.page == 2:
        {failure}
    return rows

@shop.table(description='Orders.')
def orders(ctx):
    return api.paginate('/pages', paginator=sl.page_number('p'), select='$.items', transform=strict_page)
"""
	project = make_project(tmp_path, api, module)
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	output = done.stdout + done.stderr
	assert "GET http://" in output and f"/v1/pages?p=2: transform strict_page failed: {expected_text}" in output, output
	assert query(project, 'SELECT count(*) FROM "RAW".shop__orders') == [(0,)]


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
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/wrapped', paginator=sl.page_number('p'))\n",
			"offset and page-number paging count the rows in a list, but select \"$\" is not one",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/long', paginator=sl.single())\n",
			"HTTP 400",
		),
		(
			"versioned = sl.http(shop, headers={'X-Version': '1', 'X-Session-Id': 'abcdef12', 'X-Credential': 'abc123secret'})\n\n@shop.table(description='T.')\ndef t(ctx):\n    return versioned.paginate('/echo', paginator=sl.single())\n",
			"version 1 session *** credential ***",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/gap', paginator=sl.page_number('p'), select='$.items')\n",
			"/v1/gap?p=2: select \"$.items\" is not in the response, which holds an empty object; an API can answer an error with a 200 status, so check the response. For an API that leaves an empty list out, set select_missing=\"empty\"",
		),
		(
			"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/quota', paginator=sl.page_number('p'), select='$.items')\n",
			"/v1/quota?p=2: select \"$.items\" is not in the response, which holds the keys error;",
		),
	],
	ids=[
		"server-error-retried-and-redacted",
		"unlisted-status-fails",
		"bad-definition-fails-before-requests",
		"python-row-loop",
		"page-number-over-an-object",
		"token-at-the-excerpt-cut",
		"only-secret-looking-headers-redacted",
		"page-without-the-selected-key-fails",
		"error-body-with-status-200-fails",
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
	("job", "expected_text", "expected_requests"),
	[
		(
			"api.paginate('/cycle', paginator=sl.cursor('$.next', 'c'), select='$.items')",
			"GET {base}/cycle?c=B: cursor A repeated after page 3, so paging would read page 2 again; check the paginator's pointer",
			3,
		),
		(
			"api.paginate('/search-stuck', method='POST', paginator=sl.cursor('$.meta.next', '$.page.cursor', location='body'), select='$.items')",
			"POST {base}/search-stuck: cursor 4 repeated after page 2, so paging would read page 2 again; check the paginator's pointer",
			2,
		),
		(
			"api.paginate('/loop-next', paginator=sl.next_link('$.next'), select='$.items')",
			"GET {base}/loop-next?page=1: next link /v1/loop-next?page=1 repeated after page 2, so paging would read page 2 again; check the paginator's pointer",
			2,
		),
		(
			"api.paginate('/loop-link', paginator=sl.link_header())",
			"GET {base}/loop-link?page=1: Link header next {base}/loop-link?page=1 repeated after page 2, so paging would read page 2 again; check the paginator's pointer",
			2,
		),
		(
			"api.paginate('/wrapped', paginator=sl.offset(limit=2), select='$.items')",
			"GET {base}/wrapped?offset=2&limit=2: the response repeats the previous page; the API may ignore the paginator's parameter",
			2,
		),
		(
			"api.paginate('/wrapped', paginator=sl.page_number('p'), select='$.items')",
			"GET {base}/wrapped?p=2: the response repeats the previous page; the API may ignore the paginator's parameter",
			2,
		),
	],
	ids=["cursor-in-the-query", "cursor-in-the-body", "next-link", "link-header", "offset-api-ignoring-the-offset", "page-number-api-ignoring-the-page"],
)
def test_given_a_paginator_sent_back_to_a_page_it_read_when_loading_then_the_load_fails_naming_the_value_and_the_page(
	tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_text: str, expected_requests: int
) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	host, port = api.server_address[:2]
	assert expected_text.format(base=f"http://{host}:{port}/v1") in done.stdout + done.stderr, done.stdout + done.stderr
	assert len(api.log) + len(api.bodies) == expected_requests  # type: ignore[attr-defined]


def test_given_a_null_next_link_or_cursor_when_paging_then_paging_ends_after_that_page(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	tables = (
		"@shop.table(description='T.')\ndef by_link(ctx):\n    return api.paginate('/next', paginator=sl.next_link('$.next'), select='$.data.items')\n\n"
		"@shop.table(description='T.')\ndef by_cursor(ctx):\n    return api.paginate('/search-cursor', method='POST', paginator=sl.cursor('$.meta.next', '$.page.cursor', location='body'), select='$.items')\n"
	)
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert [query for path, query, _ in api.log if path == "/v1/next"] == [{}, {"page": "1"}, {"page": "2"}]  # type: ignore[attr-defined]
	assert [body for path, _, body in api.bodies] == [{}, {"page": {"cursor": "4"}}]  # type: ignore[attr-defined]
	assert query(project, 'SELECT (SELECT count(*) FROM "RAW".shop__by_link), (SELECT count(*) FROM "RAW".shop__by_cursor)') == [(7, 7)]


#: A page transform logging each page it is called with to `pages.log`.
PAGE_LOG = """
@sl.transform
def log_page(rows, ctx):
    with open("pages.log", "a") as log:
        log.write(f"{ctx.page}\\n")
    return rows

"""


@pytest.mark.parametrize("value", ["null", "false", '""', "[]", "{}"])
def test_given_error_path_when_a_page_holds_nothing_there_then_every_page_loads(tmp_path: Path, api: ThreadingHTTPServer, value: str) -> None:
	job = f"api.paginate('/alert', params={{'e': {value!r}}}, paginator=sl.page_number('p'), select='$.items', error_path='$.errors')"
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT count(*) FROM "RAW".shop__t') == [(len(ORDERS),)]


@pytest.mark.parametrize(
	("job", "expected_text", "unexpected_text", "expected_pages"),
	[
		(
			"api.paginate('/alert', params={'e': '\"daily quota used\"'}, paginator=sl.page_number('p'), select='$.items', error_path='$.errors', transform=log_page)",
			"&p=2: the API reported an error at $.errors: daily quota used",
			'"daily quota used"',
			["1"],
		),
		(
			"api.paginate('/alert', params={'e': '0'}, paginator=sl.page_number('p'), select='$.items', error_path='$.errors', transform=log_page)",
			"/v1/alert?e=0&p=2: the API reported an error at $.errors: 0",
			'select "$.items"',
			["1"],
		),
		(
			"api.paginate('/alert-long', paginator=sl.single(), select='$.items', error_path='$.errors', transform=log_page)",
			'/v1/alert-long: the API reported an error at $.errors: {"code":429,"detail":"' + "x" * 178,
			"x" * 179,
			[],
		),
		(
			"api.paginate('/alert-only', paginator=sl.single(), select='$.items', error_path='$.errors', transform=log_page)",
			'/v1/alert-only: the API reported an error at $.errors: [{"message":"token *** expired"}]',
			TOKEN,
			[],
		),
	],
	ids=["text-beside-rows", "zero-beside-rows", "long-object-beside-rows-cut-at-200-characters", "error-only-body-with-a-secret"],
)
def test_given_error_path_when_a_page_reports_an_error_then_the_table_fails_with_it_before_that_pages_rows_load(
	tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_text: str, unexpected_text: str, expected_pages: list[str]
) -> None:
	project = make_project(tmp_path, api, PAGE_LOG + f"@shop.table(description='T.')\ndef t(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	assert unexpected_text not in done.stdout + done.stderr, done.stdout + done.stderr
	logged = project / "pages.log"
	assert (logged.read_text().split() if logged.exists() else []) == expected_pages
	# A table whose first page fails is never created; one whose later page fails is left without rows.
	created = query(project, "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'RAW' AND table_name = 'shop__t'") == [(1,)]
	assert not created or query(project, 'SELECT count(*) FROM "RAW".shop__t') == [(0,)]


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
	project = make_project(tmp_path, api, f"quick = api\n\n@shop.table(description='Items.')\ndef items(ctx):\n    return {job}\n", QUICK)
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
quick = api

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
	project = make_project(tmp_path, api, FAIL_FAST_TABLES.format(path=path), QUICK)
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


@pytest.mark.parametrize("read_ahead", ["", ", read_ahead=3"], ids=["one-page-at-a-time", "reading-ahead"])
def test_given_offset_paging_when_the_api_caps_its_pages_below_limit_then_the_load_fails_naming_the_page_size(tmp_path: Path, api: ThreadingHTTPServer, read_ahead: str) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/capped', paginator=sl.offset(limit=5{read_ahead}))\n")
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "the first page held 3 rows, fewer than limit 5, but the API returned 3 more at offset 3, so it caps its pages at 3 rows; set limit=3" in done.stdout + done.stderr, done.stdout + done.stderr


@pytest.mark.parametrize("read_ahead", ["", ", read_ahead=3"], ids=["one-page-at-a-time", "reading-ahead"])
def test_given_offset_paging_when_a_short_first_page_is_the_whole_table_then_one_more_request_confirms_the_end(tmp_path: Path, api: ThreadingHTTPServer, read_ahead: str) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/offset', paginator=sl.offset(limit=10{read_ahead}))\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT count(*) FROM "RAW".shop__t') == [(len(ORDERS),)]
	assert [(path, query["offset"]) for path, query, _ in api.log] == [("/v1/offset", "0"), ("/v1/offset", str(len(ORDERS)))]  # type: ignore[attr-defined]
	assert "WARN" not in done.stderr, done.stderr


def test_given_a_fan_out_over_a_capped_api_when_one_key_ends_on_a_short_page_then_a_later_key_is_still_checked(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api, "@shop.table(description='T.')\ndef t(ctx):\n    return api.fan_out('/{part}', over=['capped_small', 'capped'], concurrency=1, paginator=sl.offset(limit=5))\n")
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "/v1/capped?offset=0&limit=5: the first page held 3 rows, fewer than limit 5, but the API returned 3 more at offset 3" in done.stdout + done.stderr, done.stdout + done.stderr


@pytest.mark.parametrize(
	("path", "expected_orders"),
	[("/ahead", [order["order_id"] for order in ORDERS]), ("/ahead_short", [1, 2, 3, 4, 5])],
	ids=["to-the-last-page", "stops-at-a-short-page-with-later-pages-in-flight"],
)
def test_given_read_ahead_when_later_pages_answer_first_and_one_is_retried_then_rows_load_in_page_order(
	tmp_path: Path, api: ThreadingHTTPServer, path: str, expected_orders: list[int]
) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('{path}', paginator=sl.offset(limit=2, read_ahead=3))\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert [order for (order,) in query(project, 'SELECT order_id FROM "RAW".shop__t')] == expected_orders
	answered = api.ahead_answered  # type: ignore[attr-defined]
	assert max(index for index, start in enumerate(answered) if start == 4) < answered.index(2), answered
	assert [start for start, _ in api.ahead].count(4) == 2  # type: ignore[attr-defined]
	assert 2 <= api.most_open <= 3, api.most_open  # type: ignore[attr-defined]


def test_given_read_ahead_under_a_memory_budget_when_pages_are_larger_than_a_quarter_of_the_tables_share_then_one_page_is_requested_at_a_time(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	# At a 1000MB budget, DuckDB's 488 MiB leaves the tables under 512 MiB, so each table's share is under 128 MiB and a quarter of it,
	# what the pages requested ahead may hold, is under the two 20 MiB pages that a second request in flight would add.
	project = make_project(tmp_path, api, "@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/ahead_wide', paginator=sl.offset(limit=1, read_ahead=3))\n")
	done = sqlload(project, "load", "--memory-limit", "1000MB")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT count(*), min(length(note)) FROM "RAW".shop__t') == [(len(ORDERS), 20 << 20)]
	assert api.most_open == 1, api.most_open  # type: ignore[attr-defined]


def test_given_read_ahead_and_a_rate_limit_when_loading_then_every_request_waits_its_turn(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	tables = "@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/ahead', paginator=sl.offset(limit=1, read_ahead=4))\n"
	project = make_project(tmp_path, api, tables, BEARER + '\nrate_limit = "10/s"')
	done = sqlload(project, "load", "--events", "jsonl")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT count(*) FROM "RAW".shop__t') == [(len(ORDERS),)]
	events = [json.loads(line) for line in done.stderr.splitlines() if line.startswith("{")]
	waits = [event for event in events if event["event"] == "rate_limited"]
	# Read ahead, every page after the first goes in a task of its own, so pages that waited for their turn show the tasks take turns
	# from the limiter; the limiter's spacing of turns is checked on tokio's paused clock (test_limiter.rs), not by the wall clock here.
	assert api.most_open >= 2 and waits and {event["rate"] for event in waits} == {"10/s"}, done.stderr  # type: ignore[attr-defined]


@pytest.mark.parametrize("paginator", ["sl.cursor('$.meta.next', 'after', read_ahead=2)", "sl.next_link('$.next', read_ahead=2)", "sl.link_header(read_ahead=2)"])
def test_given_a_token_paginator_when_it_asks_to_read_ahead_then_the_load_fails_before_any_request(tmp_path: Path, api: ThreadingHTTPServer, paginator: str) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/cursor', paginator={paginator})\n")
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "cannot take read_ahead: each page's address comes from the page before it; read_ahead needs sl.offset or sl.page_number" in done.stdout + done.stderr
	assert api.log == []  # type: ignore[attr-defined]


def test_given_offset_paging_when_the_api_rounds_the_offset_to_a_page_then_a_short_first_page_is_the_end(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api, "@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/rounded', paginator=sl.offset(limit=10))\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT count(*) FROM "RAW".shop__t') == [(len(ORDERS),)]


def test_given_an_api_key_in_the_query_when_loading_then_it_is_sent_only_to_the_connection(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	tables = "@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/offset', paginator=sl.offset(limit=10))\n"
	project = make_project(tmp_path, api, tables, 'auth = { kind = "api_key", query = "key" }')
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert [(path, query.get("key"), auth) for path, query, auth in api.log] == [("/v1/offset", TOKEN, None)] * 2  # type: ignore[attr-defined]


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
	tables = f"keyed = sl.http(shop, headers={{'X-Team': 'blue'}})\n\n@shop.table(description='T.')\ndef t(ctx):\n    return keyed.paginate('{path}', paginator={paginator}, select='$.items')\n"
	project = make_project(tmp_path, api, tables, 'auth = { kind = "api_key", header = "X-Api-Key" }')
	done = sqlload(project, "load")
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	sent = {name.lower() for headers in elsewhere.seen for name in headers}  # type: ignore[attr-defined]
	assert not sent & {"x-api-key", "x-team", "authorization"}


def test_given_a_datetime_param_when_loading_then_it_is_sent_as_iso_text(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	tables = "import datetime\n\n@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/offset', paginator=sl.offset(limit=10), params={'since': datetime.datetime(2024, 1, 2, 3, 4)})\n"
	project = make_project(tmp_path, api, tables)
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert [query.get("since") for _, query, _ in api.log] == ["2024-01-02T03:04:00"] * 2  # type: ignore[attr-defined]


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
	("options", "expected_columns"),
	[
		("", ["status", "order_id", "customer"]),
		(", fields={'group': sl.parent('$.name'), 'status': '$.status', 'order_id': '$.order_id', 'customer': '$.customer'}, keep_record='raw'", ["group", "status", "order_id", "customer", "raw"]),
	],
	ids=["select-through-two-lists", "parent-field-and-kept-record"],
)
def test_given_rows_rebuilt_before_they_load_when_they_load_then_they_keep_the_api_key_order(
	tmp_path: Path, api: ThreadingHTTPServer, options: str, expected_columns: list[str]
) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return api.paginate('/grouped', paginator=sl.single(), select='$.groups[*].rows[*]'{options})\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	columns = [name for (name,) in query(project, "SELECT column_name FROM information_schema.columns WHERE table_name = 'shop__t' AND column_name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY ordinal_position")]
	assert columns == expected_columns
	assert query(project, 'SELECT customer FROM "RAW".shop__t') == [('{"name":"Ann","city":"Leeds"}',)]
	if options:
		(raw,) = query(project, 'SELECT raw FROM "RAW".shop__t')[0]
		assert list(json.loads(raw)) == ["status", "order_id", "customer"]


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
	"""A project whose connection sets `auth` (TOML, after `auth = `), with one table running `job`."""
	tables = f"@shop.table(description='T.')\ndef t(ctx):\n    return api.{job}\n"
	return make_project(tmp_path, server, tables, f"auth = {auth}")


@pytest.mark.parametrize(
	("auth", "expected_grant"),
	[('{ kind = "oauth2_client_credentials", token_url = "TOKEN_URL", scope = "orders:read" }', "client_credentials"), ('{ kind = "oauth2_password", token_url = "TOKEN_URL", scope = "orders:read" }', "password")],
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
		('{ kind = "oauth2_client_credentials", token_url = "TOKEN_URL" }', [("client_credentials", None)] * 3),
		('{ kind = "oauth2_refresh_token", token_url = "TOKEN_URL", client_auth = "post" }', [("refresh_token", REFRESH_TOKEN), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-3")]),
		('{ kind = "oauth2_password", token_url = "TOKEN_URL" }', [("password", None), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-3")]),
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
			'{ kind = "oauth2_client_credentials", token_url = "TOKEN_URL", client_auth = "post" }',
			"paginate('/offset', paginator=sl.offset(limit=10))",
			"/oauth/token (OAuth2 token): HTTP 401: invalid_client: unknown client ['app', '***'] or refresh token; check the connection's OAuth2 credentials",
		),
		(
			'{ kind = "oauth2_client_credentials", token_url = "http://auth.example.com/token" }',
			"paginate('/offset', paginator=sl.offset(limit=10))",
			"token_url http://auth.example.com/token must be https",
		),
		(
			'{ kind = "oauth2_client_credentials", token_url = "TOKEN_URL" }',
			"paginate('/whoami', paginator=sl.single())",
			'HTTP 400: {"message": "you are Bearer ***"}',
		),
		(
			'{ kind = "oauth2_client_credentials", token_url = "TOKEN_URL" }',
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
	project = oauth_project(tmp_path, api, '{ kind = "oauth2_password", token_url = "TOKEN_URL", client_auth = "post" }', "paginate('/offset', paginator=sl.offset(limit=1))")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__t ORDER BY order_id') == ALL_ORDERS
	expected = [("password", "loader", PASSWORD, None), ("refresh_token", None, None, "stale")] * 2 + [("password", "loader", PASSWORD, None)]
	sent = [(request["grant_type"], request.get("username"), request.get("password"), request.get("refresh_token")) for request in api.token_requests]  # type: ignore[attr-defined]
	assert sent == expected
	assert all(request["client"] == ["app", CLIENT_SECRET] for request in api.token_requests)  # type: ignore[attr-defined]


def test_given_a_rotating_refresh_token_when_a_page_read_ahead_is_renewing_as_the_run_ends_then_the_rotated_token_is_kept(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	api.oauth = {"rotate": True, "slow_refresh": 1.0}  # type: ignore[attr-defined]
	tables = (
		"oauth = api\n\n"
		"@shop.table(description='T.')\ndef a_ahead(ctx):\n    return oauth.paginate('/ahead_end', paginator=sl.offset(limit=1, read_ahead=2))\n\n"
		"@shop.table(description='T.')\ndef b_after(ctx):\n    return oauth.paginate('/refuse_once', paginator=sl.single())\n"
	)
	project = make_project(tmp_path, api, tables, 'auth = { kind = "oauth2_refresh_token", token_url = "TOKEN_URL" }')
	done = sqlload(project, "load", "--concurrency", "1")
	assert done.returncode == 0, done.stdout + done.stderr
	for table in ("a_ahead", "b_after"):
		assert query(project, f'SELECT order_id, status FROM "RAW".shop__{table} ORDER BY order_id') == ALL_ORDERS
	sent = [(request["grant_type"], request.get("refresh_token")) for request in api.token_requests]  # type: ignore[attr-defined]
	assert sent == [("refresh_token", REFRESH_TOKEN), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-3")]


def test_given_a_token_endpoint_down_once_when_refreshing_after_a_password_grant_then_the_refresh_is_retried_without_the_password(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	api.oauth = {"uses": 3, "rotate": True, "refresh_503_once": True}  # type: ignore[attr-defined]
	project = oauth_project(tmp_path, api, '{ kind = "oauth2_password", token_url = "TOKEN_URL" }', "paginate('/offset', paginator=sl.offset(limit=1))")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT order_id, status FROM "RAW".shop__t ORDER BY order_id') == ALL_ORDERS
	expected = [("password", None), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-2"), ("refresh_token", "r3fresh-4")]
	assert [(request["grant_type"], request.get("refresh_token")) for request in api.token_requests] == expected  # type: ignore[attr-defined]


def test_given_a_token_endpoint_down_when_refreshing_without_retries_then_the_run_is_retryable_and_the_password_is_not_resent(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	api.oauth = {"uses": 3, "rotate": True, "refresh_503_once": True}  # type: ignore[attr-defined]
	project = oauth_project(tmp_path, api, '{ kind = "oauth2_password", token_url = "TOKEN_URL" }\nretries = 0', "paginate('/offset', paginator=sl.offset(limit=1))")
	done = sqlload(project, "load")
	assert done.returncode == 75, done.stdout + done.stderr
	assert [(request["grant_type"], request.get("refresh_token")) for request in api.token_requests] == [("password", None), ("refresh_token", "r3fresh-2")]  # type: ignore[attr-defined]


def test_given_a_wrong_password_when_loading_with_json_then_it_fails_for_good_and_hides_the_password(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = oauth_project(tmp_path, api, '{ kind = "oauth2_password", token_url = "TOKEN_URL" }', "paginate('/offset', paginator=sl.offset(limit=10))")
	(project / "sqlload.toml").write_text((project / "sqlload.toml").read_text().replace("${ENV:SHOP_PASSWORD}", "wr0ng+p\u00e4ss\\\\word"))
	done = sqlload(project, "load", "--json")
	assert done.returncode == 1, done.stdout + done.stderr
	result = json.loads(done.stdout)
	assert (result["ok"], result["retryable"], result["tables"][0]["retryable"]) == (False, False, False)
	assert 'HTTP 400: {"message":"wrong password *** for loader"}' in result["tables"][0]["error"]
	assert "wr0ng" not in done.stdout + done.stderr
	assert len(api.token_requests) == 1 and api.log == []  # type: ignore[attr-defined]


def test_given_401_listed_as_skip_when_the_api_answers_it_then_the_request_is_skipped_without_a_new_token(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = oauth_project(tmp_path, api, '{ kind = "oauth2_client_credentials", token_url = "TOKEN_URL" }', "fan_out('/denied?k={k}', over=range(5), concurrency=1, paginator=sl.single(), on_status={401: 'skip'})")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert len([path for path, _, _ in api.log if path == "/v1/denied"]) == 5  # type: ignore[attr-defined]
	assert len(api.token_requests) == 1  # type: ignore[attr-defined]


def test_given_a_401_from_another_origin_when_loading_then_no_token_is_sent_there_or_renewed_for_it(
	tmp_path: Path, api: ThreadingHTTPServer, elsewhere: ThreadingHTTPServer
) -> None:
	project = oauth_project(tmp_path, api, '{ kind = "oauth2_client_credentials", token_url = "TOKEN_URL" }', "paginate('/away401', paginator=sl.next_link('$.next'), select='$.items')")
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
		"typed = sl.http(shop, headers={'Content-Type': 'application/json; charset=utf-8'})\n\n"
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
	assert [(body["filter"]["after"], body["page"]["offset"]) for _, _, body in api.bodies] == [(None, 0), (None, 7), (7, 0)]  # type: ignore[attr-defined]
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


CHANGELOG = (
	"api.fan_out('/{route}', over=[10, 11, 12], method='POST', json_body={{'ids': '{{id}}'}}, batch=2, "
	"paginator=sl.cursor('$.next', '$.next', location='body'), select='$.issueChangeLogs[*].changeHistories[*]', "
	"fields={{'issue_id': sl.parent('$.issueId'), 'ids_sent': sl.parent('$.total', levels=2), 'history_id': sl.field('$.id', required=True), "
	"'created': sl.field('$.created', parse='epoch_ms'), 'items': '$.items'}}, key_path=sl.parent('$.issueId'))"
)


def test_given_a_select_through_two_lists_when_loading_then_each_history_carries_its_issue_the_envelope_and_a_parsed_time(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return {CHANGELOG.format(route='changelog')}\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	rows = query(project, 'SELECT issue_id, ids_sent, history_id, epoch_ms(created), items FROM "RAW".shop__t ORDER BY history_id')
	expected = [(str(issue), sent, issue * 10 + k, 1_700_000_000_000 + k * 1_500, f'[{{"field":"status","to":"{k}"}},{{"field":"assignee","to":"u{k}"}}]') for issue, sent in ((10, 2), (11, 2), (12, 1)) for k in range(2)]
	assert [(issue, sent, history, created, items.replace(" ", "")) for issue, sent, history, created, items in rows] == expected
	assert sorted(body["ids"] for path, _, body in api.bodies) == [[10, 11], [10, 11], [12]]  # type: ignore[attr-defined]


def test_given_key_path_when_a_page_inside_a_batch_answers_for_another_key_then_the_load_fails_naming_it(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return {CHANGELOG.format(route='changelog-wrong')}\n")
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert "a record's key 99 at key_path $.issueId is not among the keys its request sent (10, 11), so the API answered for another key" in done.stdout + done.stderr


INDEXED = (
	"api.fan_out('/changelog', over=[10, 12], method='POST', json_body={{'ids': '{{id}}'}}, batch=2, "
	"paginator=sl.cursor('$.next', '$.next', location='body'), select='$.issueChangeLogs[*].changeHistories[*]', "
	"fields={{'history_id': '$.id', 'first_to': '$.items[0].to', 'last_field': sl.field('$.items[-1].field'), "
	"'last_history': sl.parent('$.changeHistories[-1].id'), 'third_to': {third}}})"
)


@pytest.mark.parametrize(
	("third", "expected"),
	[
		("'$.items[2].to'", [(100, "0", "assignee", 101, None), (101, "1", "assignee", 101, None), (120, "0", "assignee", 121, None), (121, "1", "assignee", 121, None)]),
		("sl.field('$.items[-1].to', required=True)", [(100, "0", "assignee", 101, "u0"), (101, "1", "assignee", 101, "u1"), (120, "0", "assignee", 121, "u0"), (121, "1", "assignee", 121, "u1")]),
		("sl.field('$.items[2].to', required=True)", "field third_to needs a value at $.items[2].to, and the row has none"),
	],
	ids=["past-the-end-loads-null", "required-last-item-loads", "past-the-end-required-fails"],
)
def test_given_list_indexes_in_fields_when_loading_then_each_reads_its_item_and_one_past_the_end_is_null(
	tmp_path: Path, api: ThreadingHTTPServer, third: str, expected: Any
) -> None:
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return {INDEXED.format(third=third)}\n")
	done = sqlload(project, "load")
	if isinstance(expected, str):
		assert done.returncode == 1 and expected in done.stdout + done.stderr, done.stdout + done.stderr
		return
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT history_id, first_to, last_field, last_history, third_to FROM "RAW".shop__t ORDER BY history_id') == expected


@pytest.mark.parametrize(
	("path", "uses"),
	[
		("$.items[?(@.to)].to", "uses the filter [?(@.to)]"),
		("$.items[0:2]", "uses the slice [0:2]"),
		("$..to", "uses recursive descent (..)"),
		("$.items[*].to", "uses the wildcard [*]"),
	],
	ids=["filter", "slice", "recursive-descent", "wildcard"],
)
def test_given_unsupported_path_syntax_in_fields_when_loading_then_it_fails_before_any_request_naming_the_field_and_path(
	tmp_path: Path, api: ThreadingHTTPServer, path: str, uses: str
) -> None:
	job = INDEXED.format(third=repr(path))
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert f'fields["third_to"]: field path "{path}" {uses}' in " ".join((done.stdout + done.stderr).split()), done.stdout + done.stderr
	assert api.log == [] and api.bodies == []  # type: ignore[attr-defined]


@pytest.mark.parametrize(
	("required", "expected"),
	[("False", [("10", 100), ("10", 101), (None, 110), (None, 111), ("12", 120), ("12", 121)]), ("True", "field issue_id needs a value at $.issueId, and the row has none")],
	ids=["loads-null", "required-fails"],
)
def test_given_sl_parent_when_the_enclosing_object_lacks_the_value_then_required_decides(tmp_path: Path, api: ThreadingHTTPServer, required: str, expected: Any) -> None:
	job = (
		"api.fan_out('/changelog-noid', over=[10, 11, 12], method='POST', json_body={'ids': '{id}'}, batch=3, "
		"paginator=sl.cursor('$.next', '$.next', location='body'), select='$.issueChangeLogs[*].changeHistories[*]', "
		f"fields={{'issue_id': sl.parent('$.issueId', required={required}), 'history_id': '$.id'}})"
	)
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    return {job}\n")
	done = sqlload(project, "load")
	if isinstance(expected, str):
		assert done.returncode == 1 and expected in done.stdout + done.stderr, done.stdout + done.stderr
		return
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT issue_id, history_id FROM "RAW".shop__t ORDER BY history_id') == expected


def test_given_sl_key_when_loading_one_and_two_fan_out_levels_then_each_row_carries_its_requests_keys(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	notes = NOTES.replace("select='$.items')", "select='$.items', fields={'o': sl.key('order_id'), 'i': sl.key('item_id'), 'note': '$.note'})")
	items = "items_only = api.fan_out('/tree/orders/{order_id}/items', over=orders, select='$.items', on_status={404: 'skip'}, fields={'o': sl.key('order_id'), 'item': '$.item_id'})\n"
	project = tree_project(tmp_path, api, notes)
	tables = (project / "sources" / "shop.py").read_text()
	(project / "sources" / "shop.py").write_text(tables + "\n\n@shop.table(description='Items.')\ndef items(ctx):\n" + textwrap.indent(TREE.replace("AFTER", "None").replace("ON_STATUS", "{404: 'skip'}") + items, "    ") + "    return items_only\n")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	expected = sorted((item // 10, item, f"{item}-{page}-{n}") for item in TREE_ITEMS for page in (1, 2) for n in range(2))
	assert query(project, 'SELECT o, i, note FROM "RAW".shop__notes ORDER BY i, note') == expected
	assert query(project, 'SELECT o, item FROM "RAW".shop__items ORDER BY item') == [(item // 10, item) for item in TREE_ITEMS]


@pytest.mark.parametrize(
	("fields", "extra", "expected_text"),
	[
		("{'o': sl.key('order')}", "", "fields[\"o\"]: sl.key(\"order\") names no key of this fan-out; its keys are order_id, item_id"),
		("{'up': sl.parent('$.x', levels=2)}", "", "fields[\"up\"]: sl.parent(levels=2) needs 2 objects around each row, but select \"$.items\" passes through 1"),
		("{'o': sl.key('item_id')}", "batch=2, ", "fields[\"o\"]: sl.key(\"item_id\") can't tell which of a batch's keys a row is for"),
	],
	ids=["a-key-no-level-has", "more-parent-levels-than-select-has", "a-key-in-a-batch"],
)
def test_given_a_field_reading_around_rows_that_cannot_work_when_loading_then_it_fails_before_any_request(
	tmp_path: Path, api: ThreadingHTTPServer, fields: str, extra: str, expected_text: str
) -> None:
	notes = NOTES.replace("select='$.items')", f"{extra}select='$.items', fields={fields})")
	if extra:
		notes = notes.replace("'/tree/orders/{order_id}/items/{item_id}/notes'", "'/notes', method='POST', json_body={'ids': '{item_id}'}")
	done = sqlload(tree_project(tmp_path, api, notes), "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	assert api.log == [] and api.bodies == []  # type: ignore[attr-defined]


@pytest.mark.parametrize(
	("field", "expected"),
	[
		("sl.field('$.when', parse={'date': '%d/%m/%Y'}, on_error='null')", [(1, "2026-12-31"), (2, None)]),
		("sl.field('$.when', parse={'date': '%d/%m/%Y'})", "row 2: field when holds \"2026-12-31\", which is not a date in the format \"%d/%m/%Y\""),
	],
	ids=["on-error-null-loads-null", "a-bad-value-fails-naming-it"],
)
def test_given_parse_in_sl_json_when_a_value_does_not_parse_then_on_error_decides(tmp_path: Path, api: ThreadingHTTPServer, field: str, expected: Any) -> None:
	rows = '[{"id": 1, "when": "31/12/2026"}, {"id": 2, "when": "2026-12-31"}]'
	project = make_project(tmp_path, api, f"@shop.table(description='T.')\ndef t(ctx):\n    yield sl.json({rows!r}, fields={{'id': '$.id', 'when': {field}}})\n")
	done = sqlload(project, "load")
	if isinstance(expected, str):
		assert done.returncode == 1 and expected in done.stdout + done.stderr, done.stdout + done.stderr
		return
	assert done.returncode == 0, done.stdout + done.stderr
	assert query(project, 'SELECT id, CAST("when" AS VARCHAR) FROM "RAW".shop__t ORDER BY id') == expected


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
	shop_py.write_text(shop_py.read_text().replace("\n\n@shop.table", "\nonce = api\n\n@shop.table", 1))
	toml = project / "sqlload.toml"
	toml.write_text(toml.read_text().replace("retries = 3", "retries = 0"))
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
	'{"kind": "fan_out", "client": {"connection": "shop", "headers": {}}, '
	'"path": "/orders/{order_id}/items", "params": {}, "method": "GET", "json_body": null, "paginator": {"kind": "single"}, "select": "$.items", '
	'"on_status": {"404": "skip"}, "fields": null, "keep_record": null, "over": {"kind": "paginate", "client": {"connection": "shop", '
	'"headers": {}}, "path": "/orders", "params": {}, "method": "GET", '
	'"json_body": null, "paginator": {"kind": "page_number", "param": "p", "start": 1, "location": "query"}, "select": "$.items[*].order_id", '
	'"on_status": {}, "fields": null, "keep_record": null}, "concurrency": 3}'
)


def test_given_a_one_level_fan_out_when_defined_then_its_job_is_unchanged_by_multi_level_support() -> None:
	code = (
		"import sqlload as sl\nshop = sl.source('shop', write_strategy='replace', connection='shop')\napi = sl.http(shop)\n"
		"orders = api.paginate('/orders', paginator=sl.page_number('p'), select='$.items[*].order_id')\n"
		"print(api.fan_out('/orders/{order_id}/items', over=orders, concurrency=3, select='$.items', on_status={404: 'skip'}).__sqlload_http_job__())"
	)
	done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
	assert done.stdout.strip() == ONE_LEVEL_JOB, done.stderr
