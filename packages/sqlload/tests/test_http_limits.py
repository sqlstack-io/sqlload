"""The limits an `http` connection sets for every request to its API in one run: `rate_limit` paces request starts and
`max_in_flight` caps open requests, both shared by every table, page, `read_ahead` and retry on the connection. The stub
API answers slowly and records when each request arrived and how many were open at once."""

import json
import os
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest

import sqlload as sl

PAGES = 16
TOKEN = "l1mits-token"


class Api(BaseHTTPRequestHandler):
	"""`/<table>?offset&limit`: `PAGES` rows of one row a page, each answer after `self.server.delay` seconds; every request's
	arrival time goes in `self.server.arrivals`, and the most requests open at once in `self.server.most_open`. With
	`self.server.throttle`, every third request is answered 429 with `Retry-After: 0`."""

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		server: Any = self.server
		url = urlparse(self.path)
		query = {key: values[0] for key, values in parse_qs(url.query).items()}
		with server.lock:
			server.arrivals.append(time.monotonic())
			server.open += 1
			server.most_open = max(server.most_open, server.open)
			server.auth.add(self.headers.get("Authorization"))
			throttled = server.throttle and len(server.arrivals) % 3 == 0
		try:
			time.sleep(server.delay)
			if throttled:
				self.send_response(429)
				self.send_header("Retry-After", "0")
				self.send_header("Content-Length", "0")
				self.end_headers()
				return
			offset, limit = int(query.get("offset", 0)), int(query.get("limit", 1))
			rows = [{"id": n, "table": url.path} for n in range(offset, min(offset + limit, PAGES))]
			data = json.dumps(rows).encode()
			self.send_response(200)
			self.send_header("Content-Type", "application/json")
			self.send_header("Content-Length", str(len(data)))
			self.end_headers()
			self.wfile.write(data)
		finally:
			with server.lock:
				server.open -= 1


@pytest.fixture
def api() -> Iterator[ThreadingHTTPServer]:
	server = ThreadingHTTPServer(("127.0.0.1", 0), Api)
	server.daemon_threads = True
	server.lock = threading.Lock()  # type: ignore[attr-defined]
	server.arrivals, server.auth = [], set()  # type: ignore[attr-defined]
	server.open, server.most_open, server.delay, server.throttle = 0, 0, 0.0, False  # type: ignore[attr-defined]
	threading.Thread(target=server.serve_forever, daemon=True).start()
	yield server
	server.shutdown()


def make_project(tmp_path: Path, server: ThreadingHTTPServer, limits: str, tables: int) -> Path:
	"""A project whose `shop` connection sets `limits`, with `tables` tables of one source paging `/t<n>` at the same time."""
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
			auth = {{ kind = "bearer" }}
			{limits}

			[targets.dev]
			connection = "wh"
			loader_schema = "RAW"
		""")
	)
	module = "import sqlload as sl\n\nshop = sl.source('shop', write_strategy='replace', connection='shop', parallel_tables=True)\napi = sl.http(shop)\n"
	for n in range(tables):
		module += f"\n@shop.table(name='t{n}', description='T.')\ndef t{n}(ctx):\n    return api.paginate('/t{n}', paginator=sl.offset(limit=1, read_ahead=20))\n"
	(tmp_path / "sources" / "shop.py").write_text(module)
	return tmp_path


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "SHOP_TOKEN": TOKEN}
	env.pop("NO_COLOR", None)
	script = "from sqlload._cli import main; main()"
	return subprocess.run([sys.executable, "-c", script, *args], cwd=project, env=env, capture_output=True, text=True, timeout=120)


def loaded(project: Path, tables: int) -> list[int]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return [connection.execute(f'SELECT count(*) FROM "RAW".shop__t{n}').fetchall()[0][0] for n in range(tables)]


def test_given_two_tables_on_one_connection_when_they_load_at_once_then_together_they_stay_under_its_rate_limit(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	project = make_project(tmp_path, api, 'rate_limit = "20/s"', tables=2)
	done = sqlload(project, "load", "--concurrency", "4", "--events", "jsonl")
	assert done.returncode == 0, done.stdout + done.stderr
	assert loaded(project, 2) == [PAGES, PAGES]
	arrivals: list[float] = sorted(api.arrivals)  # type: ignore[attr-defined]
	# One limit of 20 a second for both tables: the requests' starts span at least (n - 1) / 20 seconds. Two limits, one per
	# table, would let them through in about half that. A slow machine only stretches the span, so the bound holds anywhere.
	assert arrivals[-1] - arrivals[0] >= (len(arrivals) - 1) / 20 * 0.95, (len(arrivals), arrivals[-1] - arrivals[0])
	assert api.auth == {f"Bearer {TOKEN}"}  # type: ignore[attr-defined]
	events = [json.loads(line) for line in done.stderr.splitlines() if line.startswith("{")]
	waits = [event for event in events if event["event"] == "rate_limited"]
	assert waits and {(event["connection"], event["rate"]) for event in waits} == {("shop", "20/s")}, done.stderr


def test_given_max_in_flight_when_tables_read_far_ahead_at_once_then_the_api_never_sees_more_open_requests(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	api.delay = 0.1  # type: ignore[attr-defined]
	project = make_project(tmp_path, api, "max_in_flight = 3", tables=4)
	done = sqlload(project, "load", "--concurrency", "4")
	assert done.returncode == 0, done.stdout + done.stderr
	assert loaded(project, 4) == [PAGES] * 4
	# Four tables reading 20 pages ahead could hold 80 requests open; the cap holds them at 3, and more than one was open at once.
	assert 2 <= api.most_open <= 3, api.most_open  # type: ignore[attr-defined]


def test_given_429s_when_requests_are_retried_then_each_retry_counts_against_both_limits(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	api.delay, api.throttle = 0.05, True  # type: ignore[attr-defined]
	project = make_project(tmp_path, api, 'rate_limit = "20/s"\nmax_in_flight = 2', tables=2)
	done = sqlload(project, "load", "--concurrency", "4")
	assert done.returncode == 0, done.stdout + done.stderr
	assert loaded(project, 2) == [PAGES, PAGES]
	arrivals: list[float] = sorted(api.arrivals)  # type: ignore[attr-defined]
	# A third of the requests were answered 429 and sent again: every attempt, retries included, took a turn and a permit.
	assert len(arrivals) >= 2 * PAGES * 3 // 2, len(arrivals)
	assert arrivals[-1] - arrivals[0] >= (len(arrivals) - 1) / 20 * 0.95, (len(arrivals), arrivals[-1] - arrivals[0])
	assert api.most_open <= 2, api.most_open  # type: ignore[attr-defined]


def test_given_an_http_connection_with_limits_when_planning_then_plan_shows_them_and_no_secret(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = make_project(tmp_path, api, 'rate_limit = "600/m"\nmax_in_flight = 4\nretries = 2', tables=1)
	done = sqlload(project, "plan", "-v")
	assert done.returncode == 0, done.stdout + done.stderr
	assert "http  shop  rate_limit 600/m, max_in_flight 4, retries 2, auth bearer" in done.stdout, done.stdout
	assert TOKEN not in done.stdout + done.stderr


@pytest.mark.parametrize(
	("argument", "expected_text"),
	[
		({"rate_limit": "10/s"}, "sl.http no longer takes rate_limit: set rate_limit on [connections.shop] in sqlload.toml"),
		({"retries": 3}, "sl.http no longer takes retries: set retries on [connections.shop] in sqlload.toml"),
		({"auth": "bearer"}, "sl.http no longer takes auth: set auth on [connections.shop] in sqlload.toml"),
	],
	ids=["rate_limit", "retries", "auth"],
)
def test_given_a_limit_passed_to_sl_http_when_building_the_client_then_it_names_the_connection_key_to_set(
	argument: dict[str, Any], expected_text: str
) -> None:
	shop = sl.source("shop", write_strategy="replace", connection="shop")
	with pytest.raises(TypeError) as raised:
		sl.http(shop, **argument)
	assert str(raised.value) == expected_text
