"""`sl.page_number(page_size=N)` ends a run at a page holding fewer than `N` rows, rather than asking for an empty page.
The stub API pages `/items/<key>` (`page`, `per_page`), holding as many rows as the key names, records every request it
answers, and can cap its pages below what is asked for."""

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


class Api(BaseHTTPRequestHandler):
	"""`/v1/items/<rows>?page=P&per_page=N`: `<rows>` rows in pages of `N` (or the server's `cap` when lower), page `P`
	counted from 1; each request's key, page and query go in `self.server.requests`."""

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		server: Any = self.server
		url = urlparse(self.path)
		query = {key: values[0] for key, values in parse_qs(url.query).items()}
		total = int(url.path.rsplit("/", 1)[-1])
		page = int(query.get("page", 1))
		size = min(int(query.get("per_page", server.fixed)), server.cap)
		with server.lock:
			server.requests.append((total, page, query))
		rows = [{"key": total, "n": n} for n in range((page - 1) * size, min(page * size, total))]
		data = json.dumps({"items": rows}).encode()
		self.send_response(200)
		self.send_header("Content-Type", "application/json")
		self.send_header("Content-Length", str(len(data)))
		self.end_headers()
		self.wfile.write(data)


@pytest.fixture
def api() -> Iterator[ThreadingHTTPServer]:
	server = ThreadingHTTPServer(("127.0.0.1", 0), Api)
	server.daemon_threads = True
	server.lock = threading.Lock()  # type: ignore[attr-defined]
	server.requests, server.cap, server.fixed = [], 10_000, 20  # type: ignore[attr-defined]
	threading.Thread(target=server.serve_forever, daemon=True).start()
	yield server
	server.shutdown()


def make_project(tmp_path: Path, server: ThreadingHTTPServer, job: str) -> Path:
	"""A project whose one table `items` is `job`, built with `api` on the stub's connection."""
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

			[targets.dev]
			connection = "wh"
			loader_schema = "RAW"
		""")
	)
	module = "import sqlload as sl\n\nshop = sl.source('shop', write_strategy='replace', connection='shop')\napi = sl.http(shop)\n"
	module += f"\n@shop.table(name='items', description='T.')\ndef items(ctx):\n    return {job}\n"
	(tmp_path / "sources" / "shop.py").write_text(module)
	return tmp_path


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config")}
	env.pop("NO_COLOR", None)
	script = "from sqlload._cli import main; main()"
	return subprocess.run([sys.executable, "-c", script, *args], cwd=project, env=env, capture_output=True, text=True, timeout=120)


def loaded(project: Path) -> list[tuple[int, int]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute('SELECT "key", n FROM "RAW".shop__items ORDER BY "key", n').fetchall()


def requests_per_key(server: ThreadingHTTPServer) -> dict[int, int]:
	counts: dict[int, int] = {}
	for key, _, _ in server.requests:  # type: ignore[attr-defined]
		counts[key] = counts.get(key, 0) + 1
	return counts


def fan_out(keys: list[int], paginator: str) -> str:
	return f"api.fan_out('/items/{{rows}}', over={[{'rows': key} for key in keys]!r}, concurrency=1, paginator={paginator}, select='$.items')"


@pytest.mark.parametrize(
	("keys", "expected_requests"),
	[
		([20, 19, 5, 12, 18, 41], {20: 2, 19: 1, 5: 1, 12: 1, 18: 1, 41: 3}),
		([12, 5, 19, 18, 25, 8], {12: 2, 5: 1, 19: 2, 18: 1, 25: 2, 8: 1}),
	],
	ids=["after-a-full-page", "before-any-full-page"],
)
def test_given_page_size_when_fanning_out_over_keys_then_a_short_page_ends_each_key_once_the_api_shows_it_does_not_cap_lower(
	tmp_path: Path, api: ThreadingHTTPServer, keys: list[int], expected_requests: dict[int, int]
) -> None:
	# A full page shows the API sends whole pages, so every short page after it ends its key in one request; a key with
	# exactly page_size rows still needs the empty page after it. Before any full page, a short first page at least as
	# long as any proven last page is checked with one more request, as the API might cap its pages there.
	project = make_project(tmp_path, api, fan_out(keys, "sl.page_number('page', page_size=20, page_size_param='per_page')"))
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert loaded(project) == sorted((key, n) for key in keys for n in range(key))
	assert requests_per_key(api) == expected_requests
	assert {query.get("per_page") for _, _, query in api.requests} == {"20"}  # type: ignore[attr-defined]


def test_given_an_api_capping_its_pages_below_page_size_when_paging_then_every_row_still_loads_and_the_cap_is_named(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	api.cap = 8  # type: ignore[attr-defined]
	project = make_project(tmp_path, api, fan_out([30, 5, 19], "sl.page_number('page', page_size=20, page_size_param='per_page')"))
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert loaded(project) == sorted((key, n) for key in (30, 5, 19) for n in range(key))
	# 30 rows: pages of 8, 8, 8 and 6, the second page showing the cap; then 5 rows in one page, and 19 in 8, 8 and 3.
	assert requests_per_key(api) == {30: 4, 5: 1, 19: 3}
	assert "set page_size=8" in done.stdout + done.stderr


def test_given_page_size_without_a_parameter_when_paging_then_it_only_describes_the_apis_fixed_page_size(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	project = make_project(tmp_path, api, "api.paginate('/items/45', paginator=sl.page_number('page', page_size=20, page_size_param=None), select='$.items')")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert len(loaded(project)) == 45
	assert [(page, query) for _, page, query in api.requests] == [(1, {"page": "1"}), (2, {"page": "2"}), (3, {"page": "3"})]  # type: ignore[attr-defined]


def test_given_page_size_and_read_ahead_when_the_last_page_is_short_then_paging_ends_there_with_every_row(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	project = make_project(tmp_path, api, "api.paginate('/items/93', paginator=sl.page_number('page', page_size=10, page_size_param='per_page', read_ahead=4), select='$.items')")
	done = sqlload(project, "load")
	assert done.returncode == 0, done.stdout + done.stderr
	assert loaded(project) == [(93, n) for n in range(93)]
	# Pages 1 to 10 hold the rows; pages read ahead past the short tenth are dropped, never an empty eleventh asked for first.
	assert max(page for _, page, _ in api.requests) <= 13  # type: ignore[attr-defined]


@pytest.mark.parametrize(
	("paginator", "params", "expected_text"),
	[
		("sl.page_number('page', page_size=0)", "None", "sl.page_number page_size must be at least 1, not 0"),
		("sl.page_number('page', page_size_param='per_page')", "None", "sl.page_number page_size_param needs page_size"),
		(
			"sl.page_number('page', page_size=20, page_size_param='per_page')",
			"{'per_page': 20}",
			"params sets per_page, which page_size sends; leave it out of params",
		),
	],
	ids=["zero", "param-without-size", "param-twice"],
)
def test_given_a_wrong_page_size_when_loading_then_the_load_fails_before_any_request_saying_what_to_fix(
	tmp_path: Path, api: ThreadingHTTPServer, paginator: str, params: str, expected_text: str
) -> None:
	project = make_project(tmp_path, api, f"api.paginate('/items/5', paginator={paginator}, params={params}, select='$.items')")
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert expected_text in done.stdout + done.stderr
	assert api.requests == []  # type: ignore[attr-defined]
