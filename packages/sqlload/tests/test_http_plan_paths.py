"""`plan` checks the sl.http field paths a table function writes as literals, read from its source without calling it."""

import os
import subprocess
import sys
import textwrap
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

WILDCARD = 'fields["sku"]: field path "$.lines[*].sku" uses the wildcard [*], which one column can\'t hold; take one item with [n] or [-1], or select the list\'s items as rows'


class Recorder(BaseHTTPRequestHandler):
	"""An API that records every request it gets and answers none usefully."""

	def log_message(self, *args: object) -> None:
		pass

	def do_GET(self) -> None:
		self.server.requests.append(self.path)  # type: ignore[attr-defined]
		self.send_response(500)
		self.end_headers()


@pytest.fixture
def api() -> Iterator[ThreadingHTTPServer]:
	server = ThreadingHTTPServer(("127.0.0.1", 0), Recorder)
	server.requests = []  # type: ignore[attr-defined]
	thread = threading.Thread(target=server.serve_forever, daemon=True)
	thread.start()
	yield server
	server.shutdown()


def make_project(tmp_path: Path, server: ThreadingHTTPServer, module: str) -> Path:
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
	(tmp_path / "sources" / "shop.py").write_text(textwrap.dedent(module))
	return tmp_path


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config")}
	return subprocess.run([sys.executable, "-c", "from sqlload._cli import main; main()", *args], cwd=project, env=env, capture_output=True, text=True, timeout=120)


def words(text: str) -> str:
	"""`text` without the plan's tree lines, one space between words."""
	return " ".join(word for word in text.split() if word not in ("│", "├──", "└──"))


#: A paginate whose `fields=` holds `{sku}`, its other paths fine; the function marks a file first, so a call shows.
ORDERS = """
@shop.table(description="Orders, one row per order.")
def orders(ctx):
    open("called", "w").close()
    return api.paginate("/orders", paginator=sl.offset(limit=50), select="$.orders[*]", fields={{"id": "$.id", "sku": {sku}}})
"""

#: The module header: sqlload imported as `sl`, a source and its client.
HEADER = """
import sqlload as sl

shop = sl.source("shop", connection="shop", write_strategy="replace")
api = sl.http(shop)
"""


@pytest.mark.parametrize(
	"sku",
	['"$.lines[*].sku"', 'sl.field("$.lines[*].sku", required=True)', 'sl.parent("$.lines[*].sku")'],
	ids=["string", "sl.field", "sl.parent"],
)
def test_given_a_bad_literal_path_when_planning_then_plan_fails_naming_the_table_field_and_path_without_calling_the_function(
	tmp_path: Path, api: ThreadingHTTPServer, sku: str
) -> None:
	project = make_project(tmp_path, api, HEADER + ORDERS.format(sku=sku))
	for command in ("plan", "load"):
		done = sqlload(project, command)
		assert done.returncode == 1, done.stdout + done.stderr
		shown = words(done.stdout + done.stderr)
		assert f"RAW.shop__orders reason {WILDCARD}" in shown, shown
	assert not (project / "called").exists()
	assert api.requests == []  # type: ignore[attr-defined]


def test_given_aliased_sqlload_imports_when_planning_then_their_calls_are_checked_and_other_libraries_paginate_is_not(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	module = """
	import sqlload as loader
	from sqlload import http as client_for, parent as around

	shop = loader.source("shop", connection="shop", write_strategy="replace")


	class Pages:
	    def paginate(self, path, **settings):
	        return []


	@shop.table(description="Order lines, one row per line.")
	def lines(ctx):
	    lines_api = client_for(shop)
	    return lines_api.fan_out("/orders/{id}", over=[1, 2], select="$.lines", key_path=around("$.ids[*]"), fields={"id": "$.id"})


	@shop.table(description="Refunds, one row per refund.")
	def refunds(ctx):
	    yield from Pages().paginate("/refunds", fields={"sku": "$.lines[*].sku"})
	"""
	done = sqlload(make_project(tmp_path, api, module), "plan")
	shown = words(done.stdout + done.stderr)
	assert done.returncode == 1, shown
	assert 'RAW.shop__lines reason key_path "$.ids[*]" uses the wildcard [*]' in shown, shown
	assert "shop__refunds" in shown and "refunds reason" not in shown, shown
	assert api.requests == []  # type: ignore[attr-defined]


def test_given_a_computed_path_when_planning_then_plan_passes_and_only_that_table_says_load_checks_it(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	module = (
		HEADER
		+ """
SKU = "$.lines[0].sku"


@shop.table(description="Orders, one row per order.")
def orders(ctx):
    return api.paginate("/orders", paginator=sl.single(), select="$.orders[*]", fields={"id": "$.id", "sku": SKU, "at": sl.field(f"$.{ctx.cursor_column}")})


@shop.table(description="Customers, one row per customer.")
def customers(ctx):
    return api.paginate("/customers", paginator=sl.single(), select="$.customers[*]", fields={"id": "$.id"})
"""
	)
	done = sqlload(make_project(tmp_path, api, module), "plan")
	shown = words(done.stdout + done.stderr)
	assert done.returncode == 0, shown
	assert "RAW.shop__orders replace" in shown and 'paths checked at load, as they\'re computed: fields["sku"], fields["at"]' in shown, shown
	assert shown.count("paths checked at load") == 1, shown
	assert api.requests == []  # type: ignore[attr-defined]


def test_given_a_table_function_without_source_when_planning_then_plan_passes_and_says_load_checks_its_paths(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	module = (
		HEADER
		+ """
built = {}
exec('def orders(ctx):\\n    return api.paginate("/orders", paginator=sl.single(), fields={"sku": "$.lines[*].sku"})\\n', globals(), built)
orders = shop.table(description="Orders, one row per order.")(built["orders"])
"""
	)
	done = sqlload(make_project(tmp_path, api, module), "plan")
	shown = words(done.stdout + done.stderr)
	assert done.returncode == 0, shown
	assert "paths checked at load, as the table function's source can't be read" in shown, shown
	assert api.requests == []  # type: ignore[attr-defined]


def test_given_a_page_transform_named_by_a_name_nothing_registered_when_planning_then_plan_fails_listing_the_registered_ones(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	module = (
		HEADER
		+ """
@sl.transform
def tidy_page(rows, ctx):
    return rows


@shop.table(description="Orders, one row per order.")
def orders(ctx):
    return api.paginate("/orders", paginator=sl.single(), select="$.orders[*]", transform="tidy_pages")
"""
	)
	done = sqlload(make_project(tmp_path, api, module), "plan")
	shown = words(done.stdout + done.stderr)
	assert done.returncode == 1, shown
	assert "RAW.shop__orders reason transform tidy_pages: no function is registered as tidy_pages with @sl.transform; the registered ones are tidy_page" in shown, shown
	assert api.requests == []  # type: ignore[attr-defined]
