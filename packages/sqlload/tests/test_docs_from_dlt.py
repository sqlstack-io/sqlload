"""The examples of the "Coming from dlt" guide, each a project under docs_examples/from_dlt that the page shows as it is.

Each test copies one example and loads it twice. The filesystem example's tables and row ids are checked against values
recorded from dlt 1.32.0 (an upsert merge on OrderID of the same two files); the dlt side isn't rerun here.
"""

import json
import os
import shutil
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import duckdb
import pytest

EXAMPLES = Path(__file__).parent / "docs_examples" / "from_dlt"
TOKEN = "example-token"
PAGE_SIZE = 2

#: The columns SQLLoad adds that dlt has no counterpart for: when a row loaded, and a files source's file, version and line.
SQLLOAD_ONLY = {"_sqlload_loaded_at", "_sqlload_source_file", "_sqlload_source_version", "_sqlload_source_row"}
#: dlt writes the load id on root rows only; SQLLoad writes it on child rows too.
CHILD_LOAD_ID = "_dlt_load_id"

#: Row ids dlt 1.32.0 gives the filesystem example's rows under its upsert merge on OrderID, after both files.
DLT_ORDER_IDS = [(1, "shipped", "E7zK8EiO51eQ2A"), (2, "open", "+AfrtWtvbsW93w"), (3, "open", "uh9DeJo8VgLavQ")]
DLT_LINES = [
	("gear", 1, "+AfrtWtvbsW93w", "+AfrtWtvbsW93w", 0, "DuJY8AIkJ6Zhig"),
	("bolt", 3, "E7zK8EiO51eQ2A", "E7zK8EiO51eQ2A", 0, "2XJ+MfjrBXUANw"),
	("washer", 12, "uh9DeJo8VgLavQ", "uh9DeJo8VgLavQ", 0, "PGoWoo56ddUyCQ"),
]
DLT_TAGS = [
	("web", "E7zK8EiO51eQ2A", "E7zK8EiO51eQ2A", 0, "YcUJKjjxfXopOg"),
	("gift", "E7zK8EiO51eQ2A", "E7zK8EiO51eQ2A", 1, "l0M7l3gszuWIsA"),
	("phone", "uh9DeJo8VgLavQ", "uh9DeJo8VgLavQ", 0, "IDeWc4RGahzgvw"),
]

FIRST_ORDERS = [
	{"id": 1, "status": "open", "updatedAt": "2026-11-01T09:00:00Z", "lines": [{"sku": "bolt", "qty": 3}, {"sku": "nut", "qty": 6}]},
	{"id": 2, "status": "open", "updatedAt": "2026-11-01T10:00:00Z", "lines": [{"sku": "gear", "qty": 1}]},
	{"id": 3, "status": "open", "updatedAt": "2026-11-01T11:00:00Z", "lines": []},
]
#: Order 2 after it shipped: a later updatedAt and one line fewer.
SHIPPED = {"id": 2, "status": "shipped", "updatedAt": "2026-11-02T08:00:00Z", "lines": []}
PAYMENTS = {1: [{"id": 501, "amount": 12.5}], 2: [{"id": 502, "amount": 4.0}, {"id": 503, "amount": 1.0}]}


class Shop(BaseHTTPRequestHandler):
	"""A stub shop API: orders paged by a next link and filtered by updated_after, and each order's payments (404 for none)."""

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		url = urlparse(self.path)
		query = {key: values[0] for key, values in parse_qs(url.query).items()}
		self.server.requests.append((url.path, query))  # type: ignore[attr-defined]
		if self.headers.get("Authorization") != f"Bearer {TOKEN}":
			return self.reply(401, {"message": "bad token"})
		parts = url.path.strip("/").split("/")
		if parts == ["v1", "orders"]:
			return self.orders(query)
		if len(parts) == 4 and parts[3] == "payments" and int(parts[2]) in PAYMENTS:
			return self.reply(200, {"items": PAYMENTS[int(parts[2])]})
		return self.reply(404, {"message": "not found"})

	def orders(self, query: dict[str, str]) -> None:
		after = query.get("updated_after", "").replace("+00:00", "Z")
		rows = [row for row in self.server.orders if row["updatedAt"] > after]  # type: ignore[attr-defined]
		start = int(query.get("start", 0))
		page = rows[start : start + PAGE_SIZE]
		host, port = self.server.server_address[:2]
		more = start + PAGE_SIZE < len(rows)
		params = urlencode({**query, "start": start + PAGE_SIZE})
		self.reply(200, {"items": page, "next": f"http://{host}:{port}/v1/orders?{params}" if more else None})

	def reply(self, status: int, body: object) -> None:
		data = json.dumps(body).encode()
		self.send_response(status)
		self.send_header("Content-Type", "application/json")
		self.send_header("Content-Length", str(len(data)))
		self.end_headers()
		self.wfile.write(data)


@pytest.fixture
def shop() -> Iterator[ThreadingHTTPServer]:
	server = ThreadingHTTPServer(("127.0.0.1", 0), Shop)
	server.orders = list(FIRST_ORDERS)  # type: ignore[attr-defined]
	server.requests = []  # type: ignore[attr-defined]
	threading.Thread(target=server.serve_forever, daemon=True).start()
	yield server
	server.shutdown()
	server.server_close()


def example(tmp_path: Path, name: str) -> Path:
	project = tmp_path / name
	shutil.copytree(EXAMPLES / name, project)
	return project


def load(project: Path, url: str = "") -> dict[str, Any]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "SHOP_API_URL": url, "SHOP_TOKEN": TOKEN}
	done = subprocess.run(
		[sys.executable, "-c", "from sqlload._cli import main; main()", "load", "--json"], cwd=project, env=env, capture_output=True, text=True, timeout=120
	)
	assert done.returncode == 0, done.stderr
	return json.loads(done.stdout)


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "warehouse.duckdb")) as connection:
		return connection.execute(sql).fetchall()


def listing(project: Path) -> str:
	"""Each table of schema shop, as `table: column, column, ...` in column order."""
	rows = query(
		project,
		"SELECT table_name, string_agg(column_name, ', ' ORDER BY ordinal_position) FROM information_schema.columns "
		"WHERE table_schema = 'shop' AND table_name NOT LIKE '\\_sqlload%' ESCAPE '\\' GROUP BY table_name ORDER BY table_name",
	)
	return "".join(f"{table}: {columns}\n" for table, columns in rows)


def as_dlt_lists_them(text: str) -> str:
	"""A listing without the columns dlt doesn't write: SQLLOAD_ONLY, and the load id of child tables."""
	lines = (line.split(": ") for line in text.splitlines())
	return "".join(f"{table}: {', '.join(c for c in columns.split(', ') if c not in SQLLOAD_ONLY | ({CHILD_LOAD_ID} if '__' in table else set()))}\n" for table, columns in lines)


def test_given_the_filesystem_example_when_a_second_file_merges_then_its_tables_and_row_ids_are_dlts(tmp_path: Path) -> None:
	project = example(tmp_path, "filesystem")
	load(project)
	shutil.move(project / "later" / "2026-11-02.ndjson", project / "landing" / "orders")
	load(project)

	assert listing(project) == (project / "tables.txt").read_text()
	assert as_dlt_lists_them(listing(project)) == (project / "tables-dlt.txt").read_text()
	assert query(project, "SELECT order_id, status, _dlt_id FROM shop.orders ORDER BY order_id") == DLT_ORDER_IDS
	assert query(project, "SELECT sku, qty, _dlt_root_id, _dlt_parent_id, _dlt_list_idx, _dlt_id FROM shop.orders__lines ORDER BY _dlt_root_id, _dlt_list_idx") == DLT_LINES
	assert query(project, "SELECT value, _dlt_root_id, _dlt_parent_id, _dlt_list_idx, _dlt_id FROM shop.orders__tags ORDER BY _dlt_root_id, _dlt_list_idx") == DLT_TAGS
	assert sorted(path.name for path in (project / "landing" / "loaded").iterdir()) == ["2026-11-01.ndjson", "2026-11-02.ndjson"]


def test_given_the_rest_api_example_when_an_order_changes_then_only_it_is_requested_again_and_merged(tmp_path: Path, shop: ThreadingHTTPServer) -> None:
	project = example(tmp_path, "rest_api")
	host, port = shop.server_address[:2]
	url = f"http://{host}:{port}/v1"
	first = load(project, url)
	ids = dict(query(project, "SELECT id, _dlt_id FROM shop.orders"))
	first_requests = list(shop.requests)  # type: ignore[attr-defined]
	shop.orders[1] = SHIPPED  # type: ignore[attr-defined]
	shop.requests.clear()  # type: ignore[attr-defined]
	second = load(project, url)
	listed = listing(project)
	query(project, (project / "dlt_loads.sql").read_text())

	assert listed == (project / "tables.txt").read_text()
	assert [(table["table"], table["rows"]) for table in first["tables"]] == [("shop.orders", 3), ("shop.payments", 3)]
	assert [(table["table"], table["rows"]) for table in second["tables"]] == [("shop.orders", 1), ("shop.payments", 3)]
	assert ("/v1/orders", {"updated_after": "2026-01-01T00:00:00+00:00", "start": "2"}) in first_requests
	assert ("/v1/orders", {"updated_after": "2026-11-01T11:00:00+00:00"}) in shop.requests  # type: ignore[attr-defined]
	assert query(project, "SELECT id, status, CAST(updated_at AS VARCHAR) FROM shop.orders ORDER BY id") == [
		(1, "open", "2026-11-01 09:00:00+00"),
		(2, "shipped", "2026-11-02 08:00:00+00"),
		(3, "open", "2026-11-01 11:00:00+00"),
	]
	assert dict(query(project, "SELECT id, _dlt_id FROM shop.orders")) == ids
	assert query(project, "SELECT o.id, l.sku, l.qty FROM shop.orders__lines l JOIN shop.orders o ON l._dlt_root_id = o._dlt_id ORDER BY 1, l._dlt_list_idx") == [
		(1, "bolt", 3),
		(1, "nut", 6),
	]
	assert query(project, "SELECT id, amount FROM shop.payments ORDER BY id") == [(501, 12.5), (502, 4.0), (503, 1.0)]
	# Run 1's orders 1 and 3, run 2's order 2, and run 2's payments, which replaced run 1's: one load per table and run.
	assert query(project, "SELECT count(*), min(status), max(schema_name) FROM shop._dlt_loads") == [(3, 0, "shop")]
	assert query(project, "SELECT load_id FROM shop._dlt_loads ORDER BY 1") == query(
		project, "SELECT _dlt_load_id FROM shop.orders UNION SELECT _dlt_load_id FROM shop.payments ORDER BY 1"
	)
