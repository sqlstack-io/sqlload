"""sl.cells end to end: rows of cells from a local stub API pivot into one column per name, from a listed column, the header row or the cell itself."""

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

TOKEN = "s3cret-token"

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
#: Work orders as an EAM grid answers them: each record a list of fields, each naming itself; the third has no due date.
EAM_RECORDS = [
	[("wo_code", "1001"), ("Description", "Pump check"), ("DueDate", "2026-11-01"), ("EstHours", 2.5)],
	[("wo_code", "1002"), ("Description", "Valve swap"), ("DueDate", "2026-11-03"), ("EstHours", 4)],
	[("wo_code", "1003"), ("Description", "Filter"), ("EstHours", 1)],
	[("wo_code", "1004"), ("Description", "Belt"), ("DueDate", "2026-11-09"), ("EstHours", 0.5)],
	[("wo_code", "1005"), ("Description", "Seal"), ("DueDate", "2026-11-12"), ("EstHours", 3)],
]
#: Records an EAM page holds.
EAM_PAGE = 2
#: With `times` in the query, the due dates as the EAM writes them in Paris: ISO or one of three US formats, around the change to summer time on
#: 29 March 2026 (02:00 CET becomes 03:00 CEST), the last with its own offset; with `bad` too, the fourth is text no format reads.
EAM_TIMES = {"1001": "2026-03-29T01:30:00", "1002": "03/29/2026 03:30:00", "1004": "03/29/2026 12:00", "1005": "2026-03-29T09:00:00+02:00"}


class Api(BaseHTTPRequestHandler):
	"""A stub API of rows of cells; `self.server.log` records every request's path and query, `self.server.bodies` every POST body."""

	def log_message(self, *args: Any) -> None:
		pass

	def do_GET(self) -> None:
		url = urlparse(self.path)
		query = {key: values[0] for key, values in parse_qs(url.query).items()}
		self.server.log.append((url.path, query))  # type: ignore[attr-defined]
		if self.headers.get("Authorization") != f"Bearer {TOKEN}":
			return self.reply(401, {"message": "bad token"})
		route = {
			"/v1/sheet": self.sheet,
			"/v1/grid": lambda query: self.reply(200, [GRID, GRID_BLANK_PAGE, GRID_END][int(query["page"]) - 1]),
			"/v1/positional": lambda query: self.reply(200, {"columns": ["Name", "Qty"], "rows": [{"id": 1, "cells": [{"value": "Bolt"}, {"value": 3}]}, {"id": 2, "cells": [{"value": "Nut"}]}]}),
		}.get(url.path)
		if route is not None:
			return route(query)
		return self.reply(404, {"message": "no route"})

	def do_POST(self) -> None:
		"""The EAM grid: `$.CURSORPOSITION` in the body is where the page starts; `dup`, `nameless` and `clash` in the query break page 2."""
		url = urlparse(self.path)
		query = {key: values[0] for key, values in parse_qs(url.query).items()}
		body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
		self.server.log.append((url.path, query))  # type: ignore[attr-defined]
		self.server.bodies.append(body)  # type: ignore[attr-defined]
		start = int(body.get("CURSORPOSITION") or 0)
		records: list[dict[str, Any]] = [
			{"DATAFIELD": [{"FIELDNAME": name, "FIELDVALUE": value, "DATATYPE": "VARCHAR"} for name, value in fields]} for fields in EAM_RECORDS[start : start + EAM_PAGE]
		]
		if start == EAM_PAGE and query.get("dup"):
			records[0]["DATAFIELD"].append({"FIELDNAME": "wo_code", "FIELDVALUE": "9999"})
		if start == EAM_PAGE and query.get("nameless"):
			records[1]["DATAFIELD"].insert(1, {"FIELDVALUE": "orphan"})
		if start == EAM_PAGE and query.get("clash"):
			records[0]["Description"] = "the record's own"
		if query.get("times"):
			for record in records:
				fields = record["DATAFIELD"]
				code = fields[0]["FIELDVALUE"]
				for cell in fields:
					if cell["FIELDNAME"] == "DueDate":
						cell["FIELDVALUE"] = "soon" if query.get("bad") and code == "1004" else EAM_TIMES[code]
		more = start + EAM_PAGE < len(EAM_RECORDS)
		self.reply(200, {"Result": {"ResultData": {"NEXTCURSORPOSITION": start + EAM_PAGE if more else None, "DATARECORD": records}}})

	def reply(self, status: int, body: Any) -> None:
		data = json.dumps(body).encode()
		self.send_response(status)
		self.send_header("Content-Type", "application/json")
		self.send_header("Content-Length", str(len(data)))
		self.end_headers()
		self.wfile.write(data)

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


@pytest.fixture
def api() -> Iterator[ThreadingHTTPServer]:
	server = ThreadingHTTPServer(("127.0.0.1", 0), Api)
	server.log = []  # type: ignore[attr-defined]
	server.bodies = []  # type: ignore[attr-defined]
	server.sheet_title = "Qty"  # type: ignore[attr-defined]
	threading.Thread(target=server.serve_forever, daemon=True).start()
	yield server
	server.shutdown()


def table_project(tmp_path: Path, server: ThreadingHTTPServer, job: str, settings: str = "") -> Path:
	host, port = server.server_address[:2]
	(tmp_path / "sources").mkdir(exist_ok=True)
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
			retries = 0

			[targets.dev]
			connection = "wh"
			loader_schema = "RAW"
		""")
	)
	module = (
		"import sqlload as sl\n\nshop = sl.source('shop', write_strategy='replace', connection='shop')\napi = sl.http(shop)\n\n"
		f"@shop.table(description='Rows.'{settings})\ndef rows(ctx):\n    return {job}\n"
	)
	(tmp_path / "sources" / "shop.py").write_text(module)
	return tmp_path


def sqlload(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
	home = project / "home"
	home.mkdir(exist_ok=True)
	env = {**os.environ, "HOME": str(home), "XDG_CONFIG_HOME": str(home / ".config"), "SHOP_TOKEN": TOKEN}
	return subprocess.run([sys.executable, "-c", "from sqlload._cli import main; main()", *args], cwd=project, env=env, capture_output=True, text=True, timeout=120)


def query(project: Path, sql: str) -> list[tuple[Any, ...]]:
	with duckdb.connect(str(project / "wh.duckdb"), read_only=True) as connection:
		return connection.execute(sql).fetchall()


SHEET = "api.paginate('/sheet', paginator=sl.page_number('page'), select='$.rows', cells=sl.cells(value=['$.value', '$.displayValue'], name=sl.listed('$.columns')){extra})"
GRID_JOB = (
	"api.paginate('/grid', paginator=sl.page_number('page'), select='$.sheets[0].data[0].rowData', select_missing='empty', cells=sl.cells('$.values', "
	"value=['$.effectiveValue.stringValue', '$.effectiveValue.numberValue', '$.effectiveValue.boolValue'], name=sl.header_row()))"
)
EAM = (
	"api.paginate('/eam{flags}', method='POST', json_body={{'GRID': {{'NAME': 'WSJOBS'}}}}, paginator=sl.cursor('$.Result.ResultData.NEXTCURSORPOSITION', '$.CURSORPOSITION', location='body'), "
	"select='$.Result.ResultData.DATARECORD', cells=sl.cells('$.DATAFIELD', value='$.FIELDVALUE', name='$.FIELDNAME'))"
)
SHEET_COLUMNS = [("id", "BIGINT"), ("row_number", "BIGINT"), ("name", "VARCHAR"), ("qty", "BIGINT"), ("price", "DOUBLE"), ("done", "BOOLEAN")]


@pytest.mark.parametrize(
	("job", "expected_columns", "expected_rows"),
	[
		(
			SHEET.format(extra=""),
			SHEET_COLUMNS,
			[(1, 1, "Bolt", 3, 0.25, True), (2, 2, "Nut", None, 1.5, None), (3, 3, "Washer", 12, None, False), (4, 4, None, None, None, None), (5, 5, "Gear", 4, 9.0, None)],
		),
		(
			SHEET.format(extra="").replace("['$.value', '$.displayValue']", "'$.value'"),
			SHEET_COLUMNS,
			[(1, 1, "Bolt", 3, 0.25, True), (2, 2, "Nut", None, 1.5, None), (3, 3, "Washer", 12, None, False), (4, 4, None, None, None, None), (5, 5, "Gear", None, 9.0, None)],
		),
		(
			"api.paginate('/positional', paginator=sl.single(), select='$.rows', cells=sl.cells(name=sl.listed('$.columns', name_key=None, by_position=True)))",
			[("id", "BIGINT"), ("name", "VARCHAR"), ("qty", "BIGINT")],
			[(1, "Bolt", 3), (2, "Nut", None)],
		),
		(
			GRID_JOB,
			[("name", "VARCHAR"), ("qty", "BIGINT"), ("done", "BOOLEAN")],
			[("Bolt", 3, True), ("Nut", None, False), ("Washer", None, None), (None, None, None)],
		),
		(
			EAM.format(flags=""),
			[("wo_code", "VARCHAR"), ("description", "VARCHAR"), ("due_date", "VARCHAR"), ("est_hours", "DOUBLE")],
			[
				("1001", "Pump check", "2026-11-01", 2.5),
				("1002", "Valve swap", "2026-11-03", 4.0),
				("1003", "Filter", None, 1.0),
				("1004", "Belt", "2026-11-09", 0.5),
				("1005", "Seal", "2026-11-12", 3.0),
			],
		),
	],
	ids=["listed-by-id-value-then-display-value", "listed-by-id-value-only", "listed-by-position", "header-row", "cells-name-themselves-eam"],
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


def test_given_cells_that_name_themselves_paged_by_a_body_cursor_when_loading_then_a_null_cursor_ends_paging(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = table_project(tmp_path, api, EAM.format(flags=""))
	assert sqlload(project, "load").returncode == 0
	assert api.bodies == [  # type: ignore[attr-defined]
		{"GRID": {"NAME": "WSJOBS"}},
		{"GRID": {"NAME": "WSJOBS"}, "CURSORPOSITION": 2},
		{"GRID": {"NAME": "WSJOBS"}, "CURSORPOSITION": 4},
	]


def eam_parse(extra: str = "") -> str:
	"""The table's `parse` of the EAM due dates: ISO or the three US formats, in Paris, and `extra` keys."""
	return ", parse={'due_date': {'timestamp': ['iso', '%m/%d/%Y %H:%M:%S', '%m/%d/%Y %H:%M', '%m/%d/%Y'], 'tz': 'Europe/Paris'" + extra + "}}"


def test_given_cells_with_iso_and_us_times_in_paris_when_loading_with_parse_then_each_loads_as_its_utc_instant_across_the_dst_change(
	tmp_path: Path, api: ThreadingHTTPServer
) -> None:
	project = table_project(tmp_path, api, EAM.format(flags="?times=1"), settings=eam_parse())
	loaded = sqlload(project, "load")
	assert loaded.returncode == 0, loaded.stdout + loaded.stderr
	assert query(project, """SELECT wo_code, CAST(timezone('UTC', due_date) AS VARCHAR), typeof(due_date) FROM "RAW".shop__rows ORDER BY wo_code""") == [
		("1001", "2026-03-29 00:30:00", "TIMESTAMP WITH TIME ZONE"),
		("1002", "2026-03-29 01:30:00", "TIMESTAMP WITH TIME ZONE"),
		("1003", None, "TIMESTAMP WITH TIME ZONE"),
		("1004", "2026-03-29 10:00:00", "TIMESTAMP WITH TIME ZONE"),
		("1005", "2026-03-29 07:00:00", "TIMESTAMP WITH TIME ZONE"),
	]


def test_given_a_cell_no_format_reads_when_loading_with_parse_then_the_load_fails_naming_the_row_column_and_value(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = table_project(tmp_path, api, EAM.format(flags="?times=1&bad=1"), settings=eam_parse())
	loaded = sqlload(project, "load")
	assert loaded.returncode == 1
	assert (
		'row 4: column due_date holds "soon", which is not a timestamp in any of the formats iso, "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y"; '
		'fix the value, or set on_error = "null" in its parse to load NULL'
	) in loaded.stdout + loaded.stderr


def test_given_a_cell_no_format_reads_when_loading_with_on_error_null_then_it_loads_null(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = table_project(tmp_path, api, EAM.format(flags="?times=1&bad=1"), settings=eam_parse(", 'on_error': 'null'"))
	loaded = sqlload(project, "load")
	assert loaded.returncode == 0, loaded.stdout + loaded.stderr
	assert query(project, 'SELECT wo_code, due_date IS NOT NULL FROM "RAW".shop__rows ORDER BY wo_code') == [
		("1001", True),
		("1002", True),
		("1003", False),
		("1004", False),
		("1005", True),
	]


def test_given_a_sheet_listing_its_columns_on_its_first_page_when_paging_then_later_pages_keep_the_names(tmp_path: Path, api: ThreadingHTTPServer) -> None:
	project = table_project(tmp_path, api, SHEET.format(extra=""))
	assert sqlload(project, "load").returncode == 0
	assert [query["page"] for path, query in api.log if path == "/v1/sheet"] == ["1", "2", "3", "4"]  # type: ignore[attr-defined]
	assert query(project, 'SELECT "name" FROM "RAW".shop__rows WHERE id = 3') == [("Washer",)]


@pytest.mark.parametrize(
	("settings", "expected_code", "expected_text", "expected_columns"),
	[
		("", 0, "", ["id", "row_number", "name", "qty", "price", "done", "quantity"]),
		(", on_schema_change='fail'", 1, "new field quantity is not in the table", ["id", "row_number", "name", "qty", "price", "done"]),
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
	("job", "expected_text", "expected_requests"),
	[
		(
			SHEET.format(extra="").replace("paginator=sl.page_number('page')", "paginator=sl.page_number('page'), params={'unknown': 1}"),
			"page=2: cells: row 1: a cell's columnId 7999 is not the id of a column $.columns names",
			2,
		),
		(
			"api.paginate('/sheet', paginator=sl.single(), select='$.rows', cells=sl.cells('$.rows[*].cells', name=sl.listed('$.columns')))",
			"cells path \"$.rows[*].cells\" cannot pick from a list with [*]",
			0,
		),
		(
			"api.paginate('/sheet', paginator=sl.single(), select='$.rows', cells=sl.cells(name='$.names[*].text'))",
			"cells path \"$.names[*].text\" cannot pick from a list with [*]",
			0,
		),
		(
			"api.fan_out('/orders/{order_id}/items', over=api.paginate('/sheet', paginator=sl.single(), select='$.rows[*].id', cells=sl.cells(name=sl.listed('$.columns'))), select='$.items')",
			"fan_out's over picks keys with its select; cells are for the fan-out's own rows",
			0,
		),
		(
			SHEET.format(extra="").replace("paginator=sl.page_number('page')", "paginator=sl.page_number('page'), params={'twice': 1}"),
			"page=1: cells: row 1: it has two cells for the column Qty",
			1,
		),
		(
			"api.paginate('/sheet', paginator=sl.page_number('page'), select='$.rows', cells=sl.cells('$.cell', name=sl.listed('$.columns')))",
			'cells: no row has a cell field, so cells pointer "$.cell" finds no cells; check it',
			1,
		),
		(EAM.format(flags="?dup=1"), "/v1/eam?dup=1: cells: row 1: it has two cells for the column wo_code", 2),
		(EAM.format(flags="?nameless=1"), "/v1/eam?nameless=1: cells: row 2: its cell 2 has no name at $.FIELDNAME", 2),
		(
			EAM.format(flags="?clash=1"),
			"/v1/eam?clash=1: cells: row 1: its own field Description has the name of the cells' column Description, so the two would load as one column",
			2,
		),
	],
	ids=[
		"unknown-column-id",
		"pointer-into-a-list",
		"self-name-through-a-list",
		"cells-on-an-over",
		"two-cells-for-one-listed-column",
		"pointer-naming-no-field",
		"two-cells-naming-one-column",
		"cell-without-a-name",
		"cell-named-like-the-records-own-field",
	],
)
def test_given_cells_that_cannot_pivot_when_loading_then_it_fails_naming_why(tmp_path: Path, api: ThreadingHTTPServer, job: str, expected_text: str, expected_requests: int) -> None:
	project = table_project(tmp_path, api, job)
	done = sqlload(project, "load")
	assert done.returncode == 1, done.stdout + done.stderr
	assert expected_text in done.stdout + done.stderr, done.stdout + done.stderr
	assert len(api.log) == expected_requests  # type: ignore[attr-defined]


@pytest.mark.parametrize("arguments", [{"key": "columnId"}, {"names_from": "$.columns"}, {"id_key": "id"}, {"name_key": "title"}, {"header_row": True}, {}])
def test_given_a_parameter_sl_cells_does_not_take_when_building_cells_then_it_fails_as_a_type_error(arguments: dict[str, Any]) -> None:
	import sqlload as sl

	unexpected = next(iter(arguments), None)
	with pytest.raises(TypeError, match=f"unexpected keyword argument '{unexpected}'" if unexpected else "missing 1 required keyword-only argument: 'name'"):
		sl.cells(**arguments)


def test_given_each_name_source_when_building_cells_then_the_job_holds_it_and_a_job_without_cells_has_no_cells_field() -> None:
	import sqlload as sl

	api = sl.http(sl.source("shop", write_strategy="replace", connection="shop"))
	plain = json.loads(api.paginate("/orders", paginator=sl.single(), select="$.items").__sqlload_http_job__())
	pivoted = json.loads(api.paginate("/orders", paginator=sl.single(), select="$.items", cells=sl.cells(name=sl.listed("$.columns"))).__sqlload_http_job__())
	assert "cells" not in plain
	assert pivoted["cells"] == {"pointer": "$.cells", "value": ["$.value"], "name": {"kind": "listed", "from": "$.columns", "key": "columnId", "id_key": "id", "name_key": "title"}}
	assert {key: value for key, value in pivoted.items() if key != "cells"} == plain
	assert sl.cells(name=sl.listed("$.cols", by_position=True)).spec["name"]["key"] is None
	assert sl.cells(name=sl.header_row()).spec["name"] == {"kind": "header_row"}
	assert sl.cells("$.DATAFIELD", value="$.FIELDVALUE", name="$.FIELDNAME").spec == {"pointer": "$.DATAFIELD", "value": ["$.FIELDVALUE"], "name": {"kind": "field", "path": "$.FIELDNAME"}}
	with pytest.raises(TypeError, match=r'sl.cells name= takes a path in the cell such as "\$.name", sl.listed\(...\) or sl.header_row\(\), not 3'):
		sl.cells(name=3)  # type: ignore[arg-type]
