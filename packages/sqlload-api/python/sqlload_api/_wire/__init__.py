"""The wire every engine reads, pinned: one item of each kind a loader yields, and what each crosses as.

`CASES` builds each item through an `http` client its caller makes (`make_http(**options)`: SQLLoad's `sl.http(source,
...)`, or a context's `http(...)` in another tool), and `expected(name)` is what it must cross as, as committed here.
Building every case both ways and comparing with `expected` proves the two give the engine the same items; the engine's own
tests parse every expected item.

Set `SQLLOAD_UPDATE_WIRE=1` when running `sqlload_api`'s tests to rewrite the expected files from the cases.
"""

import json
from collections.abc import Callable, Mapping
from importlib import resources
from typing import Any

from sqlload_api import _data
from sqlload_api import _http as sl
from sqlload_api._data import field, key, parent

#: Builds an `http` client for the cases: `make_http(headers=...)` and `make_http()`.
MakeHttp = Callable[..., sl.Http]


def crossed(item: Any) -> Any:
	"""What `item` crosses into the engine as, parsed: a job's definition, a JSON item's text and read, a checkpoint's state."""
	if hasattr(item, "__sqlload_http_job__"):
		return {"job": json.loads(item.__sqlload_http_job__())}
	if hasattr(item, "__sqlload_json__"):
		data, read = item.__sqlload_json__()
		return {"json": {"data": bytes(data).decode(), "read": json.loads(read)}}
	if hasattr(item, "__sqlload_checkpoint__"):
		return {"checkpoint": json.loads(item.__sqlload_checkpoint__())}
	raise TypeError(f"{type(item).__name__} does not cross as a job, JSON data or a checkpoint")


def expected(name: str) -> Any:
	"""What case `name` must cross as."""
	return json.loads(resources.files(__package__).joinpath(f"{name}.json").read_text())


def _paginate_next_link(make_http: MakeHttp) -> Any:
	return make_http().paginate("/orders", paginator=sl.next_link("$.next"), select="$.items")


def _paginate_link_header(make_http: MakeHttp) -> Any:
	api = make_http(headers={"Accept": "application/json"})
	return api.paginate("/orders", paginator=sl.link_header(), select="$", on_status={404: "skip"})


def _paginate_offset(make_http: MakeHttp) -> Any:
	paginator = sl.offset("start", limit=100, limit_param="count", read_ahead=4)
	return make_http().paginate("/orders", paginator=paginator, params={"status": "open"}, select="$.data")


def _paginate_page_number_post(make_http: MakeHttp) -> Any:
	paginator = sl.page_number("$.page.number", page_size=50, page_size_param="$.page.size", location="body")
	body = {"filter": {"status": "open"}, "page": {}}
	return make_http().paginate("/search", paginator=paginator, method="POST", json_body=body, select="$.results")


def _paginate_cursor(make_http: MakeHttp) -> Any:
	return make_http().paginate("/events", paginator=sl.cursor("$.next_token", "token"), select="$.events")


def _paginate_projected(make_http: MakeHttp) -> Any:
	fields: Mapping[str, Any] = {
		"id": "$.id",
		"city": field("$.address.city", required=True),
		"updated_at": field("$.updated", parse="epoch_ms", on_error="null"),
		"page_total": parent("$.total", levels=2),
	}
	return make_http().paginate(
		"/accounts",
		paginator=sl.single(),
		select="$.data.items",
		fields=fields,
		keep_record="raw",
		select_missing="empty",
		error_path="$.errors",
		transform="account_page",
	)


def _paginate_cells(make_http: MakeHttp) -> Any:
	cells = sl.cells("$.cells", value=["$.value", "$.displayValue"], name=sl.listed("$.columns"))
	return make_http().paginate("/sheets/1", paginator=sl.single(), select="$.rows", cells=cells)


def _paginate_header_row(make_http: MakeHttp) -> Any:
	cells = sl.cells("$.values", value="$", name=sl.header_row())
	return make_http().paginate("/values/A1:Z", paginator=sl.single(), select="$.rows", cells=cells)


def _fan_out_values(make_http: MakeHttp) -> Any:
	return make_http().fan_out("/regions/{region}/stock", over=["north", "south"], concurrency=2, select="$.items")


def _fan_out_nested(make_http: MakeHttp) -> Any:
	api = make_http()
	orders = api.paginate("/orders", paginator=sl.next_link("$.next"), select="$.items[*].id", key="order_id")
	items = api.fan_out("/orders/{order_id}/items", over=orders, select="$.items[*].id", key="item_id")
	fields = {"order_id": key("order_id"), "item_id": key("item_id"), "sku": "$.sku"}
	return api.fan_out("/orders/{order_id}/items/{item_id}/parts", over=items, select="$.parts", fields=fields)


def _fan_out_batch(make_http: MakeHttp) -> Any:
	api = make_http()
	issues = api.paginate("/issues", paginator=sl.offset(limit=50), select="$.issues[*].id")
	return api.fan_out(
		"/comments",
		over=issues,
		method="POST",
		json_body={"ids": "{id}"},
		batch=100,
		concurrency=4,
		select="$.comments",
		key_path=parent("$.issueId"),
	)


def _json_document(_: MakeHttp) -> Any:
	return _data.json('{"data": {"items": [{"id": 1}]}}', path="data.items", fields={"id": "$.id"}, keep_record="raw")


def _ndjson_lines(_: MakeHttp) -> Any:
	return _data.ndjson(b'{"id": 1}\n{"id": 2}\n', fields={"id": field("$.id", required=True)})


def _checkpoint(_: MakeHttp) -> Any:
	return _data.checkpoint({"page": 3, "token": "abc"})


#: Each case's name and how it is built.
CASES: dict[str, Callable[[MakeHttp], Any]] = {
	"paginate_next_link": _paginate_next_link,
	"paginate_link_header": _paginate_link_header,
	"paginate_offset": _paginate_offset,
	"paginate_page_number_post": _paginate_page_number_post,
	"paginate_cursor": _paginate_cursor,
	"paginate_projected": _paginate_projected,
	"paginate_cells": _paginate_cells,
	"paginate_header_row": _paginate_header_row,
	"fan_out_values": _fan_out_values,
	"fan_out_nested": _fan_out_nested,
	"fan_out_batch": _fan_out_batch,
	"json_document": _json_document,
	"ndjson_lines": _ndjson_lines,
	"checkpoint": _checkpoint,
}
