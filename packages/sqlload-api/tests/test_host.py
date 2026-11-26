"""What every host shares: `Context.from_read`, the state bookkeeping, and the transform callbacks of one registry."""

import gc
import json
import weakref
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import pytest

import sqlload_api as sl
from sqlload_api import _host

#: A read as the engine sends it (`TableRead` as JSON, parsed): one connection withheld, a timestamp cursor in a zone.
READ: dict[str, Any] = {
	"source": "shop",
	"table": "orders",
	"connection": "shop_api",
	"connections": {"shop_api": {"base_url": "https://api.example.com"}, "other": {"token": "t"}},
	"withheld": ["warehouse"],
	"cursor": {
		"column": "updated_at",
		"column_type": "timestamp_tz",
		"start": "2026-11-01T10:00:00Z",
		"end": None,
		"inclusive": True,
		"timezone": "Europe/Paris",
	},
	"logged": True,
	"state": '{"page": 2}',
	"is_reload": False,
	"masked": [],
	"arrays": [],
	"host": {"model": "orders_model"},
}


class LoaderContext(sl.Context):
	"""A host's context: SQLLoad's, with a field of its own."""

	def __init__(self, *, model: str, **fields: Any) -> None:
		super().__init__(**fields)
		self.model = model


def test_given_a_read_when_building_its_context_then_it_is_filled_as_sqlload_fills_it() -> None:
	ctx = sl.Context.from_read(READ)
	assert (ctx.table, ctx.source, ctx.cursor_column, ctx.is_reload, ctx.state) == ("orders", "shop", "updated_at", False, {"page": 2})
	assert ctx.cursor_value == datetime(2026, 11, 1, 11, 0, tzinfo=ZoneInfo("Europe/Paris"))
	assert ctx.cursor_end is None
	assert dict(ctx.connection) == {"base_url": "https://api.example.com"}
	assert dict(ctx.connection("other")) == {"token": "t"}
	with pytest.raises(PermissionError, match="connection warehouse is used by a \\[targets\\] entry"):
		ctx.connection("warehouse")


def test_given_a_subclass_with_a_field_of_its_own_when_building_from_a_read_then_both_are_set() -> None:
	ctx = LoaderContext.from_read(READ, model=READ["host"]["model"])
	assert isinstance(ctx, LoaderContext)
	assert (ctx.model, ctx.table, ctx.state) == ("orders_model", "orders", {"page": 2})


def test_given_a_logged_context_when_its_state_moves_then_the_crossing_and_final_states_follow() -> None:
	ctx = sl.Context.from_read(READ)
	assert (_host.current_state(ctx), _host.final_state(ctx)) == ('{"page": 2}', None)
	ctx.state = {"page": 3}
	ctx.state = {"page": 4}
	assert (_host.current_state(ctx), _host.final_state(ctx)) == ('{"page": 3}', '{"page": 4}')
	assert (_host.current_state(None), _host.final_state(None)) == (None, None)


@pytest.mark.parametrize(
	("value", "expected"),
	[(time(9, 30), "09:30:00"), (Decimal("1.5"), TypeError), (date(2026, 1, 1), TypeError), (b"x", TypeError), (object, "<class 'object'>")],
	ids=["time", "decimal", "date", "bytes", "other"],
)
def test_given_a_value_orjson_cannot_write_when_crossing_dicts_then_the_default_writes_it_or_refuses(value: Any, expected: Any) -> None:
	if expected is TypeError:
		with pytest.raises(TypeError):
			_host.crossing_default(value)
	else:
		assert _host.crossing_default(value) == expected


def test_given_pages_a_transform_returned_when_listing_items_then_each_page_ends_with_a_marker() -> None:
	pages = iter([[{"a": 1}, {"a": 2}], [{"a": 3}], None])
	items = list(_host.page_items(lambda: next(pages)))
	markers = [item for item in items if hasattr(item, "__sqlload_checkpoint__")]
	assert [item for item in items if isinstance(item, dict)] == [{"a": 1}, {"a": 2}, {"a": 3}]
	assert [json.loads(marker.__sqlload_checkpoint__()) for marker in markers] == [None, None]


def test_given_a_registered_transform_when_called_through_the_host_then_it_gets_its_context() -> None:
	pa = pytest.importorskip("pyarrow")

	@sl.transform("host_test_page")
	def page(rows: Any, ctx: sl.TransformContext) -> Any:
		return [{"n": rows.num_rows, "unit": ctx.unit, "page": ctx.page, "at": ctx.modified_at}]

	returned, problem = _host.call_transform(
		"host_test_page", pa.table({"x": [1, 2]}), json.dumps({"unit": "page", "table": "t", "page": 3, "modified_at": "2026-11-01T00:00:00Z"})
	)
	assert (problem, list(returned)) == (None, [[{"n": 2, "unit": "page", "page": 3, "at": datetime(2026, 11, 1, tzinfo=timezone.utc)}]])


def test_a_function_passed_as_transform_is_released_when_the_run_finishes() -> None:
	from sqlload_api._host import finish
	from sqlload_api._transform import TRANSFORMS

	def page_transform() -> Any:
		return lambda rows, ctx: rows

	function = page_transform()
	released = weakref.ref(function)
	key, _ = TRANSFORMS.key(function)
	assert TRANSFORMS.lookup(key)[0] is function
	del function
	finish()
	gc.collect()
	assert released() is None
	assert TRANSFORMS.lookup(key)[0] is None


def test_a_transform_whose_file_cant_be_read_is_versioned_by_its_code() -> None:
	from sqlload_api._host import transform_version
	from sqlload_api._transform import TRANSFORMS

	def version_of(body: str) -> str | None:
		namespace: dict[str, Any] = {}
		exec(compile(f"def unread_file_transform(rows, ctx):\n\t{body}\n", "<notebook cell>", "exec"), namespace)
		TRANSFORMS.register("unread_file_transform", namespace["unread_file_transform"])
		return transform_version("unread_file_transform")[0]

	first = version_of("return rows")
	assert first is not None and first.startswith("unread_file_transform@")
	assert version_of("return rows.slice(1)") != first
	assert version_of("return rows") == first


if __name__ == "__main__":
	pytest.main([__file__, "-vv"])
