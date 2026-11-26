"""The items `sqlload_api` builds cross as the committed wire, whichever way the connection is given, without the engine."""

import json
import os
import subprocess
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

import sqlload_api as sl
from sqlload_api import _wire

WIRE = Path(_wire.__file__).parent


@dataclass
class Owner:
	"""A source as `sl.http` reads one: a name and settings, as SQLLoad's `sl.source(...)` has them."""

	name: str
	settings: Mapping[str, Any] = field(default_factory=dict)


def by_name(**options: Any) -> sl.Http:
	return sl.http(connection="shop_api", **options)


def by_owner(**options: Any) -> sl.Http:
	return sl.http(Owner("shop", {"connection": "shop_api"}), **options)


@pytest.mark.parametrize("name", sorted(_wire.CASES))
def test_given_each_case_when_built_by_connection_name_then_it_crosses_as_the_committed_wire(name: str) -> None:
	crossed = _wire.crossed(_wire.CASES[name](by_name))
	if os.environ.get("SQLLOAD_UPDATE_WIRE") == "1":
		(WIRE / f"{name}.json").write_text(json.dumps(crossed, indent=2, sort_keys=True) + "\n")
	assert crossed == _wire.expected(name)


@pytest.mark.parametrize("name", sorted(_wire.CASES))
def test_given_each_case_when_built_from_a_source_then_it_crosses_byte_for_byte_as_by_connection_name(name: str) -> None:
	from_owner, from_name = _wire.CASES[name](by_owner), _wire.CASES[name](by_name)
	assert _raw(from_owner) == _raw(from_name)


def test_given_every_committed_wire_file_when_listing_them_then_each_has_its_case() -> None:
	assert sorted(path.stem for path in WIRE.glob("*.json")) == sorted(_wire.CASES)


def test_given_a_transform_passed_as_a_function_when_built_either_way_then_both_jobs_cross_alike() -> None:
	def page(rows: Any, ctx: Any) -> Any:
		return rows

	from_name, from_owner = (make().paginate("/orders", paginator=sl.single(), transform=page) for make in (by_name, by_owner))
	assert from_name.__sqlload_http_job__() == from_owner.__sqlload_http_job__()
	assert json.loads(from_name.__sqlload_http_job__())["transform"]["name"].endswith("<locals>.page")


@pytest.mark.parametrize(
	("make", "expected_message"),
	[
		(lambda: sl.http(), "sl.http needs a connection: pass connection=, or a source that sets one"),
		(lambda: sl.http(Owner("shop")), "sl.http needs a connection: set connection= on source shop or pass connection="),
		(lambda: sl.http(connection="shop_api", auth="x"), "sl.http no longer takes auth: set auth on [connections.shop_api]"),
	],
	ids=["no-connection", "source-without-connection", "moved-setting"],
)
def test_given_no_connection_or_a_moved_setting_when_making_a_client_then_it_says_what_to_set(make: Any, expected_message: str) -> None:
	with pytest.raises((ValueError, TypeError)) as raised:
		make()
	assert expected_message in str(raised.value)


def test_given_a_fresh_interpreter_when_importing_the_package_then_no_engine_module_loads() -> None:
	code = "import sys, sqlload_api; assert not any(name.split('.')[0] == 'sqlload' for name in sys.modules), sorted(sys.modules)"
	done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
	assert done.returncode == 0, done.stderr


def _raw(item: Any) -> Any:
	if hasattr(item, "__sqlload_http_job__"):
		return item.__sqlload_http_job__()
	if hasattr(item, "__sqlload_json__"):
		return item.__sqlload_json__()
	return item.__sqlload_checkpoint__()


if __name__ == "__main__":
	pytest.main([__file__, "-vv"])
