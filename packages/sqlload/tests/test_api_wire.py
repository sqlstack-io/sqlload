"""`sqlload` re-exports `sqlload_api` unchanged, and a job built from a SQLLoad source crosses as one built by connection name."""

from typing import Any

import pytest
import sqlload_api
from sqlload_api import _wire

import sqlload as sl

SHOP = sl.source("shop", connection="shop_api", write_strategy="replace")


def from_source(**options: Any) -> sqlload_api.Http:
	return sl.http(SHOP, **options)


def by_name(**options: Any) -> sqlload_api.Http:
	return sqlload_api.http(connection="shop_api", **options)


@pytest.mark.parametrize("name", sorted(_wire.CASES))
def test_given_each_case_when_built_from_a_sqlload_source_then_it_crosses_as_the_committed_wire_and_as_by_name(name: str) -> None:
	from_sqlload, from_api = _wire.CASES[name](from_source), _wire.CASES[name](by_name)
	assert _wire.crossed(from_sqlload) == _wire.expected(name)
	assert _wire.crossed(from_sqlload) == _wire.crossed(from_api)


@pytest.mark.parametrize(
	"name",
	[name for name in sl.__all__ if name in sqlload_api.__all__ and name != "__version__"],
)
def test_given_a_name_sqlload_re_exports_when_compared_then_it_is_sqlload_apis_own(name: str) -> None:
	assert getattr(sl, name) is getattr(sqlload_api, name)


def test_given_both_packages_when_comparing_versions_then_they_match() -> None:
	assert sl.__version__ == sqlload_api.__version__
