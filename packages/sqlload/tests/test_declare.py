"""Declaring sources and tables."""

import logging

import pytest

import sqlload as sl
from sqlload._declare import Connection


def test_given_a_source_when_tables_attach_then_settings_fall_back_to_the_source() -> None:
	shop = sl.source("shop", connection="shop", destination="prod", write_strategy="merge")

	@shop.table(unique_key="order_id")
	def shop_orders(ctx: sl.Context):
		yield {"order_id": 1}

	@shop.table(name="stores", write_strategy="replace")
	def shop_stores(ctx: sl.Context):
		yield []

	assert [table.name for table in shop.tables] == ["shop_orders", "stores"]
	assert shop_orders.setting("write_strategy") == "merge"
	assert shop_stores.setting("write_strategy") == "replace"
	assert shop_orders.setting("lookback") is None
	ctx = sl.Context(table="shop_orders", source="shop", connection=Connection({}, {}), logger=logging.getLogger("test"))
	assert list(shop_orders(ctx)) == [{"order_id": 1}]


def test_given_a_standalone_table_when_declared_bare_or_called_then_it_has_no_source() -> None:
	@sl.table
	def orders(ctx: sl.Context):
		yield {"id": 1}

	assert orders.name == "orders" and orders.source is None


def test_given_unknown_settings_when_declaring_then_it_fails_closed() -> None:
	with pytest.raises(TypeError, match="unknown source setting.*colour"):
		sl.source("shop", colour="blue")
	with pytest.raises(TypeError, match="unknown table setting.*uniq_key"):
		sl.table(uniq_key="id")


def test_given_a_client_when_declared_twice_then_it_fails() -> None:
	shop = sl.source("shop")
	shop.client(lambda ctx: object())
	with pytest.raises(ValueError, match="already has a client"):
		shop.client(lambda ctx: object())


def test_given_a_connection_when_called_by_name_then_another_connection_is_returned() -> None:
	connection = Connection({"base_url": "https://api.shop.example"}, {"other": {"token": "t"}})
	assert connection["base_url"] == "https://api.shop.example"
	assert dict(connection("other")) == {"token": "t"}
	with pytest.raises(KeyError, match="no connection named missing"):
		connection("missing")


def test_given_a_source_client_when_tables_ask_for_it_then_it_is_built_once() -> None:
	built: list[int] = []
	ctx = sl.Context(
		table="t", source="s", connection=Connection({}, {}), logger=logging.getLogger("test"), client_factory=lambda ctx: built.append(1) or "client"
	)
	assert ctx.client == "client" and ctx.client == "client"
	assert built == [1]
	with pytest.raises(LookupError, match="declares no @source.client"):
		_ = sl.Context(table="t", source="s", connection=Connection({}, {}), logger=logging.getLogger("test")).client
