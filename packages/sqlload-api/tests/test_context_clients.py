"""`literal_paths(..., context_clients=...)` reads the paths of jobs built on a client the function's context makes."""

from typing import Any

import pytest

import sqlload_api as sl


def assigned(ctx: Any) -> Any:
	api = ctx.http()
	return api.paginate("/orders", paginator=sl.single(), fields={"id": "$.id", "city": sl.field("$.address.city")})


def chained(ctx: Any) -> Any:
	return ctx.http(connection="other").fan_out("/orders/{id}", over=[1, 2], key_path="$.orderId", fields={"total": "$.total"})


def other_method(ctx: Any) -> Any:
	return ctx.client().paginate("/orders", paginator=sl.single(), fields={"id": "$.id"})


def module_client(ctx: Any) -> Any:
	return API.paginate("/orders", paginator=sl.single(), fields={"id": "$.id"})


API = sl.http(connection="shop_api")


@pytest.mark.parametrize(
	("function", "context_clients", "expected_paths"),
	[
		(assigned, {"http"}, {"fields": [["id", "$.id"], ["city", "$.address.city"]], "key_paths": []}),
		(chained, {"http"}, {"fields": [["total", "$.total"]], "key_paths": ["$.orderId"]}),
		(assigned, set(), None),
		(other_method, {"http"}, None),
		(module_client, set(), {"fields": [["id", "$.id"]], "key_paths": []}),
		(module_client, {"http"}, {"fields": [["id", "$.id"]], "key_paths": []}),
	],
	ids=["assigned", "chained", "hook-off", "unnamed-method", "module-client", "module-client-hook-on"],
)
def test_given_a_context_client_when_reading_literal_paths_then_only_named_context_methods_make_clients(
	function: Any, context_clients: set[str], expected_paths: dict[str, Any] | None
) -> None:
	found = sl.literal_paths(function, context_clients=context_clients)
	paths = None if found is None else {"fields": found["fields"], "key_paths": found["key_paths"]}
	assert paths == expected_paths


if __name__ == "__main__":
	pytest.main([__file__, "-vv"])
