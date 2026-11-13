"""The synthetic cases loaded by both dlt and SQLLoad: each a table, how it is written, and the files of each run."""

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Case:
	"""One table loaded run after run.

	`runs` holds each run's file text; `fmt` is "ndjson" or "xml" (rows under `orders/order`).
	`disposition` is "append", "replace" or "merge" (dlt's upsert, SQLLoad's merge), keyed by `key`.
	`unnest` is what SQLLoad's table sets; dlt unnests every list down to `max_depth` levels.
	`placed_by` is the key's stored name, when naming renames it (`OrderID` is stored as `order_id`).
	"""

	name: str
	fmt: str
	disposition: str
	runs: tuple[str, ...]
	unnest: tuple[str, ...] | str = "all"
	key: str = "id"
	max_depth: int = 1000
	lists: tuple[str, ...] = field(default=())
	placed_by: str = ""

	@property
	def stored_key(self) -> str:
		return self.placed_by or self.key


def ndjson(*rows: str) -> str:
	return "".join(f"{row}\n" for row in rows)


def xml(*orders: str) -> str:
	return "<orders>" + "".join(f"<order>{order}</order>" for order in orders) + "</orders>"


CASES: tuple[Case, ...] = (
	Case(
		name="xml_scalar_list",
		fmt="xml",
		disposition="append",
		unnest=("tag",),
		runs=(xml("<id>1</id><tag>a</tag><tag>b</tag>", "<id>2</id><tag>c</tag><tag>d</tag>"),),
	),
	Case(
		name="json_scalar_list",
		fmt="ndjson",
		disposition="append",
		runs=(ndjson('{"id": 1, "tags": ["a", "b"]}', '{"id": 2, "tags": [3, 4]}'),),
	),
	Case(
		name="mixed_lists",
		fmt="ndjson",
		disposition="append",
		runs=(ndjson('{"id": 1, "items": [{"sku": "a", "qty": 2}, {"sku": "b", "qty": 1}], "tags": ["x", "y"]}'),),
	),
	Case(
		name="empty_and_missing_scalar_list",
		fmt="ndjson",
		disposition="append",
		runs=(ndjson('{"id": 1, "tags": ["x"]}', '{"id": 2, "tags": []}', '{"id": 3}'),),
	),
	Case(
		name="xml_single_then_many",
		fmt="xml",
		disposition="append",
		unnest=("tag",),
		runs=(
			xml("<id>1</id><tag>a</tag>"),
			xml("<id>2</id><tag>b</tag><tag>c</tag>"),
		),
	),
	Case(
		name="lists_in_lists",
		fmt="ndjson",
		disposition="append",
		runs=(ndjson('{"id": 1, "items": [{"sku": "a", "parts": [{"p": 1}, {"p": 2}]}], "grid": [[1, 2], [3]]}'),),
	),
	Case(
		name="max_depth_cut",
		fmt="ndjson",
		disposition="append",
		max_depth=1,
		runs=(ndjson('{"id": 1, "items": [{"sku": "a", "parts": [1, 2]}]}'),),
	),
	Case(
		name="replace_lists",
		fmt="ndjson",
		disposition="replace",
		runs=(
			ndjson('{"id": 1, "tags": ["a", "b"]}', '{"id": 2, "tags": ["c"]}'),
			ndjson('{"id": 1, "tags": ["z"]}'),
		),
	),
	Case(
		name="merge_lists_change",
		fmt="ndjson",
		disposition="merge",
		runs=(
			ndjson('{"id": 1, "items": [{"sku": "a"}, {"sku": "b"}, {"sku": "c"}]}', '{"id": 2, "items": [{"sku": "d"}]}', '{"id": 3, "items": [{"sku": "e"}]}'),
			ndjson('{"id": 1, "items": [{"sku": "a"}]}', '{"id": 2, "items": []}', '{"id": 3}'),
			ndjson('{"id": 1, "items": [{"sku": "a"}, {"sku": "f"}, {"sku": "g"}, {"sku": "h"}]}', '{"id": 3, "items": [{"sku": "e"}]}'),
		),
	),
	Case(
		name="null_lists",
		fmt="ndjson",
		disposition="append",
		runs=(ndjson('{"id": 1, "tags": ["x"]}', '{"id": 2, "tags": null}', '{"id": 3, "items": null, "tags": []}'),),
	),
	Case(
		name="list_then_object",
		fmt="ndjson",
		disposition="append",
		runs=(
			ndjson('{"id": 1, "tags": ["x"]}'),
			ndjson('{"id": 2, "tags": {"k": "y"}}'),
		),
	),
	Case(
		name="null_then_list",
		fmt="ndjson",
		disposition="append",
		runs=(
			ndjson('{"id": 1, "tags": null}'),
			ndjson('{"id": 2, "tags": ["a", "b"]}'),
		),
	),
	Case(
		name="naming_source_spellings",
		fmt="ndjson",
		disposition="append",
		key="OrderID",
		placed_by="order_id",
		runs=(ndjson('{"OrderID": 1, "Unit Price": "a", "a__B": 1, "x__": 2, "getHTTPCode": 3, "LineItems": [{"UnitPrice": 5, "Item ID": "s1"}]}', '{"OrderID": 2, "Unit Price": "b", "a__B": 4, "x__": 5, "getHTTPCode": 6, "LineItems": []}'),),
	),
	Case(
		name="naming_merge_key",
		fmt="ndjson",
		disposition="merge",
		key="OrderID",
		placed_by="order_id",
		runs=(
			ndjson('{"OrderID": 1, "Status": "new", "LineItems": [{"UnitPrice": 5}, {"UnitPrice": 6}]}', '{"OrderID": 2, "Status": "new", "LineItems": [{"UnitPrice": 7}]}'),
			ndjson('{"OrderID": 1, "Status": "paid", "LineItems": [{"UnitPrice": 9}]}'),
		),
	),
	Case(
		name="naming_collision",
		fmt="ndjson",
		disposition="append",
		runs=(ndjson('{"id": 1, "OrderID": 1, "Order ID": 2}'),),
	),
	Case(
		name="nested_objects",
		fmt="ndjson",
		disposition="merge",
		runs=(
			ndjson(
				'{"id": 1, "Customer": {"Name": "a", "Address": {"City": "x", "Zip": 1}}, "meta": {}, "items": [{"sku": "s", "dims": {"w": 2}}]}',
				'{"id": 2, "Customer": null, "items": []}',
			),
			ndjson('{"id": 2, "Customer": {"Name": "b", "Address": null}, "items": [{"sku": "t"}]}'),
		),
	),
	Case(
		name="list_in_object",
		fmt="ndjson",
		disposition="append",
		runs=(ndjson('{"id": 1, "customer": {"tags": ["a", "b"]}}'),),
	),
	Case(
		name="new_list_field_later",
		fmt="ndjson",
		disposition="append",
		runs=(
			ndjson('{"id": 1, "items": [{"sku": "a"}]}'),
			ndjson('{"id": 2, "items": [{"sku": "b"}], "tags": ["t"]}'),
		),
	),
)
