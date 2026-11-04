"""The crossing from table generators into Arrow, through the `sqlload._native.cross` test hook."""

import dataclasses
import enum
import json as stdlib_json
import random
import string
import threading
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import orjson
import pyarrow as pa
import pytest

import sqlload as sl
from sqlload import _native

CROSSINGS = ["walk", "serialise"]
TEXT = string.ascii_letters + " \"\\\n\t/é😀"


def random_table(seed: int) -> tuple[list[dict[str, Any]], pa.Schema]:
	"""Random API-shaped rows and the Arrow schema their columns infer to; every column has a value in row 0."""
	rnd = random.Random(seed)
	start = datetime(2026, 1, 1, tzinfo=timezone.utc)
	kinds = {
		"id": (pa.int64(), lambda i: i),
		"score": (pa.float64(), lambda i: rnd.uniform(-1e6, 1e6)),
		"ok": (pa.bool_(), lambda i: rnd.random() < 0.5),
		"name": (pa.string(), lambda i: "".join(rnd.choice(TEXT) for _ in range(rnd.randint(0, 12)))),
		"at": (pa.timestamp("us", tz="UTC"), lambda i: start + timedelta(microseconds=rnd.randint(0, 10**12))),
		"customer": (
			pa.struct([("country", pa.string()), ("tier", pa.int64())]),
			lambda i: {"country": rnd.choice(["GB", "FR"]), "tier": rnd.randint(0, 3)},
		),
		"tags": (pa.list_(pa.string()), lambda i: rnd.sample(["a", "b", "c", "d"], k=rnd.randint(0, 3))),
	}
	columns = list(kinds)[: rnd.randint(3, len(kinds))]
	rows = []
	for i in range(rnd.randint(1, 60)):
		rows.append(
			{
				column: (kinds[column][1](i) if i == 0 or rnd.random() > 0.3 else None)
				for column in columns
			}
		)
	schema = pa.schema([(column, kinds[column][0]) for column in columns])
	return rows, schema


def as_json_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
	"""Rows as an API's JSON would carry them: timestamps as ISO text."""
	return [{key: (value.isoformat() if isinstance(value, datetime) else value) for key, value in row.items()} for row in rows]


def read(source: Any, **options: Any) -> pa.Table:
	return _native.cross(source, **options).read_all()


@pytest.mark.parametrize("seed", range(25))
@pytest.mark.parametrize("crossing", CROSSINGS)
def test_given_the_same_rows_when_yielded_as_each_kind_then_the_tables_are_identical(seed: int, crossing: str) -> None:
	rows, schema = random_table(seed)
	api_rows = as_json_rows(rows)
	batch_rows = random.Random(seed).randint(1, 20)

	def single() -> Iterator[Any]:
		yield from api_rows

	def pages() -> Iterator[Any]:
		for start in range(0, len(api_rows), 7):
			yield api_rows[start : start + 7]

	forms = {
		"dicts": single(),
		"lists": pages(),
		"json": iter([sl.json(stdlib_json.dumps({"items": api_rows}, indent=2), path="items")]),
		"ndjson": iter([sl.ndjson(b"\n".join(orjson.dumps(row) for row in api_rows))]),
		"arrow": iter([pa.Table.from_pylist(rows, schema=schema)]),
	}
	tables = {name: read(form, batch_rows=batch_rows, crossing=crossing) for name, form in forms.items()}

	expected = tables["dicts"]
	for name, table in tables.items():
		assert table.schema.equals(expected.schema, check_metadata=True), name
		assert table.to_pylist() == expected.to_pylist(), name


def test_given_python_values_when_crossed_then_types_follow_the_files_rules() -> None:
	rows = [
		{"i": 1, "f": 1.5, "b": True, "t": "2026-01-02T03:04:05Z", "s": "x", "n": {"z": [1, "a"]}, "big": 2**70, "nan": float("nan")},
		{"i": 2, "f": 2, "b": None, "t": None, "s": None, "n": None, "big": 1, "nan": None},
	]
	for crossing in CROSSINGS:
		table = read(iter(rows), crossing=crossing)
		assert [str(field.type) for field in table.schema] == [
			"int64", "double", "bool", "timestamp[us, tz=UTC]", "string", "string", "string", "string",
		]
		assert table.schema.field("n").metadata == {b"sqlload.type": b"json"}
		assert table.column("n").to_pylist() == ['{"z":[1,"a"]}', None]
		assert table.column("big").to_pylist() == [str(2**70), "1"]


def test_given_many_single_dicts_when_crossed_then_they_arrive_in_batches_of_batch_rows() -> None:
	for crossing in CROSSINGS:
		sizes = [batch.num_rows for batch in _native.cross(({"a": i} for i in range(1050)), batch_rows=100, crossing=crossing)]
		assert sizes == [100] * 10 + [50]


def test_given_large_pages_and_bytes_when_crossed_then_pages_cross_whole_and_bytes_split_by_batch_rows() -> None:
	pages = [[{"a": i} for i in range(300)]]
	assert [batch.num_rows for batch in _native.cross(iter(pages), batch_rows=100)] == [300]
	data = sl.ndjson(b"\n".join(b'{"a":%d}' % i for i in range(250)))
	assert [batch.num_rows for batch in _native.cross(iter([data]), batch_rows=100)] == [100, 100, 50]


def test_given_a_slow_consumer_when_the_generator_runs_ahead_then_it_pauses() -> None:
	produced = [0]

	def endless() -> Iterator[dict[str, int]]:
		while True:
			produced[0] += 1
			yield {"a": produced[0]}

	stream = _native.cross(endless(), batch_rows=10, max_queued=1, parse_threads=1)
	next(stream)
	time.sleep(0.5)
	ahead = produced[0]
	time.sleep(0.2)
	assert ahead == produced[0]
	assert ahead < 1_000


def test_given_large_bytes_when_rust_parses_them_then_another_python_thread_keeps_running() -> None:
	data = sl.ndjson(b"\n".join(orjson.dumps({"id": i, "name": f"row {i}", "meta": {"k": i}}) for i in range(300_000)))
	ticks = [0]
	stop = threading.Event()

	def tick() -> None:
		while not stop.is_set():
			ticks[0] += 1

	ticker = threading.Thread(target=tick)
	ticker.start()
	started = time.perf_counter()
	table = read(iter([data]), parse_threads=1)
	elapsed = time.perf_counter() - started
	stop.set()
	ticker.join()
	assert table.num_rows == 300_000
	assert ticks[0] / elapsed > 100_000


def test_given_a_generator_that_raises_when_crossed_then_its_exception_propagates() -> None:
	class SourceDown(Exception):
		pass

	def failing() -> Iterator[dict[str, int]]:
		yield {"a": 1}
		raise SourceDown("the API returned 503")

	for crossing in CROSSINGS:
		with pytest.raises(SourceDown, match="503"):
			read(failing(), crossing=crossing)


@pytest.mark.parametrize(
	("items", "error", "message"),
	[
		([b'{"a":1}'], TypeError, r"yielded a bytes.*sl\.json"),
		([42], TypeError, "yielded a int"),
		([[{"a": 1}, 2]], TypeError, "row 1 of a yielded batch is a int"),
		([{1: "a"}], TypeError, "dict keys must be str"),
		([sl.ndjson(b'{"a":1}\n{"a":')], ValueError, "line 2"),
		([sl.json(b'{"data":{}}', path="data.items")], ValueError, 'data has no key "items"'),
	],
)
def test_given_bad_items_when_crossed_then_the_error_says_what_and_where(items: list[Any], error: type[Exception], message: str) -> None:
	with pytest.raises(error, match=message):
		read(iter(items), crossing="walk")


def test_given_json_data_when_constructed_then_ndjson_takes_no_path() -> None:
	assert sl.json("[]", path="items").path == "items"
	assert sl.ndjson(memoryview(b"{}")).lines
	with pytest.raises(ValueError, match="NDJSON"):
		_native.JsonData(b"", lines=True, path="items")


def test_given_polars_and_pandas_frames_when_crossed_then_they_match_pyarrow() -> None:
	polars = pytest.importorskip("polars")
	pandas = pytest.importorskip("pandas")
	data = {"id": [1, 2, 3], "name": ["a", None, "c"], "score": [0.5, 1.5, None]}
	expected = read(iter([pa.table(data)]))
	assert read(iter([polars.DataFrame(data)])).equals(expected)
	assert read(iter([pandas.DataFrame(data)])).to_pylist() == expected.to_pylist()
	assert read(iter([pa.record_batch(data)])).equals(expected)


class Colour(enum.Enum):
	RED = "red"
	NESTED = {"code": 7}


@dataclasses.dataclass
class Point:
	x: int
	label: Colour
	_hidden: int = 0


def numpy_rows() -> list[dict[str, Any]]:
	"""Rows of values orjson has its own rules for: numpy scalars, enums, dataclasses, dates and decimals, also nested."""
	np = pytest.importorskip("numpy")
	london = timezone(timedelta(hours=1))
	return [
		{
			"i": np.int64(i),
			"f": np.float64(i + 0.5),
			"f32": np.float32(0.1),
			"b": np.bool_(i % 2 == 0),
			"u": np.uint64(2**64 - 1),
			"colour": Colour.RED,
			"point": Point(i, Colour.NESTED),
			"at": datetime(2026, 1, 2, 3, 4, 5, 123, tzinfo=london),
			"naive": datetime(2026, 1, 2, 3, 4, 5),
			"day": date(2026, 1, 2),
			"price": Decimal("1.10"),
			"nested": {"n": np.int64(i), "c": Colour.RED, "d": date(2026, 1, 2), "p": Decimal("2.50"), "a": np.array([1.5, 2.0], dtype=np.float32)},
			"nan": np.float64("nan"),
			"when": np.datetime64("2021-01-01T00:00:00.123456789"),
		}
		for i in range(5)
	]


def test_given_orjson_typed_values_when_walked_or_serialised_then_the_tables_are_identical() -> None:
	rows = numpy_rows()
	walked = read(iter(rows), crossing="walk")
	assert read(iter(rows), crossing="serialise").equals(walked, check_metadata=True)
	types = {field.name: str(field.type) for field in walked.schema}
	assert types["i"] == "int64" and types["f"] == "double" and types["b"] == "bool"
	assert types["at"] == "timestamp[us, tz=UTC]"
	assert walked.column("f32").to_pylist()[0] == 0.1
	assert walked.column("colour").to_pylist()[0] == "red"
	assert walked.column("point").to_pylist()[0] == '{"x":0,"label":{"code":7}}'
	assert walked.column("price").to_pylist()[0] == "1.10"
	assert walked.column("nested").to_pylist()[0] == '{"n":0,"c":"red","d":"2026-01-02","p":"2.50","a":[1.5,2.0]}'


def test_given_a_batch_orjson_refuses_when_serialised_then_the_stream_types_as_if_walked() -> None:
	rows = numpy_rows()
	rows[3]["huge"] = 2**65
	options = {"batch_rows": 2, "parse_threads": 1}
	serialised = read(iter(rows), crossing="serialise", **options)
	assert serialised.equals(read(iter(rows), crossing="walk", **options), check_metadata=True)
	assert serialised.column("huge").to_pylist() == [None, None, None, str(2**65), None]


@pytest.mark.parametrize("crossing", CROSSINGS)
def test_given_one_dict_reused_for_every_row_when_crossed_then_each_row_keeps_its_values(crossing: str) -> None:
	def reused() -> Iterator[dict[str, int]]:
		row: dict[str, int] = {}
		for i in range(25):
			row["a"] = i
			yield row

	assert read(reused(), batch_rows=10, crossing=crossing).column("a").to_pylist() == list(range(25))


@pytest.mark.parametrize("crossing", CROSSINGS)
def test_given_the_same_dicts_refilled_for_every_page_when_crossed_then_each_row_keeps_its_values(crossing: str) -> None:
	def pages() -> Iterator[list[dict[str, int]]]:
		page: list[dict[str, int]] = [{}, {}, {}]
		for number in range(4):
			for position, row in enumerate(page):
				row["a"] = number * 3 + position
			yield page

	assert read(pages(), batch_rows=10, crossing=crossing).column("a").to_pylist() == list(range(12))


class Cursor:
	"""An iterator that is its own iterator, like a database cursor."""

	def __init__(self) -> None:
		self.rows = iter([{"a": 1}, {"a": 2}])
		self.closed = threading.Event()

	def __iter__(self) -> "Cursor":
		return self

	def __next__(self) -> dict[str, int]:
		return next(self.rows)

	def close(self) -> None:
		self.closed.set()


def test_given_an_iterator_passed_directly_when_the_read_ends_then_it_is_closed() -> None:
	cursor = Cursor()
	assert read(cursor).column("a").to_pylist() == [1, 2]
	assert cursor.closed.wait(2)


@pytest.mark.parametrize("site", ["producer", "parser"])
@pytest.mark.parametrize("crossing", CROSSINGS)
def test_given_a_crossing_thread_that_panics_when_read_then_the_read_fails(site: str, crossing: str) -> None:
	rows = ({"a": i} for i in range(100))
	with pytest.raises(ValueError, match=f"panicked: injected {site} panic"):
		read(rows, batch_rows=10, crossing=crossing, panic_at=site)


@pytest.mark.parametrize(("max_queued", "parse_threads"), [(1, 1), (4, 1), (2, 3), (4, 4)])
def test_given_a_slow_consumer_when_the_generator_runs_ahead_then_queued_batches_stay_within_the_bound(max_queued: int, parse_threads: int) -> None:
	yielded = [0]

	def endless() -> Iterator[dict[str, int]]:
		while True:
			yielded[0] += 1
			yield {"a": yielded[0]}

	stream = _native.cross(endless(), batch_rows=10, max_queued=max_queued, parse_threads=parse_threads)
	consumed = next(stream).num_rows
	time.sleep(0.5)
	bound = max_queued + parse_threads
	assert stream.stats()["batches"] - 1 <= bound
	assert yielded[0] - consumed <= (bound + 1) * 10


def test_given_a_consumer_that_stops_early_when_dropped_then_the_generator_is_closed() -> None:
	closed = threading.Event()

	def endless() -> Iterator[dict[str, int]]:
		try:
			while True:
				yield {"a": 1}
		finally:
			closed.set()

	stream = _native.cross(endless(), batch_rows=10, max_queued=1, parse_threads=1)
	next(stream)
	del stream
	assert closed.wait(2)


def test_given_a_generator_that_finishes_when_read_then_the_stream_ends_and_stays_ended() -> None:
	stream = _native.cross(iter([{"a": 1}]))
	assert [batch.num_rows for batch in stream] == [1]
	assert list(stream) == []
