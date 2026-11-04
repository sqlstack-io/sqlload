from collections.abc import Iterable, Iterator
from typing import Any, Literal

def run_cli(argv: list[str]) -> int:
	"""Run the `sqlload` command line `argv` (the program name first); return its exit code."""

def cross(
	source: Iterable[Any],
	*,
	batch_rows: int = 1_000,
	max_queued: int = 4,
	crossing: Literal["auto", "walk", "serialise"] = "auto",
	parse_threads: int | None = None,
	panic_at: Literal["producer", "parser"] | None = None,
) -> BatchStream:
	"""Read a table generator into Arrow batches (the crossing's test hook).

	At most `max_queued + parse_threads` batches wait between the generator and the consumer, besides
	the one being gathered; each yielded JSON bytes or Arrow object counts as one. `panic_at` makes a
	crossing thread panic, to test that the read fails.

	When the read ends (finished, failed or dropped), `close()` is called on the iterator if it has
	one, including an iterator passed in directly (`iter(x) is x`, such as a database cursor or a
	file).
	"""

class JsonData:
	def __new__(
		cls, data: bytes | bytearray, *, lines: bool = False, path: str | None = None
	) -> JsonData: ...
	@property
	def lines(self) -> bool: ...
	@property
	def path(self) -> str | None: ...
	def __len__(self) -> int: ...

class BatchStream(Iterator[Any]):
	def __iter__(self) -> BatchStream: ...
	def __next__(self) -> Any: ...
	def schema(self) -> Any: ...
	def stats(self) -> dict[str, float]: ...
	def read_all(self) -> Any: ...
