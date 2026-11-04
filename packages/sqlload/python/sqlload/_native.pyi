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
) -> BatchStream:
	"""Read a table generator into Arrow batches (the crossing's test hook)."""

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
