"""Python-side benchmark data: deterministic generators every loader's environment imports.

Standard library only, so the harness and each competitor's environment produce identical rows
from the same seed. Rows come in pages (lists of dicts), the shape a paginated API gives.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

PAGE_ROWS = 1000
EPOCH = datetime(2026, 1, 1, tzinfo=UTC)
WORDS = ("alpha", "bravo", "cobalt", "delta", "ember", "fjord", "garnet", "harbor", "indigo", "juniper")
STATUSES = ("new", "paid", "packed", "shipped", "delivered", "returned")


def mix(value: int, seed: int, salt: int) -> int:
	"""A 31-bit hash of `value`, stable across Python versions (no `hash()` randomisation)."""
	x = (value * 0x9E3779B1 + seed * 0x85EBCA77 + salt * 0xC2B2AE3D) & 0xFFFFFFFF
	x ^= x >> 15
	x = (x * 0x2C1B3C6D) & 0xFFFFFFFF
	x ^= x >> 12
	return x & 0x7FFFFFFF


def flat_row(index: int, seed: int) -> dict[str, Any]:
	"""One flat 20-field order row."""
	h = [mix(index, seed, salt) for salt in range(8)]
	return {
		"id": index,
		"customer_id": h[0] % 100_000,
		"product_id": h[1] % 5_000,
		"quantity": 1 + h[2] % 20,
		"warehouse_id": h[3] % 50,
		"priority": h[4] % 5,
		"sku": f"SKU-{h[1] % 5_000:05d}",
		"status": STATUSES[h[2] % len(STATUSES)],
		"customer_name": f"{WORDS[h[0] % 10]} {WORDS[h[3] % 10]} {h[0] % 997}",
		"city": WORDS[h[5] % 10].title(),
		"channel": ("web", "app", "partner")[h[6] % 3],
		"unit_price": (h[5] % 100_000) / 100,
		"discount": (h[6] % 5_000) / 100,
		"tax": (h[7] % 2_000) / 100,
		"weight_kg": (h[4] % 10_000) / 100,
		"is_gift": h[7] % 7 == 0,
		"is_priority": h[4] % 3 == 0,
		"created_at": EPOCH + timedelta(seconds=index * 7 + h[0] % 7),
		"updated_at": EPOCH + timedelta(seconds=index * 7 + 3_600 + h[1] % 3_600),
		"note": None if h[2] % 4 else f"note {h[3] % 1_000}",
	}


def nested_row(index: int, seed: int) -> dict[str, Any]:
	"""One API-shaped order: a customer object, a list of line objects and a map of attributes."""
	h = [mix(index, seed, salt) for salt in range(8)]
	lines = [
		{
			"line": line,
			"product_id": mix(index * 8 + line, seed, 11) % 5_000,
			"quantity": 1 + mix(index * 8 + line, seed, 12) % 9,
			"price": (mix(index * 8 + line, seed, 13) % 50_000) / 100,
		}
		for line in range(1 + h[0] % 5)
	]
	return {
		"id": index,
		"status": STATUSES[h[2] % len(STATUSES)],
		"created_at": (EPOCH + timedelta(seconds=index * 7)).isoformat(),
		"customer": {
			"id": h[0] % 100_000,
			"name": f"{WORDS[h[0] % 10]} {h[0] % 997}",
			"address": {"city": WORDS[h[5] % 10].title(), "postcode": f"{h[6] % 100_000:05d}"},
		},
		"lines": lines,
		"attributes": {f"k{key}": WORDS[(h[7] + key) % 10] for key in range(h[3] % 4)},
		"tags": [WORDS[(h[4] + tag) % 10] for tag in range(h[1] % 3)],
	}


def nested_lines(rows: int, seed: int) -> int:
	"""How many `lines` objects `rows` nested rows hold, for checking child tables."""
	return sum(1 + mix(index, seed, 0) % 5 for index in range(1, rows + 1))


def pages(rows: int, seed: int, shape: str) -> Iterator[list[dict[str, Any]]]:
	"""`rows` rows of `shape` (`flat` or `nested`) in pages of PAGE_ROWS, ids from 1."""
	make = flat_row if shape == "flat" else nested_row
	for start in range(1, rows + 1, PAGE_ROWS):
		yield [make(index, seed) for index in range(start, min(start + PAGE_ROWS, rows + 1))]


def ndjson_chunks(path: str, chunk_bytes: int = 4 << 20) -> Iterator[bytes]:
	"""The NDJSON file at `path` in chunks of about `chunk_bytes`, each ending at a line end."""
	with open(path, "rb") as handle:
		tail = b""
		while block := handle.read(chunk_bytes):
			block = tail + block
			cut = block.rfind(b"\n") + 1
			tail = block[cut:]
			if cut:
				yield block[:cut]
		if tail:
			yield tail
