"""Summaries of runs: medians, and the markdown table."""

import statistics
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Result:
	"""Every measured run of one loader on one scenario."""

	scenario: str
	competitor: str
	rows: int
	status: str = "ok"  # ok, invalid, failed, n/a, skipped
	note: str = ""
	runs: list[dict[str, Any]] = field(default_factory=list)

	def median(self, key: str) -> float | None:
		values = [run[key] for run in self.runs if run.get("valid")]
		return statistics.median(values) if values else None

	def summary(self) -> dict[str, Any]:
		wall = self.median("wall_s")
		return {
			"scenario": self.scenario,
			"competitor": self.competitor,
			"rows": self.rows,
			"status": self.status,
			"note": self.note,
			"wall_s": wall,
			"cpu_s": self.median("cpu_s"),
			"user_s": self.median("user_s"),
			"sys_s": self.median("sys_s"),
			"peak_rss_mb": self.median("peak_rss_mb"),
			"rows_per_s": self.rows / wall if wall else None,
			"runs": self.runs,
		}


def _number(value: float | None, digits: int = 2) -> str:
	return "" if value is None else f"{value:,.{digits}f}"


def markdown(summaries: list[dict[str, Any]], header: str) -> str:
	"""One table: a row per scenario and loader, with wall time relative to sqlload."""
	lines = [
		header,
		"",
		"| Scenario | Loader | Rows | Wall s | CPU s (user+sys) | Peak RSS MiB | Rows/s | vs sqlload (wall) | Status |",
		"|---|---|---:|---:|---:|---:|---:|---:|---|",
	]
	sqlload_wall = {s["scenario"]: s["wall_s"] for s in summaries if s["competitor"] == "sqlload" and s["wall_s"]}
	for s in summaries:
		base = sqlload_wall.get(s["scenario"])
		ratio = f"{s['wall_s'] / base:.2f}x" if base and s["wall_s"] else ""
		status = s["status"] if not s["note"] else f"{s['status']}: {s['note']}"
		lines.append(
			f"| {s['scenario']} | {s['competitor']} | {s['rows']:,} | {_number(s['wall_s'])} | {_number(s['cpu_s'])} "
			f"| {_number(s['peak_rss_mb'], 0)} | {_number(s['rows_per_s'], 0)} | {ratio} | {status} |"
		)
	return "\n".join(lines) + "\n"
