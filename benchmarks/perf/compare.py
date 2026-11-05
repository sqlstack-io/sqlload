"""Compare two runs of the hot-path benchmarks by Callgrind instruction count.

Each input is gungraun's `--output-format=json` output: one summary object per line. Writes a
markdown table (to stdout and, with --summary, appended to that file) and exits 1 when a benchmark's
instructions grew by more than --threshold percent, unless --accept is given.

Standard library only, so CI runs it with any Python 3.10+.
"""

import argparse
import json
import sys
from pathlib import Path


def instructions(path: Path) -> dict[str, int]:
	"""Each benchmark's total Callgrind `Ir`, keyed by `group::function id`."""
	counts: dict[str, int] = {}
	if not path.exists():
		return counts
	for line in path.read_text().splitlines():
		line = line.strip()
		if not line.startswith("{"):
			continue
		summary = json.loads(line)
		for profile in summary.get("profiles", []):
			if profile.get("tool") != "Callgrind":
				continue
			value = profile["data"]["total"]["metrics"]["Ir"]["values"]["new"]
			name = summary["module_path"].split("::", 1)[-1]
			if summary.get("id"):
				name += f" {summary['id']}"
			counts[name] = int(value)
	return counts


def compare(base: dict[str, int], head: dict[str, int], threshold: float) -> tuple[list[str], list[str]]:
	"""The table's lines and the names of benchmarks that regressed beyond `threshold` percent."""
	lines = ["| Benchmark | Base Ir | Head Ir | Change | |", "|---|---:|---:|---:|---|"]
	regressed: list[str] = []
	for name in sorted(head.keys() | base.keys()):
		old, new = base.get(name), head.get(name)
		if old is None or new is None:
			shown = ["" if value is None else f"{value:,}" for value in (old, new)]
			lines.append(f"| `{name}` | {shown[0]} | {shown[1]} | | {'new' if old is None else 'removed'} |")
			continue
		change = (new - old) / old * 100 if old else 0.0
		mark = ""
		if change > threshold:
			mark = "regressed"
			regressed.append(name)
		elif change < -threshold:
			mark = "improved"
		lines.append(f"| `{name}` | {old:,} | {new:,} | {change:+.2f}% | {mark} |")
	return lines, regressed


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
	parser.add_argument("--base", type=Path, required=True)
	parser.add_argument("--head", type=Path, required=True)
	parser.add_argument("--threshold", type=float, default=5.0, help="percent instruction growth that fails")
	parser.add_argument("--accept", action="store_true", help="report regressions without failing")
	parser.add_argument("--base-label", default="merge base")
	parser.add_argument("--summary", type=Path, help="markdown file to append the table to")
	args = parser.parse_args(argv)
	base, head = instructions(args.base), instructions(args.head)
	if not head:
		print("no benchmark results for the head", file=sys.stderr)
		return 1
	lines, regressed = compare(base, head, args.threshold)
	report = [f"### Hot-path instruction counts (head vs {args.base_label})", ""]
	if not base:
		report += [f"No results for the {args.base_label} (it has no benchmarks yet); nothing to compare.", ""]
	report += lines + [""]
	if regressed and args.accept:
		report.append(f"{len(regressed)} benchmark(s) regressed by more than {args.threshold:g}%; accepted by the `perf-regression-accepted` label.")
	elif regressed:
		report.append(
			f"**{len(regressed)} benchmark(s) regressed by more than {args.threshold:g}%.** "
			"Add the `perf-regression-accepted` label to accept it."
		)
	else:
		report.append(f"No benchmark regressed by more than {args.threshold:g}%.")
	text = "\n".join(report) + "\n"
	print(text)
	if args.summary:
		with args.summary.open("a") as handle:
			handle.write(text)
	return 1 if regressed and not args.accept else 0


if __name__ == "__main__":
	sys.exit(main())
