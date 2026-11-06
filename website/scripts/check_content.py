"""Fail when a docs page is missing from the sidebar, or the site names a system SQLLoad does not ship."""

import json
import re
import sys
from pathlib import Path

WEBSITE = Path(__file__).resolve().parent.parent
DOCS = WEBSITE / "src" / "content" / "docs"
# Systems on no released route; the site documents only what main ships.
UNSHIPPED = re.compile(r"\b(BigQuery|Databricks|Redshift|ClickHouse|SQL Server|MySQL|CDC)\b", re.IGNORECASE)


def sidebar_slugs(items: list) -> set[str]:
    slugs: set[str] = set()
    for item in items:
        if isinstance(item, str):
            slugs.add(item)
        elif "slug" in item:
            slugs.add(item["slug"])
        else:
            slugs |= sidebar_slugs(item.get("items", []))
    return slugs


def main() -> int:
    problems: list[str] = []
    listed = sidebar_slugs(json.loads((WEBSITE / "src" / "sidebar.json").read_text()))
    for page in sorted(DOCS.rglob("*.mdx")):
        slug = page.relative_to(DOCS).with_suffix("").as_posix().removesuffix("/index")
        if slug not in listed:
            problems.append(f"{page.relative_to(WEBSITE)}: not in src/sidebar.json")
    for path in [*sorted(DOCS.rglob("*.mdx")), WEBSITE / "public" / "index.html"]:
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if match := UNSHIPPED.search(line):
                problems.append(f"{path.relative_to(WEBSITE)}:{number}: names {match.group(0)}, which SQLLoad does not ship")
    for problem in problems:
        print(problem, file=sys.stderr)
    print(f"{len(listed)} sidebar pages, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
