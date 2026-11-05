"""Warehouse-specific code lives in the adapters (CHI-1045)."""

from __future__ import annotations

import re

from fensu import AnalyzerId, Family, Fault, File, RuleContext, rule

WAREHOUSE_CODE: re.Pattern[str] = re.compile(
    r"\b(?:Destination|Writer|Warehouse)::(?:DuckDb|Snowflake|Files|Parquet)\b"
    r"|\b(?:StageRoute|ForcedStage|StageObject)::(?:\w+|\{|\*)"
    r"|\badapters::(?:snowflake|duckdb)::(?:main|_helpers)\b"
)
COMMENT: str = "//"
PRODUCT_ROOTS: tuple[tuple[str, ...], ...] = (
    ("crates", "sqlload", "src"),
    ("crates", "sqlload-python", "src"),
)
ADAPTERS: str = "adapters"
TESTS: str = "tests"


@rule(
    code="XSL001",
    family=Family.CUSTOM,
    slug="warehouse-outside-adapters",
    message="warehouse-specific code belongs in crates/sqlload/src/adapters",
    remediation=(
        "Ask the SqlAdapter or TableWriter contract, or call an adapters::dispatch entry,"
        " instead of naming a destination, writer, warehouse or stage-route variant"
        " or reaching into a warehouse adapter's internals."
    ),
    analyzers=(AnalyzerId.RUST,),
)
def warehouse_outside_adapters(*, file: File, ctx: RuleContext) -> list[Fault]:
    parts: tuple[str, ...] = tuple(file.path.value.split("/"))
    root: tuple[str, ...] | None = next(
        (root for root in PRODUCT_ROOTS if parts[: len(root)] == root), None
    )
    if root is None:
        return []
    rest: tuple[str, ...] = parts[len(root) :]
    if rest[:1] == (ADAPTERS,):
        return []
    if TESTS in rest or parts[-1] == f"{TESTS}.rs":
        return []
    faults: list[Fault] = []
    for number, line in enumerate(ctx.source.splitlines(), start=1):
        code: str = line.split(COMMENT, 1)[0]
        for found in WAREHOUSE_CODE.finditer(code):
            faults.append(ctx.fault_for(path=ctx.path, line=number, column=found.start() + 1))
    return faults
