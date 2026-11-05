"""Warehouse-specific code lives in the adapters (CHI-1045)."""

from __future__ import annotations

import re

from fensu import AnalyzerId, Family, Fault, File, RuleContext, rule

WAREHOUSE_VARIANT: re.Pattern[str] = re.compile(
    r"\b(?:Destination|Writer|Warehouse)::(?:DuckDb|Snowflake|Files|Parquet)\b"
)
PRODUCT_ROOT: tuple[str, ...] = ("crates", "sqlload", "src")
ADAPTERS: str = "adapters"
TESTS: str = "tests"


@rule(
    code="XSL001",
    family=Family.CUSTOM,
    slug="warehouse-outside-adapters",
    message="warehouse-specific code belongs in crates/sqlload/src/adapters",
    remediation=(
        "Ask the SqlAdapter or TableWriter contract, or call an adapters::dispatch entry,"
        " instead of naming a destination, writer or warehouse variant."
    ),
    analyzers=(AnalyzerId.RUST,),
)
def warehouse_outside_adapters(*, file: File, ctx: RuleContext) -> list[Fault]:
    parts: tuple[str, ...] = tuple(file.path.value.split("/"))
    rest: tuple[str, ...] = parts[len(PRODUCT_ROOT) :]
    if parts[: len(PRODUCT_ROOT)] != PRODUCT_ROOT or rest[:1] == (ADAPTERS,):
        return []
    if TESTS in rest or parts[-1] == f"{TESTS}.rs":
        return []
    faults: list[Fault] = []
    for number, line in enumerate(ctx.source.splitlines(), start=1):
        for found in WAREHOUSE_VARIANT.finditer(line):
            faults.append(ctx.fault_for(path=ctx.path, line=number, column=found.start() + 1))
    return faults
