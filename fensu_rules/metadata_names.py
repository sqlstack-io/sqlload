"""The columns a load adds are named by `load_naming` alone, so `naming = "dlt"` renames every one of them (CHI-1234 E10)."""

from __future__ import annotations

import re

from fensu import AnalyzerId, Family, Fault, File, RuleContext, rule

LOAD_NAME: re.Pattern[str] = re.compile(r'"_dlt_|"_sqlload_(?:load_id|loaded_at)"')
PREFIX: re.Pattern[str] = re.compile(r'"_sqlload_"')
COMMENT: str = "//"
PRODUCT_ROOT: str = "crates/sqlload/src/"
TESTS: str = "tests"
CONSTANTS_FILE: str = "constants.rs"
NAMING_CONSTANTS: str = "crates/sqlload/src/load_naming/constants.rs"


@rule(
    code="XSL003",
    family=Family.CUSTOM,
    slug="metadata-name-outside-load-naming",
    message="a load column's name is written outside load_naming",
    remediation=(
        "Name the load id, load time and every `_dlt_*` column through"
        " load_naming::main::metadata_name or metadata_role, and test for SQLLoad's columns"
        " with is_load_column; spell those names only in load_naming/constants.rs, and the"
        " bare `_sqlload_` prefix only in a constants.rs file."
    ),
    analyzers=(AnalyzerId.RUST,),
)
def metadata_name_outside_load_naming(*, file: File, ctx: RuleContext) -> list[Fault]:
    path: str = file.path.value
    if not path.startswith(PRODUCT_ROOT) or path == NAMING_CONSTANTS:
        return []
    parts: list[str] = path.split("/")
    if TESTS in parts or parts[-1] == f"{TESTS}.rs":
        return []
    faults: list[Fault] = []
    for number, line in enumerate(ctx.source.splitlines(), start=1):
        code: str = line.split(COMMENT, 1)[0]
        flagged: list[re.Match[str]] = [
            *LOAD_NAME.finditer(code),
            *(found for found in PREFIX.finditer(code) if parts[-1] != CONSTANTS_FILE),
        ]
        faults.extend(
            ctx.fault_for(path=ctx.path, line=number, column=found.start() + 1)
            for found in flagged
        )
    return faults
