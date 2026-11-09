"""Files a warehouse reads itself go through SQLLoad only for hmac masking (CHI-1044); files on an SFTP server, which no warehouse reads, always do."""

from __future__ import annotations

import re

from fensu import AnalyzerId, Family, Fault, File, RuleContext, rule

READ_FILES: re.Pattern[str] = re.compile(r"(?<!fn )\bread_files\s*\(")
FILE_READER: re.Pattern[str] = re.compile(r"\bStep::FileReader\b")
LOADER_ROUTE: re.Pattern[str] = re.compile(
    r"\b(?:loader_reads|deliver_arrow)\s*\(|\bStep::FileReader\b"
)
FUNCTION: re.Pattern[str] = re.compile(r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?fn\s+(\w+)")
WAREHOUSE_ARM: re.Pattern[str] = re.compile(r"\(Some\(\w+\),\s*None\)\s*=>")
COMMENT: str = "//"
PRODUCT_ROOT: str = "crates/sqlload/src/"
TESTS: str = "tests"
READER_FILE: str = "crates/sqlload/src/loader/_helpers/sources.rs"
ROUTE_FILE: str = "crates/sqlload/src/routing/main/plan_route.rs"
FILES_ROUTE_FILE: str = "crates/sqlload/src/routing/_helpers/files_route.rs"
UNLOAD_FILE: str = "crates/sqlload/src/routing/_helpers/warehouse_source.rs"
LOADER_FUNCTION: str = "loader_reads"
WAREHOUSE_FUNCTION: str = "warehouse_reads"


@rule(
    code="XSL002",
    family=Family.CUSTOM,
    slug="files-through-loader-outside-hmac",
    message="files a warehouse reads itself may reach SQLLoad's reader only from the hmac branch",
    remediation=(
        "Call read_files only from loader/_helpers/sources.rs, build Step::FileReader only in"
        " routing's loader_reads (or the warehouse-source unload route), keep the"
        " (Some(warehouse), None) arm of plan_route on warehouse_reads, and never reach"
        " loader_reads, deliver_arrow or Step::FileReader from warehouse_reads."
    ),
    analyzers=(AnalyzerId.RUST,),
)
def files_through_loader_outside_hmac(*, file: File, ctx: RuleContext) -> list[Fault]:
    path: str = file.path.value
    if not path.startswith(PRODUCT_ROOT):
        return []
    parts: list[str] = path.split("/")
    if TESTS in parts or parts[-1] == f"{TESTS}.rs":
        return []
    faults: list[Fault] = []
    function: str = ""
    for number, line in enumerate(ctx.source.splitlines(), start=1):
        code: str = line.split(COMMENT, 1)[0]
        named = FUNCTION.match(code)
        function = named.group(1) if named else function
        flagged: list[re.Match[str]] = [
            *(found for found in READ_FILES.finditer(code) if path != READER_FILE),
            *(
                found
                for found in FILE_READER.finditer(code)
                if path not in (FILES_ROUTE_FILE, UNLOAD_FILE)
                or (path == FILES_ROUTE_FILE and function != LOADER_FUNCTION)
            ),
            *(
                found
                for found in LOADER_ROUTE.finditer(code)
                if path == FILES_ROUTE_FILE and function == WAREHOUSE_FUNCTION
            ),
            *(
                found
                for found in WAREHOUSE_ARM.finditer(code)
                if path == ROUTE_FILE and f"{WAREHOUSE_FUNCTION}(" not in code
            ),
        ]
        faults.extend(
            ctx.fault_for(path=ctx.path, line=number, column=found.start() + 1)
            for found in flagged
        )
    if path == ROUTE_FILE and not WAREHOUSE_ARM.search(ctx.source):
        faults.append(ctx.fault_for(path=ctx.path, line=1, column=1))
    return faults
