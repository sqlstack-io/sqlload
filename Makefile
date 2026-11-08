SHELL := /bin/bash
FENSU := uvx --from 'fensu-cli==0.28.3' --with 'fensu==0.28.3' fensu

.PHONY: check check-ci clean-partial-duckdb-sources test test-unit test-containers test-python verify bench-perf bench-wheel bench-compare bench-snowflake bench-e2e test-bench-harness

# Lints the default build (DuckDB included) and the build without DuckDB, so both keep compiling.
check:
	cargo fmt --all
	cargo clippy --workspace --all-targets --features sqlload/container-tests --quiet -- -D warnings
	cargo clippy -p sqlload --all-targets --no-default-features --features cli,container-tests --quiet -- -D warnings
	cargo clippy -p sqlload --lib --no-default-features --features cli --quiet -- -D warnings
	$(FENSU) check

check-ci:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --features sqlload/container-tests --quiet -- -D warnings
	cargo clippy -p sqlload --all-targets --no-default-features --features cli,container-tests --quiet -- -D warnings
	cargo clippy -p sqlload --lib --no-default-features --features cli --quiet -- -D warnings
	$(FENSU) check

test-unit:
	cargo test --workspace --lib --quiet

# CI's rust-cache can restore DuckDB's unpacked bundled sources without their `.complete` marker;
# libduckdb-sys then fails to unpack over them ("Directory not empty") whenever its build script
# reruns, such as after a feature change. Removes only such incomplete trees.
clean-partial-duckdb-sources:
	@for dir in target/duckdb-sources/*/; do \
		[ -d "$$dir" ] && [ ! -e "$$dir.complete" ] && echo "removing incomplete $$dir" && rm -rf "$$dir"; \
	done; true

# Needs Docker: Redpanda, Postgres, SeaweedFS (S3) and ClickHouse containers, a local fake Snowflake, and DuckDB.
# The AWS variables are the test containers' credentials.
test-containers:
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test cargo test -p sqlload --features container-tests --test exporter --quiet -- --test-threads=2
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test cargo test -p sqlload --features container-tests --test loader --quiet -- --test-threads=2

# Builds the sqlload extension (debug) into .venv and runs the package's tests.
test-python:
	uv venv --quiet --allow-existing --python 3.12 .venv
	uv pip install --quiet --python .venv/bin/python 'maturin>=1.9,<2' pytest pytest-xdist pyarrow polars pandas orjson duckdb
	cd packages/sqlload && VIRTUAL_ENV=$(CURDIR)/.venv $(CURDIR)/.venv/bin/maturin develop --uv --quiet
	.venv/bin/python -m pytest packages/sqlload/tests -n auto --dist loadfile

test: test-unit test-containers

verify: check-ci test

# Instruction counts of the hot paths under Callgrind; needs valgrind and gungraun-runner 0.20.
# Extra arguments go to gungraun, for example BENCH_ARGS='--save-summary=json'.
bench-perf:
	cargo bench -p sqlload-benches --bench hot_paths --quiet -- $(BENCH_ARGS)

# The sqlload wheel, built from this tree with release settings, in the harness's environment.
bench-wheel:
	cd benchmarks && uv sync --quiet --locked $(BENCH_GROUPS)
	rm -rf benchmarks/.wheels
	cd packages/sqlload && VIRTUAL_ENV=$(CURDIR)/benchmarks/.venv $(CURDIR)/benchmarks/.venv/bin/maturin build --release --quiet --out $(CURDIR)/benchmarks/.wheels
	uv pip install --quiet --python benchmarks/.venv/bin/python --reinstall --no-deps benchmarks/.wheels/*.whl

# Compares sqlload with dlt and ingestr (needs Docker); on demand, not in CI. Results go to
# BENCH_OUT; BENCH_ARGS takes the harness's options (see benchmarks/README.md).
BENCH_OUT ?= benchmarks/results
bench-compare: bench-wheel
	cd benchmarks/envs/dlt && uv sync --quiet --locked
	cd benchmarks/envs/ingestr && uv sync --quiet --locked
	cd benchmarks && .venv/bin/python -m sqlload_bench --out $(abspath $(BENCH_OUT)) $(BENCH_ARGS)

# The Snowflake scenarios, on a schema of your own; every variable they need is in benchmarks/README.md.
bench-snowflake:
	$(MAKE) bench-compare BENCH_GROUPS='--group snowflake' BENCH_ARGS='--scenarios snowflake $(BENCH_ARGS)'

# The non-gating end-to-end timing CI reports: sqlload alone, Postgres to DuckDB, 1M narrow rows.
bench-e2e: bench-wheel
	cd benchmarks && .venv/bin/python -m sqlload_bench --out $(abspath $(BENCH_OUT)) --size e2e --scenarios pg_narrow --competitors sqlload --runs 1 --warmup 0

test-bench-harness:
	cd benchmarks && uv sync --quiet --locked --group dev && .venv/bin/python -m pytest tests/test_harness.py -q -p no:cacheprovider
