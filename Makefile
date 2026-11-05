SHELL := /bin/bash
FENSU := uvx --from 'fensu-cli==0.28.3' --with 'fensu==0.28.3' fensu

.PHONY: check check-ci test test-unit test-containers test-python verify

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

# Needs Docker: Redpanda, Postgres and SeaweedFS (S3) containers, a local fake Snowflake, and DuckDB.
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
