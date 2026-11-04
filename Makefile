SHELL := /bin/bash
FENSU := uvx --from 'fensu-cli==0.28.3' fensu

.PHONY: check check-ci test test-unit test-containers test-loader verify

# Local checks also lint the DuckDB destination, which CI skips because the bundled DuckDB is slow to compile.
check:
	cargo fmt --all
	cargo clippy --workspace --all-targets --features sqlload/container-tests --quiet -- -D warnings
	cargo clippy --workspace --all-targets --features sqlload/container-tests,sqlload/duckdb --quiet -- -D warnings
	$(FENSU) check

check-ci:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --features sqlload/container-tests --quiet -- -D warnings
	$(FENSU) check

test-unit:
	cargo test --workspace --lib --quiet

# Needs Docker: starts Redpanda and Postgres containers and a local fake Snowflake, which is also the S3 bucket.
test-containers:
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test cargo test -p sqlload --features container-tests --test exporter --quiet -- --test-threads=2

# Local only (not in CI): the loader's Postgres and DuckDB tests, which need the bundled DuckDB.
test-loader:
	cargo test -p sqlload --features duckdb --lib --quiet
	cargo test -p sqlload --features container-tests,duckdb --test loader --quiet -- --test-threads=2

test: test-unit test-containers test-loader

verify: check-ci test
