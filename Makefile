SHELL := /bin/bash
FENSU := uvx --from 'fensu-cli==0.28.3' fensu

.PHONY: check check-ci test test-unit test-containers verify

# Lints the default build (DuckDB included) and the build without DuckDB, so both keep compiling.
check:
	cargo fmt --all
	cargo clippy --workspace --all-targets --features sqlload/container-tests --quiet -- -D warnings
	cargo clippy -p sqlload --all-targets --no-default-features --features cli,container-tests --quiet -- -D warnings
	$(FENSU) check

check-ci:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --features sqlload/container-tests --quiet -- -D warnings
	cargo clippy -p sqlload --all-targets --no-default-features --features cli,container-tests --quiet -- -D warnings
	$(FENSU) check

test-unit:
	cargo test --workspace --lib --quiet

# Needs Docker: Redpanda, Postgres and SeaweedFS (S3) containers, a local fake Snowflake, and DuckDB.
# The AWS variables are the test containers' credentials.
test-containers:
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test cargo test -p sqlload --features container-tests --test exporter --quiet -- --test-threads=2
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test cargo test -p sqlload --features container-tests --test loader --quiet -- --test-threads=2

test: test-unit test-containers

verify: check-ci test
