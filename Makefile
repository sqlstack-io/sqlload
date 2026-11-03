SHELL := /bin/bash
FENSU := uvx --from 'fensu-cli==0.28.3' fensu

.PHONY: check check-ci test test-unit test-containers verify

check:
	cargo fmt --all
	cargo clippy --workspace --all-targets --features sqlload/container-tests --quiet -- -D warnings
	$(FENSU) check

check-ci:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --features sqlload/container-tests --quiet -- -D warnings
	$(FENSU) check

test-unit:
	cargo test --workspace --lib --quiet

# Needs Docker: starts Redpanda containers and a local fake Snowflake, which is also the S3 bucket.
test-containers:
	AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test cargo test -p sqlload --features container-tests --test exporter --quiet -- --test-threads=2

test: test-unit test-containers

verify: check-ci test
