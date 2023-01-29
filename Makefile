.PHONY: run
run:
	cargo run

.PHONY: build
build:
	cargo +nightly build
	ls -lah target/debug/hub

.PHONY: prod
prod:
	cargo +nightly build --release
	ls -lah target/release/hub
