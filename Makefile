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

.PHONY: docker-build
docker-build:
	docker build --squash --build-arg BINARY=hub -t hub:latest -f Dockerfile .

.PHONY: docker-run
docker-run:
	docker run -it --rm --init \
		-e RUST_BACKTRACE=1 \
		-e RUST_LOG=1 \
		-e BINARY=hub \
		-e REDIS_PASSWORD=123 \
		--network host \
		--name hub \
		hub:latest