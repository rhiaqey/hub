export REDIS_PASSWORD=7tgbBSO2Yu
export REDIS_ADDRESS=localhost:6379
export REDIS_SENTINELS=localhost:26379

export ID=1
export NAME=hub
export NAMESPACE=rhiaqey
export RUST_BACKTRACE=full
export RUST_LOG=trace
export DEBUG=true

.PHONY: hub
hub: run

.PHONY: run
run:
	cargo +nightly run

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
		-e REDIS_PASSWORD=${REDIS_PASSWORD} \
		--network host \
		--name hub \
		hub:latest

.PHONY: redis
redis:
	docker run -it --rm --name redis -p 6379:6379 \
		-e ALLOW_EMPTY_PASSWORD=yes \
		bitnami/redis:7.0.8

.PHONY: sentinel
sentinel:
	docker run -it --rm --name redis-sentinel -p 26379:26379 \
		-e ALLOW_EMPTY_PASSWORD=yes \
		-e REDIS_MASTER_HOST=localhost \
		bitnami/redis-sentinel:7.0.8

.PHONY: sentinel2
sentinel2:
	docker run -it --rm --name redis-sentinel-2 -p 26380:26379 \
		-e ALLOW_EMPTY_PASSWORD=yes \
		-e REDIS_MASTER_HOST=localhost \
		bitnami/redis-sentinel:7.0.8