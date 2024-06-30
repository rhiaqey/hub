export REDIS_MODE=standalone
export REDIS_PASSWORD=welcome
export REDIS_ADDRESS=0.0.0.0:6379
export REDIS_SENTINEL_MASTER=mymaster
export REDIS_SENTINEL_ADDRESSES=localhost:26379,localhost:26380,localhost:26381
export REDIS_VERSION=7.2.5
export REDIS_INSIGHT_VERSION=2.48.0

export ID=fc5e1420-cbec-11ed-afa1-0242ac120002
export NAME=hub
export NAMESPACE=rhiaqey
export RUST_BACKTRACE=full
export RUST_LOG=rhiaqey=trace
export PRIVATE_PORT=3001
export PUBLIC_PORT=3002
export XXX_SKIP_SECURITY=true

.PHONY: hub
hub: run

.PHONY: run
run:
	cargo run -- run

.PHONY: run-release
run-release:
	RUST_LOG=rhiaqey_hub=info \
		cargo run --release -- run

.PHONY: settings
settings:
	 cargo run -- load-settings --file=./data/hub-settings.json --name hub
	 cargo run -- load-settings --file=./data/ws1-settings.json --name ws1
	 cargo run -- create-channels --file=./data/create-channels.json
	 cargo run -- assign-channels --file=./data/assign-channels.json

.PHONY: keys
keys:
	cargo run --release -- generate-keys --skip --write .

.PHONY: hub1
hub1: run

.PHONY: hub2
hub2:
	ID=hub2 \
	PRIVATE_PORT=3010 \
	PUBLIC_PORT=3020 \
		cargo run -- run

.PHONY: dev
dev: build

.PHONY: test
test:
	cargo test --all-features

.PHONY: build
build:
	cargo build
	ls -lah target/debug/rhiaqey-hub

.PHONY: prod
prod:
	cargo build --release
	ls -lah target/release/rhiaqey-hub

.PHONY: docker-build
docker-build:
	docker build . \
 		--build-arg BINARY=rhiaqey-hub \
 		-t rhiaqey/hub:dev \
 		-f Dockerfile \
 		--squash

.PHONY: docker-push
docker-push:
	docker push rhiaqey/hub:dev

.PHONY: docker-run
docker-run:
	docker run -it --rm --init \
		-e RUST_BACKTRACE=1 \
		-e RUST_LOG=1 \
		-e BINARY=rhiaqey-hub \
		-e XXX_SKIP_SECURITY=${XXX_SKIP_SECURITY} \
		-e REDIS_PASSWORD=${REDIS_PASSWORD} \
		--network host \
		--name hub \
		--entrypoint rhiaqey-hub \
		rhiaqey/hub:dev run

.PHONY: redis
redis:
	docker run -it --rm --name redis -p 6379:6379 \
		-e ALLOW_EMPTY_PASSWORD=no \
		-e REDIS_PASSWORD=${REDIS_PASSWORD} \
		--network host \
		rhiaqey/redis:${REDIS_VERSION}

.PHONY: redisinsight
redisinsight:
	docker run -it --rm --name redisinsight -p 5540:5540 \
		--network host \
		redis/redisinsight:${REDIS_INSIGHT_VERSION}

.PHONY: docker
docker: docker-build docker-push

.PHONY: docker-multi
docker-multi:
	docker buildx build \
		--platform linux/arm64/v8 \
		-t rhiaqey/hub:dev \
		-o type=image \
		.
