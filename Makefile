export REDIS_PASSWORD=r6uq43vNsq
export REDIS_ADDRESS=localhost:6379
export REDIS_SENTINEL_ADDRESSES=localhost:26379

export ID=fc5e1420-cbec-11ed-afa1-0242ac120002
export NAME=hub
export NAMESPACE=rhiaqey
export RUST_BACKTRACE=full
export RUST_LOG=rhiaqey=trace
export PRIVATE_PORT=3001
export PUBLIC_PORT=3002

define PUBLIC_KEY
-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw8mFnJJrfA+r0eVz4kAT
YYEXaG1mTvgR5COuijDOWOTwm0Hd5ppkzSduCo2ci5h470FmNyOUNB8CkzDxrhCi
9HvZhjykewDhbgR0EKjfk6HbnZDfOCupeGx1YdgYXriXkVqGtcD0+1DV94P8PdBO
cbNn4cfgLLos8zOlMEKcPsO7wwdlRiW9J+60IybFloQeJlWOaD/oT+j/EB0oiKBg
LcoEkKiMTqkRfiW/tFKzLjIqEmID619jJbPUUEFHY1pp/3otilhTqgxDG1r59efP
24eIpYwxZO1GE/Mjbe6ANHpAPYMH1quXDjb0Y5SERHfWcjq30jAfolaGIlrUSw6d
3wIDAQAB
-----END PUBLIC KEY-----
endef

export PUBLIC_KEY

define PRIVATE_KEY
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDDyYWckmt8D6vR
5XPiQBNhgRdobWZO+BHkI66KMM5Y5PCbQd3mmmTNJ24KjZyLmHjvQWY3I5Q0HwKT
MPGuEKL0e9mGPKR7AOFuBHQQqN+TodudkN84K6l4bHVh2BheuJeRWoa1wPT7UNX3
g/w90E5xs2fhx+AsuizzM6UwQpw+w7vDB2VGJb0n7rQjJsWWhB4mVY5oP+hP6P8Q
HSiIoGAtygSQqIxOqRF+Jb+0UrMuMioSYgPrX2Mls9RQQUdjWmn/ei2KWFOqDEMb
Wvn158/bh4iljDFk7UYT8yNt7oA0ekA9gwfWq5cONvRjlIREd9ZyOrfSMB+iVoYi
WtRLDp3fAgMBAAECggEAfPnkih+E8Ppn6WIYaPIR7QmkUYqT5hDACusj/R5Oebwa
QmD3Lr6bXcGvopjmts0rVT5f6w6RCfxJfn+dpkkEXB+6qM+JBuN3Au1g0Uma/fgx
4hCaDJcCZNaGz2BLnhsi1Sv+FYMIXmwpSQg9OZAAot+sjhkyZhqpmsz6wyWh6wWT
zefxOWOyw9LHHCO11urOYKs4qGMffMkJTZ9ZBAfXKDqxFyV6bbnO+Pu/QYEMlgAF
kyTJMVCrbrf5uJUaXsW32C0vfM98yzOzU5YDgEy71rb2Uf7lnCVj1Ofk3ZE4izvm
BHMIXeozMssZMgwfnK2M846JVVvVep/d/j8nZoEBkQKBgQDOVj2J2kcCyvXJyjK9
f8B+gw66xq+cW9Zlx1ZRmtUpjEGoSkOmL7naGrAMFtITiDW5ZBHIiTcnX7XQJMrE
U0TS9ks04QV6Tl0vW+Yr3V3IeZtOpsb8Nl+9boIAdPpx2yBuGV706tcQSGuqghVd
7P8u9eTYua8CW4zig4YGJ8uiFQKBgQDy6T81YYjoga3vPMY/BnhlTGVBr2tcUTtH
uHbnW3LamCu9snvyf8qOhja08Q4g7uOidh9lm1mjHBie8FghGpWws43fmKX6TQft
jgcQnLFdGDQJq5y2OM2LB/EeoYfdK6kYsi22j6GnrUQAHDwuTeS2mysRF5Eqppjs
vBAXpXfhIwKBgQCE0YqnU/Rl3dO9YwSqarO0PBSdMgwUsCEgPuJXgT05k2koNTW6
ofoWZRtxjLcJj6JVhg7UcU8pbziPlT9YhOlGivf6P+bQxeTB+Xv+PG6D/5NzW3O3
IiEaxSm1tZcI9y628GnpacmqV5PGnBm47jeNOQdoYo4/DENyA4ugJrmzyQKBgQDD
W2kVYlq8O0cKh8McfvSm61joCc97UG0vkiA2kyp8uTM8feYHMlVSaIho3xEw1U9H
ol4/1j+x2W/Hq54FCZ9nnBA2ykp6UidVGwt9hbdzGnsHZ/hB6M8NyJZXvytIacu1
696t2zf0ZXmx6QNRbh3J6mMpfN2oApIsmlcK3W3bJwKBgECmpSb07IR1xD339Wdx
RUmyeXHNznAstW5qDIpwh4cjOjhjvkEsBPDpnpdu5P3lXQD7rae4c+vV/2rVCj1/
e0HojIfYZKE3WWfx9hMB1V7EF5lWLEfmSzY6ARit6MP27PsO7FSDbGXZ/kVBhWWn
C3mxja/ej85UOxy8VhmWr7WK
-----END PRIVATE KEY-----
endef

export PRIVATE_KEY

.PHONY: hub
hub: run

.PHONY: run
run:
	cargo run --bin hub

.PHONY: ops
ops:
	cargo run --bin \
		ops -- generate-keys --write .

.PHONY: hub1
hub1: run

.PHONY: hub2
hub2:
	ID=hub2 \
	PRIVATE_PORT=3010 \
	PUBLIC_PORT=3020 \
		cargo run --bin hub

.PHONY: run-prod
run-prod:
	cargo run --release --bin hub

.PHONY: dev
dev: build

.PHONY: build
build:
	cargo build
	ls -lah target/debug/hub

.PHONY: prod
prod:
	cargo build --release --bin hub
	cargo build --release --bin ops --features=cli
	ls -lah target/release

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
		bitnami/redis:7.0.12

.PHONY: sentinel
sentinel:
	docker run -it --rm --name redis-sentinel -p 26379:26379 \
		-e ALLOW_EMPTY_PASSWORD=yes \
		-e REDIS_MASTER_HOST=localhost \
		bitnami/redis-sentinel:7.0.12

.PHONY: sentinel2
sentinel2:
	docker run -it --rm --name redis-sentinel-2 -p 26380:26379 \
		-e ALLOW_EMPTY_PASSWORD=yes \
		-e REDIS_MASTER_HOST=localhost \
		bitnami/redis-sentinel:7.0.12

.PHONY: test
test:
	cargo test
