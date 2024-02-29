FROM --platform=$BUILDPLATFORM rust:1.76-slim-bullseye as builder

ARG TARGETPLATFORM

ENV RUST_BACKTRACE=1

RUN echo Building $TARGETPLATFORM on $BUILDPLATFORM

RUN apt-get update \
    && apt-get install -y \
      gcc-aarch64-linux-gnu \
      build-essential \
      pkg-config \
      libssl-dev \
      cmake \
      gcc

WORKDIR /usr/src/

COPY . .

RUN case "${TARGETPLATFORM}" in \
      "linux/amd64") rust_target="x86_64-unknown-linux-gnu" ;; \
      "linux/arm64") rust_target="aarch64-unknown-linux-gnu" ;; \
      *) echo "Unsupported platform: ${TARGETPLATFORM}" ; exit 1 ;; \
    esac \
    && rustup target add ${rust_target} \
    && cargo install --target ${rust_target} --bin hub --path . \
    && cargo install --target ${rust_target} --bin ops --features=cli --path .
