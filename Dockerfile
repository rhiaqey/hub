FROM --platform=$BUILDPLATFORM rust:1.76-slim-bullseye as builder

ARG TARGETPLATFORM

ENV RUST_BACKTRACE=1

RUN apt-get update \
    && apt-get install -y \
      cmake \
      pkg-config \
      libssl-dev

WORKDIR /usr/src/

COPY . .

RUN case "${TARGETPLATFORM}" in \
      "linux/amd64") rust_target="x86_64-unknown-linux-gnu" ;; \
      "linux/arm64") rust_target="aarch64-unknown-linux-gnu" ;; \
      *) echo "Unsupported platform: ${TARGETPLATFORM}" ; exit 1 ;; \
    esac \
    && rustup target add ${rust_target}

RUN case "${TARGETPLATFORM}" in \
      "linux/amd64") rust_target="x86_64-unknown-linux-gnu" ;; \
      "linux/arm64") rust_target="aarch64-unknown-linux-gnu" ;; \
      *) echo "Unsupported platform: ${TARGETPLATFORM}" ; exit 1 ;; \
    esac \
    && cargo install --target ${rust_target} --bin hub --path . \
    && cargo install --target ${rust_target} --bin ops --features=cli --path .

FROM debian:bullseye-slim

ARG BINARY
ARG USER=1001

ENV BINARY=$BINARY
ENV DEBIAN_FRONTEND=noninteractive
ENV RUST_BACKTRACE=1
ENV RUST_LOG=trace
ENV USER=$USER

LABEL org.opencontainers.image.description="Rhiaqey Hub ${BINARY}"

RUN apt-get update \
    && apt-get install -y \
      ca-certificates \
      net-tools \
      libssl-dev \
      curl \
    && rm -rf /var/lib/apt/lists/*
RUN update-ca-certificates
RUN useradd -ms /bin/bash $USER

COPY --from=builder --chown=$USER:$USER /usr/local/cargo/bin/hub /usr/local/bin/hub
COPY --from=builder --chown=$USER:$USER /usr/local/cargo/bin/ops /usr/local/bin/ops

USER $USER

CMD [ "sh", "-c", "${BINARY}" ]
