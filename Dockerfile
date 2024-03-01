FROM --platform=$BUILDPLATFORM rust:1.76-slim-bookworm as builder

ARG TARGETPLATFORM

ENV RUST_BACKTRACE=1

RUN echo Building $TARGETPLATFORM on $BUILDPLATFORM

RUN    apt-get update \
    && apt-get install -y \
        gcc-aarch64-linux-gnu \
        libc6-dev-arm64-cross \
        build-essential \
        pkg-config \
        libssl-dev \
        cmake \
        gcc \
        libc-bin \
        libc6-dev \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

# Set the default target to ARM64
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
ENV CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc

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

FROM debian:bookworm-slim

ARG BINARY
ARG USER=1000
ARG GROUP=1000

ENV BINARY=$BINARY
ENV DEBIAN_FRONTEND=noninteractive
ENV RUST_BACKTRACE=1
ENV RUST_LOG=trace
ENV USER=$USER
ENV GROUP=$GROUP

LABEL org.opencontainers.image.description="Rhiaqey Hub ${BINARY}"

RUN    apt-get update \
    && apt-get install -y \
        ca-certificates \
        net-tools \
        libssl-dev \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && update-ca-certificates

# Create the specified group and user, and add the user to the group
RUN groupadd -g $GROUP $GROUP \
    && useradd -u $USER -ms /bin/bash -g $GROUP $USER

USER $USER

COPY --from=builder --chown=$USER:$GROUP /usr/local/cargo/bin/hub /usr/local/bin/hub
COPY --from=builder --chown=$USER:$GROUP /usr/local/cargo/bin/ops /usr/local/bin/ops

CMD [ "sh", "-c", "${BINARY}" ]
