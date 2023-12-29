FROM rust:1.75-slim-bullseye as builder
ARG BINARY
RUN apt-get update \
    && apt-get install -y \
      cmake \
      pkg-config \
      libssl-dev
WORKDIR /usr/src/
COPY . .
ENV RUST_BACKTRACE=1
RUN cargo install --bin ${BINARY} --path .

FROM debian:bullseye-slim
ARG BINARY
ENV BINARY=$BINARY
ENV DEBIAN_FRONTEND=noninteractive
LABEL org.opencontainers.image.description="Rhiaqey ${BINARY}"
RUN apt-get update \
    && apt-get install -y \
      ca-certificates \
      net-tools \
      libssl-dev \
    && rm -rf /var/lib/apt/lists/*
RUN update-ca-certificates
COPY --from=builder /usr/local/cargo/bin/${BINARY} /usr/local/bin/${BINARY}
ENV RUST_BACKTRACE=1
ENV RUST_LOG=trace
CMD [ "sh", "-c", "${BINARY}" ]
