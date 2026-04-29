FROM rust:1.85 AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY docs ./docs
COPY examples ./examples

RUN cargo build --release --bin stratum-server

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /app/target/release/stratum-server /usr/local/bin/stratum-server

ENV STRATUM_LISTEN=0.0.0.0:8080
ENV STRATUM_DATA_DIR=/data

EXPOSE 8080

CMD ["stratum-server"]
