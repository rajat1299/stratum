FROM rust:1.85 AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY docs ./docs
COPY examples ./examples

RUN cargo build --release --bin lattice-server

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /app/target/release/lattice-server /usr/local/bin/lattice-server

ENV LATTICE_LISTEN=0.0.0.0:8080
ENV LATTICE_DATA_DIR=/data

EXPOSE 8080

CMD ["lattice-server"]
