FROM rust:1.94.0-bookworm AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      cmake \
      pkg-config \
      perl && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release --locked --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --system --gid 10001 labaclaw && \
    useradd --system --uid 10001 --gid 10001 --create-home --home-dir /home/labaclaw labaclaw

COPY --from=builder /app/target/release/agent-factory /usr/local/bin/agent-factory
COPY --from=builder /app/target/release/agent-runner /usr/local/bin/agent-runner

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/agent-factory"]

