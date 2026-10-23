# Pin reviewed image digests at the P12 release gate.
FROM rust:1-bookworm AS build
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY LICENSE ./
COPY scripts/release_artifacts.py ./scripts/release_artifacts.py
COPY src ./src
RUN cargo fetch --locked && cargo build --locked --release \
    && python3 scripts/release_artifacts.py

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 bridge \
    && useradd --uid 10001 --gid bridge --no-create-home --shell /usr/sbin/nologin bridge
COPY --from=build /build/target/release/armada-telegram-bridge /usr/local/bin/armada-telegram-bridge
COPY --from=build /build/release-artifacts /usr/share/doc/armada-telegram-bridge
USER 10001:10001
WORKDIR /state
EXPOSE 9090
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/armada-telegram-bridge"]
CMD ["--config", "/config/bridge.toml"]
