# syntax=docker/dockerfile:1

# ---- build ----
FROM rust:1.97-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
# Cache mounts keep the crate registry and build artefacts between builds.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked \
 && cp target/release/aero-ingest /usr/local/bin/aero-ingest

# ---- runtime ----
FROM debian:bookworm-slim
RUN groupadd --system --gid 10001 aero \
 && useradd --system --uid 10001 --gid 10001 --no-create-home aero
# TLS roots come from the build image, so the runtime stage needs no apt.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /usr/local/bin/aero-ingest /usr/local/bin/aero-ingest

USER aero
# Listen on all interfaces inside the container; publish the port to decide
# who can reach it from outside.
ENV AERO_HTTP_ADDR=0.0.0.0:9090 \
    RUST_LOG=aero_ingest=info
EXPOSE 9090
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["aero-ingest", "healthcheck"]
ENTRYPOINT ["aero-ingest"]
