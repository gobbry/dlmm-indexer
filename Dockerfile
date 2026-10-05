# syntax=docker/dockerfile:1

FROM rust:1.98-slim-trixie AS build
WORKDIR /build
# rust-toolchain.toml is left out on purpose: the image pins the toolchain, and the file
# would make rustup download components inside the build.
COPY Cargo.toml Cargo.lock ./
COPY core core
COPY api api
COPY migrations migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --bin indexer --bin api \
    && mkdir -p /out \
    && cp target/release/indexer target/release/api /out/

FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home dlmm
COPY --from=build /out/indexer /out/api /usr/local/bin/
USER dlmm
EXPOSE 8080
CMD ["api"]
