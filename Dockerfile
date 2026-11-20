# syntax=docker/dockerfile:1.7
# Modified by Ryuz Corp (Cello): the build stage bakes CELLO_VERSION into the binary for --version.
# Tests and clippy run natively in CI (ci.yml, `--profile ci`); the image only builds the
# release binary. publish.yml keeps the "what ships was tested" guarantee by refusing a
# tag whose commit lacks a passing `ci-ok` check.

ARG RUST_VERSION=1.98.1
ARG CELLO_COMMIT=unknown

FROM rust:${RUST_VERSION}-bookworm AS build
ARG TARGETARCH
# `release` for shipped artifacts; a fast-loop caller passes `lab` to skip
# the fat-LTO relink and keep incremental state in the target cache.
ARG CELLO_PROFILE=release
# The full build version the binary's --version reports
# (`<version>`, `<version>-cello.ci` in CI). Unset here,
# build.rs falls back to the crate version.
ARG CELLO_VERSION
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=cello-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=cello-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=cello-target-${TARGETARCH},target=/src/target,sharing=locked \
    mkdir -p /out && \
    CELLO_VERSION="${CELLO_VERSION}" cargo build --profile "${CELLO_PROFILE}" --locked -p cello && \
    install -m 755 "target/${CELLO_PROFILE}/cello" /out/cello

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
ARG CELLO_COMMIT
ARG CELLO_VERSION=unknown
LABEL org.opencontainers.image.title="cello" \
      org.opencontainers.image.revision="${CELLO_COMMIT}" \
      org.opencontainers.image.version="${CELLO_VERSION}"
COPY LICENSE NOTICE /usr/share/doc/cello/
COPY --from=build /out/cello /usr/local/bin/cello
ENTRYPOINT ["/usr/local/bin/cello"]
