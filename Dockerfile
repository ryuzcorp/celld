# syntax=docker/dockerfile:1.7
# Modified by ryuzcorp (ryuzcorp/celld): the test stage's clippy loads the workspace clippy.toml; the test stage installs esbuild for the deploy --sourcemap tests.

ARG RUST_VERSION=1.98.1
ARG CELLD_COMMIT=unknown

FROM rust:${RUST_VERSION}-bookworm AS build
ARG TARGETARCH
# `release` for shipped artifacts; a fast-loop caller passes `lab` to skip
# the fat-LTO relink and keep incremental state in the target cache.
ARG CELLD_PROFILE=release
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,id=celld-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=celld-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=celld-target-${TARGETARCH},target=/src/target,sharing=locked \
    mkdir -p /out && \
    cargo build --profile "${CELLD_PROFILE}" --locked -p celld && \
    install -m 755 "target/${CELLD_PROFILE}/celld" /out/celld

# The final image depends on this stage, so a break in the engine's tests or
# lints stops the build.
FROM build AS test
ARG TARGETARCH
RUN rustup component add clippy
# The ltx fault-injection oracle diffs databases with the sqlite3 CLI.
RUN apt-get update && \
    apt-get install -y --no-install-recommends sqlite3 curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*
# The deploy `--sourcemap` tests bundle a fixture through esbuild, like a
# Worker deploy does. Pinned by version and by the npm tarballs' sha512
# (this stage builds the shipped binary, so nothing unverified runs here);
# test stage only, the shipped image has no esbuild, as before.
ARG ESBUILD_VERSION=0.28.2
ARG ESBUILD_SHA512_AMD64=e314d9af5154992a105b85c85a68addedcd0ad44d933e3773f45d5f3144e298179d177c8ef178ef74fb56d9bcdc3122e7d0f610d0551247e588603d517c03f1d
ARG ESBUILD_SHA512_ARM64=a56e000b43f78adf1cedda3d315338a79d45cc7cdd33f4d9adeaeeae045c1c9d964dad55435088ab5f319dc7e9049c38a23922659aca5b6c8805604ff768fa52
RUN case "${TARGETARCH}" in \
      amd64) ESBUILD_PKG=linux-x64 ESBUILD_SHA512="${ESBUILD_SHA512_AMD64}" ;; \
      arm64) ESBUILD_PKG=linux-arm64 ESBUILD_SHA512="${ESBUILD_SHA512_ARM64}" ;; \
      *) echo "unsupported arch ${TARGETARCH} for esbuild" >&2; exit 1 ;; \
    esac && \
    curl -fsSL -o /tmp/esbuild.tgz "https://registry.npmjs.org/@esbuild/${ESBUILD_PKG}/-/${ESBUILD_PKG}-${ESBUILD_VERSION}.tgz" && \
    echo "${ESBUILD_SHA512}  /tmp/esbuild.tgz" | sha512sum -c - && \
    tar -xzf /tmp/esbuild.tgz -C /tmp package/bin/esbuild && \
    install -m 755 /tmp/package/bin/esbuild /usr/local/bin/esbuild && \
    rm -rf /tmp/esbuild.tgz /tmp/package && \
    esbuild --version
# clippy reads the workspace's disallowed methods, types and macros from here.
COPY clippy.toml ./
RUN --mount=type=cache,id=celld-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=celld-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=celld-target-${TARGETARCH},target=/src/target,sharing=locked \
    cargo test --profile "${CELLD_PROFILE}" --locked && \
    cargo clippy --profile "${CELLD_PROFILE}" --all-targets --locked -- -D warnings

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
ARG CELLD_COMMIT
ARG CELLD_VERSION=unknown
LABEL org.opencontainers.image.title="celld" \
      org.opencontainers.image.revision="${CELLD_COMMIT}" \
      org.opencontainers.image.version="${CELLD_VERSION}"
COPY LICENSE NOTICE /usr/share/doc/celld/
COPY --from=test /out/celld /usr/local/bin/celld
ENTRYPOINT ["/usr/local/bin/celld"]
