---
title: Install
description: Install the cello binary or run it from Docker, and check the requirements.
---

## Binary

```sh
curl -fsSL https://cello.ryuz.eu/install.sh | sh
```

The installer downloads the `cello` binary for Linux amd64 or arm64 from [GitHub Releases](https://github.com/ryuzcorp/cello/releases). It installs the latest stable release. Set `CELLO_INSTALL_DIR` to choose the directory; otherwise the binary goes to `/usr/local/bin`, or to `~/.local/bin` when that directory is not writable. Put the directory on your `PATH` if the installer asks you to.

To install a different release, set `CELLO_VERSION` (for example `0.2.1` or `v0.2.1`) or `CELLO_CHANNEL=beta` for prereleases. Verify a release tarball with `gh attestation verify cello-<version>-linux-<arch>.tar.gz --repo ryuzcorp/cello`.

Check the install:

```sh
cello --version
```

## Docker

The release image is published for Linux amd64 and arm64 at `ghcr.io/ryuzcorp/cello`. The `latest` tag moves on stable releases only; `beta` also moves on prereleases. Pin by digest for production.

```sh
docker run --rm ghcr.io/ryuzcorp/cello:latest --version
```

## Requirements

- **Linux.** Cello nodes run on Linux (the release image is bookworm-based, so glibc ≥ 2.36).
- **esbuild on `PATH`** for any project with Worker code. It must be the self-contained release binary, not a wrapper script: on Linux, `cello deploy` runs it with reads confined to the project. If your `esbuild` is a shim (for example from mise), point `CELLO_ESBUILD` at the real binary instead. Asset-only projects need no bundler.
- **A bucket** for every fleet except local `cello dev`: S3-compatible, Google Cloud Storage, or Azure Blob Storage. See [Configure object storage](guides/run-fleet.md#configure-object-storage).

## Next step

Run your first Worker and Durable Object in [Quickstart](quickstart.md). It takes about 15 minutes and ends with a two-node fleet that shares one bucket.
