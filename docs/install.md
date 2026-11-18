---
title: Install
description: Install the cello binary or run it from Docker, and check the requirements.
---

## Binary

```sh
curl -fsSL https://cello.ryuz.eu/install.sh | sh
```

The installer downloads the `cello` binary for Linux amd64 or arm64 from
[GitHub Releases](https://github.com/ryuzcorp/cello/releases). By default
it installs the latest stable release. To live on prereleases instead,
pass `--beta` or set `CELLO_CHANNEL=beta`: the installer then takes the
newest `-beta.N` / `-rc.N` tag (unrelated to the `beta` image tag, which
tracks the newest release of any kind). `CELLO_VERSION` (for example
`0.1.0` or `v0.1.0`) installs one exact release and overrides both
paths; to go back, run the installer again with the previous tag.

Install location: a set, non-empty `CELLO_INSTALL_DIR` wins outright.
Otherwise the binary goes to `/usr/local/bin` when that directory is
writable or the installer runs as root, else to `~/.local/bin` (created
as needed). Put the directory on your `PATH` if the installer asks you
to.

Every release carries build provenance. Verify a release tarball with
`gh attestation verify cello-<version>-linux-<arch>.tar.gz --repo
ryuzcorp/cello`, and the container image with
`gh attestation verify oci://ghcr.io/ryuzcorp/cello@<digest> --repo
ryuzcorp/cello`. The digest is printed in the GitHub Release body
(`Image: ghcr.io/ryuzcorp/cello:<version> (<digest>)`); on stable
releases one attestation covers the `<version>`, `<major>.<minor>`,
`beta`, and `latest` tags, which all point at the same index digest
(prereleases attest `<version>` plus `beta`).

## Docker

The release image is published for Linux amd64 and arm64 at
`ghcr.io/ryuzcorp/cello`, tagged `0.1.0`, `0.1`, `beta`, and `latest`.
`latest` moves only on stable releases; `beta` also moves on `-beta.N`
prereleases.

```sh
docker run --rm ghcr.io/ryuzcorp/cello:latest --version
```

Pin by digest for production, and verify the SLSA provenance
attestation as described in [Binary](#binary).

Persist the runtime's local state and pass the standard AWS credential
environment through:

```sh
docker volume create cello-state
docker run --rm --network host \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e CELLO_WATCH=/var/lib/cello/state \
  -v cello-state:/var/lib/cello \
  ghcr.io/ryuzcorp/cello:latest \
  --bucket s3://my-cells-bucket \
  --endpoint https://ACCOUNT.r2.cloudflarestorage.com \
  --region auto \
  --listen 0.0.0.0:8080 \
  --internal-listen 10.0.0.12:8081 \
  --advertise node-a.internal:8081
```

Drop `--endpoint` and `--region` for AWS S3. Expose port 8080 through the
load balancer, and keep port 8081 on the private network.

## Requirements

- **Linux.** Cello nodes run on Linux (the release image is
  bookworm-based, so glibc ≥ 2.36).
- **esbuild on `PATH`** for any project with Worker code. `cello deploy`
  needs the self-contained release binary, not a wrapper script; on
  Linux it runs under Landlock with reads confined to the project.
  Asset-only projects need no bundler.
- **A bucket** for every fleet except local `cello dev`: S3-compatible,
  Google Cloud Storage, or Azure Blob Storage. See
  [Operations](operations.md#configure-object-storage).

## First run

```sh
cello dev
```

That starts one node with a local object store — no Docker or cloud
bucket — and serves the Worker at `http://127.0.0.1:9876`. Then deploy a
Wrangler project and start a fleet, as described in
[Operations](operations.md#deploy-an-application).
