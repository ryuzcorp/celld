---
title: Configure storage
description: Point your fleet at an S3, Google Cloud Storage, or Azure bucket that meets the storage contract.
---

You point every node at one bucket with `CELLO_BUCKET`. The nodes find each other through leases in that bucket, so there is no join command. Keep the bucket credentials safe: they give full control of the fleet.

## Pick a bucket

The scheme names are case-insensitive, and Cello ignores leading and trailing whitespace in `CELLO_BUCKET`. A value can add a key prefix, `s3://YOUR-BUCKET/PREFIX`, so two fleets can share one bucket. Without a prefix, objects stay at the bucket root.

For an S3-compatible bucket, Cello uses the standard AWS credential chain, including an EC2 instance role and EKS Pod Identity. For Cloudflare R2, create a bucket and an S3 API token with access to it, then set:

```sh
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=auto
export S3_ENDPOINT=https://ACCOUNT_ID.r2.cloudflarestorage.com
export CELLO_BUCKET=s3://YOUR-BUCKET
```

For Google Cloud Storage, Cello uses Application Default Credentials. Run `gcloud auth application-default login`, or point `GOOGLE_APPLICATION_CREDENTIALS` at a service-account key. Then set `CELLO_BUCKET=gs://YOUR-BUCKET`. On Compute Engine, the default instance scope permits only storage reads, so create the instance with the `cloud-platform` scope.

For Azure Blob Storage, the bucket name is the container:

```sh
export AZURE_STORAGE_ACCOUNT_NAME=YOUR-ACCOUNT
export AZURE_STORAGE_ACCOUNT_KEY=...
export CELLO_BUCKET=az://YOUR-CONTAINER
```

You must configure exactly one Azure credential family: an account key, a managed identity, or a workload identity. A system-assigned managed identity needs only the account name. A user-assigned identity needs exactly one of `AZURE_CLIENT_ID`, `AZURE_OBJECT_ID`, and `AZURE_MSI_RESOURCE_ID`. An identity needs the `Storage Blob Data Contributor` role or equal permissions. Managed identity works on an Azure VM and an AKS node, but not on App Service or Container Apps; use a workload identity or an account key there. The AKS workload identity environment (`AZURE_AUTHORITY_HOST`, `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_FEDERAL_TOKEN_FILE`) must use the public authority host, `https://login.microsoftonline.com`. Cello rejects every other recognized Azure configuration variable, such as another credential source or an endpoint override. For local development against Azurite, set `AZURE_STORAGE_USE_EMULATOR=true`. Cello does not qualify Azurite for a fleet.

A `gs://` or `az://` bucket takes no `S3_ENDPOINT` and no `AWS_*` credentials, and Cello ignores the storage region.

## Check the storage contract

Your store must provide conditional writes, exact ranged reads, and read-after-write consistency. Amazon S3, Cloudflare R2, Google Cloud Storage, Tigris, and Azure Blob Storage qualify; Backblaze B2, Hetzner, and Digital Object Storage do not. MinIO (community edition) passes the storage test but is not qualified for production; do not use `RELEASE.2025-09-06T17-38-46Z`, which rejects the first deploy.

A node checks the contract at startup. A violation prevents startup; an ambiguous transport error produces a warning. See [what Cello guarantees](../concepts/durability.md) for the full contract and the reserved prefixes your lifecycle rules must avoid.
