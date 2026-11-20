# Limitations

Cello v0.6.3 is a beta release with these operational limits. See
[Cloudflare compatibility](cloudflare-compat.md) for the supported
services, APIs, and Wrangler configuration.

## Run one application per fleet

- A fleet runs one application. Cello has no account service,
  multi-tenant scheduler, or managed ingress.
- A fleet stores its durable state in an S3-compatible bucket, a Google
  Cloud Storage bucket, or an Azure Blob Storage container. Only
  `cello dev` uses the local SQLite object store.
- Ownership balancing counts cells by node weight, not by CPU or memory
  use. It moves only hibernated cells, so a fleet without idle eviction
  balances only the cells that hibernate on their own.

## Terminate TLS outside Cello

- Cello does not terminate TLS. Terminate public TLS at an ingress proxy,
  and put the internal listener on a private network or an encrypted
  overlay.
- Peer traffic is plaintext HTTP. The fleet HMAC authenticates tunnel
  establishment and control requests but does not encrypt data, so your
  network must provide confidentiality.
- The fleet bucket controls the fleet. Give its credentials access to one
  fleet only. See [Security](../guides/security.md).

## Match credentials to your provider

- The credential methods differ by provider. See
  [Configure object storage](../guides/run-fleet.md#configure-object-storage).
- Azure identity works only in the public Azure cloud. A managed identity
  from Azure App Service or Azure Container Apps does not work. Use a
  workload identity or a storage account key there.

## Reconnect WebSockets after a move

- An outbound Durable Object WebSocket keeps its cell resident. It closes
  when the cell moves to another node, so store the connection intent and
  reconnect.
- A node limits the resident cells and outbound WebSockets.

## Run on Linux or Apple Silicon

The installer supplies binaries for Linux x86-64, Linux ARM64, and Apple
Silicon. Windows is not supported.
