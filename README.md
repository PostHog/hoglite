# 🐗 hoglite

hoglite is a multi-tenant SQLite service with an HTTP API and durable storage in S3.

## Good at

- Multi-tenant workloads with many small databases.
- Short transactions with one writer per tenant.
- Recovering tenants on another replica after a failure.
- Reducing storage traffic through group commits, compression, and delta checkpoints.

## Not good at

- Parallel writes within one tenant.
- SQL joins across tenants.
- Large analytical queries.
- Direct access from untrusted clients. The API is internal raw SQL, without built-in TLS.

## Scale

- **Per replica:** `-max-open` defaults to 5,000 open tenants.
  This is a soft eviction target, not a tested capacity guarantee.
- **Across replicas:** 100 replicas at that target provide 500,000 nominal open-tenant slots.
  More tenants can remain stored in S3 without staying open.
- **Per tenant:** One tenant uses one replica's local disk.
  Its data can exceed RAM because recovery streams snapshots.
  A 10 GiB database needs more than 10 GiB of disk for WAL and checkpoint headroom.
- **Larger replicas:** More CPU, RAM, and disk can support more active tenants or larger databases.
  One tenant's writes still execute serially.

These figures describe configuration and sizing, not benchmarked limits.
Actual capacity depends on workload, database sizes, local storage, and S3 throughput.
The project does not publish a verified maximum tenant size or fleet size.

## Quick start

Download a binary from [Releases](https://github.com/PostHog/hoglite/releases).
Rename it to `hoglited`.
Configure AWS credentials for an S3 bucket that supports conditional writes.

```sh
chmod +x hoglited
AWS_REGION=us-east-1 ./hoglited \
  -id replica-1 \
  -addr 127.0.0.1:9101 \
  -dir ./data \
  -bucket your-bucket \
  -prefix hoglite-demo
```

Use a fresh prefix for each deployment.
The local directory is a cache, not a durable backup.

### Configuration

Run `./hoglited -help` for all options.
Flags override their environment defaults.

| Flag | Environment |
| --- | --- |
| `-id` | `HL_ID` |
| `-addr` | `HL_ADDR` |
| `-metrics-port` | `HL_METRICS_PORT` |
| `-dir` | `HL_DIR` |
| `-bucket` | `HL_BUCKET` |
| `-prefix` | `HL_PREFIX` |
| `-data-bucket` | `HL_DATA_BUCKET` |
| `-peers` | `HL_PEERS` |
| `-compress` | `HL_COMPRESS` |
| `-delta-checkpoints` | `HL_DELTA_CHECKPOINTS` |
| `-interior-reads` | `HL_INTERIOR_READS` |
| `-auth-mode` | `HL_AUTH_MODE` |
| `-auth-issuer` | `HL_AUTH_ISSUER` |
| `-auth-backend-id` | `HL_AUTH_BACKEND_ID` |
| `-auth-jwks-file` | `HL_AUTH_JWKS_FILE` |
| `-auth-jwks-url` | `HL_AUTH_JWKS_URL` |

Boolean settings use Go's standard parser.
Invalid environment values stop startup.

### Offline JWT authentication

Authentication defaults to `off` for existing local development.
Set `-auth-mode=enforce`, the issuer URL, this backend's UUID, and exactly one public JWKS source.
Missing configuration, private keys, malformed keys, or an unavailable initial source fail startup.
No signing keys belong in hoglite.

Hoglite accepts ES256 and RS256, with a required `kid`, issuer, expiry, subject, and token identifier.
Metadata tokens carry the explicit audience pair `["hoglake","hoglite"]`, never `hogtower-vend`.
Their `rtg.b` must be `hoglite`.
Their `rtg.be` must match the configured backend UUID.
Their opaque `rtg.t` must equal every requested tenant identifier.
Catalog names and organization identifiers never become tenant identifiers.

Tenant tokens require one catalog grant containing `read`, `write`, `maintenance`, or `admin`.
Unknown grant words are ignored only when a recognized permission remains.

Reads use SQLite's existing `query_only` connections, including read-only transactions.
Predicate grants, scoped tokens, and unsupported scope constraints are refused on reads and writes.
Provisioning, deletion, ownership control, status, metrics, and fleet operations require operator authority.
Catalog-bound operators cannot administer another tenant or the fleet.
Health remains unauthenticated.

Verified unsupported grants receive 403.
Malformed or expired credentials receive 401.
Clock checks use Hoglake's sixty-second tolerance.

The configured file or URL supplies public keys, not token introspection.
Keys refresh asynchronously after one minute on use, with globally bounded unknown-key refreshes every thirty seconds.
Unknown keys fail closed during refresh.
Cached verification never waits for network access.
Failed refreshes preserve cached keys.
Expiry checks still apply.

Publish overlapping keys until previously issued tokens expire.
Before enabling enforcement, update issuance and drain legacy single-audience tokens for their remaining fifteen-minute lifetime.

For custom metadata audiences, set `HL_AUTH_UPSTREAM_AUDIENCE` or `-auth-upstream-audience`.
Its default is `hoglake`.
The token must name exactly that audience and `hoglite`.

Fleet operators use explicit metadata-pair tokens without catalog routing.
Scrapers on the API listener need operator authority when enforcement is enabled.
Optionally set `HL_METRICS_PORT` or `-metrics-port` to expose only `GET /metrics` on a separate listener.
It uses the API listener's bind host.
It defaults to disabled (`0`).

Restrict that port to monitoring workloads through network policy.
It exposes no health, SQL, tenant administration, or shutdown routes.
The API listener's `/metrics` route still requires operator authority.
Unauthenticated shutdown hooks do not work under enforcement.
Use SIGTERM for graceful shutdown.

Hoglake forwards its request's verified JWT through the real adapter and peer routing.
Keep this service on its existing trusted network.
JWT verification does not make raw SQL a public customer API or provide TLS.

## HTTP API

Create a tenant:

```sh
curl --fail-with-body -X PUT \
  -H 'Content-Type: application/json' -d '{}' \
  http://127.0.0.1:9101/v1/tenants/demo
```

Submit a transaction:

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"client":"example","txid":"1","statements":[{"sql":"CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT)"},{"sql":"INSERT INTO notes VALUES (?, ?)","args":[1,"hello"]}]}' \
  http://127.0.0.1:9101/v1/t/demo/tx
```

Query the tenant:

```sh
curl --fail-with-body -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM notes"}' \
  http://127.0.0.1:9101/v1/t/demo/query
```

### Errors and shutdown

Inspect the response's `code` field when a request fails.
Retry uncertain writes with the same `client`, `txid`, and payload.
Send SIGINT or SIGTERM for graceful shutdown.

## Development

Use Go 1.27.3 or newer.

```sh
CGO_ENABLED=0 go build -trimpath -o hoglited ./cmd/hoglited
go test -race -tags slow -timeout 720s ./...
```

## License

[MIT](LICENSE).
