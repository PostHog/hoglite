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
  -H 'Content-Type: application/json' -d '{"data_path":"s3://your-data-bucket/catalogs/demo"}' \
  http://127.0.0.1:9101/v1/tenants/demo
```

`data_path` is the S3 prefix the tenant's catalog owns, as an `s3://<bucket>[/<prefix>]` URI.
Hoglite stores it in the tenant manifest, which only hoglite writes, and sends it back in the `X-Hoglite-Data-Path` header of every tenant-scoped response (`/tx`, `/query`, `/query-batch`, `/status`, `/release`, and the create and delete themselves), from the owner's memory, through a proxying replica too.
Clients that act on S3 for a tenant check paths against that header, never against a value stored in the tenant's own SQL.
The value never changes: a create of an existing tenant with a different `data_path`, or with one when the tenant has none, is refused with 409 and `code` `data_path_mismatch`; delete the tenant and create it again to move it.
A create that omits `data_path` makes no claim and stays idempotent; so does an empty body or `{}`.
The body is empty or exactly one JSON object with nothing after it: a body that is not an object (`null` included), anything after the object (`{...} garbage`, two objects), a `class` that is not a string, or a key other than `class` and `data_path` (a misspelled `dataPath` or `data-path`, or a known key in another case such as `DATA_PATH`), is refused with 400. Keys match exactly and a repeated key takes its last value, as in hoglake's JSON reading (Jackson defaults), so the two sides read one body the same way.
A body that does not arrive whole (a reset or cut connection) is refused with 400 rather than read as empty; one over 64 MiB with 413.
A `data_path` that is present but not a non-empty string (`""`, `null`, a number) is refused with 400 and `code` `invalid_data_path`: before, such a body silently created a tenant without a `data_path`, which can never gain one.
A malformed value (not `s3://`, empty bucket, whitespace or control characters, a `.` or `..` segment, over 512 bytes) is refused with 400 and `code` `invalid_data_path`.
A value inside hoglite's own storage (`s3://<-bucket>/<-prefix>`, and `s3://<-data-bucket>/<-prefix>` when the data plane is separate), or above it, is refused with 400 and `code` `invalid_data_path`.
A value that overlaps the `data_path` of any other tenant on this backend (equal, above or below it, trailing slashes aside, the rule hoglake applies between its catalogs) is refused with 409 and `code` `data_path_overlap`; the message names the other tenant and its `data_path`.
That check reads every other tenant's manifest (hoglite keeps no index of `data_path`s): one listing (the 256 tenant shards, 16 at a time, well under a second on S3) plus one GET per tenant, 16 at a time, paid by a create that names a `data_path` for a tenant that does not exist yet, not by an idempotent recreate. Two concurrent creates of different tenants with overlapping paths can both pass it; serialize catalog creates in front of hoglite.
A tenant created without a `data_path` sends no header.
Only operator authority creates tenants, so only an operator sets a `data_path`.
Each `/query-batch` item repeats its tenant's value in a `data_path` field; the batch response carries the header when every item names the same tenant.

A tenant with a `data_path` is marked in its manifest's `format` (a flag above the generation's codec), and every hoglited build before this one refuses such a tenant (`tenant uses a newer storage format`, HTTP 500 on its routes) rather than serving it: those builds decode the manifest without the field and their next manifest write would erase it.
Roll every pod to a build with `data_path` before creating tenants with one, as for `-compress` and `-delta-checkpoints`.
A rollback to an older build leaves such tenants unserved until the roll forward, never rewritten; tenants without a `data_path` are unaffected either way.
Builds from before compression have no format gate at all and must not run in a pool whose tenants have a `data_path`.

The format flag protects a tenant that has a `data_path`; it cannot protect a create.
A hoglited from before `data_path` does not know the field: a create of a NEW tenant that lands on such a pod ignores `data_path`, creates the tenant without one, and answers 200 with no `X-Hoglite-Data-Path` header, and that tenant can never gain one (delete it and create it again).
In a mixed pool the outcome of a create therefore depends on which pod the Service picks, and a client that sends `data_path` cannot tell a rejected claim from a pod that never read it except by the header's absence on the create's answer.
So the rollout order is fixed: roll every hoglited to a build with `data_path` first, wait for the roll to finish, and only then deploy a hoglake that sends `data_path`.
A hoglake that sends `data_path` to a pool that is still rolling, or that is rolled back, should treat a create answer without the header as a failed create: do not proceed to bootstrap, leave the tenant for an operator, and retry once the pool is level.
Never delete the tenant on that signal alone: a hoglited from before `data_path` answers a create of an EXISTING tenant it owns the same way (200, no `existing`, no header), so a delete on that answer would destroy a live catalog.

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
