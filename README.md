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
- Direct access from untrusted clients. The API has no authentication or TLS.

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
| `-dir` | `HL_DIR` |
| `-bucket` | `HL_BUCKET` |
| `-prefix` | `HL_PREFIX` |
| `-data-bucket` | `HL_DATA_BUCKET` |
| `-peers` | `HL_PEERS` |
| `-compress` | `HL_COMPRESS` |
| `-delta-checkpoints` | `HL_DELTA_CHECKPOINTS` |
| `-interior-reads` | `HL_INTERIOR_READS` |

Boolean settings use Go's standard parser.
Invalid environment values stop startup.

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
