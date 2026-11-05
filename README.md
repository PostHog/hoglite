# 🐗 hoglite

hoglite serves independent SQLite databases over HTTP with durable storage in S3.

## Good at

- Many small, independent databases.
- Short transactions with one writer per tenant.
- Recovering tenants on another replica after a failure.
- Reducing storage traffic through group commits, compression, and delta checkpoints.

## Not good at

- Parallel writes within one tenant.
- SQL joins across tenants.
- Large analytical queries.
- Direct access from untrusted clients. The API has no authentication or TLS.

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
