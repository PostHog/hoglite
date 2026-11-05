# hoglite

hoglite serves per-tenant SQLite databases through an HTTP API.
It persists SQLite write-ahead log (WAL) segments and checkpoints in S3.
The core daemon needs no PostHog infrastructure.

## How it works

- Each tenant has one writer and a local SQLite cache.
- Replica leases and conditional manifest updates coordinate ownership and fence stale writers.
- Group commits upload WAL segments before successful write acknowledgements.
- Read snapshots normally wait for their observed writes to reach durability.
- Checkpoints fold WAL into database snapshots.
- Zstandard compression reduces checkpoint and WAL storage.
- Delta checkpoints upload changed pages and periodically rebase into full snapshots.
- Recovery rebuilds local state from a base, ordered deltas, and WAL segments.
- A sweeper removes orphaned objects.

The local directory holds cache files, not an independent durable backup.
S3 availability, permissions, and correct conditional-write behavior matter for durability and recovery.
Keep clocks synchronized when running multiple replicas.

## Build from source

Install Go 1.27.3 or newer.
The daemon uses pure-Go SQLite and needs no C compiler.

```sh
mkdir -p bin
CGO_ENABLED=0 go build -trimpath -o ./bin/hoglited ./cmd/hoglited
./bin/hoglited -version
./bin/hoglited -help
```

Use `-version` alone, without other flags or arguments.
`cmd/hoglited` builds the database daemon.
`cmd/hoglake-reimpl` builds an optional hoglake REST subset adapter.
The adapter maps each catalog to one tenant.
It implements selected catalog, namespace, table, commit, snapshot, and consumer-offset routes.
It does not promise full hoglake compatibility.
The public hoglake and duckgres projects provide conceptual context.

For a local, single-replica adapter, override both upstream URLs:

```sh
CGO_ENABLED=0 go build -trimpath -o ./bin/hoglake-reimpl ./cmd/hoglake-reimpl
./bin/hoglake-reimpl \
  -addr 127.0.0.1:8080 \
  -hoglite http://127.0.0.1:9101 \
  -hoglite-whale http://127.0.0.1:9101
```

## Binary release workflow

The binary workflow publishes prereleases after CI passes for the current main tip.
It skips superseded revisions.
A completed draft may remain if main advances during uploads.
Each release tag follows `build-<full 40-hex commit SHA>`.
The workflow does not publish a mutable `latest` tag.
Native runners build and smoke-test these four targets:

| Platform | Raw executable |
| --- | --- |
| Linux amd64 | `hoglited-linux-amd64` |
| Linux arm64 | `hoglited-linux-arm64` |
| macOS amd64 | `hoglited-darwin-amd64` |
| macOS arm64 | `hoglited-darwin-arm64` |

macOS binaries require macOS 13 or later.
The workflow does not sign or notarize executables.

Each release also carries `checksums.txt`, `LICENSE`, and `THIRD_PARTY_NOTICES`.
The initial workflow excludes Windows.
Cross-compilation alone does not certify runtime support.
The existing ECR workflow remains separate.

The first successful workflow run must create assets before anyone can download them.
Public downloads require the repository owner to enable public visibility separately.
Choose an existing prerelease tag and replace the placeholder below.
Run these commands in an empty download directory.
They require `curl`, `shasum`, and standard Unix tools.

```sh
tag='build-<full-40-hex-commit-sha>'
os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  arm64|aarch64) arch=arm64 ;;
  *) echo 'Unsupported architecture' >&2; exit 1 ;;
esac
case "$os" in linux|darwin) ;; *) exit 1 ;; esac
asset="hoglited-${os}-${arch}"
base="https://github.com/PostHog/hoglite/releases/download/${tag}"
for file in "$asset" checksums.txt LICENSE THIRD_PARTY_NOTICES; do
  curl --fail --location --output "$file" "${base}/${file}" || exit 1
done
awk -v asset="$asset" \
  '$2 == asset || $2 == "LICENSE" || $2 == "THIRD_PARTY_NOTICES"' \
  checksums.txt > selected-checksums.txt
test "$(wc -l < selected-checksums.txt)" -eq 3 || exit 1
shasum -a 256 -c selected-checksums.txt || exit 1
chmod 0755 "$asset"
mkdir -p "$HOME/.local/bin" "$HOME/.local/share/hoglite"
install -m 0755 "$asset" "$HOME/.local/bin/hoglited"
install -m 0644 LICENSE THIRD_PARTY_NOTICES "$HOME/.local/share/hoglite/"
"$HOME/.local/bin/hoglited" -version
```

Checksums detect corruption; they do not provide an independent signature.
Keep both notice files with redistributed binaries.

## Run one replica

Provide these resources:

- AWS credentials and an AWS region.
- An S3 bucket that supports conditional control-plane writes.
- A fresh, dedicated object prefix.
- A writable local directory with room for database caches and recovery.

The daemon uses the AWS SDK's standard credential providers.
These include environment credentials, shared profiles, and workload or instance roles.
Prefer short-lived credentials.
Do not place credentials in source files or command arguments.

Scope S3 permissions to the selected buckets and prefix.
Grant `s3:GetObject`, `s3:PutObject`, and `s3:DeleteObject` on the deployment's objects.
Grant `s3:ListBucket` with a prefix restriction.
Allow conditional writes for manifests and coordination objects.
Grant encryption-key permissions when your bucket configuration requires them.
S3 Express deployments may also require `s3express:CreateSession`.

Replace the bucket, region, and directory placeholders.
The example sets a 512 MiB Go budget and a 256 MiB SQLite budget.
Size both budgets for your workload and leave room for other process memory.

```sh
export AWS_REGION='<aws-region>'
export GOMEMLIMIT=512MiB
./bin/hoglited \
  -id demo-replica-1 \
  -addr 127.0.0.1:9101 \
  -dir '<writable-cache-directory>' \
  -bucket '<s3-control-bucket>' \
  -prefix hoglite-demo-fresh \
  -sqlite-heap-limit 268435456
```

Use the installed executable path instead of `./bin/hoglited` when downloading a binary.
Keep the prefix exclusive to this deployment.
Start with one replica and an explicit replica ID.
Assign distinct IDs and separate local directories before adding replicas.
Configure `-peers` with reachable replica URLs when routing across replicas.

The daemon uses one bucket by default.
Optionally set `-data-bucket` to a separate data-plane bucket, including S3 Express.
Keep manifests and coordination objects in the conditional-write-capable control bucket.

### Network boundary

**The daemon provides no authentication or TLS.**
It defaults to listening on all interfaces.
Use the explicit loopback address above or an authenticated external proxy with TLS.
Protect all routes, including SQL, metrics, tenant deletion, and shutdown.
Tenant IDs do not authenticate callers or authorize access.
Keep `HL_DEBUG_ENDPOINTS` unset.
The optional adapter also needs a protected network boundary.

### Configuration and memory

Run `hoglited -help` for the complete flag list.
Flags override their corresponding environment defaults.
These mappings cover the main storage and network settings:

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

Memory flags have no corresponding `HL_*` variables.
Under a finite cgroup limit, defaults allocate 40% to SQLite's hard heap limit.
The Go soft limit receives 90% of the cgroup limit minus that SQLite budget.
An explicit `GOMEMLIMIT` overrides the automatic Go budget.
Outside cgroups, the daemon sets neither automatic heap limit.
Set `GOMEMLIMIT` and `-sqlite-heap-limit` explicitly there, especially on macOS.
Go's soft limit does not cap the SQLite heap.
Neither heap budget guarantees a process-wide resident-memory ceiling.
Tune `-max-open`, `-max-queries`, `-max-readers`, and `-sqlite-cache-kib` for your workload.

## HTTP API example

These examples use the synthetic tenant `demo-tenant`.
Create it before submitting SQL:

```sh
curl --fail-with-body -X PUT \
  -H 'Content-Type: application/json' \
  -d '{}' \
  http://127.0.0.1:9101/v1/tenants/demo-tenant
```

The creation response contains `tenant` and `owner`.
The owner identifies the current process incarnation.
An existing-tenant response also includes `"existing":true`.

Create a table through a transaction:

```sh
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  -d '{"client":"demo-client","txid":"create-events-1","statements":[{"sql":"CREATE TABLE events (id INTEGER PRIMARY KEY, message TEXT NOT NULL)","args":[]}]}' \
  http://127.0.0.1:9101/v1/t/demo-tenant/tx
```

Insert a row with an affected-row guard:

```sh
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  -d '{"client":"demo-client","txid":"insert-event-1","statements":[{"sql":"INSERT INTO events (id, message) VALUES (?, ?)","args":[1,"hello"],"expect_affected":1}]}' \
  http://127.0.0.1:9101/v1/t/demo-tenant/tx
```

A first successful insertion returns this JSON shape:

```json
{"results":[{"cols":null,"rows":null,"affected":1}],"dup":false}
```

Transactions require `client`, `txid`, and a nonempty `statements` array.
Use one SQL statement per array entry.
An optional `expect_scalar_int` guard checks the first result value.
Guard failures roll back the whole transaction.
Preserve the same identifiers and payload when retrying a write.
The optional `fp` field supplies a logical-payload fingerprint.
Never reuse an idempotency key for a different operation.
A deduplicated retry returns the original results with `"dup":true`.

Query the tenant with a single statement:

```sh
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT id, message FROM events ORDER BY id","args":[]}' \
  http://127.0.0.1:9101/v1/t/demo-tenant/query
```

The query returns this JSON shape:

```json
{"cols":["id","message"],"rows":[[1,"hello"]],"affected":0}
```

`GET /v1/healthz` checks lease health.
`GET /v1/info`, `GET /metrics`, and `GET /v1/t/{tid}/status` expose operational information.
`POST /v1/t/{tid}/release` checkpoints and releases a tenant.
`DELETE /v1/tenants/{tid}` deletes tenant data.

### Errors and shutdown

Errors contain an `error` string and may include a machine-readable `code`.
Inspect `code`, not only the HTTP status.

| HTTP | Transaction code | Meaning |
| --- | --- | --- |
| 409 | `expect_failed` | A transaction guard failed |
| 422 | `refused` | The daemon rejected the statement set |
| 503 | `not_applied` | The transaction did not apply |
| 504 | `outcome_unknown` | Local commit occurred; durability remains unconfirmed |
| 504 | `query_timeout` | Execution exceeded its deadline |
| 413 | `result_too_large` | The result exceeded its budget |
| 507 | `out_of_memory` | SQLite could not satisfy its memory budget |

An `outcome_unknown` response does not prove that the write failed.
Retry it with the same `client`, `txid`, payload, and fingerprint.
Recovery may replay the write and return its stored receipt.
Transport failures can also leave the outcome unknown.
Do not treat every 504 as a safe rollback.

Send SIGINT or SIGTERM for normal shutdown.
The daemon attempts to checkpoint and release tenants before exiting.
It stops waiting for tenant drain after 30 seconds.
Allow sufficient shutdown time and inspect logs for checkpoint, upload, lease, and drain failures.
Recovery still depends on durable S3 objects when shutdown cannot finish.

## Storage compatibility

The daemon checks the prefix's `FORMAT` marker before serving.
This source uses layout version `2`.
It initializes a fresh prefix automatically.
It rejects incompatible layouts and unmarked prefixes containing tenant data.
Use a fresh prefix or an explicit migration plan.
Do not use `-force-format` for ordinary startup.
That dangerous override does not migrate old data.

Compression and delta formats also affect tenant takeover compatibility.
Both features default to enabled.
When older replicas lack compression support, first deploy with `-compress=false -delta-checkpoints=false`.
When older replicas only lack delta support, first deploy with `-delta-checkpoints=false`.
Enable each feature in a second rollout after every possible owner supports it.
Writer flags change formats at subsequent checkpoints, not immediately.
Disabling a feature does not instantly rewrite existing objects.
Verify every tenant's format before rolling back to an older reader.
Keep the control bucket, data bucket, and prefix consistent across replicas.

## Development checks

CI builds, vets, and runs race tests.
The slow suite uses the same selection as CI:

```sh
go build ./...
go vet ./...
go vet -tags slow ./...
go test -race -timeout 720s ./...
tests=$(sed -n 's/^func \(Test[A-Za-z0-9_]*\)(t \*testing\.T).*/\1/p' internal/hoglite/*_slow_test.go | paste -sd'|' -)
test -n "$tests"
go test -race -tags slow -timeout 720s -run "^(${tests})\$" ./internal/hoglite
```

Race tests require a supported native platform and a C compiler.
Core tests use an in-memory object backend and need no AWS account.

## License

PostHog distributes hoglite under the MIT license in `LICENSE`.
`THIRD_PARTY_NOTICES` preserves dependency and runtime license notices.
