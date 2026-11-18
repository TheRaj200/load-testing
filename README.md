# Last Server Standing backend

Go 1.23+ and local PostgreSQL. All data is in PostgreSQL; the in-process product-detail cache holds at most 20,000 entries for at most one second. Defaults are conservative starting points for the 2 vCPU / 8 GiB box, not a measured throughput claim.

## Windows PowerShell, local

Install PostgreSQL and Go first. In `psql` as an administrator, create a dedicated local database and user (choose your own password):

```sql
CREATE ROLE competition LOGIN PASSWORD 'replace-this-password';
CREATE DATABASE competition OWNER competition;
CREATE DATABASE competition_test OWNER competition;
```

From this directory:

```powershell
Copy-Item .env.example .env
# Edit DATABASE_URL in .env, then load it into this shell:
Get-Content .env | Where-Object { $_ -match '^[A-Za-z_][A-Za-z_0-9]*=' } | ForEach-Object { $k,$v = $_ -split '=',2; [Environment]::SetEnvironmentVariable($k,$v,'Process') }
go mod download
go run ./cmd/server
# Optional production-style local build:
New-Item -ItemType Directory -Force bin | Out-Null
go build -o bin/flashsale.exe ./cmd/server
```

In a second PowerShell window, load the same environment and run:

```powershell
$env:TEST_DATABASE_URL='postgres://competition:YOUR_PASSWORD@127.0.0.1:5432/competition_test?sslmode=disable'
go test ./...
go vet ./...
go test -race ./... # requires CGO_ENABLED=1 and a working C compiler
go run ./loadtest --smoke # same-host smoke measurement
```

Integration tests create and drop only a uniquely named schema in `TEST_DATABASE_URL`. Use a disposable test database; they do not reset the application database. `go test` without `TEST_DATABASE_URL` compiles the suite but skips database tests.

## Ubuntu 24.04 build and service

Do this before code freeze, while package and Go module downloads are possible. Install `postgresql`, `postgresql-client`, and Go 1.23+; create a local role and database using the SQL above. Keep PostgreSQL listening on localhost. Build a self-contained binary:

```bash
go mod download
mkdir -p bin
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='-s -w' -o bin/flashsale ./cmd/server
sudo install -d -o ubuntu -g ubuntu /opt/competition
sudo install -d -o ubuntu -g ubuntu /opt/competition/bin /opt/competition/migrations
sudo install -o ubuntu -g ubuntu -m 0755 bin/flashsale /opt/competition/bin/flashsale
sudo install -o ubuntu -g ubuntu -m 0644 migrations/001_init.sql /opt/competition/migrations/001_init.sql
sudo install -o ubuntu -g ubuntu -m 0600 .env /opt/competition/.env
sudo install -m 0644 deploy/competition.service /etc/systemd/system/competition.service
sudo systemctl daemon-reload
sudo systemctl enable --now competition
```

Edit `/opt/competition/.env` with the local database URL before starting. The server applies idempotent schema DDL before accepting traffic; `/health` checks the database on each call. Do not use the sample password. PostgreSQL should keep its default `fsync=on` and `synchronous_commit=on`; verify with `sudo -u postgres psql -Atc 'SHOW fsync; SHOW synchronous_commit;'`. The service restarts after a crash without downloads or external calls. Restart it offline with `sudo systemctl restart competition`.

For a staged same-host benchmark, use `go run ./loadtest --smoke` first. The full profile is `go run ./loadtest`; it seeds 10,000 products, warms for 60 seconds at 200 requests/s, then runs the stated 17 levels. This generator competes with the server for the same CPUs, so treat its latency as rough. It uses fresh IDs on every run and audits accepted stock after a three-second wait. It reports wrong answers separately from transport errors.

## Exact competition pre-flight commands

These are the commands from section 11 of `docs/last-server-standing-rules.html`. Use fresh product and user IDs when repeating them.

```bash
ss -tlnp | grep 8080
curl localhost:8080/health
curl -s -w '\n' -X POST localhost:8080/products -H 'content-type: application/json' \
  -d '{"id":"plast1","name":"Last One","category":"ctest","price":100,"stock":1}'
args=()
for i in $(seq 50); do
  [ "$i" -gt 1 ] && args+=(--next)
  args+=(-s -o /dev/null -w '%{http_code}\n' -X POST -H 'content-type: application/json' \
         -d "{\"userId\":\"u$i\",\"productId\":\"plast1\"}" http://localhost:8080/orders)
done
curl -Z --parallel-immediate --parallel-max 50 "${args[@]}" | sort | uniq -c
curl -s -w '\n' -X POST localhost:8080/products -H 'content-type: application/json' \
  -d '{"id":"pclick1","name":"Click Test","category":"ctest","price":100,"stock":1000}'
args=()
for i in $(seq 20); do
  [ "$i" -gt 1 ] && args+=(--next)
  args+=(-s -o /dev/null -w '%{http_code}\n' -X POST -H 'content-type: application/json' \
         -d '{"userId":"udouble1","productId":"pclick1"}' http://localhost:8080/orders)
done
curl -Z --parallel-immediate --parallel-max 20 "${args[@]}" | sort | uniq -c
curl -s -w '\n' localhost:8080/products/pclick1
curl -s -w '\n' localhost:8080/users/udouble1/orders
sudo ss -tlnp | grep 8080
sudo kill -9 <pid>
curl -s -w '\n' localhost:8080/health
curl -s -w '\n' localhost:8080/products/pclick1
```

The two parallel checks should show `1 201` and respectively `49 409` and `19 409`. After the crash, systemd should restore `/health` and the product with stock 999. Also verify that the database is local, every route matches section 04, and no startup command needs the internet.

An order locks its product row, checks stock and prior purchase, inserts a uniquely constrained order, and decrements stock in one transaction. A 201 is sent only after commit. If the commit result is uncertain, the connection closes without an explicit error response. Product-detail cache entries expire at a fixed deadline and are invalidated after successful writes; order decisions always read PostgreSQL.
