# Device enrollment consumer

Lambda (Python 3.12) that consumes device-enrollment events from SQS and reserves a
per-tenant slot, keyed by `(tenant_id, imei)`, in Postgres. Default limit: 5 active devices
per tenant (`ENROLLMENT_LIMIT`). Returns the standard `{"batchItemFailures": [...]}` response.

## Code Layout

```
enrollment/handler.py          Lambda entry point (lambda_handler) + batch loop
enrollment/models.py           body parsing / validation
enrollment/postgres_store.py   production store (psycopg2, per-tenant advisory lock)
enrollment/sqlite_store.py     test-only store (stdlib sqlite3, BEGIN IMMEDIATE)
enrollment/schema_postgres.sql table DDL
tests/                         unittest suite
DESIGN.md                      trade-offs
```

## Build & test

No build step required and no third-party dependencies are needed to run the suite (stdlib only).

```bash
python3 -W error -m unittest discover -v     # runs SQLite-backed suite; warnings are errors
```

## Commands to run the test in docker(inside a virtual env)

```bash
python3 -m venv .venv
source .venv/bin/activate 
pip install -r requirements.txt
docker compose up -d
export TEST_DATABASE_URL=postgresql://postgres:test@localhost:55432/enrollment_test  # Execute export everytime trying to run the test in a new terminal
python3 -W error -m unittest discover -v     # PostgresTests now run instead of being skipped
```

## What the tests use

- **Harness:** hand-written fixtures in `tests/helpers.py` that build events in the exact AWS
  SQS-trigger shape (`Records[].messageId/body/attributes/...`); the real `lambda_handler` is
  invoked. No moto/localstack - the handler never calls the SQS API (Lambda deletes
  successful messages itself), so there is nothing to emulate.
- **DB:** SQLite file per test (stand-in for RDS Postgres), selected via
  `DATABASE_URL=sqlite:///path`. One scenario class (`EnrollmentScenarios`) is mixed into a
  SQLite test case (always runs) and a Postgres test case (runs only if `TEST_DATABASE_URL` is set).
- **Concurrency test:** 16 threads released by a barrier, each calling `lambda_handler` with its
  own DB connection, same tenant, distinct IMEIs, limit 5 -> exactly 5 succeed, 11 reported failed.


## Deployment notes

- Event source mapping: enable `ReportBatchItemFailures`; set a DLQ / `maxReceiveCount` on the
  queue (limit-exceeded and poison messages are retried, then dead-lettered).
- Env vars: `DATABASE_URL` (postgresql DSN), `ENROLLMENT_LIMIT` (optional, default 5).
- Apply `enrollment/schema_postgres.sql` out-of-band (migration), not from the Lambda.
- Package `psycopg2-binary` (Need a Lambda layer or bundled for Amazon Linux to run in actual AWS).
