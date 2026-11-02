# Design

## 1. Atomic per-tenant limit under concurrency
- Chosen: `pg_advisory_xact_lock(ns, hashtext(tenant_id))` at the start of each reservation txn, then check-and-insert inside it.
- Serialises writers per tenant only; other tenants never block. Lock auto-releases at commit/rollback (crash-safe, no leaked locks).
- Runs at READ COMMITTED: after the lock is acquired each statement sees a fresh snapshot, so `count(*)` sees every earlier committed reservation. No serialization-failure retry loop needed.
- Rejected: row lock on a `tenants` table (`SELECT ... FOR UPDATE`) - needs a tenant row to exist, extra provisioning step; otherwise equivalent.
- Rejected: SERIALIZABLE - correct, but aborts with 40001 under contention; needs retry logic and wastes work exactly when a tenant is hot.
- Rejected: unique constraints alone - can enforce "one row per device" but not "at most N rows per tenant".
- Rejected: counter column + `UPDATE ... WHERE used < N` - atomic and cheap, but a second source of truth that can drift from the rows; needs reconciliation.
- Rejected: check-then-insert at READ COMMITTED with no lock - the race the brief warns about.
- PK `(tenant_id, imei)` is a backstop against double-insert, not the limit mechanism.
- `lock_timeout = 5s`: a stuck holder fails the record (retried by SQS) instead of burning the Lambda timeout.
- Tests use SQLite (`BEGIN IMMEDIATE`) as a stand-in; that proves handler logic, not Postgres lock semantics (see README verification status).

## 2. "Actively enrolled"
- Row has `status IN ('active','released')`, `enrolled_at`, `released_at`. Only `active` rows count toward the limit.
- Assumption: devices can be de-enrolled (otherwise a tenant is permanently bricked at N devices).
- Implemented: `release(tenant, imei)` store operation (frees slot); no consumer for a de-enrol event yet - the brief only defines the enrol event.
- Re-enrolling a released device reuses its row (UPDATE back to `active`), still subject to the limit.
- Limit is a global env var; per-tenant overrides would be a `tenant_limits` table read inside the same txn.

## 3. Duplicate delivery vs. new attempt
- Identity is the business key `(tenant_id, imei)`, not SQS `messageId`.
- Active row exists -> duplicate -> no-op success. This also absorbs producer-side retries that arrive with a fresh `messageId`, which a messageId dedupe table would miss.
- Duplicate check runs before the limit check: a redelivery for an already-enrolled device at full capacity succeeds instead of being rejected (tested).
- Row absent or `released` -> genuinely new attempt -> consumes a slot.
- Trade-off: cannot distinguish "same event twice" from "device legitimately re-requested while still active"; both are no-ops. Acceptable: end state is identical.
- Same IMEI under two tenants = two independent enrollments (follows the stated pair key). If an IMEI must be globally unique, add a unique partial index on `imei WHERE status='active'`.

## 4. Commit succeeds, invocation dies before ack
- SQS redelivers the message; reprocessing hits the active row -> DUPLICATE -> success, no extra slot. That is why idempotency is on the DB row.
- Reservation and "I processed this" are the same atomic fact, so there is no window where the slot is taken but the dedupe marker is missing.
- Edge: device was released between the first commit and the redelivery -> redelivery re-enrols it. Accepted; stale redelivery within the visibility window is rare, and a de-enrol event ordering guarantee is out of scope.
- Set queue visibility timeout >= 6x function timeout (AWS guidance) to avoid concurrent redelivery; the lock makes even that safe.
- Connection frozen across Lambda freeze/thaw: one transparent reconnect-and-retry; safe because the txn is rolled back and the op is idempotent.

## 5. Partial batch failure semantics
- Per-record try/except; only malformed, DB-error, and limit-exceeded records go to `batchItemFailures`. No-op duplicates count as success.
- Limit-exceeded is reported as failure (per the brief) -> retried until `maxReceiveCount`, then DLQ. If slots free up (release) the retry can succeed.
- Assumes a standard queue. FIFO would require stopping at the first failure and failing the rest of the group to preserve ordering.
- Records processed sequentially within a batch; concurrency comes from overlapping invocations. Lock makes either safe.
- Malformed messages are retried until the DLQ too; no way to tell poison from transient without a redrive policy. IMEIs are masked in logs.

## 6. With more time
- Add a de-enrol event consumer, `expires_at`, and per-tenant limits table.
- Use `expires_at` greater than current time to check if the record for the `tenant_id` is active, another way of get around using `status` and updating it by another process. 
- Emit metrics (enrolled / duplicate / limit-exceeded / malformed) and alarm on DLQ depth and lock timeouts.
- Check `context.get_remaining_time_in_millis()` and fail unprocessed records early rather than being killed mid-batch.
- Mark limit-exceeded as a terminal outcome (write to a rejections table) instead of burning retries.


## 7. Atternate Solution

Instead of using Postgres there two alternate solution can be possible DynamoDB or Aurora Serverless v2

### Option 1: Dynamodb

It’s cheaper and faster, doesn't need VPC or instances, scales up automatically

- #####  No fixed cost: pricing is on-demand per request, so idle costs near zero. A transactional enrollment is about 4 write units, which is a few dollars per million enrollments.
- #####  No VPC : no VPC, connection pool, secret, endpoint or migration step. Cold starts are shorter without the VPC network attachment.
- #####  Scale: it scales to very high request rates with no capacity planning.


####  Counter contention: 
Concurrent transactions on one tenant’s counter item can conflict and retry.

####  Solution: 
Keep one item per tenant and store its active IMEIs in a string set on that item. Then do one plain conditional update per reservation:

{"SS": [imei1, imei2, imei3, imei4, imei5]}

Useful for samll limits like 5, duplicate try results in no-op.


### Option 2: Aurora Serverless v2

Postgres-compatible, so the code stays the same, and it scales capacity up and down automatically. It can pause when idle, which suits dev and test environments.