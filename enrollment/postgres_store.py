"""Postgres backend.

Limit enforcement: every reservation first takes a transaction-scoped advisory lock keyed on
the tenant, so reservations for one tenant are strictly serialised while different tenants
never block each other. 

Under READ COMMITTED each later statement takes a fresh snapshot, so
once the lock is held the count(*) sees everything previously committed by other holders.
"""
from __future__ import annotations

from importlib import resources

from .models import EnrollmentRequest
from .store import Outcome

_LOCK_NAMESPACE = 7301  # first key of the two-int advisory lock; avoids clashing with other users


class PostgresStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn = None

    # -- connection ----
    def _connection(self):
        import psycopg2  # imported lazily so the SQLite test-suite needs no Postgres driver

        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn, connect_timeout=5)
        return self._conn

    def _discard(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001 - the connection is already unusable
                pass

    def close(self) -> None:
        self._discard()

    # -- reserving the row for an operation 
    def reserve(self, request: EnrollmentRequest, limit: int) -> Outcome:
        import psycopg2

        for attempt in (1, 2):
            try:
                return self._reserve_once(request, limit)
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                self._discard()
                if attempt == 2:
                    raise
            except Exception:
                self._discard()
                raise
        raise AssertionError("unreachable")

    def _reserve_once(self, request: EnrollmentRequest, limit: int) -> Outcome:
        conn = self._connection()
        with conn:  
            with conn.cursor() as cur:
                cur.execute("SET LOCAL lock_timeout = '5s'")
                cur.execute(
                    "SELECT pg_advisory_xact_lock(%s, hashtext(%s::text))",
                    (_LOCK_NAMESPACE, request.tenant_id),
                )
                cur.execute(
                    "SELECT status FROM enrollments WHERE tenant_id = %s AND imei = %s FOR UPDATE",
                    (request.tenant_id, request.imei),
                )
                row = cur.fetchone()
                if row is not None and row[0] == "active":
                    return Outcome.DUPLICATE

                cur.execute(
                    "SELECT count(*) FROM enrollments WHERE tenant_id = %s AND status = 'active'",
                    (request.tenant_id,),
                )
                (active,) = cur.fetchone()
                if active >= limit:
                    return Outcome.LIMIT_EXCEEDED

                if row is not None:  # re-enrolling previously released device 
                    cur.execute(
                        "UPDATE enrollments SET status = 'active', device_type = %s, requested_at = %s,"
                        " enrolled_at = now(), released_at = NULL WHERE tenant_id = %s AND imei = %s",
                        (request.device_type, request.requested_at, request.tenant_id, request.imei),
                    )
                else:
                    cur.execute(
                        "INSERT INTO enrollments (tenant_id, imei, device_type, requested_at)"
                        " VALUES (%s, %s, %s, %s)",
                        (request.tenant_id, request.imei, request.device_type, request.requested_at),
                    )
                return Outcome.ENROLLED

    def release(self, tenant_id: str, imei: str) -> bool:
        conn = self._connection()
        with conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE enrollments SET status = 'released', released_at = now()"
                " WHERE tenant_id = %s AND imei = %s AND status = 'active'",
                (tenant_id, imei),
            )
            return cur.rowcount > 0

    def active_count(self, tenant_id: str) -> int:
        conn = self._connection()
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM enrollments WHERE tenant_id = %s AND status = 'active'",
                (tenant_id,),
            )
            return cur.fetchone()[0]

    # -- test helpers ---
    def init_schema(self) -> None:
        sql = resources.files("enrollment").joinpath("schema_postgres.sql").read_text()
        conn = self._connection()
        with conn, conn.cursor() as cur:
            cur.execute(sql)

    def truncate_all(self) -> None:
        conn = self._connection()
        with conn, conn.cursor() as cur:
            cur.execute("TRUNCATE enrollments")
