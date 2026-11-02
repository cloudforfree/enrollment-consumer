"""SQLite backend, used only by the local test-suite.

The analogue of the per-tenant advisory lock used by the 
Postgres backend.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

from .models import EnrollmentRequest
from .store import Outcome

_SCHEMA = """
CREATE TABLE IF NOT EXISTS enrollments (
    tenant_id    TEXT NOT NULL,
    imei         TEXT NOT NULL,
    device_type  TEXT NOT NULL,
    requested_at TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'released')),
    enrolled_at  TEXT NOT NULL,
    released_at  TEXT,
    PRIMARY KEY (tenant_id, imei)
);
CREATE INDEX IF NOT EXISTS enrollments_active_idx
    ON enrollments (tenant_id) WHERE status = 'active';
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class SqliteStore:
    def __init__(self, path: str, busy_timeout: float = 30.0) -> None:
        self._conn = sqlite3.connect(path, timeout=busy_timeout, isolation_level=None)

    def init_schema(self) -> None:
        self._conn.executescript(_SCHEMA)

    def truncate_all(self) -> None:
        self._conn.execute("DELETE FROM enrollments")

    def reserve(self, request: EnrollmentRequest, limit: int) -> Outcome:
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT status FROM enrollments WHERE tenant_id = ? AND imei = ?",
                (request.tenant_id, request.imei),
            ).fetchone()
            if row is not None and row[0] == "active":
                outcome = Outcome.DUPLICATE
            else:
                (active,) = conn.execute(
                    "SELECT COUNT(*) FROM enrollments WHERE tenant_id = ? AND status = 'active'",
                    (request.tenant_id,),
                ).fetchone()
                if active >= limit:
                    outcome = Outcome.LIMIT_EXCEEDED
                elif row is not None:
                    conn.execute(
                        "UPDATE enrollments SET status = 'active', device_type = ?, requested_at = ?,"
                        " enrolled_at = ?, released_at = NULL WHERE tenant_id = ? AND imei = ?",
                        (request.device_type, request.requested_at.isoformat(), _now(),
                         request.tenant_id, request.imei),
                    )
                    outcome = Outcome.ENROLLED
                else:
                    conn.execute(
                        "INSERT INTO enrollments (tenant_id, imei, device_type, requested_at, enrolled_at)"
                        " VALUES (?, ?, ?, ?, ?)",
                        (request.tenant_id, request.imei, request.device_type,
                         request.requested_at.isoformat(), _now()),
                    )
                    outcome = Outcome.ENROLLED
            conn.execute("COMMIT")
            return outcome
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def release(self, tenant_id: str, imei: str) -> bool:
        cur = self._conn.execute(
            "UPDATE enrollments SET status = 'released', released_at = ?"
            " WHERE tenant_id = ? AND imei = ? AND status = 'active'",
            (_now(), tenant_id, imei),
        )
        return cur.rowcount > 0

    def active_count(self, tenant_id: str) -> int:
        (count,) = self._conn.execute(
            "SELECT COUNT(*) FROM enrollments WHERE tenant_id = ? AND status = 'active'",
            (tenant_id,),
        ).fetchone()
        return count

    def close(self) -> None:
        self._conn.close()
